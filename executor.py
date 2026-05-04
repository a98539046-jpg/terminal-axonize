"""
executor.py — Исполнитель сделок через Terminal (http://localhost:8080).
Все операции идут через Terminal API, который проксирует на BingX.
Поддерживает несколько исполнителей: каждый — отдельный JSON-файл
в каталоге <папка_executor.py>/исполнители/.
Новые файлы подхватываются горячо (проверка каждые 30 сек).
Резервный режим: слушает сигналы напрямую если мозг лёг.
"""

import os
import sys
import hmac
import hashlib
import json
import random
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from threading import Thread, Lock

import requests
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parent
load_dotenv(str(ROOT_DIR / ".env"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

EXECUTOR_SECRET = os.getenv("EXECUTOR_SECRET", "").strip()
BRAIN_URL = os.getenv("BRAIN_URL", "http://localhost:8082").rstrip("/")
HMAC_SKEW_SECONDS = 300

EXECUTORS_DIR = ROOT_DIR / "ispolniteli"
EXECUTORS_DIR.mkdir(parents=True, exist_ok=True)
SIGNALS_DIR = ROOT_DIR / "signals"
SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
HISTORY_DIR = ROOT_DIR / "history"
HISTORY_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_TERMINAL_URL = "http://localhost:8080"

CHECK_INTERVAL = 5  # секунд — проверка сигналов
EXECUTORS_RELOAD_INTERVAL = 30  # секунд — пересканирование каталога исполнителей

logging.basicConfig(level=logging.INFO, format="%(asctime)s [EXECUTOR] %(message)s")
log = logging.getLogger(__name__)


# ─────────────────────────── TERMINAL API ───────────────────────────

def terminal_request(method: str, path: str, terminal_url: str,
                     params: dict = None, json_body: dict = None) -> dict:
    """Запрос к Terminal API."""
    url = terminal_url + path
    try:
        if method.upper() == "GET":
            r = requests.get(url, params=params, timeout=10)
        else:
            r = requests.post(url, json=json_body, timeout=15)
        return r.json()
    except Exception as e:
        log.error(f"Terminal request error {path}: {e}")
        return {"code": -1, "msg": str(e)}


# ─────────────────────────── EXECUTOR CLASS ───────────────────────────

class TradeExecutor:
    """Один исполнитель — работает через Terminal API."""

    def __init__(self, config: dict):
        self.id = config["id"]
        self.name = config.get("name", self.id)
        self.mode = config.get("mode", "real")  # "virtual" | "real"
        self.terminal_url = config.get("terminal_url", DEFAULT_TERMINAL_URL)
        # legacy single-slot fields (для обратной совместимости)
        self.max_positions = config.get("max_positions", 5)
        self.risk_per_trade = config.get("risk_per_trade", 0.01)
        # новый формат: пер-ботовые слоты {bot_source: {max_positions, margin_pct}}
        self.slots: dict[str, dict] = config.get("slots", {}) or {}
        self.max_daily_drawdown = config.get("max_daily_drawdown", 0.05)
        self.active = config.get("active", True)
        # Telegram (на исполнителя — не на брейн)
        self.telegram_token = config.get("telegram_token", "") or ""
        self.telegram_chat_id = config.get("telegram_chat_id", "") or ""
        self.show_buttons = bool(config.get("show_buttons", False))

        # Random pause between trades (per executor)
        self.pause_min_sec = int(config.get("pause_min_sec", 120))
        self.pause_max_sec = int(config.get("pause_max_sec", 360))
        self._next_trade_at = 0.0  # epoch seconds; 0 = free

        self.daily_pnl = 0.0
        self.daily_start_balance = None
        self.stopped_today = False
        self.paused = False
        self.lock = Lock()

        # Папка истории
        self.history_dir = HISTORY_DIR / self.id
        self.history_dir.mkdir(parents=True, exist_ok=True)

        log.info(f"Executor создан: {self.id} ({self.name}) mode={self.mode} -> {self.terminal_url}")

    # ── Random pause между сделками ──

    def is_trade_paused(self) -> bool:
        return time.time() < self._next_trade_at

    def trade_pause_remaining(self) -> int:
        return max(0, int(self._next_trade_at - time.time()))

    def mark_traded(self) -> int:
        lo = max(0, int(self.pause_min_sec))
        hi = max(lo, int(self.pause_max_sec))
        delay = random.randint(lo, hi) if hi > 0 else 0
        self._next_trade_at = time.time() + delay
        return delay

    # ── Слоты по источнику бота ──

    def slot_for(self, source: str) -> dict:
        """Параметры слота для конкретного бота-источника. Дефолт — legacy поля."""
        s = self.slots.get(source)
        if s:
            return {
                "max_positions": int(s.get("max_positions", self.max_positions)),
                "margin_pct": float(s.get("margin_pct", self.risk_per_trade)),
            }
        return {"max_positions": self.max_positions, "margin_pct": self.risk_per_trade}

    # ── Баланс ──

    def get_balance(self) -> dict:
        """Получить баланс через Terminal GET /api/balance."""
        data = terminal_request("GET", "/api/balance", self.terminal_url)
        if data.get("code") == 0:
            b = data.get("data", {}).get("balance", {})
            bot = data.get("bot", {})
            return {
                "executor_id": self.id,
                "balance": float(b.get("balance", 0)),
                "equity": float(b.get("equity", 0)),
                "available": float(b.get("availableMargin", 0)),
                "unrealized_pnl": float(b.get("unrealizedProfit", 0)),
                "trades": bot.get("trades", 0),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        return {"executor_id": self.id, "balance": 0, "equity": 0, "available": 0,
                "unrealized_pnl": 0, "error": data.get("msg", "unknown")}

    # ── Позиции ──

    def get_positions(self) -> list[dict]:
        """Получить открытые позиции через Terminal GET /api/positions."""
        data = terminal_request("GET", "/api/positions", self.terminal_url)
        positions = []
        for pos in data.get("positions", []):
            positions.append({
                "executor_id": self.id,
                "symbol": pos.get("symbol", ""),
                "side": pos.get("positionSide", ""),
                "entry_price": float(pos.get("avgPrice", 0)),
                "mark_price": float(pos.get("markPrice", 0)) if pos.get("markPrice", "--") != "--" else 0,
                "pnl": float(pos.get("unrealizedProfit", 0)),
                "leverage": int(pos.get("leverage", 1)),
            })
        return positions

    # ── Закрыть позицию ──

    def close_position(self, symbol: str, side: str) -> dict:
        """Закрыть позицию через Terminal POST /api/close."""
        data = terminal_request("POST", "/api/close", self.terminal_url,
                                json_body={"symbol": symbol, "positionSide": side})
        result = {
            "executor_id": self.id,
            "action": "CLOSE",
            "symbol": symbol,
            "side": side,
            "response": data,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._save_trade(result)
        notify_brain("position_closed", {
            "executor_id": self.id,
            "symbol": symbol,
            "direction": side,
            "result": data,
        })
        return result

    # ── Расчёт размера позиции ──

    def calculate_position_size(self, symbol: str, entry: float, sl: float,
                                leverage: int = 10) -> float:
        """Рассчитывает размер позиции по risk_per_trade."""
        balance = self.get_balance()
        available = balance.get("available", 0)
        if available <= 0 or entry <= 0:
            return 0

        risk_amount = available * self.risk_per_trade
        price_diff = abs(entry - sl)
        if price_diff == 0:
            return 0

        qty = (risk_amount * leverage) / entry
        max_loss_per_unit = price_diff / leverage
        qty = min(qty, risk_amount / max_loss_per_unit) if max_loss_per_unit > 0 else qty

        return round(qty, 4)

    # ── Проверка дневной просадки ──

    def check_daily_drawdown(self) -> bool:
        """True = можно торговать, False = остановить."""
        balance = self.get_balance()
        equity = balance.get("equity", 0)

        if self.daily_start_balance is None:
            self.daily_start_balance = equity
            return True

        if self.daily_start_balance <= 0:
            return True

        drawdown = (self.daily_start_balance - equity) / self.daily_start_balance
        if drawdown >= self.max_daily_drawdown:
            self.stopped_today = True
            log.warning(f"Executor {self.id}: просадка {drawdown:.1%} >= {self.max_daily_drawdown:.1%} — СТОП")
            send_telegram(
                f"🛑 <b>Executor {self.name} остановлен!</b>\n"
                f"Просадка за день: {drawdown:.1%}\n"
                f"Лимит: {self.max_daily_drawdown:.1%}"
            )
            return False
        return True

    # ── Сброс дневной статистики ──

    def reset_daily(self):
        """Сброс на начало нового дня."""
        balance = self.get_balance()
        self.daily_start_balance = balance.get("equity", 0)
        self.stopped_today = False
        self.daily_pnl = 0.0

    # ── История ──

    def _save_trade(self, trade: dict):
        """Сохраняет сделку в историю: history/trader_1/2026_04.json."""
        now = datetime.now(timezone.utc)
        filename = f"{now.year}_{now.month:02d}.json"
        filepath = self.history_dir / filename

        trades = []
        if filepath.exists():
            try:
                trades = json.loads(filepath.read_text(encoding="utf-8"))
            except Exception:
                trades = []

        trades.append(trade)
        filepath.write_text(json.dumps(trades, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


# ─────────────────────────── МЕНЕДЖЕР ИСПОЛНИТЕЛЕЙ ───────────────────────────

class ExecutorManager:
    """Управляет всеми исполнителями."""

    def __init__(self):
        self.executors: dict[str, TradeExecutor] = {}
        self.lock = Lock()
        self._last_reload = 0
        self.reload_executors()

    def reload_executors(self):
        """Пересканирует каталог исполнителей — каждый JSON-файл = один исполнитель."""
        try:
            if not EXECUTORS_DIR.exists():
                log.warning(f"Каталог исполнителей не найден: {EXECUTORS_DIR}")
                return

            configs = []
            for path in sorted(EXECUTORS_DIR.glob("*.json")):
                try:
                    cfg = json.loads(path.read_text(encoding="utf-8"))
                    cfg.setdefault("id", path.stem)
                    cfg.setdefault("name", path.stem)
                    configs.append(cfg)
                except Exception as e:
                    log.error(f"Ошибка чтения {path.name}: {e}")

            new_ids = set()
            with self.lock:
                for cfg in configs:
                    eid = cfg["id"]
                    new_ids.add(eid)
                    if eid not in self.executors:
                        self.executors[eid] = TradeExecutor(cfg)
                        log.info(f"Добавлен executor: {eid} mode={cfg.get('mode', 'real')} active={cfg.get('active', True)}")
                    else:
                        ex = self.executors[eid]
                        ex.active = cfg.get("active", True)
                        ex.mode = cfg.get("mode", ex.mode)
                        ex.max_positions = cfg.get("max_positions", ex.max_positions)
                        ex.risk_per_trade = cfg.get("risk_per_trade", ex.risk_per_trade)
                        ex.slots = cfg.get("slots", ex.slots) or {}
                        ex.max_daily_drawdown = cfg.get("max_daily_drawdown", ex.max_daily_drawdown)
                        ex.name = cfg.get("name", eid)
                        ex.terminal_url = cfg.get("terminal_url", DEFAULT_TERMINAL_URL)
                        ex.telegram_token = cfg.get("telegram_token", ex.telegram_token) or ""
                        ex.telegram_chat_id = cfg.get("telegram_chat_id", ex.telegram_chat_id) or ""
                        ex.show_buttons = bool(cfg.get("show_buttons", ex.show_buttons))
                        ex.pause_min_sec = int(cfg.get("pause_min_sec", ex.pause_min_sec))
                        ex.pause_max_sec = int(cfg.get("pause_max_sec", ex.pause_max_sec))

                # Деактивируем удалённых
                for eid in list(self.executors.keys()):
                    if eid not in new_ids:
                        self.executors[eid].active = False
                        log.info(f"Executor {eid} деактивирован (файл удалён)")

            self._last_reload = time.time()
            active = sum(1 for ex in self.executors.values() if ex.active)
            log.info(f"Executors загружены: {len(self.executors)} всего, {active} активных")
        except Exception as e:
            log.error(f"Ошибка загрузки каталога исполнителей: {e}")

    def get_next_free(self, source: str | None = None,
                      open_count_provider=None):
        """Живая очередь: первый активный исполнитель, у которого
        для бота `source` есть свободные слоты.

        open_count_provider(executor_id, source) -> int — функция, которая
        возвращает текущее число открытых позиций по этому боту в этом
        исполнителе. Для virtual передаётся VirtualEngine.positions_for(...).
        Для real — берётся из ex.get_positions() (по флагу source в meta).
        """
        with self.lock:
            for ex in self.executors.values():
                if not ex.active or getattr(ex, "paused", False):
                    continue
                if ex.is_trade_paused():
                    log.debug(f"{ex.id}: trade-pause {ex.trade_pause_remaining()}s left")
                    continue
                slot = ex.slot_for(source) if source else {
                    "max_positions": ex.max_positions, "margin_pct": ex.risk_per_trade}
                if open_count_provider is not None:
                    try:
                        cur = int(open_count_provider(ex.id, source))
                    except Exception:
                        cur = 0
                else:
                    try:
                        cur = len(ex.get_positions()) if ex.mode == "real" else 0
                    except Exception:
                        cur = 0
                if cur < slot["max_positions"]:
                    return ex, slot
        return None, None

    def maybe_reload(self):
        """Перечитывает конфиг если прошло достаточно времени."""
        if time.time() - self._last_reload > EXECUTORS_RELOAD_INTERVAL:
            self.reload_executors()

    def get_active_executors(self) -> list[TradeExecutor]:
        """Возвращает активных исполнителей."""
        with self.lock:
            return [ex for ex in self.executors.values() if ex.active]

    def get_executor(self, executor_id: str) -> TradeExecutor | None:
        return self.executors.get(executor_id)

    def get_all_balances(self) -> list[dict]:
        """Балансы всех активных исполнителей."""
        return [ex.get_balance() for ex in self.get_active_executors()]

    def get_all_positions(self) -> list[dict]:
        """Позиции всех активных исполнителей."""
        positions = []
        for ex in self.get_active_executors():
            positions.extend(ex.get_positions())
        return positions

    def execute_signal(self, signal: dict):
        """
        Исполняет торговый сигнал через Terminal /api/limit_batch.
        signal: {symbol, direction, entry, take_profit, stop_loss, ...}
        """
        symbol = signal.get("symbol", "")
        direction = signal.get("direction", "").upper()
        entry = signal.get("entry", 0)
        tp = signal.get("take_profit", 0)
        sl = signal.get("stop_loss", 0)

        if not symbol or direction not in ("LONG", "SHORT") or not entry:
            log.warning(f"Невалидный сигнал: {signal}")
            return

        for ex in self.get_active_executors():
            try:
                if not ex.check_daily_drawdown():
                    continue

                positions = ex.get_positions()
                already_has = any(
                    p["symbol"] == symbol and p["side"].upper() == direction
                    for p in positions
                )
                if already_has:
                    log.info(f"{ex.id}: уже есть {direction} по {symbol}")
                    continue

                if len(positions) >= ex.max_positions:
                    log.info(f"{ex.id}: лимит позиций ({ex.max_positions})")
                    continue

                # Получаем депозит и отправляем через limit_batch
                balance = ex.get_balance()
                deposit = balance.get("balance", 0)
                if deposit <= 0:
                    continue

                result = self.send_limit_batch(
                    {"symbol": symbol, "direction": direction,
                     "entry": entry, "tp": tp, "sl": sl},
                    deposit, ex.terminal_url,
                )

                if isinstance(result, dict) and result.get("code") == 0:
                    notify_brain("orders_placed", {
                        "executor_id": ex.id,
                        "symbol": symbol,
                        "direction": direction,
                        "entry": entry,
                        "tp": tp,
                        "sl": sl,
                        "result": result,
                    })

                send_telegram(
                    f"✅ <b>{ex.name}</b>: {direction} {symbol}\n"
                    f"Вход: {entry} | TP: {tp} | SL: {sl}"
                )

            except Exception as e:
                log.error(f"{ex.id}: ошибка исполнения {symbol}: {e}")

    def send_limit_batch(self, signal: dict, deposit: float,
                         terminal_url: str = DEFAULT_TERMINAL_URL) -> dict:
        """
        Отправляет 8 лимитных ордеров в Terminal через POST /api/limit_batch.
        """
        symbol = signal.get("symbol", "")
        direction = signal.get("direction", "").upper()
        entry = float(signal.get("entry", 0))
        tp = signal.get("tp", 0)
        sl = signal.get("sl", 0)

        log.info(f"send_limit_batch: {direction} {symbol} entry={entry} tp={tp} sl={sl} deposit={deposit}")

        if not symbol or not entry or direction not in ("LONG", "SHORT"):
            log.warning(f"send_limit_batch: невалидный сигнал {signal}")
            return {"error": "invalid signal"}

        # 2% от депо / 8 = размер каждого ордера
        total_margin = deposit * 0.02
        order_margin = total_margin / 8
        log.info(f"send_limit_batch: margin={total_margin:.2f} per_order={order_margin:.4f}")

        offsets = [0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008]
        orders = []
        side = "BUY" if direction == "LONG" else "SELL"

        for i, offset in enumerate(offsets):
            if direction == "LONG":
                price = entry * (1 - offset)
            else:
                price = entry * (1 + offset)

            quantity = (order_margin * 50) / price

            orders.append({
                "symbol": symbol,
                "side": side,
                "positionSide": direction,
                "price": str(round(price, 8)),
                "quantity": str(round(quantity, 4)),
                "takeProfit": str(tp) if tp else "",
                "stopLoss": str(sl) if sl else "",
            })
            log.info(f"  order[{i+1}]: price={price:.8f} qty={quantity:.4f} offset=-{offset*100:.1f}%")

        log.info(f"send_limit_batch: POST {terminal_url}/api/limit_batch ({len(orders)} orders)")
        result = terminal_request("POST", "/api/limit_batch", terminal_url,
                                  json_body={"orders": orders})
        log.info(f"send_limit_batch: response code={result.get('code')} msg={result.get('msg', '')} error={result.get('error', '')}")
        return result

    def monitor_orders(self, symbol: str, order_ids: list[str],
                       cancel_after: int = 5,
                       terminal_url: str = DEFAULT_TERMINAL_URL) -> dict:
        """
        Проверяет статус ордеров каждые 10 сек через Terminal.
        Как только cancel_after исполнились — отменяет оставшиеся.
        """
        filled = set()
        cancelled = set()
        pending = set(order_ids)

        while pending:
            for oid in list(pending):
                data = terminal_request("GET", f"/api/order_status/{oid}",
                                        terminal_url, params={"symbol": symbol})
                status = data.get("data", {}).get("status", "") if data.get("data") else ""
                if status in ("FILLED", "filled"):
                    filled.add(oid)
                    pending.discard(oid)
                    log.info(f"Order {oid} FILLED")
                elif status in ("CANCELLED", "CANCELED", "cancelled", "canceled"):
                    cancelled.add(oid)
                    pending.discard(oid)

            if len(filled) >= cancel_after and pending:
                log.info(f"{len(filled)} filled >= {cancel_after}, cancelling {len(pending)} remaining")
                remaining_ids = list(pending)
                terminal_request("POST", "/api/cancel_batch", terminal_url,
                                 json_body={"symbol": symbol, "orderIds": remaining_ids})
                for oid in remaining_ids:
                    cancelled.add(oid)
                    pending.discard(oid)
                break

            if pending:
                time.sleep(10)

        return {"filled": len(filled), "cancelled": len(cancelled), "total": len(order_ids)}

    def check_daily_reset(self):
        """Сброс дневной статистики в 00:00 UTC."""
        now = datetime.now(timezone.utc)
        if now.hour == 0 and now.minute < 6:
            for ex in self.get_active_executors():
                ex.reset_daily()


# ─────────────────────────── РЕЗЕРВНЫЙ РЕЖИМ ───────────────────────────

def watch_signals_direct(manager: ExecutorManager):
    """
    Резервный режим: читает сигналы напрямую из JSON файлов,
    если мозг не работает.
    """
    signal_files = [
        SIGNALS_DIR / "tech_signal.json",
        SIGNALS_DIR / "smc_signal.json",
        SIGNALS_DIR / "pump_signal.json",
        SIGNALS_DIR / "signal_bot_signal.json",
    ]
    last_seen: dict[str, str] = {}

    while True:
        try:
            for sf in signal_files:
                if not sf.exists():
                    continue
                try:
                    raw = json.loads(sf.read_text(encoding="utf-8"))
                    if isinstance(raw, list) and raw:
                        latest = raw[-1]
                    elif isinstance(raw, dict):
                        latest = raw
                    else:
                        continue

                    sig_id = f"{latest.get('symbol', '')}_{latest.get('timestamp', '')}"
                    if sig_id == last_seen.get(str(sf)):
                        continue
                    last_seen[str(sf)] = sig_id

                    # Проверяем свежесть (< 10 минут)
                    ts = latest.get("timestamp", "")
                    if ts:
                        sig_time = datetime.fromisoformat(ts)
                        age = (datetime.now(timezone.utc) - sig_time).total_seconds()
                        if age > 600:
                            continue

                    score = latest.get("score", latest.get("probability", 0))
                    if score >= 7 or score >= 75:
                        direction = latest.get("direction", "")
                        if direction in ("LONG", "SHORT"):
                            log.info(f"[РЕЗЕРВ] Сигнал из {sf.name}: {latest.get('symbol')} {direction}")
                            manager.execute_signal(latest)

                except Exception as e:
                    log.debug(f"Ошибка чтения {sf}: {e}")

        except Exception as e:
            log.error(f"Ошибка резервного режима: {e}")

        time.sleep(CHECK_INTERVAL)


# ─────────────────────────── TELEGRAM ───────────────────────────

def send_telegram(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        log.error(f"Telegram: {e}")


# ─────────────────────────── HMAC ───────────────────────────

def _sign(timestamp: str, body: bytes) -> str:
    msg = timestamp.encode("utf-8") + b"." + (body or b"")
    return hmac.new(EXECUTOR_SECRET.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def verify_signature(timestamp: str, signature: str, body: bytes) -> tuple[bool, str]:
    if not timestamp or not signature:
        return False, "missing X-Signature/X-Timestamp"
    try:
        ts_int = int(timestamp)
    except ValueError:
        return False, "invalid X-Timestamp"
    if abs(int(time.time()) - ts_int) > HMAC_SKEW_SECONDS:
        return False, "stale timestamp"
    expected = _sign(timestamp, body)
    if not hmac.compare_digest(expected, signature):
        return False, "invalid signature"
    return True, ""


# ─────────────────────────── BRAIN CALLBACK ───────────────────────────

def notify_brain(event: str, payload: dict) -> None:
    """Signed POST to brain /executor_event. Never raises — failures are logged."""
    body = dict(payload or {})
    body["event"] = event
    body["timestamp"] = datetime.now(timezone.utc).isoformat()
    raw = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ts = str(int(time.time()))
    sig = _sign(ts, raw)
    try:
        requests.post(
            f"{BRAIN_URL}/executor_event",
            data=raw,
            headers={
                "Content-Type": "application/json",
                "X-Timestamp": ts,
                "X-Signature": sig,
            },
            timeout=3,
        )
    except Exception as e:
        log.warning(f"notify_brain {event}: {e}")


# ─────────────────────────── HTTP СЕРВЕР (порт 8081) ───────────────────────────

from http.server import HTTPServer, BaseHTTPRequestHandler

_manager_ref = None  # will be set in main()


class ExecutorHTTPHandler(BaseHTTPRequestHandler):
    """HTTP API для приёма сигналов от brain.py."""

    def log_message(self, format, *args):
        log.debug(f"HTTP: {format % args}")

    def _send_json(self, code: int, data: dict):
        body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        # /executor/{id}/positions
        import re as _re
        m = _re.match(r"/executor/([^/]+)/positions", self.path)
        if m:
            ex_id = m.group(1)
            if not _manager_ref:
                self._send_json(500, {"error": "not initialized"})
                return
            ex = _manager_ref.get_executor(ex_id)
            if not ex:
                self._send_json(404, {"error": f"executor {ex_id} not found"})
                return
            positions = ex.get_positions()
            balance = ex.get_balance()
            self._send_json(200, {"code": 0, "executor": ex_id, "name": ex.name,
                                  "balance": balance, "positions": positions})
            return

        if self.path == "/status":
            exs = _manager_ref.get_active_executors() if _manager_ref else []
            self._send_json(200, {"code": 0, "executors": len(exs),
                                  "ids": [{"id": e.id, "name": e.name} for e in exs]})
            return

        self._send_json(404, {"error": "not found"})

    def do_POST(self):
        if self.path == "/execute":
            try:
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length)

                ok, reason = verify_signature(
                    self.headers.get("X-Timestamp", ""),
                    self.headers.get("X-Signature", ""),
                    raw,
                )
                if not ok:
                    log.warning(f"HTTP /execute auth failed: {reason}")
                    self._send_json(401, {"error": reason})
                    return

                signal = json.loads(raw) if raw else {}
                log.info(f"HTTP /execute: {signal.get('direction')} {signal.get('symbol')}")

                if not _manager_ref:
                    self._send_json(500, {"error": "manager not initialized"})
                    return

                # Получаем депозит и отправляем через limit_batch
                results = []
                for ex in _manager_ref.get_active_executors():
                    balance = ex.get_balance()
                    deposit = balance.get("balance", 0)
                    if deposit <= 0:
                        results.append({"executor": ex.id, "error": "no balance"})
                        continue
                    result = _manager_ref.send_limit_batch(signal, deposit, ex.terminal_url)
                    results.append({"executor": ex.id, "result": result})
                    if isinstance(result, dict) and result.get("code") == 0:
                        notify_brain("orders_placed", {
                            "executor_id": ex.id,
                            "symbol": signal.get("symbol", ""),
                            "direction": (signal.get("direction") or "").upper(),
                            "entry": signal.get("entry"),
                            "tp": signal.get("take_profit") or signal.get("tp"),
                            "sl": signal.get("stop_loss") or signal.get("sl"),
                            "result": result,
                        })

                self._send_json(200, {"code": 0, "executors": results})
            except Exception as e:
                log.error(f"HTTP /execute error: {e}")
                self._send_json(500, {"error": str(e)})
        else:
            self._send_json(404, {"error": "not found"})


def run_http_server(port: int = 8081):
    server = HTTPServer(("0.0.0.0", port), ExecutorHTTPHandler)
    log.info(f"Executor HTTP server listening on :{port}")
    server.serve_forever()


# ─────────────────────────── MAIN ───────────────────────────

def main():
    global _manager_ref
    if not EXECUTOR_SECRET:
        log.error("EXECUTOR_SECRET is empty — refusing to start (set it in bots/.env)")
        sys.exit(1)
    log.info("Executor запущен")

    manager = ExecutorManager()
    _manager_ref = manager

    send_telegram(
        f"⚡ <b>Executor запущен</b>\n"
        f"Исполнителей: {len(manager.get_active_executors())}\n"
        f"HTTP API: порт 8081\n"
        f"Резервный режим: активен"
    )

    # HTTP сервер в отдельном потоке
    http_thread = Thread(target=run_http_server, args=(8081,), daemon=True)
    http_thread.start()

    # Резервный режим в отдельном потоке
    backup_thread = Thread(target=watch_signals_direct, args=(manager,), daemon=True)
    backup_thread.start()

    while True:
        try:
            manager.maybe_reload()
            manager.check_daily_reset()

            for ex in manager.get_active_executors():
                ex.check_daily_drawdown()

        except Exception as e:
            log.error(f"Ошибка главного цикла: {e}")

        time.sleep(60)


if __name__ == "__main__":
    main()
