import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import List, Dict, Optional
import aiohttp
import config
import database as db

logger = logging.getLogger("axonize.rest")

class BingXRestClient:
    BASE = "https://open-api.bingx.com"
    REQ_INTERVAL = 0.05

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_req = 0.0

    async def __aenter__(self):
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        return self

    async def __aexit__(self, *_):
        if self._session:
            await self._session.close()

    async def _get(self, path: str, params: dict = None) -> dict:
        p = params or {}
        wait = self._last_req + self.REQ_INTERVAL - time.time()
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_req = time.time()
        async with self._session.get(f"{self.BASE}{path}", params=p) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_symbols(self) -> List[Dict]:
        data = await self._get("/openApi/swap/v2/quote/contracts")
        contracts = data.get("data", [])
        usdt = [c for c in contracts if str(c.get("currency","")).upper() == "USDT"]
        usdt.sort(key=lambda x: float(x.get("turnover24h", 0)), reverse=True)
        logger.info(f"Found {len(usdt)} USDT contracts")
        return usdt[:config.TOP_SYMBOLS_COUNT]

    async def get_klines(self, symbol, interval, start_ms, end_ms, limit=1000):
        data = await self._get("/openApi/swap/v3/quote/klines", params={
            "symbol": symbol.replace("USDT", "-USDT"),
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": limit,
        })
        return data.get("data", [])

    async def get_open_interest(self, symbol):
        data = await self._get("/openApi/swap/v2/quote/openInterest", params={"symbol": symbol.replace("USDT", "-USDT")})
        return data.get("data")

    async def get_funding_rate(self, symbol):
        data = await self._get("/openApi/swap/v2/quote/premiumIndex", params={"symbol": symbol.replace("USDT", "-USDT")})
        return data.get("data")

async def load_symbols() -> List[str]:
    async with BingXRestClient() as client:
        contracts = await client.get_symbols()
    symbols_data = []
    for c in contracts:
        raw = c.get("contractId") or c.get("symbol", "")
        sym = raw.replace("-", "")
        if not sym.endswith("USDT"):
            sym = sym + "USDT"
        symbols_data.append({"symbol": sym, "baseAsset": sym.replace("USDT", "")})
    await db.upsert_symbols(symbols_data)
    return [s["symbol"] for s in symbols_data]

async def load_historical_candles(symbols: List[str]):
    logger.info(f"Loading {config.HISTORY_DAYS}d history for {len(symbols)} symbols...")
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - config.HISTORY_DAYS * 24 * 3600 * 1000
    semaphore = asyncio.Semaphore(5)
    async def fetch_one(symbol):
        async with semaphore:
            await _fetch_symbol_history(symbol, start_ms, end_ms)
    await asyncio.gather(*[asyncio.create_task(fetch_one(s)) for s in symbols], return_exceptions=True)
    logger.info("Historical load complete")

async def _fetch_symbol_history(symbol, start_ms, end_ms):
    async with BingXRestClient() as client:
        rows = []
        cur = start_ms
        while cur < end_ms:
            cur_end = min(cur + 1000 * 60 * 1000, end_ms)
            klines = await client.get_klines(symbol, "1m", cur, cur_end, 1000)
            if not klines:
                break
            for k in klines:
                try:
                    ts = datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc)
                    rows.append((symbol, "1m", ts, float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]), float(k[7]) if len(k) > 7 else 0.0, True))
                except:
                    continue
            if len(klines) < 1000:
                break
            cur = int(klines[-1][0]) + 60000
            await asyncio.sleep(0.1)
        if rows:
            await db.upsert_candles_bulk(rows)

async def run_rest_poller(symbols: List[str]):
    logger.info("REST poller started")
    while True:
        await asyncio.sleep(config.OI_POLL_INTERVAL_SEC)
        try:
            await _poll_oi_and_funding(symbols)
        except Exception as e:
            logger.error(f"REST poll error: {e}")

async def _poll_oi_and_funding(symbols):
    logger.info(f"Polling OI + Funding for {len(symbols)} symbols...")
    semaphore = asyncio.Semaphore(10)
    now = datetime.now(tz=timezone.utc)
    oi_rows = []
    funding_rows = []
    async def fetch_one(symbol):
        async with semaphore:
            async with BingXRestClient() as client:
                try:
                    oi = await client.get_open_interest(symbol)
                    if oi:
                        oi_rows.append({"symbol": symbol, "ts": now, "oi_value": float(oi.get("openInterest", 0)), "oi_value_coin": 0.0, "oi_change_pct": 0.0})
                except:
                    pass
                try:
                    fr = await client.get_funding_rate(symbol)
                    if fr:
                        funding_rows.append({"symbol": symbol, "ts": now, "funding_rate": float(fr.get("lastFundingRate", 0)), "next_funding_time": None, "mark_price": float(fr.get("markPrice", 0))})
                except:
                    pass
    await asyncio.gather(*[asyncio.create_task(fetch_one(s)) for s in symbols], return_exceptions=True)
    if oi_rows:
        await db.upsert_open_interest(oi_rows)
    if funding_rows:
        await db.upsert_funding_rates(funding_rows)
