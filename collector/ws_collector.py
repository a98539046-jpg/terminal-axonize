import asyncio
import json
import gzip
import logging
import time
from datetime import datetime, timezone
from typing import Optional, List
from collections import deque
import websockets
from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
import config
import database as db

logger = logging.getLogger("axonize.ws")

class Metrics:
    def __init__(self):
        self.msgs_total=0; self.writes_total=0; self.errors_total=0
        self.reconnects=0; self.start_time=time.time()
        self._msg_times=deque(maxlen=1000)
    def record_msg(self):
        self.msgs_total+=1; self._msg_times.append(time.time())
    def record_error(self): self.errors_total+=1
    @property
    def msgs_per_sec(self):
        if len(self._msg_times)<2: return 0.0
        span=self._msg_times[-1]-self._msg_times[0]
        return len(self._msg_times)/span if span>0 else 0.0
    @property
    def uptime_sec(self): return int(time.time()-self.start_time)

metrics=Metrics()
_agg_queue: Optional[asyncio.Queue]=None

def set_agg_queue(q):
    global _agg_queue
    _agg_queue=q

def decode_message(raw):
    try:
        if isinstance(raw,bytes):
            try: data=gzip.decompress(raw)
            except: data=raw
            return json.loads(data.decode("utf-8"))
        return json.loads(raw)
    except: return None

def parse_kline(msg):
    try:
        data_type=msg.get("dataType","")
        if "@kline" not in data_type: return None
        symbol=data_type.split("@")[0].replace("-","")
        k=msg.get("data",{})
        if not k: return None
        return {
            "symbol": symbol,
            "timeframe": "1m",
            "ts": datetime.fromtimestamp(int(k["t"])/1000, tz=timezone.utc),
            "open": float(k["o"]),
            "high": float(k["h"]),
            "low": float(k["l"]),
            "close": float(k["c"]),
            "volume": float(k["v"]),
            "quote_vol": float(k.get("q",0)),
            "is_closed": bool(k.get("x",False))
        }
    except: return None

class WriteBuffer:
    FLUSH_INTERVAL=2.0
    FLUSH_SIZE=500
    def __init__(self): self._buffer={}; self._last_flush=time.time()
    def add(self,candle):
        key=(candle["symbol"],candle["ts"])
        self._buffer[key]=(
            candle["symbol"],candle["timeframe"],candle["ts"],
            candle["open"],candle["high"],candle["low"],candle["close"],
            candle["volume"],candle["quote_vol"],candle["is_closed"]
        )
    def should_flush(self):
        return len(self._buffer)>=self.FLUSH_SIZE or (time.time()-self._last_flush)>=self.FLUSH_INTERVAL
    async def flush(self):
        if not self._buffer: return
        rows=list(self._buffer.values())
        self._buffer.clear()
        self._last_flush=time.time()
        try:
            await db.upsert_candles_bulk(rows)
            metrics.writes_total+=len(rows)
        except Exception as e:
            logger.error(f"DB flush error: {e}")
            metrics.record_error()

write_buffer=WriteBuffer()

async def _handle_message(raw):
    metrics.record_msg()
    msg=decode_message(raw)
    if msg is None: return
    if "pong" in msg: return
    candle=parse_kline(msg)
    if candle is None: return
    write_buffer.add(candle)
    if candle["is_closed"] and _agg_queue is not None:
        try: _agg_queue.put_nowait(candle)
        except asyncio.QueueFull: pass

async def _ping_loop(ws, interval):
    try:
        while True:
            await asyncio.sleep(interval)
            try: await ws.send(json.dumps({"ping":int(time.time()*1000)}))
            except: break
    except asyncio.CancelledError: pass

async def _ws_worker(worker_id: int, symbols: List[str], delay: float, ping_interval: float):
    while True:
        try:
            async with websockets.connect(
                config.BINGX_WS_URL,
                ping_interval=None,
                ping_timeout=10,
                close_timeout=5,
                max_size=2**24,
                compression=None
            ) as ws:
                logger.info(f"WS worker {worker_id} connected ({len(symbols)} symbols)")
                # Подписка по одному символу
                for sym in symbols:
                    sub=json.dumps({
                        "id": f"{worker_id}_{sym}",
                        "reqType": "sub",
                        "dataType": f"{sym.replace('USDT','-USDT')}@kline_1m"
                    })
                    await ws.send(sub)
                    await asyncio.sleep(0.05)
                ping_task=asyncio.ensure_future(_ping_loop(ws,ping_interval))
                try:
                    async for raw in ws:
                        await _handle_message(raw)
                finally:
                    ping_task.cancel()
                    try: await ping_task
                    except asyncio.Canc
