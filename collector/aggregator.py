import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict
from dataclasses import dataclass
from math import floor
import config
import database as db

logger = logging.getLogger("axonize.agg")

@dataclass
class AggCandle:
    symbol: str
    timeframe: str
    ts: datetime
    open: float = 0.0
    high: float = 0.0
    low: float = float("inf")
    close: float = 0.0
    volume: float = 0.0
    quote_vol: float = 0.0
    count: int = 0

    def update(self, c):
        if self.count == 0: self.open = c["open"]
        self.high = max(self.high, c["high"])
        self.low = min(self.low, c["low"])
        self.close = c["close"]
        self.volume += c["volume"]
        self.quote_vol += c["quote_vol"]
        self.count += 1

    def to_row(self):
        return (self.symbol, self.timeframe, self.ts,
            self.open, self.high, self.low, self.close,
            self.volume, self.quote_vol, True)

def bucket_start(ts, tf):
    minutes = config.TF_MINUTES[tf]
    bucket = floor(ts.timestamp() / (minutes * 60)) * (minutes * 60)
    return datetime.fromtimestamp(bucket, tz=timezone.utc)

class CandleAggregator:
    def __init__(self, timeframes):
        self.timeframes = [tf for tf in timeframes if tf in config.TF_MINUTES]
        self._state: Dict[tuple, AggCandle] = {}

    def process(self, candle_1m):
        symbol = candle_1m["symbol"]
        ts_1m = candle_1m["ts"]
        ready = []
        for tf in self.timeframes:
            key = (symbol, tf)
            bucket_ts = bucket_start(ts_1m, tf)
            existing = self._state.get(key)
            if existing is None:
                agg = AggCandle(symbol=symbol, timeframe=tf, ts=bucket_ts)
                agg.update(candle_1m)
                self._state[key] = agg
            elif existing.ts == bucket_ts:
                existing.update(candle_1m)
            else:
                ready.append(existing.to_row())
                agg = AggCandle(symbol=symbol, timeframe=tf, ts=bucket_ts)
                agg.update(candle_1m)
                self._state[key] = agg
        return ready

async def run_aggregator(queue: asyncio.Queue):
    agg = CandleAggregator(config.AGG_TIMEFRAMES)
    batch = []
    BATCH_SIZE = 200
    FLUSH_TIMEOUT = 5.0
    logger.info(f"Aggregator started | TFs: {config.AGG_TIMEFRAMES}")
    last_flush = asyncio.get_event_loop().time()
    while True:
        try:
            try:
                candle_1m = await asyncio.wait_for(queue.get(), timeout=FLUSH_TIMEOUT)
                rows = agg.process(candle_1m)
                batch.extend(rows)
                queue.task_done()
            except asyncio.TimeoutError:
                pass
            now = asyncio.get_event_loop().time()
            if len(batch) >= BATCH_SIZE or (now - last_flush) >= FLUSH_TIMEOUT:
                if batch:
                    try:
                        await db.upsert_candles_bulk(batch)
                        logger.debug(f"Aggregator flushed {len(batch)} rows")
                    except Exception as e:
                        logger.error(f"Aggregator DB error: {e}")
                    batch = []
                last_flush = now
        except asyncio.CancelledError:
            if batch:
                await db.upsert_candles_bulk(batch)
            logger.info("Aggregator stopped")
            return
        except Exception as e:
            logger.error(f"Aggregator error: {e}")
            await asyncio.sleep(1)
