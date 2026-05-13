import asyncio
import logging
import asyncpg
from decimal import Decimal
from typing import List, Dict, Optional
from datetime import datetime, timezone
import config

logger = logging.getLogger("axonize.db")
_pool: Optional[asyncpg.Pool] = None

async def init_pool():
    global _pool
    _pool = await asyncpg.create_pool(
        dsn=config.DB_DSN,
        min_size=config.DB_POOL_MIN,
        max_size=config.DB_POOL_MAX,
        command_timeout=30,
        server_settings={"application_name":"axonize_collector","synchronous_commit":"off"},
    )
    logger.info("DB pool ready")
    return _pool

async def close_pool():
    if _pool:
        await _pool.close()

def get_pool():
    if _pool is None:
        raise RuntimeError("DB pool not initialized")
    return _pool

UPSERT_CANDLE = """
INSERT INTO candles (symbol,timeframe,ts,open,high,low,close,volume,quote_volume,is_closed)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
ON CONFLICT (symbol,timeframe,ts) DO UPDATE SET
high=GREATEST(EXCLUDED.high,candles.high),low=LEAST(EXCLUDED.low,candles.low),
close=EXCLUDED.close,volume=EXCLUDED.volume,quote_volume=EXCLUDED.quote_volume,is_closed=EXCLUDED.is_closed
"""

async def upsert_candle(symbol,timeframe,ts,o,h,l,c,vol,quote_vol=0.0,is_closed=False):
    async with get_pool().acquire() as conn:
        await conn.execute(UPSERT_CANDLE,symbol,timeframe,ts,Decimal(str(o)),Decimal(str(h)),Decimal(str(l)),Decimal(str(c)),Decimal(str(vol)),Decimal(str(quote_vol)),is_closed)

async def upsert_candles_bulk(rows):
    if not rows:
        return
    async with get_pool().acquire() as conn:
        await conn.executemany(UPSERT_CANDLE,rows)

async def upsert_open_interest(rows):
    sql="""INSERT INTO open_interest(symbol,ts,oi_value,oi_change_pct) VALUES($1,$2,$3,$4)
    ON CONFLICT(symbol,ts) DO UPDATE SET oi_value=EXCLUDED.oi_value,oi_change_pct=EXCLUDED.oi_change_pct"""
    data=[(r["symbol"],r["ts"],Decimal(str(r.get("oi_value",0))),Decimal(str(r.get("oi_change_pct",0)))) for r in rows]
    async with get_pool().acquire() as conn:
        await conn.executemany(sql,data)

async def upsert_funding_rates(rows):
    sql="""INSERT INTO funding_rates(symbol,ts,funding_rate,mark_price) VALUES($1,$2,$3,$4)
    ON CONFLICT(symbol,ts) DO UPDATE SET funding_rate=EXCLUDED.funding_rate,mark_price=EXCLUDED.mark_price"""
    data=[(r["symbol"],r["ts"],Decimal(str(r.get("funding_rate",0))),Decimal(str(r.get("mark_price",0)))) for r in rows]
    async with get_pool().acquire() as conn:
        await conn.executemany(sql,data)

async def upsert_ticker_latest(rows):
    sql="""INSERT INTO ticker_latest(symbol,ts,price,volume_24h,high_24h,low_24h) VALUES($1,$2,$3,$4,$5,$6)
    ON CONFLICT(symbol) DO UPDATE SET ts=EXCLUDED.ts,price=EXCLUDED.price,volume_24h=EXCLUDED.volume_24h,high_24h=EXCLUDED.high_24h,low_24h=EXCLUDED.low_24h"""
    data=[(r["symbol"],r["ts"],Decimal(str(r.get("price",0))),Decimal(str(r.get("volume_24h",0))),Decimal(str(r.get("high_24h",0))),Decimal(str(r.get("low_24h",0)))) for r in rows]
    async with get_pool().acquire() as conn:
        await conn.executemany(sql,data)

async def upsert_symbols(symbols_data):
    sql="""INSERT INTO symbols(symbol,base_asset,volume_rank) VALUES($1,$2,$3)
    ON CONFLICT(symbol) DO UPDATE SET volume_rank=EXCLUDED.volume_rank,is_active=TRUE"""
    data=[(s["symbol"],s.get("baseAsset",""),i) for i,s in enumerate(symbols_data)]
    async with get_pool().acquire() as conn:
        await conn.executemany(sql,data)

async def get_active_symbols():
    async with get_pool().acquire() as conn:
        rows=await conn.fetch("SELECT symbol FROM symbols WHERE is_active=TRUE ORDER BY volume_rank")
        return [r["symbol"] for r in rows]

async def get_1m_candles_for_agg(symbol,since,until):
    sql="""SELECT ts,open,high,low,close,volume,quote_volume FROM candles
    WHERE symbol=$1 AND timeframe='1m' AND ts>=$2 AND ts<$3 AND is_closed=TRUE ORDER BY ts ASC"""
    async with get_pool().acquire() as conn:
        return await conn.fetch(sql,symbol,since,until)
