import asyncio
import sys
sys.path.insert(0, '/root/bingx-collector')
import config
import database as db
from datetime import datetime, timezone

async def test():
    print("Connecting to DB...")
    await db.init_pool()
    print("DB connected!")
    
    from decimal import Decimal
    rows = [('TESTUSDT', '1m', datetime.now(tz=timezone.utc),
             Decimal('100'), Decimal('101'), Decimal('99'), Decimal('100.5'),
             Decimal('1000'), Decimal('0'), True)]
    
    await db.upsert_candles_bulk(rows)
    print("Write OK!")
    
    pool = db.get_pool()
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM candles WHERE symbol='TESTUSDT'")
        print(f"Count in DB: {count}")
    
    await db.close_pool()

asyncio.run(test())
