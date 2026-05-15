import sys
sys.path.insert(0, '/root/bingx-collector')
import json
import gzip
import asyncio
import websockets

async def test():
    url = "wss://open-api-swap.bingx.com/swap-market"
    async with websockets.connect(url, max_size=2**24) as ws:
        sub = json.dumps({"id":"test","reqType":"sub","dataType":"BTC-USDT@kline_1m"})
        await ws.send(sub)
        for i in range(3):
            raw = await asyncio.wait_for(ws.recv(), timeout=30)
            if isinstance(raw, bytes):
                try: raw = gzip.decompress(raw)
                except: pass
                raw = raw.decode("utf-8")
            msg = json.loads(raw)
            print(f"RAW: {json.dumps(msg)[:300]}")
            from ws_collector import parse_kline
            result = parse_kline(msg)
            print(f"PARSED: {result}")

asyncio.run(test())
