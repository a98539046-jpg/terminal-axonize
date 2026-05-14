import asyncio
import websockets
import json
import gzip

async def test():
    url = "wss://open-api-swap.bingx.com/swap-market"
    print(f"Connecting to {url}...")
    async with websockets.connect(url, max_size=2**24) as ws:
        print("Connected!")
        sub = json.dumps({"id":"test","reqType":"sub","dataType":"BTC-USDT@kline_1m"})
        await ws.send(sub)
        print(f"Sent: {sub}")
        for i in range(5):
            msg = await asyncio.wait_for(ws.recv(), timeout=30)
            if isinstance(msg, bytes):
                try:
                    msg = gzip.decompress(msg)
                except Exception:
                    pass
                msg = msg.decode("utf-8")
            print(f"MSG {i}: {msg[:300]}")

asyncio.run(test())
