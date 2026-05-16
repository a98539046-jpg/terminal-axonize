import asyncio, websockets, json

async def test():
    url = "wss://open-api-swap.bingx.com/swap-market"
    print("connecting...")
    async with websockets.connect(url) as ws:
        sub = {"id":"1","reqType":"sub","dataType":"BTC-USDT@kline_1m"}
        await ws.send(json.dumps(sub))
        print("subscribed, waiting...")
        for i in range(5):
            msg = await asyncio.wait_for(ws.recv(), timeout=15)
            print("msg:", msg[:300])

asyncio.run(test())
