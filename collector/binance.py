import asyncio
import time

import websockets
import httpx

symbols: list[str] = [
    "BTCUSDT",
    "ETHUSDT",
]

async def connect_ws(symbol: str, depth: int | None = None):

    parsed_depth = str(depth) if depth is not None else ""
    ws_url = f'wss://stream.binance.com:9443/ws/{symbol.lower()}@depth{parsed_depth}'
    print(f"WS Connecting to: {ws_url} ...")

    async with websockets.connect(ws_url) as ws:
        print(f"WS Connected to: {ws_url}")
        async for msg in ws:
            print(msg)

    print(f"WS Connection ended: {ws_url}")

async def collect_binance():
    # print(f"Exchange info {await fetch_exchange_info()}")

    server_time = (await fetch_server_time())["serverTime"]
    print(f"Time offset {time.time() * 1000 - server_time}")

    ws_conns = [connect_ws(sym) for sym in symbols]
    await asyncio.gather(*ws_conns)

async def fetch_exchange_info():
    url = f"https://api.binance.com/api/v3/exchangeInfo"
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.json()

async def fetch_server_time():
    url = f"https://api.binance.com/api/v3/time"
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        return response.json()