import asyncio
import json
import time

import websockets
import websockets.asyncio.client
import httpx

from storage.repository import OrderBookStorage
from collector.orderbook import OrderBook, DepthUpdateEvent, SequenceGapError

SYMBOLS: list[str] = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDC"
]

BINANCE_WS_API = "wss://stream.binance.com:9443/ws"
BINANCE_WS_REFRESH_RATE_MS = 100

BINANCE_REST_API = "https://api.binance.com/api/v3"

SNAPSHOT_DEPTH = 1000
FLUSH_EVERY_N = 50
FLUSH_RATE_MS = 500
MAX_DEPTH = 20


async def collect_binance(storage: OrderBookStorage) -> None:
    server_time = (await _fetch_server_time())["serverTime"]
    print(f"Time offset {time.time() * 1000 - server_time:.1f}ms")

    await asyncio.gather(*[_run_symbol(storage, sym) for sym in SYMBOLS])


async def _run_symbol(storage: OrderBookStorage, symbol: str) -> None:
    """
    Opens one persistent WS connection for a symbol.
    On sequence gap: resets the book and re-syncs — stays connected.
    """
    ws_url = f"{BINANCE_WS_API}/{symbol.lower()}@depth@{BINANCE_WS_REFRESH_RATE_MS}ms"
    print(f"[{symbol}] connecting to {ws_url}")

    async with websockets.connect(ws_url) as ws:
        queue: asyncio.Queue[dict] = asyncio.Queue()
        reader = asyncio.create_task(_ws_reader(ws, queue, symbol))

        book = OrderBook(symbol)
        try:
            while True:
                try:
                    await _sync(queue, book, storage, symbol)
                except SequenceGapError as e:
                    print(f"[{symbol}] {e} — resyncing")
                    book.reset()
                    while not queue.empty():
                        queue.get_nowait()
        finally:
            reader.cancel()


async def _ws_reader(ws: websockets.asyncio.client.ClientConnection, queue: asyncio.Queue, symbol: str) -> None:
    """
    Reads raw messages from the WS connection and puts parsed dicts in the queue.
    Runs as a background task — the sync logic never touches ws directly.
    """
    async for raw in ws:
        msg = json.loads(raw)
        await queue.put(msg)
    print(f"[{symbol}] WS stream ended")


async def _sync(
        queue: asyncio.Queue,
        book: OrderBook,
        storage: OrderBookStorage,
        symbol: str,
) -> None:
    """
    # https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#diff-depth-stream
    Implements the Binance LOB sync protocol:

    1. Fetch REST snapshot  ->  initial LOB state + lastUpdateId anchor
    2. Discard buffered WS events that are older than the snapshot (u <= lastUpdateId)
    3. Find the overlap event: U <= lastUpdateId+1 <= u
    4. Apply that event with apply_first() (no sequence check)
    5. Apply every subsequent event with apply() (strict sequence check U = u + 1)
    6. Persist to storage following custom flush policy
    """

    print(f"[{symbol}] fetching REST snapshot (depth={SNAPSHOT_DEPTH})...")
    snapshot = await _fetch_depth_snapshot(symbol)
    last_id = snapshot["lastUpdateId"]
    print(f"[{symbol}] snapshot at lastUpdateId={last_id}")

    while True:
        msg = await queue.get()
        event = DepthUpdateEvent(msg)

        if event.final_update_id <= last_id:
            continue

        if event.first_update_id <= last_id + 1 <= event.final_update_id:
            # Found the overlap. This is our entry point.
            break

        print(f"[{symbol}] missed overlap (snapshot={last_id}, event.U={event.first_update_id}) — refetching")
        snapshot = await _fetch_depth_snapshot(symbol)
        last_id = snapshot["lastUpdateId"]

    book.initialize(snapshot)
    book.apply_first(event)
    print(f"[{symbol}] synced at updateId={event.final_update_id}")

    update_count = 0
    last_flush_time = time.monotonic() * 1000
    while True:
        msg = await asyncio.wait_for(queue.get(), timeout=30.0)
        event = DepthUpdateEvent(msg)
        book.apply(event)

        update_count += 1
        if update_count % FLUSH_EVERY_N == 0 or time.monotonic() * 1000 - last_flush_time > FLUSH_RATE_MS:
            _log_bbo(book, symbol)

            await storage.save_snapshot(
                symbol,
                book.top_bids(MAX_DEPTH),
                book.top_asks(MAX_DEPTH),
                book.last_update_id,
            )

            last_flush_time = time.monotonic() * 1000

            print(f"[{symbol}] saved snapshot at lastUpdateId={event.final_update_id}")


def _log_bbo(book: OrderBook, symbol: str) -> None:
    bid = book.best_bid()
    ask = book.best_ask()
    spread = book.spread()
    if bid and ask and spread:
        print(
            f"[{symbol}] BBO bid={bid[0]} ({bid[1]})  ask={ask[0]} ({ask[1]})  spread={float(spread):.2f} dwp={book.depth_weighted_price()["ask"]}")


async def _fetch_depth_snapshot(symbol: str) -> dict:
    url = f"{BINANCE_REST_API}/depth?symbol={symbol}&limit={SNAPSHOT_DEPTH}"
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        response.raise_for_status()
        return response.json()


async def _fetch_server_time() -> dict:
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BINANCE_REST_API}/time")
        return response.json()
