import asyncpg


class TimescaleStorage:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def save_snapshot(self, symbol: str, bids: list, asks: list, last_update_id: int) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO ...", symbol, ...)