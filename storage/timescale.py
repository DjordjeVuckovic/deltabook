import json
import asyncpg


class TimescaleStorage:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def save_snapshot(self, symbol: str, bids: list, asks: list, last_update_id: int) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO orderbook_snapshots (ts, symbol, last_update_id, bids, asks)
                VALUES (NOW(), $1, $2, $3::jsonb, $4::jsonb)
                """,
                symbol,
                last_update_id,
                json.dumps(bids),
                json.dumps(asks),
            )