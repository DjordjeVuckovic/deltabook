import asyncpg

async def create_pool(dsn: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        dsn=dsn,
        min_size=2,
        max_size=10,
        command_timeout=60,
    )

async def ping_db(pool: asyncpg.Pool) -> bool:
    try:
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")
        return True
    except Exception as e:
        print(f"DB connection error: {e}")
        return False