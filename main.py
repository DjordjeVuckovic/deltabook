import asyncio
import os

import collector
from storage.db import create_pool
from storage.timescale import TimescaleStorage

DSN = os.getenv('DSN')

def validate_options():
    print(os.getenv('DSN'))
    if DSN is None:
        raise ValueError("DSN environment variable is not set")

async def main():
    validate_options()
    async with await create_pool(DSN) as pool:
        storage = TimescaleStorage(pool)
        await collector.collect_binance(storage)

if __name__ == "__main__":
    asyncio.run(main())
