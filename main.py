import asyncio
import signal
import os

import collector
from storage.db import create_pool
from storage.timescale import TimescaleStorage

DSN = os.getenv('DSN')

def validate_options():
    if DSN is None:
        raise ValueError("DSN environment variable is not set")

async def main():
    validate_options()

    loop = asyncio.get_running_loop()

    async with await create_pool(DSN) as pool:
        storage = TimescaleStorage(pool)
        task = asyncio.create_task(collector.collect_binance(storage))

        signal_event = asyncio.Event()

        def signal_handler():
            signal_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, signal_handler)

        try:
            await signal_event.wait()
            print("\nShutdown signal received. Closing connections gracefully...")
            task.cancel()
            await task
        except asyncio.CancelledError:
            print("Collector task cancelled.")
        finally:
            loop.remove_signal_handler(signal.SIGINT)
            loop.remove_signal_handler(signal.SIGTERM)
            print("Shutdown complete.")

if __name__ == "__main__":
    asyncio.run(main())
