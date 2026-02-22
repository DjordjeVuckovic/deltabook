import asyncio

import collector


async def main():
    return await collector.collect_binance()


if __name__ == "__main__":
  asyncio.run(main())
