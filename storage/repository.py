from typing import Protocol

class OrderBookStorage(Protocol):
    async def save_snapshot(
            self,
            symbol: str,
            bids: list,
            asks: list,
            last_update_id: int
    ) -> None: ...