class SequenceGapError(Exception):
    def __init__(self, symbol: str, expected: int, got: int):
        self.expected = expected
        self.got = got
        super().__init__(f"[{symbol}] sequence gap — expected U={expected}, got U={got}")


class DepthUpdateEvent:
    def __init__(self, data: dict):
        self.event_type = data["e"]
        self.event_time = data["E"]
        self.symbol = data["s"]
        self.first_update_id = data["U"]  # first update ID in this diff batch
        self.final_update_id = data["u"]  # last update ID in this diff batch
        self.bids = data["b"]             # list of [price, qty] bid updates
        self.asks = data["a"]             # list of [price, qty] ask updates


class OrderBook:
    """
    In-memory LOB (Limit Order Book) state for one symbol.

    Holds two dicts: bids and asks, both mapping price string -> qty string.
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: dict[str, str] = {}
        self.asks: dict[str, str] = {}
        self.last_update_id: int = 0

    def initialize(self, snapshot: dict) -> None:
        """
        Seed LOB state from a REST depth snapshot.
        Called once at startup and again after every re-sync.
        """
        self.bids = {price: qty for price, qty in snapshot["bids"]}
        self.asks = {price: qty for price, qty in snapshot["asks"]}
        self.last_update_id = snapshot["lastUpdateId"]

    def apply_first(self, event: DepthUpdateEvent) -> None:
        """
        Apply the first diff event after a snapshot — no sequence check.

        The first event is special: its U may be <= lastUpdateId (overlap is expected).
        After this, every subsequent event must satisfy U == last_update_id + 1, or we have a gap (resync then).
        """
        _apply_updates(self.bids, event.bids)
        _apply_updates(self.asks, event.asks)
        self.last_update_id = event.final_update_id

    def apply(self, event: DepthUpdateEvent) -> None:
        """
        Apply a diff event. Mutates bids and asks in place.
        Raises SequenceGapError if events arrive out of order or with a gap.
        """
        if event.final_update_id < self.last_update_id:
            return  # stale — already applied, ignore silently

        expected = self.last_update_id + 1
        if event.first_update_id > expected:
            raise SequenceGapError(self.symbol, expected, event.first_update_id)

        _apply_updates(self.bids, event.bids)
        _apply_updates(self.asks, event.asks)
        self.last_update_id = event.final_update_id

    def reset(self) -> None:
        """Clear all state. Called before a re-sync."""
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = 0

    def best_bid(self) -> tuple[str, str] | None:
        """Highest bid price and its quantity. Returns None if book is empty."""
        if not self.bids:
            return None
        price = max(self.bids, key=float)
        return price, self.bids[price]

    def best_ask(self) -> tuple[str, str] | None:
        """Lowest ask price and its quantity. Returns None if book is empty."""
        if not self.asks:
            return None
        price = min(self.asks, key=float)
        return price, self.asks[price]

    def spread(self) -> str | None:
        """Spread = best ask price - best bid price. Returns None if book is empty."""
        best_bid = self.best_bid()
        best_ask = self.best_ask()
        if best_bid is None or best_ask is None:
            return None
        return str(float(best_ask[0]) - float(best_bid[0]))

    def mid_price(self) -> str | None:
        """Mid-price = (best ask price + best bid price) / 2. Returns None if book is empty."""
        best_bid = self.best_bid()
        best_ask = self.best_ask()
        if best_bid is None or best_ask is None:
            return None
        mid = (float(best_ask[0]) + float(best_bid[0])) / 2
        return str(mid)

    def top_bids(self, n: int = 20) -> list[list[str]]:
        """Top N bid levels sorted highest-first."""
        prices = sorted(self.bids, key=float, reverse=True)
        return [[p, self.bids[p]] for p in prices[:n]]

    def top_asks(self, n: int = 20) -> list[list[str]]:
        """Top N ask levels sorted lowest-first."""
        prices = sorted(self.asks, key=float)
        return [[p, self.asks[p]] for p in prices[:n]]


def _apply_updates(book: dict[str, str], updates: list[list[str]]) -> None:
    """
    Apply a list of [price, qty] updates to one side of the book.
    qty == "0.00000000" is tombstone
    """
    for price, qty in updates:
        if qty == "0.00000000":
            book.pop(price, None)
        else:
            book[price] = qty