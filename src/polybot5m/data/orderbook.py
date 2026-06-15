"""In-memory orderbook — apply WSS book snapshots, expose best bid/ask."""

from __future__ import annotations

from collections import defaultdict
from types import SimpleNamespace
from typing import Any

class BestBidAsk:
    def __init__(self, bid_price: float, bid_size: float, ask_price: float, ask_size: float):
        self.bid_price = bid_price
        self.bid_size = bid_size
        self.ask_price = ask_price
        self.ask_size = ask_size
        self.spread = round(ask_price - bid_price, 6)
        self.midpoint = round((bid_price + ask_price) / 2, 6)


class InMemoryOrderbookStore:
    """Minimal orderbook: snapshot-only updates, best bid/ask lookup."""

    def __init__(self) -> None:
        self._bids: dict[str, list[tuple[float, float]]] = defaultdict(list)
        self._asks: dict[str, list[tuple[float, float]]] = defaultdict(list)

    def apply_book_msg(self, data: dict[str, Any]) -> None:
        asset_id = data.get("asset_id", "")
        if not asset_id:
            return
        bids = [(float(b["price"]), float(b["size"])) for b in data.get("bids", [])]
        asks = [(float(a["price"]), float(a["size"])) for a in data.get("asks", [])]
        self._bids[asset_id] = sorted(bids, key=lambda x: -x[0])
        self._asks[asset_id] = sorted(asks, key=lambda x: x[0])

    def book_as_executor_view(self, asset_id: str) -> Any:
        """Snapshot as object with `.bids`/`.asks` list[dict] for executor / influence helpers."""
        bids_raw = self._bids.get(asset_id, [])
        asks_raw = self._asks.get(asset_id, [])
        bids = [{"price": float(p), "size": float(s)} for p, s in bids_raw]
        asks = [{"price": float(p), "size": float(s)} for p, s in asks_raw]
        return SimpleNamespace(bids=bids, asks=asks)

    def get_best_bid(self, asset_id: str) -> tuple[float, float] | None:
        bids = self._bids.get(asset_id, [])
        return bids[0] if bids else None

    def get_best_ask(self, asset_id: str) -> tuple[float, float] | None:
        asks = self._asks.get(asset_id, [])
        return asks[0] if asks else None

    def get_best_bid_ask(self, asset_id: str) -> BestBidAsk | None:
        bid = self.get_best_bid(asset_id)
        ask = self.get_best_ask(asset_id)
        if bid is None or ask is None:
            return None
        return BestBidAsk(
            bid_price=bid[0],
            bid_size=bid[1],
            ask_price=ask[0],
            ask_size=ask[1],
        )

    def get_all_asset_ids(self) -> list[str]:
        return list(set(self._bids.keys()) | set(self._asks.keys()))
