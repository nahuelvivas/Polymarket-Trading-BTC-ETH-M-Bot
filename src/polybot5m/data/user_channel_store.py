"""In-memory store for CLOB user-channel trade events (per market)."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any


def _outcome_side(asset_id: str, yes_token_id: str, no_token_id: str) -> str:
    aid = str(asset_id or "").strip()
    if aid == str(yes_token_id).strip():
        return "YES"
    if aid == str(no_token_id).strip():
        return "NO"
    return ""


@dataclass
class UserChannelStore:
    """Latest user-channel trades for one condition (YES/NO market)."""

    condition_id: str
    yes_token_id: str
    no_token_id: str
    max_recent: int = 50
    trades: deque[dict[str, Any]] = field(default_factory=deque)
    trade_events: int = 0
    _balance_refresh_requested: bool = field(default=False, repr=False)

    def apply_trade(self, data: dict[str, Any]) -> None:
        self.trades.append(dict(data))
        while len(self.trades) > self.max_recent:
            self.trades.popleft()
        self.trade_events += 1
        status = str(data.get("status") or "").upper()
        if status in ("MATCHED", "MINED", "CONFIRMED"):
            self._balance_refresh_requested = True

    def trade_row_for_log(self, data: dict[str, Any]) -> dict[str, Any]:
        asset_id = str(data.get("asset_id") or "")
        outcome = str(data.get("outcome") or "") or _outcome_side(
            asset_id, self.yes_token_id, self.no_token_id
        )
        return {
            "event": "USER_TRADE",
            "event_type": "trade",
            "trade_id": data.get("id"),
            "condition_id": data.get("market") or self.condition_id,
            "asset_id": asset_id,
            "outcome": outcome,
            "side": data.get("side"),
            "price": data.get("price"),
            "size": data.get("size"),
            "status": data.get("status"),
            "trader_side": data.get("trader_side"),
            "timestamp": data.get("timestamp"),
        }

    def consume_balance_refresh(self) -> bool:
        if not self._balance_refresh_requested:
            return False
        self._balance_refresh_requested = False
        return True

    def latest_trade(self) -> dict[str, Any] | None:
        if not self.trades:
            return None
        return dict(self.trades[-1])
