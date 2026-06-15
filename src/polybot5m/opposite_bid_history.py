"""Track min/max opposite-outcome best_bid after the first sell fill."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from polybot5m.trading_process_log import TradingCycleJournal


def opposite_outcome(outcome: str) -> str:
    o = str(outcome or "").upper().strip()
    return "NO" if o == "YES" else "YES"


@dataclass
class OppositeBidHistoryState:
    active: bool = False
    sold_first_outcome: str = ""
    opposite_outcome: str = ""
    opposite_min_best_bid: float | None = None
    opposite_max_best_bid: float | None = None
    min_tracking: bool = False
    logged: bool = False


class OppositeBidHistoryTracker:
    """
    After the first-leg sell fill, track opposite token best_bid:
    - min: update on new lows until opposite inventory is gone
    - max: update on new highs until finalize() at market end
    """

    def __init__(self, *, enabled: bool, balance_epsilon: float) -> None:
        self.enabled = bool(enabled)
        self._epsilon = max(0.0, float(balance_epsilon))
        self._state = OppositeBidHistoryState()

    @property
    def active(self) -> bool:
        return self._state.active

    def activate(self, sold_outcome: str, *, opp_best_bid: float) -> None:
        if not self.enabled or self._state.active:
            return
        sold = str(sold_outcome or "").upper().strip()
        if sold not in ("YES", "NO"):
            return
        opp = opposite_outcome(sold)
        self._state.active = True
        self._state.sold_first_outcome = sold
        self._state.opposite_outcome = opp
        self._state.min_tracking = True
        bid = float(opp_best_bid)
        if bid > 0:
            self._state.opposite_min_best_bid = bid
            self._state.opposite_max_best_bid = bid

    def update(
        self,
        *,
        best_bid_yes: float,
        best_bid_no: float,
        rem_yes: float,
        rem_no: float,
    ) -> None:
        if not self.enabled or not self._state.active:
            return
        opp = self._state.opposite_outcome
        bid = float(best_bid_yes if opp == "YES" else best_bid_no)
        if bid <= 0:
            return
        opp_bal = float(rem_yes if opp == "YES" else rem_no)
        if self._state.min_tracking and opp_bal <= self._epsilon:
            self._state.min_tracking = False

        cur_max = self._state.opposite_max_best_bid
        if cur_max is None or bid > cur_max:
            self._state.opposite_max_best_bid = bid
        if self._state.min_tracking:
            cur_min = self._state.opposite_min_best_bid
            if cur_min is None or bid < cur_min:
                self._state.opposite_min_best_bid = bid

    def build_log_row(self, *, reason: str) -> dict[str, Any]:
        st = self._state
        return {
            "event": "OPPOSITE_BID_HISTORY",
            "reason": reason,
            "sold_first_outcome": st.sold_first_outcome,
            "opposite_outcome": st.opposite_outcome,
            "opposite_min_best_bid": st.opposite_min_best_bid,
            "opposite_max_best_bid": st.opposite_max_best_bid,
            "min_tracking_active": st.min_tracking,
        }

    def finalize(
        self,
        journal: TradingCycleJournal | None,
        *,
        reason: str,
        strategy_extras: dict[str, Any] | None = None,
    ) -> bool:
        """Append one NDJSON row; return True if logged."""
        if not self.enabled or not self._state.active or self._state.logged:
            return False
        if journal is None:
            return False
        self._state.logged = True
        row = {**(strategy_extras or {}), **self.build_log_row(reason=reason)}
        journal.append_strategy(row)
        return True
