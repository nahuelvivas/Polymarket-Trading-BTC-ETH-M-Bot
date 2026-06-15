"""Virtual CLOB fills for paper trading — delayed settlement + partial fills like live."""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from polybot5m.execution.paper_report import PaperFill


def paper_session_from_bot_config(bot: Any) -> PaperSessionLedger:
    """Build a shared paper ledger from ``settings.bot`` (or similar)."""
    fak_min = getattr(bot, "paper_fak_partial_fill_fraction_min", None)
    fak_max = getattr(bot, "paper_fak_partial_fill_fraction_max", None)
    return PaperSessionLedger(
        float(getattr(bot, "paper_starting_usdc", 3000.0) or 3000.0),
        float(getattr(bot, "paper_fee_bps", 0.0) or 0.0),
        settlement_delay_min_s=float(getattr(bot, "paper_settlement_delay_min_s", 0.5) or 0.5),
        settlement_delay_max_s=float(getattr(bot, "paper_settlement_delay_max_s", 2.0) or 2.0),
        partial_fill_fraction_min=float(getattr(bot, "paper_partial_fill_fraction_min", 0.5) or 0.5),
        partial_fill_fraction_max=float(getattr(bot, "paper_partial_fill_fraction_max", 1.0) or 1.0),
        fak_partial_fill_fraction_min=float(fak_min) if fak_min is not None else None,
        fak_partial_fill_fraction_max=float(fak_max) if fak_max is not None else None,
        sell_limit_settle_ticks=int(getattr(bot, "paper_sell_limit_settle_ticks", 2) or 2),
    )


def _fee_on_notional(notional: float, fee_bps: float) -> float:
    if notional <= 0 or fee_bps <= 0:
        return 0.0
    return round(float(notional) * float(fee_bps) / 10000.0, 8)


@dataclass
class _PendingSettlement:
    token_id: str
    side: str
    limit_price: float
    shares: float
    settle_at_mono: float
    yes_token_id: str
    no_token_id: str
    book_bid: float | None
    order_type: str
    # When set, fill after N monitor ticks (``advance_monitor_tick``), not ``settle_at_mono``.
    ticks_until_settle: int | None = None


class PaperSessionLedger:
    """
    Shared USDC wallet and per-outcome positions.

    Accepted orders enqueue settlement (partial size, random delay) before
  ``balances()`` reflects the fill — similar to live CLOB/chain lag.
    """

    def __init__(
        self,
        starting_usdc: float,
        fee_bps: float = 0.0,
        *,
        settlement_delay_min_s: float = 0.5,
        settlement_delay_max_s: float = 2.0,
        partial_fill_fraction_min: float = 0.5,
        partial_fill_fraction_max: float = 1.0,
        fak_partial_fill_fraction_min: float | None = None,
        fak_partial_fill_fraction_max: float | None = None,
        sell_limit_settle_ticks: int = 2,
        random_seed: int | None = None,
    ) -> None:
        self.starting_usdc = float(starting_usdc)
        self.fee_bps = float(fee_bps)
        self.sell_limit_settle_ticks = max(1, int(sell_limit_settle_ticks or 2))
        self.usdc_cash = float(starting_usdc)
        self.settlement_delay_min_s = max(0.0, float(settlement_delay_min_s))
        self.settlement_delay_max_s = max(
            self.settlement_delay_min_s,
            float(settlement_delay_max_s),
        )
        self.partial_fill_fraction_min = max(0.01, min(1.0, float(partial_fill_fraction_min)))
        self.partial_fill_fraction_max = max(
            self.partial_fill_fraction_min,
            min(1.0, float(partial_fill_fraction_max)),
        )
        fak_lo = (
            float(fak_partial_fill_fraction_min)
            if fak_partial_fill_fraction_min is not None
            else self.partial_fill_fraction_min
        )
        fak_hi = (
            float(fak_partial_fill_fraction_max)
            if fak_partial_fill_fraction_max is not None
            else self.partial_fill_fraction_max
        )
        self.fak_partial_fill_fraction_min = max(0.01, min(1.0, fak_lo))
        self.fak_partial_fill_fraction_max = max(
            self.fak_partial_fill_fraction_min,
            min(1.0, fak_hi),
        )
        self._positions: dict[str, tuple[float, float]] = {}
        self.fills: list[PaperFill] = []
        self._pending: list[_PendingSettlement] = []
        self._lock = threading.Lock()
        self._rng = random.Random(random_seed)
        self._last_monitor_tick_mono: float = 0.0

    def _pos(self, token_id: str) -> tuple[float, float]:
        return self._positions.get(str(token_id), (0.0, 0.0))

    def _set_pos(self, token_id: str, shares: float, cost_basis_usdc: float) -> None:
        tid = str(token_id)
        eps = 1e-12
        if shares <= eps:
            self._positions.pop(tid, None)
        else:
            self._positions[tid] = (float(shares), float(cost_basis_usdc))

    def _fill_fraction(self, order_type: str) -> float:
        ot = str(order_type or "GTC").upper()
        if ot == "FAK":
            lo, hi = self.fak_partial_fill_fraction_min, self.fak_partial_fill_fraction_max
        else:
            lo, hi = self.partial_fill_fraction_min, self.partial_fill_fraction_max
        return self._rng.uniform(lo, hi)

    def _settlement_delay_s(self) -> float:
        if self.settlement_delay_max_s <= self.settlement_delay_min_s:
            return self.settlement_delay_min_s
        return self._rng.uniform(self.settlement_delay_min_s, self.settlement_delay_max_s)

    def settle_pending(self, now_mono: float | None = None) -> list[PaperFill]:
        """Apply pending fills whose settlement time has passed (not tick-based SELL limits)."""
        now = float(now_mono if now_mono is not None else time.monotonic())
        applied: list[PaperFill] = []
        eps = 1e-9

        with self._lock:
            still_pending: list[_PendingSettlement] = []
            for p in self._pending:
                if p.ticks_until_settle is not None:
                    still_pending.append(p)
                    continue
                if p.settle_at_mono > now:
                    still_pending.append(p)
                    continue
                fill = self._apply_settlement_locked(p, eps=eps)
                if fill is not None:
                    applied.append(fill)
            self._pending = still_pending
        return applied

    def advance_monitor_tick(
        self,
        *,
        poll_interval_s: float | None = None,
    ) -> list[PaperFill]:
        """
        Decrement tick-based pending SELL limits once per monitor poll wave.

        Parallel markets share one ledger; debounce so four symbols do not consume
        four ticks in the same instant.
        """
        gap = max(0.02, float(poll_interval_s or 0.1) * 0.85)
        now = time.monotonic()
        applied: list[PaperFill] = []
        eps = 1e-9

        with self._lock:
            if now - self._last_monitor_tick_mono < gap:
                return applied
            self._last_monitor_tick_mono = now

            still_pending: list[_PendingSettlement] = []
            for p in self._pending:
                ticks = p.ticks_until_settle
                if ticks is None:
                    still_pending.append(p)
                    continue
                ticks -= 1
                if ticks > 0:
                    p.ticks_until_settle = ticks
                    still_pending.append(p)
                    continue
                fill = self._apply_settlement_locked(p, eps=eps)
                if fill is not None:
                    applied.append(fill)
            self._pending = still_pending
        return applied

    def _apply_settlement_locked(
        self,
        p: _PendingSettlement,
        *,
        eps: float,
    ) -> PaperFill | None:
        u = str(p.side).upper()
        tid = str(p.token_id)
        px = float(p.limit_price)
        shares = float(p.shares)
        if shares <= eps or px <= 0:
            return None
        if tid == str(p.yes_token_id):
            side_label = "YES"
        elif tid == str(p.no_token_id):
            side_label = "NO"
        else:
            return None
        ref_bid = float(p.book_bid) if p.book_bid is not None else px
        now = datetime.now(UTC)
        notional = px * shares
        fee = _fee_on_notional(notional, self.fee_bps)

        if u == "BUY":
            cost = notional + fee
            if self.usdc_cash + eps < cost:
                return None
            self.usdc_cash -= cost
            sh, cb = self._pos(tid)
            self._set_pos(tid, sh + shares, cb + cost)
            fill = PaperFill(
                token_id=tid,
                side_label=side_label,
                price=px,
                size=shares,
                usdc_proceeds=-round(cost, 8),
                filled_at=now,
                best_bid_at_fill=ref_bid,
                reason=f"paper_buy_{p.order_type.lower()}",
                is_buy=True,
                fee_usdc=round(fee, 8),
                limit_price_at_decision=px,
                best_bid_at_decision=ref_bid,
            )
            self.fills.append(fill)
            return fill

        bal, cb = self._pos(tid)
        if bal + eps < shares:
            shares = max(0.0, bal)
        if shares <= eps:
            return None
        avg_cost = cb / bal if bal > eps else 0.0
        cost_sold = avg_cost * shares
        gross = px * shares
        fee_s = _fee_on_notional(gross, self.fee_bps)
        net = gross - fee_s
        self.usdc_cash += net
        cb_new = cb - cost_sold
        new_rem = bal - shares
        if new_rem <= eps:
            self._set_pos(tid, 0.0, 0.0)
        else:
            self._set_pos(tid, new_rem, max(0.0, cb_new))
        fill = PaperFill(
            token_id=tid,
            side_label=side_label,
            price=px,
            size=shares,
            usdc_proceeds=round(net, 8),
            filled_at=now,
            best_bid_at_fill=ref_bid,
            reason=f"paper_sell_{p.order_type.lower()}",
            is_buy=False,
            fee_usdc=round(fee_s, 8),
            limit_price_at_decision=px,
            best_bid_at_decision=ref_bid,
        )
        self.fills.append(fill)
        return fill

    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending)

    def place_limit_order(
        self,
        token_id: str,
        side: str,
        price: float,
        shares: float,
        *,
        book_bid: float | None = None,
        book_ask: float | None = None,
        yes_token_id: str,
        no_token_id: str,
        order_type: str = "GTC",
    ) -> tuple[bool, dict[str, float] | None]:
        """Queue partial fill with settlement delay; balances update when settled."""
        if not (price > 0 and shares > 0):
            return False, None
        u = str(side).upper()
        tid = str(token_id)
        if tid not in (str(yes_token_id), str(no_token_id)):
            return False, None

        ot = str(order_type or "GTC").upper()
        ticks_until_settle: int | None = None
        delay_s = 0.0
        if u == "SELL" and ot in ("GTC", "GTD"):
            frac = 1.0
            ticks_until_settle = self.sell_limit_settle_ticks
            settle_at = float("inf")
        else:
            frac = self._fill_fraction(order_type)
            delay_s = self._settlement_delay_s()
            settle_at = time.monotonic() + delay_s
        fill_shares = max(1e-6, float(shares) * frac)
        eps = 1e-9

        with self._lock:
            if u == "BUY":
                est_cost = float(price) * fill_shares
                est_cost += _fee_on_notional(est_cost, self.fee_bps)
                if self.usdc_cash + eps < est_cost:
                    return False, None
            else:
                bal, _ = self._pos(tid)
                if bal + eps < fill_shares:
                    fill_shares = max(0.0, bal)
                if fill_shares <= eps:
                    return False, None

            self._pending.append(
                _PendingSettlement(
                    token_id=tid,
                    side=u,
                    limit_price=float(price),
                    shares=float(fill_shares),
                    settle_at_mono=settle_at,
                    yes_token_id=str(yes_token_id),
                    no_token_id=str(no_token_id),
                    book_bid=book_bid,
                    order_type=ot,
                    ticks_until_settle=ticks_until_settle,
                )
            )
            meta: dict[str, float] = {
                "realized_pnl_usd": 0.0,
                "wallet_balance_usd": round(self.usdc_cash, 8),
                "paper_pending": 1.0,
                "paper_scheduled_shares": round(fill_shares, 6),
            }
            if ticks_until_settle is not None:
                meta["paper_settle_ticks"] = float(ticks_until_settle)
            else:
                meta["paper_settlement_delay_s"] = round(delay_s, 3)
            return True, meta

    def balances_for_pair(self, yes_token_id: str, no_token_id: str) -> tuple[float, float]:
        """Settle due pending fills, then return YES/NO settled shares."""
        self.settle_pending()
        with self._lock:
            y, _ = self._pos(yes_token_id)
            n, _ = self._pos(no_token_id)
            return float(y), float(n)


class PaperV2Account:
    """Per-market view onto a shared :class:`PaperSessionLedger`."""

    def __init__(
        self,
        yes_token_id: str,
        no_token_id: str,
        *,
        session: PaperSessionLedger | None = None,
        starting_usdc: float | None = None,
        fee_bps: float = 0.0,
        settlement_delay_min_s: float = 0.5,
        settlement_delay_max_s: float = 2.0,
        partial_fill_fraction_min: float = 0.5,
        partial_fill_fraction_max: float = 1.0,
        fak_partial_fill_fraction_min: float | None = None,
        fak_partial_fill_fraction_max: float | None = None,
    ) -> None:
        self.yes_token_id = yes_token_id
        self.no_token_id = no_token_id
        if session is not None:
            self._session = session
        else:
            su = float(starting_usdc if starting_usdc is not None else 0.0)
            self._session = PaperSessionLedger(
                su,
                float(fee_bps),
                settlement_delay_min_s=settlement_delay_min_s,
                settlement_delay_max_s=settlement_delay_max_s,
                partial_fill_fraction_min=partial_fill_fraction_min,
                partial_fill_fraction_max=partial_fill_fraction_max,
                fak_partial_fill_fraction_min=fak_partial_fill_fraction_min,
                fak_partial_fill_fraction_max=fak_partial_fill_fraction_max,
            )

    @property
    def starting_usdc(self) -> float:
        return float(self._session.starting_usdc)

    @property
    def fee_bps(self) -> float:
        return float(self._session.fee_bps)

    def balances(self) -> tuple[float, float]:
        return self._session.balances_for_pair(self.yes_token_id, self.no_token_id)

    def settle_pending(self) -> list[PaperFill]:
        return self._session.settle_pending()

    def advance_monitor_tick(self, *, poll_interval_s: float | None = None) -> list[PaperFill]:
        return self._session.advance_monitor_tick(poll_interval_s=poll_interval_s)

    def place_limit_order(
        self,
        token_id: str,
        side: str,
        price: float,
        shares: float,
        *,
        book_bid: float | None = None,
        book_ask: float | None = None,
        order_type: str = "GTC",
    ) -> tuple[bool, dict[str, float] | None]:
        return self._session.place_limit_order(
            token_id,
            side,
            price,
            shares,
            book_bid=book_bid,
            book_ask=book_ask,
            yes_token_id=self.yes_token_id,
            no_token_id=self.no_token_id,
            order_type=order_type,
        )

    def to_monitor_summary(self) -> dict[str, Any]:
        ry, rn = self.balances()
        return {
            "paper_usdc_cash": round(self._session.usdc_cash, 6),
            "paper_rem_yes": round(ry, 6),
            "paper_rem_no": round(rn, 6),
            "paper_pending_orders": self._session.pending_count(),
            "paper_starting_usdc": round(float(self._session.starting_usdc), 6),
            "paper_fills_count": len(self._session.fills),
        }
