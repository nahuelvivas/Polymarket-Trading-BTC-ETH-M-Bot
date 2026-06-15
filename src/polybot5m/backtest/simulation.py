"""Realistic execution simulation: latency, VWAP through book, fees, probabilistic fills, risk events."""

from __future__ import annotations

import math
import random
from bisect import bisect_left
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from polybot5m.config import BacktestSimulationConfig


def _parse_snap_ts(s: str | None) -> datetime | None:
    if not s:
        return None
    t = str(s).strip()
    if t.endswith("Z"):
        t = t[:-1] + "+00:00"
    try:
        d = datetime.fromisoformat(t)
        if d.tzinfo is None:
            d = d.replace(tzinfo=UTC)
        return d.astimezone(UTC)
    except ValueError:
        return None


def _levels_from_book(ob: Any, *, side: str) -> list[tuple[float, float]]:
    """Parse bids or asks from CLOB-style dict { bids: [{price,size}], asks: [...] }."""
    if not ob or not isinstance(ob, dict):
        return []
    key = "bids" if side == "bids" else "asks"
    raw = ob.get(key) or []
    out: list[tuple[float, float]] = []
    for x in raw:
        if not isinstance(x, dict):
            continue
        try:
            p = float(x.get("price", 0))
            sz = float(x.get("size", 0))
        except (TypeError, ValueError):
            continue
        if p > 0 and sz > 0:
            out.append((p, sz))
    if side == "bids":
        out.sort(key=lambda t: -t[0])
    else:
        out.sort(key=lambda t: t[0])
    return out


def vwap_sell_into_bids(
    bids: list[tuple[float, float]],
    size: float,
    min_accept_price: float,
) -> tuple[float, float, float]:
    """
    Sell `size` shares into bid levels at or above min_accept_price (limit sell).
    Returns (filled_size, vwap, remaining_unfilled).
    """
    if size <= 0:
        return 0.0, 0.0, 0.0
    rem = size
    notional = 0.0
    filled = 0.0
    for price, lvl_sz in bids:
        if price + 1e-12 < min_accept_price:
            break
        take = min(rem, lvl_sz)
        if take <= 0:
            continue
        notional += take * price
        filled += take
        rem -= take
        if rem <= 1e-12:
            break
    vwap = notional / filled if filled > 0 else 0.0
    return filled, vwap, max(0.0, size - filled)


def vwap_buy_from_asks(
    asks: list[tuple[float, float]],
    size: float,
    max_pay_price: float,
) -> tuple[float, float, float]:
    """Buy `size` shares from asks at or below max_pay_price."""
    if size <= 0:
        return 0.0, 0.0, 0.0
    rem = size
    notional = 0.0
    filled = 0.0
    for price, lvl_sz in asks:
        if price - 1e-12 > max_pay_price:
            break
        take = min(rem, lvl_sz)
        if take <= 0:
            continue
        notional += take * price
        filled += take
        rem -= take
        if rem <= 1e-12:
            break
    vwap = notional / filled if filled > 0 else 0.0
    return filled, vwap, max(0.0, size - filled)


def _apply_risk_to_bids(
    bids: list[tuple[float, float]],
    *,
    remaining_s_at_exec: float,
    cfg: BacktestSimulationConfig,
    rng: random.Random,
) -> list[tuple[float, float]]:
    out = [(p, s) for p, s in bids]
    rc = cfg.risk
    if remaining_s_at_exec <= rc.spread_widen_seconds_before_end and out:
        mult = max(0.0, min(1.0, rc.spread_widen_depth_mult))
        out = [(p, max(0.0, sz * mult)) for p, sz in out]
    if rng.random() < rc.liquidity_eviction_probability and out:
        frac = max(0.0, min(1.0, rc.eviction_size_fraction))
        p0, s0 = out[0]
        out[0] = (p0, max(0.0, s0 * (1.0 - frac)))
    return out


@dataclass
class SnapshotTimeline:
    """Sorted snapshots strictly before market end (same as replay loop)."""

    times: list[datetime]
    snaps: list[dict[str, Any]]

    @classmethod
    def from_snapshots(
        cls,
        snaps: list[dict[str, Any]],
        end_dt: datetime,
    ) -> SnapshotTimeline:
        times: list[datetime] = []
        out_snaps: list[dict[str, Any]] = []
        for s in snaps:
            t = _parse_snap_ts(s.get("time"))
            if t is None or t >= end_dt:
                continue
            times.append(t)
            out_snaps.append(s)
        return cls(times=times, snaps=out_snaps)

    def snap_at_or_after(self, t: datetime) -> tuple[dict[str, Any], datetime] | None:
        if not self.times:
            return None
        i = bisect_left(self.times, t)
        if i >= len(self.times):
            return self.snaps[-1], self.times[-1]
        return self.snaps[i], self.times[i]


@dataclass
class SimulatedFillResult:
    ok: bool
    filled_size: float
    vwap_price: float
    proceeds_after_fees: float
    fee_usdc: float
    best_bid_at_decision: float
    best_bid_or_ask_at_fill: float
    latency_ms: float
    slippage_vs_limit: float
    failure_reason: str | None = None


@dataclass
class SimulationRunMetrics:
    """Aggregated across one market or full batch."""

    slippage_cost_usdc: float = 0.0
    fee_usdc_total: float = 0.0
    latency_ms_sum: float = 0.0
    latency_ms_count: int = 0
    failed_sell_orders: int = 0
    partial_fills: int = 0
    exit_attempts: int = 0
    exit_failures: int = 0

    def avg_latency_ms(self) -> float | None:
        if self.latency_ms_count <= 0:
            return None
        return self.latency_ms_sum / self.latency_ms_count


class PaperExecutionHook(Protocol):
    async def execute_sell_limit(
        self,
        *,
        token_side: str,
        limit_price: float,
        size: float,
        reason: str,
        decision_ts: datetime,
        bid_yes: float,
        bid_no: float,
        remaining_s: float,
        orderbook_yes: Any,
        orderbook_no: Any,
    ) -> SimulatedFillResult: ...

    async def execute_buy_limit(
        self,
        *,
        token_side: str,
        limit_price: float,
        size: float,
        reason: str,
        decision_ts: datetime,
        bid_yes: float,
        bid_no: float,
        remaining_s: float,
        orderbook_yes: Any,
        orderbook_no: Any,
    ) -> SimulatedFillResult: ...


@dataclass
class RealisticPaperExecutionHook:
    """Latency + book at execution time + VWAP + fees + fill probability + risk."""

    timeline: SnapshotTimeline
    end_dt: datetime
    cfg: BacktestSimulationConfig
    rng: random.Random
    metrics: SimulationRunMetrics = field(default_factory=SimulationRunMetrics)
    include_orderbook: bool = True

    def _latency_ms(self) -> float:
        a, b = self.cfg.latency_ms_min, self.cfg.latency_ms_max
        if b <= a:
            return float(a)
        return self.rng.uniform(float(a), float(b))

    def _best_bid_side(self, ob: Any) -> float:
        lv = _levels_from_book(ob, side="bids")
        return lv[0][0] if lv else 0.0

    def _ob_for_side(self, token_side: str, ob_yes: Any, ob_no: Any) -> Any:
        return ob_yes if token_side.lower() == "yes" else ob_no

    def _exec_snapshot(
        self,
        decision_ts: datetime,
        latency_ms: float,
    ) -> tuple[dict[str, Any], datetime, float]:
        t_exec = decision_ts + timedelta(milliseconds=latency_ms)
        if t_exec >= self.end_dt:
            t_exec = self.end_dt - timedelta(microseconds=1)
        pair = self.timeline.snap_at_or_after(t_exec)
        if pair is None:
            return {}, decision_ts, max(0.0, (self.end_dt - decision_ts).total_seconds())
        snap, ts = pair
        rem = max(0.0, (self.end_dt - ts).total_seconds())
        return snap, ts, rem

    def _is_exit_attempt(self, reason: str) -> bool:
        r = reason.upper()
        return any(
            x in r
            for x in (
                "LOW_FULL",
                "HIGH_FULL",
                "HIGH_PARTIAL",
                "SECONDARY",
                "THIRD_",
                "UNWIND",
            )
        )

    async def execute_sell_limit(
        self,
        *,
        token_side: str,
        limit_price: float,
        size: float,
        reason: str,
        decision_ts: datetime,
        bid_yes: float,
        bid_no: float,
        remaining_s: float,
        orderbook_yes: Any,
        orderbook_no: Any,
    ) -> SimulatedFillResult:
        if self._is_exit_attempt(reason):
            self.metrics.exit_attempts += 1

        best_dec = bid_yes if token_side.lower() == "yes" else bid_no
        lat_ms = self._latency_ms()
        snap, _fill_ts, rem_exec = self._exec_snapshot(decision_ts, lat_ms)

        ob_yes = snap.get("orderbook_up") if snap else orderbook_yes
        ob_no = snap.get("orderbook_down") if snap else orderbook_no
        ob = self._ob_for_side(token_side, ob_yes, ob_no)

        bids: list[tuple[float, float]]
        if self.include_orderbook and ob:
            bids = _levels_from_book(ob, side="bids")
            if not bids:
                bb = self._best_bid_side(ob)
                if bb <= 0:
                    bb = best_dec
                bids = [(bb, 1.0e9)] if bb > 0 else []
        else:
            bb = best_dec
            if bb <= 0:
                self.metrics.failed_sell_orders += 1
                if self._is_exit_attempt(reason):
                    self.metrics.exit_failures += 1
                return SimulatedFillResult(
                    ok=False,
                    filled_size=0.0,
                    vwap_price=0.0,
                    proceeds_after_fees=0.0,
                    fee_usdc=0.0,
                    best_bid_at_decision=best_dec,
                    best_bid_or_ask_at_fill=bb,
                    latency_ms=lat_ms,
                    slippage_vs_limit=0.0,
                    failure_reason="no_book",
                )
            bids = [(bb, 1.0e9)]

        bids = _apply_risk_to_bids(bids, remaining_s_at_exec=rem_exec, cfg=self.cfg, rng=self.rng)

        if self.cfg.risk.no_exit_liquidity_probability > 0 and self.rng.random() < self.cfg.risk.no_exit_liquidity_probability:
            self.metrics.failed_sell_orders += 1
            if self._is_exit_attempt(reason):
                self.metrics.exit_failures += 1
            bb_at_fill = bids[0][0] if bids else 0.0
            return SimulatedFillResult(
                ok=False,
                filled_size=0.0,
                vwap_price=0.0,
                proceeds_after_fees=0.0,
                fee_usdc=0.0,
                best_bid_at_decision=best_dec,
                best_bid_or_ask_at_fill=bb_at_fill,
                latency_ms=lat_ms,
                slippage_vs_limit=0.0,
                failure_reason="no_exit_liquidity",
            )

        if not self.cfg.market_style_sell and self.rng.random() > float(
            self.cfg.limit_order_fill_probability
        ):
            self.metrics.failed_sell_orders += 1
            if self._is_exit_attempt(reason):
                self.metrics.exit_failures += 1
            bb_at_fill = bids[0][0] if bids else 0.0
            return SimulatedFillResult(
                ok=False,
                filled_size=0.0,
                vwap_price=0.0,
                proceeds_after_fees=0.0,
                fee_usdc=0.0,
                best_bid_at_decision=best_dec,
                best_bid_or_ask_at_fill=bb_at_fill,
                latency_ms=lat_ms,
                slippage_vs_limit=0.0,
                failure_reason="limit_not_filled",
            )

        min_px = max(0.01, min(0.99, limit_price))
        filled, vwap, unfilled = vwap_sell_into_bids(bids, size, min_px)
        if filled <= 0:
            self.metrics.failed_sell_orders += 1
            if self._is_exit_attempt(reason):
                self.metrics.exit_failures += 1
            bb_at_fill = bids[0][0] if bids else 0.0
            return SimulatedFillResult(
                ok=False,
                filled_size=0.0,
                vwap_price=0.0,
                proceeds_after_fees=0.0,
                fee_usdc=0.0,
                best_bid_at_decision=best_dec,
                best_bid_or_ask_at_fill=bb_at_fill,
                latency_ms=lat_ms,
                slippage_vs_limit=0.0,
                failure_reason="insufficient_liquidity",
            )

        if unfilled > 1e-8:
            self.metrics.partial_fills += 1

        gross = filled * vwap
        fee = gross * float(self.cfg.fee_bps) / 10000.0
        net = max(0.0, gross - fee)
        slip = limit_price - vwap
        self.metrics.slippage_cost_usdc += max(0.0, slip * filled)
        self.metrics.fee_usdc_total += fee
        self.metrics.latency_ms_sum += lat_ms
        self.metrics.latency_ms_count += 1

        bb_at_fill = bids[0][0] if bids else vwap
        return SimulatedFillResult(
            ok=True,
            filled_size=filled,
            vwap_price=vwap,
            proceeds_after_fees=net,
            fee_usdc=fee,
            best_bid_at_decision=best_dec,
            best_bid_or_ask_at_fill=bb_at_fill,
            latency_ms=lat_ms,
            slippage_vs_limit=slip,
            failure_reason=None,
        )

    async def execute_buy_limit(
        self,
        *,
        token_side: str,
        limit_price: float,
        size: float,
        reason: str,
        decision_ts: datetime,
        bid_yes: float,
        bid_no: float,
        remaining_s: float,
        orderbook_yes: Any,
        orderbook_no: Any,
    ) -> SimulatedFillResult:
        best_dec = bid_yes if token_side.lower() == "yes" else bid_no
        lat_ms = self._latency_ms()
        snap, _ts, rem_exec = self._exec_snapshot(decision_ts, lat_ms)
        ob_yes = snap.get("orderbook_up") if snap else orderbook_yes
        ob_no = snap.get("orderbook_down") if snap else orderbook_no
        ob = self._ob_for_side(token_side, ob_yes, ob_no)
        asks = _levels_from_book(ob, side="asks") if ob else []
        if not asks:
            # synthetic: pay up to limit
            ap = min(limit_price, 0.99) if limit_price > 0 else 0.5
            asks = [(ap, 1.0e9)]
        # Mirror risk: thin asks near end
        if rem_exec <= self.cfg.risk.spread_widen_seconds_before_end:
            mult = max(0.0, min(1.0, self.cfg.risk.spread_widen_depth_mult))
            asks = [(p, max(0.0, s * mult)) for p, s in asks]

        max_px = max(0.01, min(0.99, limit_price))
        if self.rng.random() > float(self.cfg.limit_order_fill_probability):
            return SimulatedFillResult(
                ok=False,
                filled_size=0.0,
                vwap_price=0.0,
                proceeds_after_fees=0.0,
                fee_usdc=0.0,
                best_bid_at_decision=best_dec,
                best_bid_or_ask_at_fill=asks[0][0] if asks else 0.0,
                latency_ms=lat_ms,
                slippage_vs_limit=0.0,
                failure_reason="limit_not_filled",
            )

        filled, vwap, _unfilled = vwap_buy_from_asks(asks, size, max_px)
        if filled <= 0:
            return SimulatedFillResult(
                ok=False,
                filled_size=0.0,
                vwap_price=0.0,
                proceeds_after_fees=0.0,
                fee_usdc=0.0,
                best_bid_at_decision=best_dec,
                best_bid_or_ask_at_fill=asks[0][0] if asks else 0.0,
                latency_ms=lat_ms,
                slippage_vs_limit=0.0,
                failure_reason="insufficient_liquidity",
            )

        gross = filled * vwap
        fee = gross * float(self.cfg.fee_bps) / 10000.0
        net = gross + fee
        slip = vwap - limit_price
        self.metrics.latency_ms_sum += lat_ms
        self.metrics.latency_ms_count += 1
        self.metrics.fee_usdc_total += fee
        bb_at_fill = asks[0][0] if asks else vwap
        return SimulatedFillResult(
            ok=True,
            filled_size=filled,
            vwap_price=vwap,
            proceeds_after_fees=net,
            fee_usdc=fee,
            best_bid_at_decision=best_dec,
            best_bid_or_ask_at_fill=bb_at_fill,
            latency_ms=lat_ms,
            slippage_vs_limit=slip,
            failure_reason=None,
        )


def make_rng(cfg: BacktestSimulationConfig) -> random.Random:
    seed = cfg.random_seed
    if seed is None:
        return random.Random()
    return random.Random(int(seed))


def max_drawdown_from_series(cumulative: list[float]) -> float:
    if not cumulative:
        return 0.0
    peak = cumulative[0]
    mdd = 0.0
    for x in cumulative:
        peak = max(peak, x)
        dd = peak - x
        mdd = max(mdd, dd)
    return mdd
