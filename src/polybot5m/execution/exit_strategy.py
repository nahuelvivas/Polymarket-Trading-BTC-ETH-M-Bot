"""buy1 / buy2 entry logic and risk1 / risk2 / risk3 exits."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from polybot5m.config import Buy1Config, Buy2Config, Risk1Config, Risk2Config, Risk3Config, Settings
from polybot5m.data.user_channel_store import UserChannelStore
from polybot5m.trading_process_log import (
    BtcStrategySnapshot,
    SpotMinusStrikeEpochAverage,
    TradingCycleJournal,
    enrich_strategy_row_t_minus,
    format_t_minus_suffix,
    strategy_phase_from_event,
)

ENTRY_PAIR_SYMBOLS = ("btc", "eth")
_MIN_PRICE = 0.01
_MAX_PRICE = 0.99
_FAK_NO_MATCH_ERR_MARKER = "no orders found to match with FAK order"


def _is_fak_no_match_error(err: str) -> bool:
    return _FAK_NO_MATCH_ERR_MARKER in (err or "")


def _clamp_buy_price(price: float) -> float:
    return min(_MAX_PRICE, max(_MIN_PRICE, round(float(price), 4)))


def _clamp_sell_price(best_bid: float, offset: float) -> float:
    return max(_MIN_PRICE, round(float(best_bid) - float(offset), 4))


def _in_time_window(remaining_s: float, *, lo: float, hi: float) -> bool:
    return float(lo) <= float(remaining_s) <= float(hi)


def _passes_sign_alignment(
    spot_minus_strike_btc: float,
    averge_spot_minus_btc: float | None,
    spot_minus_strike_eth: float | None,
) -> bool:
    if averge_spot_minus_btc is None or spot_minus_strike_eth is None:
        return False
    btc = float(spot_minus_strike_btc)
    avg = float(averge_spot_minus_btc)
    eth = float(spot_minus_strike_eth)
    if btc > 0:
        return avg > 0 and eth > 0
    if btc < 0:
        return avg < 0 and eth < 0
    return False


def _best_bid_for_outcome(slot: MarketSlot, outcome: str) -> float:
    return slot.best_bid_yes if outcome.upper() == "YES" else slot.best_bid_no


def _best_ask_for_outcome(slot: MarketSlot, outcome: str) -> float:
    return slot.best_ask_yes if outcome.upper() == "YES" else slot.best_ask_no


def _outcome_token_id(slot: MarketSlot, outcome: str) -> str:
    return slot.yes_token_id if outcome.upper() == "YES" else slot.no_token_id


def _log(
    tag: str,
    msg: str,
    *,
    slot: MarketSlot | None = None,
    remaining_s: float | None = None,
) -> None:
    t = remaining_s
    if t is None and slot is not None:
        t = slot.remaining_s
    print(f"  {tag} {msg}{format_t_minus_suffix(t)}")


def _jsonl(slot: MarketSlot, row: dict[str, Any]) -> None:
    if slot.trading_journal is None:
        return
    ev = str(row.get("event") or "")
    phase = strategy_phase_from_event(ev)
    if phase and "strategy" not in row:
        row = {**row, "strategy": phase}
    row = enrich_strategy_row_t_minus(row, remaining_s=slot.remaining_s)
    merged = {**slot.strategy_log_extras, **row}
    slot.trading_journal.append_strategy(merged)


@dataclass
class PositionState:
    """Open position after a confirmed buy1/buy2 fill."""

    outcome: str = ""
    entry_label: str = ""
    fill_price: float = 0.0
    order_price: float = 0.0
    target_shares: float = 0.0
    filled_shares: float = 0.0
    pending_order: bool = False
    fill_confirmed: bool = False
    closed: bool = False
    first_match_mono: float | None = None
    risk1_cycles: int = 0
    risk3_cycles: int = 0
    exit_busy: bool = False
    exit_reason: str = ""
    sell_attempts: int = 0
    baseline_rem_yes: float = 0.0
    baseline_rem_no: float = 0.0
    trades_seen: int = 0


@dataclass
class MarketSlot:
    symbol: str
    tag: str
    yes_token_id: str
    no_token_id: str
    condition_id: str = ""
    clob_client: Any | None = None
    paper_account: Any | None = None
    user_store: UserChannelStore | None = None
    refresh_balances: Callable[[], Awaitable[None]] | None = None
    trading_journal: TradingCycleJournal | None = None
    remaining_s: float = 0.0
    best_bid_yes: float = 0.0
    best_bid_no: float = 0.0
    best_ask_yes: float = 0.0
    best_ask_no: float = 0.0
    rem_yes: float = 0.0
    rem_no: float = 0.0
    spot_minus_strike: float | None = None
    position: PositionState = field(default_factory=PositionState)
    strategy_log_extras: dict[str, Any] = field(default_factory=dict)
    active_task: asyncio.Task[None] | None = None


@dataclass
class _EntryStrategyState:
    done: bool = False
    locked_outcome: str = ""
    yes_cycles: int = 0
    no_cycles: int = 0
    busy: bool = False


class EntryStrategyCoordinator:
    """BTC wave context + buy1/buy2 entries + risk1/risk2/risk3 exits."""

    def __init__(self, settings: Settings) -> None:
        self._buy1: Buy1Config = settings.buy1
        self._buy2: Buy2Config = settings.buy2
        self._risk1: Risk1Config = settings.risk1
        self._risk2: Risk2Config = settings.risk2
        self._risk3: Risk3Config = settings.risk3
        self._epsilon = max(0.0, float(settings.execution.balance_epsilon or 0.01))
        self._slots: dict[str, MarketSlot] = {}
        self._btc_snapshot = BtcStrategySnapshot()
        self._btc_sms_avg = SpotMinusStrikeEpochAverage()
        self._buy1_state = _EntryStrategyState()
        self._buy2_state = _EntryStrategyState()
        self._buy_blocked_symbols: set[str] = set()
        self._lock = asyncio.Lock()

    def reset_btc_epoch_stats(self) -> None:
        self._btc_snapshot.reset_epoch()
        self._btc_sms_avg = SpotMinusStrikeEpochAverage()
        self._buy1_state = _EntryStrategyState()
        self._buy2_state = _EntryStrategyState()
        self._buy_blocked_symbols = set()
        for slot in self._slots.values():
            slot.position = PositionState()

    def _refresh_slot_strategy_extras(self) -> None:
        extras = self._btc_snapshot.as_log_fields()
        for slot in self._slots.values():
            slot.strategy_log_extras = extras

    def update_btc_strategy_context(
        self,
        *,
        spot_minus_strike_btc: float | None = None,
        averge_spot_minus_btc: float | None = None,
        difference_rate_btc: float | None = None,
        best_bid_yes: float = 0.0,
        best_bid_no: float = 0.0,
        best_ask_yes: float = 0.0,
        best_ask_no: float = 0.0,
    ) -> None:
        snap = self._btc_snapshot
        if spot_minus_strike_btc is not None:
            snap.spot_minus_strike_btc = float(spot_minus_strike_btc)
            snap.record_spot_minus_strike_btc(snap.spot_minus_strike_btc)
        if averge_spot_minus_btc is not None:
            snap.averge_spot_minus_btc = float(averge_spot_minus_btc)
        if difference_rate_btc is not None:
            snap.difference_rate_btc = float(difference_rate_btc)
        if best_bid_yes > 0:
            snap.yes_best_bid = float(best_bid_yes)
        if best_bid_no > 0:
            snap.no_best_bid = float(best_bid_no)
        if best_ask_yes > 0:
            snap.yes_best_ask = float(best_ask_yes)
        if best_ask_no > 0:
            snap.no_best_ask = float(best_ask_no)
        self._refresh_slot_strategy_extras()

    def btc_wave_extrema(self) -> tuple[float | None, float | None]:
        snap = self._btc_snapshot
        return snap.max_spot_minus_strike_btc, snap.min_spot_minus_strike_btc

    def register_market(
        self,
        symbol: str,
        *,
        tag: str,
        yes_token_id: str,
        no_token_id: str,
        condition_id: str = "",
        clob_client: Any | None = None,
        paper_account: Any | None = None,
        user_store: UserChannelStore | None = None,
        refresh_balances: Callable[[], Awaitable[None]] | None = None,
        trading_journal: TradingCycleJournal | None = None,
    ) -> None:
        sym = str(symbol).lower().strip()
        self._slots[sym] = MarketSlot(
            symbol=sym,
            tag=tag,
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            condition_id=condition_id,
            clob_client=clob_client,
            paper_account=paper_account,
            user_store=user_store,
            refresh_balances=refresh_balances,
            trading_journal=trading_journal,
        )
        self._refresh_slot_strategy_extras()

    def unregister_market(self, symbol: str) -> None:
        sym = str(symbol).lower().strip()
        slot = self._slots.pop(sym, None)
        if slot is not None and slot.active_task is not None:
            slot.active_task.cancel()

    def _averge_spot_minus_btc(self) -> float | None:
        avg = self._btc_sms_avg.average()
        if avg is not None:
            return avg
        return getattr(self._btc_snapshot, "averge_spot_minus_btc", None)

    def _spot_minus_strike_btc(self) -> float | None:
        return getattr(self._btc_snapshot, "spot_minus_strike_btc", None)

    def _spot_minus_strike_eth(self) -> float | None:
        eth = self._slots.get("eth")
        if eth is None:
            return None
        return eth.spot_minus_strike

    def _ref_remaining_s(self) -> float:
        vals = [self._slots[s].remaining_s for s in ENTRY_PAIR_SYMBOLS if s in self._slots]
        return min(vals) if vals else 0.0

    def _symbol_buy_blocked(self, symbol: str) -> bool:
        return str(symbol).lower().strip() in self._buy_blocked_symbols

    def _block_symbol_buys(self, symbol: str) -> None:
        self._buy_blocked_symbols.add(str(symbol).lower().strip())

    def _position_shares(self, slot: MarketSlot, pos: PositionState) -> float:
        if not pos.outcome:
            return 0.0
        if pos.outcome.upper() == "YES":
            return max(0.0, float(slot.rem_yes) - float(pos.baseline_rem_yes))
        return max(0.0, float(slot.rem_no) - float(pos.baseline_rem_no))

    def _ingest_buy_fills(self, slot: MarketSlot) -> None:
        pos = slot.position
        if not pos.pending_order and not pos.fill_confirmed:
            return
        store = slot.user_store
        if store is not None and pos.outcome:
            token_id = _outcome_token_id(slot, pos.outcome)
            n = len(store.trades)
            if n > pos.trades_seen:
                new_trades = list(store.trades)[pos.trades_seen :]
                pos.trades_seen = n
                notional = float(pos.filled_shares) * float(pos.fill_price or pos.order_price or 0.0)
                shares = float(pos.filled_shares)
                for raw in new_trades:
                    row = store.trade_row_for_log(raw)
                    if str(row.get("side") or "").upper() != "BUY":
                        continue
                    if str(raw.get("asset_id") or "") != str(token_id).strip():
                        continue
                    status = str(row.get("status") or "").upper()
                    if status not in ("MATCHED", "MINED", "CONFIRMED"):
                        continue
                    try:
                        px = float(row.get("price") or 0)
                        sz = float(row.get("size") or 0)
                    except (TypeError, ValueError):
                        continue
                    if sz <= 0 or px <= 0:
                        continue
                    notional += px * sz
                    shares += sz
                if shares > 0:
                    pos.filled_shares = shares
                    pos.fill_price = notional / shares
        if self._position_shares(slot, pos) > self._epsilon:
            if not pos.fill_confirmed:
                pos.fill_confirmed = True
                pos.pending_order = False
                pos.first_match_mono = time.monotonic()
                if pos.fill_price <= 0:
                    pos.fill_price = float(pos.order_price or 0.0)
                _log(
                    slot.tag,
                    f"[{pos.entry_label}_FILL] outcome={pos.outcome} "
                    f"fill_px={pos.fill_price:.4f} fill_sz={pos.filled_shares:.4f}",
                    slot=slot,
                )
                _jsonl(
                    slot,
                    {
                        "event": f"{pos.entry_label}_FILL",
                        "buy_outcome": pos.outcome,
                        "filled_price": pos.fill_price,
                        "filled_shares": pos.filled_shares,
                        "rem_yes": slot.rem_yes,
                        "rem_no": slot.rem_no,
                    },
                )

    def _side_book_ok(
        self,
        slot: MarketSlot,
        outcome: str,
        *,
        max_spread: float,
        min_best_ask: float,
    ) -> bool:
        bid = _best_bid_for_outcome(slot, outcome)
        ask = _best_ask_for_outcome(slot, outcome)
        if bid <= 0 or ask <= 0:
            return False
        if (ask - bid) >= float(max_spread):
            return False
        return ask > float(min_best_ask)

    def _global_gates_ok(
        self,
        cfg: Buy1Config | Buy2Config,
        *,
        sms_btc: float,
        avg_btc: float,
        sms_eth: float,
    ) -> bool:
        if abs(float(sms_btc)) <= float(cfg.spot_minus_strike_btc_abs_min):
            return False
        if abs(float(avg_btc)) <= float(cfg.averge_spot_minus_btc_abs_min):
            return False
        if abs(float(sms_eth)) <= float(cfg.spot_minus_strike_eth_abs_min):
            return False
        return _passes_sign_alignment(float(sms_btc), float(avg_btc), float(sms_eth))

    def _buy1_side_ok(self, outcome: str) -> bool:
        if not self._buy1.enabled or self._symbol_buy_blocked("btc"):
            return False
        btc = self._slots.get("btc")
        eth = self._slots.get("eth")
        if btc is None or eth is None:
            return False
        if btc.position.pending_order or (btc.position.fill_confirmed and not btc.position.closed):
            return False
        sms_btc = self._spot_minus_strike_btc()
        avg_btc = self._averge_spot_minus_btc()
        sms_eth = self._spot_minus_strike_eth()
        if sms_btc is None or avg_btc is None or sms_eth is None:
            return False
        if not self._global_gates_ok(
            self._buy1,
            sms_btc=float(sms_btc),
            avg_btc=float(avg_btc),
            sms_eth=float(sms_eth),
        ):
            return False
        if not self._side_book_ok(
            btc,
            outcome,
            max_spread=self._buy1.max_spread,
            min_best_ask=self._buy1.min_best_ask,
        ):
            return False
        eth_bid = _best_bid_for_outcome(eth, outcome)
        return eth_bid > float(self._buy1.other_symbol_min_best_bid)

    def _buy2_side_ok(self, outcome: str) -> bool:
        if not self._buy2.enabled or self._symbol_buy_blocked("eth"):
            return False
        btc = self._slots.get("btc")
        eth = self._slots.get("eth")
        if btc is None or eth is None:
            return False
        if eth.position.pending_order or (eth.position.fill_confirmed and not eth.position.closed):
            return False
        sms_btc = self._spot_minus_strike_btc()
        avg_btc = self._averge_spot_minus_btc()
        sms_eth = self._spot_minus_strike_eth()
        if sms_btc is None or avg_btc is None or sms_eth is None:
            return False
        if not self._global_gates_ok(
            self._buy2,
            sms_btc=float(sms_btc),
            avg_btc=float(avg_btc),
            sms_eth=float(sms_eth),
        ):
            return False
        if not self._side_book_ok(
            eth,
            outcome,
            max_spread=self._buy2.max_spread,
            min_best_ask=self._buy2.min_best_ask,
        ):
            return False
        btc_bid = _best_bid_for_outcome(btc, outcome)
        return btc_bid > float(self._buy2.other_symbol_min_best_bid)

    def _tick_strategy(
        self,
        state: _EntryStrategyState,
        *,
        cfg: Buy1Config | Buy2Config,
        side_ok_fn: Callable[[str], bool],
        remaining_s: float,
        lo: float,
        hi: float,
    ) -> str | None:
        if state.done:
            return None
        if not _in_time_window(remaining_s, lo=lo, hi=hi):
            state.yes_cycles = 0
            state.no_cycles = 0
            return None
        need = max(1, int(cfg.monitoring_cycles))
        if state.locked_outcome:
            outcome = state.locked_outcome
            if side_ok_fn(outcome):
                return outcome
            return None
        for outcome, attr in (("YES", "yes_cycles"), ("NO", "no_cycles")):
            if side_ok_fn(outcome):
                setattr(state, attr, int(getattr(state, attr)) + 1)
            else:
                setattr(state, attr, 0)
        for outcome, attr in (("YES", "yes_cycles"), ("NO", "no_cycles")):
            if int(getattr(state, attr)) >= need:
                state.locked_outcome = outcome
                return outcome
        return None

    def _sell_offset_for_reason(self, reason: str) -> float:
        if reason == "risk1":
            return float(self._risk1.sell_offset)
        if reason == "risk2":
            return float(self._risk2.sell_offset)
        if reason == "risk3":
            return float(self._risk3.sell_offset)
        return 0.05

    def _max_sell_attempts_for_reason(self, reason: str) -> int:
        if reason == "risk1":
            return max(1, int(self._risk1.max_sell_attempts))
        if reason == "risk2":
            return max(1, int(self._risk2.max_sell_attempts))
        if reason == "risk3":
            return max(1, int(self._risk3.max_sell_attempts))
        return 5

    def _pick_risk_exit_reason(self, slot: MarketSlot, pos: PositionState) -> str | None:
        outcome = pos.outcome
        if not outcome:
            return None
        rem = self._position_shares(slot, pos)
        if rem <= self._epsilon:
            return None
        bid = _best_bid_for_outcome(slot, outcome)
        now_mono = time.monotonic()

        if self._risk1.enabled and pos.fill_price > 0 and bid > 0:
            threshold = pos.fill_price - float(self._risk1.loss_offset)
            if bid < threshold:
                pos.risk1_cycles += 1
            else:
                pos.risk1_cycles = 0
            if pos.risk1_cycles >= max(1, int(self._risk1.monitoring_cycles)):
                return "risk1"

        if self._risk2.enabled and pos.first_match_mono is not None:
            elapsed = now_mono - float(pos.first_match_mono)
            if elapsed >= float(self._risk2.hold_timeout_sec) and rem > self._epsilon:
                return "risk2"

        if self._risk3.enabled and bid > 0:
            if bid < float(self._risk3.bid_below):
                pos.risk3_cycles += 1
            else:
                pos.risk3_cycles = 0
            if pos.risk3_cycles >= max(1, int(self._risk3.monitoring_cycles)):
                return "risk3"

        return None

    async def _evaluate_risk_exits(self, slot: MarketSlot) -> None:
        pos = slot.position
        if not pos.fill_confirmed or pos.closed:
            return
        if slot.active_task is not None and not slot.active_task.done():
            return

        if slot.refresh_balances is not None:
            await slot.refresh_balances()

        outcome = pos.outcome
        rem = self._position_shares(slot, pos)
        if rem <= self._epsilon:
            pos.closed = True
            return

        bid = _best_bid_for_outcome(slot, outcome)

        if pos.exit_busy:
            reason = pos.exit_reason or "risk1"
            max_attempts = self._max_sell_attempts_for_reason(reason)
            if pos.sell_attempts >= max_attempts:
                pos.exit_busy = False
                return
            sell_offset = self._sell_offset_for_reason(reason)
            pos.sell_attempts += 1
            sell_px = _clamp_sell_price(bid, sell_offset) if bid > 0 else _MIN_PRICE
            label = reason.upper()
            _log(
                slot.tag,
                f"[{label}_RETRY] sell_outcome={outcome} rem={rem:.4f} px={sell_px:.4f} "
                f"attempt={pos.sell_attempts}/{max_attempts}",
                slot=slot,
            )
            ok = await self._place_limit_sell(
                slot,
                outcome,
                sell_px,
                rem,
                event_label=f"{label}_RETRY",
                attempt=pos.sell_attempts,
            )
            if not ok:
                _jsonl(slot, {"event": f"{label}_RETRY_FAIL", "sell_outcome": outcome})
            if slot.refresh_balances is not None:
                await slot.refresh_balances()
            if self._position_shares(slot, pos) <= self._epsilon:
                await self._finish_risk_exit(slot, reason)
            return

        reason = self._pick_risk_exit_reason(slot, pos)
        if reason is None:
            return

        max_attempts = self._max_sell_attempts_for_reason(reason)
        if pos.sell_attempts >= max_attempts:
            return

        sell_offset = self._sell_offset_for_reason(reason)
        pos.exit_busy = True
        pos.exit_reason = reason
        pos.sell_attempts += 1
        sell_px = _clamp_sell_price(bid, sell_offset) if bid > 0 else _MIN_PRICE
        label = reason.upper()
        _log(
            slot.tag,
            f"[{label}] trigger sell_outcome={outcome} rem={rem:.4f} px={sell_px:.4f} "
            f"attempt={pos.sell_attempts}/{max_attempts} fill_px={pos.fill_price:.4f}",
            slot=slot,
        )
        _jsonl(
            slot,
            {
                "event": f"{label}_TRIGGER",
                "sell_outcome": outcome,
                "rem_shares": rem,
                "sell_price": sell_px,
                "attempt": pos.sell_attempts,
                "max_attempts": max_attempts,
                "filled_price": pos.fill_price,
                "best_bid": bid,
                "rem_yes": slot.rem_yes,
                "rem_no": slot.rem_no,
            },
        )
        ok = await self._place_limit_sell(
            slot,
            outcome,
            sell_px,
            rem,
            event_label=label,
            attempt=pos.sell_attempts,
        )
        if not ok:
            _jsonl(slot, {"event": f"{label}_FAIL", "sell_outcome": outcome})
        if slot.refresh_balances is not None:
            await slot.refresh_balances()
        if self._position_shares(slot, pos) <= self._epsilon:
            await self._finish_risk_exit(slot, reason)

    async def _finish_risk_exit(self, slot: MarketSlot, reason: str) -> None:
        pos = slot.position
        label = reason.upper()
        pos.exit_busy = False
        pos.closed = True
        pos.risk1_cycles = 0
        pos.risk3_cycles = 0
        self._block_symbol_buys(slot.symbol)
        _log(
            slot.tag,
            f"[{label}_DONE] sell_outcome={pos.outcome} rem_YES={slot.rem_yes:.4f} "
            f"rem_NO={slot.rem_no:.4f} (no more buys this epoch)",
            slot=slot,
        )
        _jsonl(
            slot,
            {
                "event": f"{label}_DONE",
                "sell_outcome": pos.outcome,
                "rem_yes": slot.rem_yes,
                "rem_no": slot.rem_no,
            },
        )

    async def on_monitor_tick(
        self,
        symbol: str,
        *,
        remaining_s: float,
        rem_yes: float,
        rem_no: float,
        best_bid_yes: float,
        best_bid_no: float,
        best_ask_yes: float = 0.0,
        best_ask_no: float = 0.0,
        spot_minus_strike: float | None = None,
        spot_minus_strike_btc: float | None = None,
        averge_spot_minus_btc: float | None = None,
        difference_rate_btc: float | None = None,
    ) -> None:
        sym = str(symbol).lower().strip()
        slot = self._slots.get(sym)
        if slot is None:
            return
        slot.remaining_s = float(remaining_s)
        slot.rem_yes = float(rem_yes)
        slot.rem_no = float(rem_no)
        slot.best_bid_yes = float(best_bid_yes)
        slot.best_bid_no = float(best_bid_no)
        slot.best_ask_yes = float(best_ask_yes)
        slot.best_ask_no = float(best_ask_no)
        sms = spot_minus_strike if spot_minus_strike is not None else spot_minus_strike_btc
        if sms is not None:
            slot.spot_minus_strike = float(sms)
        if sym == "btc":
            avg = averge_spot_minus_btc
            if sms is not None:
                avg = self._btc_sms_avg.record(sms)
            self.update_btc_strategy_context(
                spot_minus_strike_btc=sms,
                averge_spot_minus_btc=avg,
                difference_rate_btc=difference_rate_btc,
                best_bid_yes=slot.best_bid_yes,
                best_bid_no=slot.best_bid_no,
                best_ask_yes=slot.best_ask_yes,
                best_ask_no=slot.best_ask_no,
            )

        self._ingest_buy_fills(slot)

        if sym not in ENTRY_PAIR_SYMBOLS:
            return
        if "btc" not in self._slots or "eth" not in self._slots:
            return

        async with self._lock:
            for s in ENTRY_PAIR_SYMBOLS:
                sl = self._slots.get(s)
                if sl is not None:
                    self._ingest_buy_fills(sl)

            for s in ENTRY_PAIR_SYMBOLS:
                sl = self._slots.get(s)
                if sl is not None:
                    await self._evaluate_risk_exits(sl)

            if slot.active_task is not None and not slot.active_task.done():
                return

            ref_remaining = self._ref_remaining_s()
            buy1_outcome = self._tick_strategy(
                self._buy1_state,
                cfg=self._buy1,
                side_ok_fn=self._buy1_side_ok,
                remaining_s=ref_remaining,
                lo=float(self._buy1.trigger_time_end_sec),
                hi=float(self._buy1.trigger_time_start_sec),
            )
            buy2_outcome = self._tick_strategy(
                self._buy2_state,
                cfg=self._buy2,
                side_ok_fn=self._buy2_side_ok,
                remaining_s=ref_remaining,
                lo=float(self._buy2.trigger_time_end_sec),
                hi=float(self._buy2.trigger_time_start_sec),
            )
            if buy1_outcome and not self._buy1_state.done and not self._buy1_state.busy:
                btc_slot = self._slots["btc"]
                self._buy1_state.busy = True
                btc_slot.active_task = asyncio.create_task(
                    self._run_buy(
                        btc_slot, buy1_outcome, label="BUY1", cfg=self._buy1, state=self._buy1_state
                    )
                )
            if buy2_outcome and not self._buy2_state.done and not self._buy2_state.busy:
                eth_slot = self._slots["eth"]
                self._buy2_state.busy = True
                eth_slot.active_task = asyncio.create_task(
                    self._run_buy(
                        eth_slot, buy2_outcome, label="BUY2", cfg=self._buy2, state=self._buy2_state
                    )
                )

    async def _run_buy(
        self,
        slot: MarketSlot,
        outcome: str,
        *,
        label: str,
        cfg: Buy1Config | Buy2Config,
        state: _EntryStrategyState,
    ) -> None:
        try:
            price = _clamp_buy_price(float(cfg.buy_limit_price))
            size = round(float(cfg.shares), 4)
            token_id = _outcome_token_id(slot, outcome)
            order_type = str(cfg.order_type or "FAK").upper()
            trigger_bid = _best_bid_for_outcome(slot, outcome)
            trigger_ask = _best_ask_for_outcome(slot, outcome)
            _log(
                slot.tag,
                f"[{label}] trigger buy_outcome={outcome} "
                f"bid={trigger_bid:.4f} ask={trigger_ask:.4f} "
                f"limit_px={price:.4f} type={order_type}",
                slot=slot,
            )
            _jsonl(
                slot,
                {
                    "event": f"{label}_TRIGGER",
                    "buy_outcome": outcome,
                    "trigger_bid": float(trigger_bid),
                    "trigger_ask": float(trigger_ask),
                    "buy_limit_price": float(price),
                    "order_type": order_type,
                    "shares": float(size),
                },
            )
            placed, order_err = await self._place_limit_buy(
                slot,
                token_id,
                price,
                size,
                outcome=outcome,
                order_type=order_type,
                event_label=label,
            )
            if placed:
                state.done = True
                pos = slot.position
                pos.outcome = outcome.upper()
                pos.entry_label = label
                pos.order_price = float(price)
                pos.target_shares = float(size)
                pos.pending_order = True
                pos.baseline_rem_yes = float(slot.rem_yes)
                pos.baseline_rem_no = float(slot.rem_no)
                pos.trades_seen = len(slot.user_store.trades) if slot.user_store is not None else 0
                if slot.refresh_balances is not None:
                    await slot.refresh_balances()
                self._ingest_buy_fills(slot)
                _log(slot.tag, f"[{label}_ORDER] outcome={outcome} sz={size:.4f} awaiting fill", slot=slot)
                _jsonl(
                    slot,
                    {
                        "event": f"{label}_ORDER",
                        "buy_outcome": outcome,
                        "price": float(price),
                        "size": float(size),
                        "order_type": order_type,
                    },
                )
            elif _is_fak_no_match_error(order_err):
                _log(slot.tag, f"[{label}_FAK_NO_MATCH] will retry when conditions hold", slot=slot)
                _jsonl(slot, {"event": f"{label}_FAK_NO_MATCH", "buy_outcome": outcome, "error": order_err})
            else:
                state.done = True
                _log(slot.tag, f"[{label}_ABORT] reason=order_rejected err={order_err}", slot=slot)
                _jsonl(
                    slot,
                    {
                        "event": f"{label}_ABORT",
                        "buy_outcome": outcome,
                        "reason": "order_rejected",
                        "error": order_err,
                    },
                )
        finally:
            state.busy = False
            slot.active_task = None

    async def _place_limit_buy(
        self,
        slot: MarketSlot,
        token_id: str,
        price: float,
        shares: float,
        *,
        outcome: str,
        order_type: str,
        event_label: str,
    ) -> tuple[bool, str]:
        if shares <= 0:
            return False, "invalid_shares"
        size = round(float(shares), 4)
        if size <= 0:
            return False, "invalid_shares"
        label = str(event_label or "BUY").upper()
        _log(
            slot.tag,
            f"[{label}] limit BUY px={price:.4f} sz={size:.4f} type={order_type}",
            slot=slot,
        )
        best_bid = _best_bid_for_outcome(slot, outcome)
        best_ask = _best_ask_for_outcome(slot, outcome)
        if slot.paper_account is not None:
            ok, meta = slot.paper_account.place_limit_order(
                token_id,
                "BUY",
                price,
                size,
                book_bid=best_bid if best_bid > 0 else None,
                book_ask=best_ask if best_ask > 0 else None,
                order_type=order_type,
            )
            if ok:
                return True, ""
            return False, str((meta or {}).get("reason") or "paper_order_rejected")
        client = slot.clob_client
        if client is None:
            return False, "no_clob_client"
        try:
            neg_risk = await client.get_neg_risk(token_id)
            tick_size = await client.get_tick_size(token_id)
            signed = client.create_order(
                token_id,
                "BUY",
                price,
                size,
                neg_risk=neg_risk,
                tick_size=tick_size,
            )
            resp = await client.post_order(signed, order_type)
            if isinstance(resp, dict) and resp.get("error"):
                err = str(resp.get("error") or "order_error")
                _log(slot.tag, f"[{label}] order error: {err}", slot=slot)
                return False, err
            return True, ""
        except Exception as e:
            err = str(e).strip() or type(e).__name__
            _log(slot.tag, f"[{label}] order exception: {err}", slot=slot)
            return False, err

    async def _place_limit_sell(
        self,
        slot: MarketSlot,
        outcome: str,
        price: float,
        shares: float,
        *,
        event_label: str,
        attempt: int,
    ) -> bool:
        if shares <= self._epsilon:
            return True
        size = round(float(shares), 4)
        if size <= 0:
            return False
        token_id = _outcome_token_id(slot, outcome)
        label = str(event_label or "SELL").upper()
        _log(
            slot.tag,
            f"[{label}] limit SELL px={price:.4f} sz={size:.4f} attempt={attempt}",
            slot=slot,
        )
        best_bid = _best_bid_for_outcome(slot, outcome)
        _jsonl(
            slot,
            {
                "event": f"{label}_ORDER",
                "sell_outcome": outcome,
                "token_id": token_id,
                "price": price,
                "size": size,
                "attempt": attempt,
                "best_bid": best_bid,
                "rem_yes": slot.rem_yes,
                "rem_no": slot.rem_no,
            },
        )
        if slot.paper_account is not None:
            ok, _meta = slot.paper_account.place_limit_order(
                token_id,
                "SELL",
                price,
                size,
                book_bid=best_bid if best_bid > 0 else price,
                order_type="GTC",
            )
            return bool(ok)
        client = slot.clob_client
        if client is None:
            return False
        try:
            neg_risk = await client.get_neg_risk(token_id)
            tick_size = await client.get_tick_size(token_id)
            signed = client.create_order(
                token_id,
                "SELL",
                price,
                size,
                neg_risk=neg_risk,
                tick_size=tick_size,
            )
            resp = await client.post_order(signed, "GTC")
            if isinstance(resp, dict) and resp.get("error"):
                return False
            return True
        except Exception:
            return False


MonitorContextCoordinator = EntryStrategyCoordinator
