"""Monitor YES/NO CLOB books until epoch end (REST poll + fixed-interval evaluation)."""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from collections.abc import Callable
from pathlib import Path
from typing import Any

import aiohttp

from polybot5m.config import StrikeSpotContext
from polybot5m.constants import WS_URL
from polybot5m.data.clob_rest import poll_books_into_store
from polybot5m.data.clob_user_ws import (
    ClobUserWebSocket,
    ClobWsCredentials,
    UserWsAuthError,
    credentials_from_clob_client,
    normalize_condition_id,
    probe_user_ws_auth,
)
from polybot5m.data.orderbook import InMemoryOrderbookStore
from polybot5m.data.user_channel_store import UserChannelStore
from polybot5m.trading_process_log import (
    TradingCycleJournal,
    append_trading_jsonl,
    enrich_strategy_row_t_minus,
    format_average_spot_minus_for_log,
    format_spot_minus_strike_for_log,
    format_t_minus_suffix,
    spot_minus_strike_usd,
    SpotMinusStrikeEpochAverage,
    utc_iso_z,
)

from polybot5m.data.chainlink_feed import run_chainlink_spot_loop
from polybot5m.data.orderbook_influence import pair_depth_metrics_for_monitor
from polybot5m.execution.exit_strategy import EntryStrategyCoordinator
from polybot5m.data.strike_price import fetch_epoch_strike
# Printed once per successful order-book poll so multi-market logs are easier to scan.
MONITOR_LOG_SEPARATOR = "-----------------------------------------------------------"
# Multi-market merged wave (see MonitorWavePrintGate).
MONITOR_WAVE_SEP_OUTER = "-" * 62
MONITOR_WAVE_SEP_INNER = "-" * 23
MONITOR_WAVE_SEP_CLOSE = "-" * 66


def _clob_order_id_from_response(resp: Any) -> str | None:
    if not isinstance(resp, dict):
        return None
    for key in ("orderID", "orderId", "order_id", "id"):
        raw = resp.get(key)
        if raw is None:
            continue
        s = str(raw).strip()
        if s:
            return s
    return None

# Wave print order (btc + eth only).
CANONICAL_MONITOR_LOG_SYMBOL_ORDER = ("btc", "eth")

# Strike / spot columns in STRIKE_SPOT and strike_spot init lines.
STRIKE_SPOT_LOG_DECIMALS = 6


def _fmt_strike_spot_price(v: float | None) -> str:
    """Format a positive strike or spot for logs; missing or non-positive → em dash."""
    if v is None:
        return "—"
    try:
        x = float(v)
    except (TypeError, ValueError):
        return "—"
    if not (x > 0):
        return "—"
    return f"{x:.{STRIKE_SPOT_LOG_DECIMALS}f}"


def monitor_bundle_print_order(symbols_present: set[str]) -> tuple[str, ...]:
    s = {str(x).lower().strip() for x in symbols_present if x and str(x).strip()}
    ordered = tuple(sym for sym in CANONICAL_MONITOR_LOG_SYMBOL_ORDER if sym in s)
    rest = tuple(sorted(s.difference(ordered)))
    return ordered + rest


@dataclass
class MonitorWavePart:
    """One symbol’s contribution to a merged monitor stdout wave."""

    symbol: str = ""
    t_minus_s: float = 0.0
    max_spot_minus_strike_btc: float | None = None
    min_spot_minus_strike_btc: float | None = None
    strike_spot_line: str | None = None
    influence_value: float | None = None
    status_line: str | None = None
    market_line: str = ""


class MonitorWavePrintGate:
    """Collect one row per active symbol per poll; print a single merged block (btc oracle + all bids)."""

    def __init__(
        self,
        symbols_present: set[str],
        *,
        wave_collect_timeout_s: float = 3.0,
    ):
        self._wave_collect_timeout_s = max(0.1, float(wave_collect_timeout_s))
        self._order = monitor_bundle_print_order(symbols_present)
        self._queues: dict[str, asyncio.Queue] = {s: asyncio.Queue(maxsize=2) for s in self._order}
        self._active: set[str] = set(self._order)
        self._lock = asyncio.Lock()
        self._writer_task: asyncio.Task[None] | None = None
        self._closed = asyncio.Event()

    def start(self) -> None:
        if self._writer_task is None:
            self._writer_task = asyncio.create_task(self._writer_loop())

    async def shutdown(self) -> None:
        self._closed.set()
        for s in self._order:
            q = self._queues[s]
            try:
                q.put_nowait(None)
            except asyncio.QueueFull:
                try:
                    _ = q.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    q.put_nowait(None)
                except asyncio.QueueFull:
                    pass
        if self._writer_task is not None:
            try:
                await asyncio.wait_for(self._writer_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self._writer_task.cancel()

    async def deactivate(self, sym: str) -> None:
        sym = str(sym).lower().strip()
        async with self._lock:
            self._active.discard(sym)
        q = self._queues.get(sym)
        if q is None:
            return
        dummy = MonitorWavePart(t_minus_s=0.0, market_line="")
        try:
            q.put_nowait(dummy)
        except asyncio.QueueFull:
            try:
                _ = q.get_nowait()
            except asyncio.QueueEmpty:
                pass
            try:
                q.put_nowait(dummy)
            except asyncio.QueueFull:
                pass

    async def submit_wave(self, sym: str, part: MonitorWavePart) -> None:
        sym = str(sym).lower().strip()
        q = self._queues.get(sym)
        if q is None:
            return
        await q.put(part)

    def _print_merged_wave(self, parts: dict[str, MonitorWavePart]) -> None:
        print(MONITOR_WAVE_SEP_OUTER)
        t0 = 0.0
        for s in self._order:
            p = parts.get(s)
            if p is not None:
                t0 = float(p.t_minus_s)
                break
        print(f"⏰t_minus={t0:.3f}s")
        for s in self._order:
            p = parts.get(s)
            if p is not None and p.strike_spot_line:
                print(p.strike_spot_line)
        print(MONITOR_WAVE_SEP_INNER)
        for s in self._order:
            p = parts.get(s)
            if p is not None and p.market_line:
                print(p.market_line)
        print(MONITOR_WAVE_SEP_CLOSE)

    async def _writer_loop(self) -> None:
        while True:
            if self._closed.is_set() and all(self._queues[s].empty() for s in self._order):
                break
            async with self._lock:
                wave_syms = [s for s in self._order if s in self._active]
            if not wave_syms:
                await asyncio.sleep(0.02)
                continue
            parts: dict[str, MonitorWavePart] = {}
            for s in wave_syms:
                try:
                    item = await asyncio.wait_for(
                        self._queues[s].get(),
                        timeout=self._wave_collect_timeout_s,
                    )
                except asyncio.TimeoutError:
                    break
                if item is None:
                    if self._closed.is_set():
                        return
                    continue
                parts[s] = item
            if len(parts) < len(wave_syms):
                if not parts:
                    continue
                for s in wave_syms:
                    if s in parts:
                        continue
                    parts[s] = MonitorWavePart(
                        symbol=s,
                        t_minus_s=float(parts[next(iter(parts))].t_minus_s),
                        market_line=(
                            f"  [{s}/5m] [MARKET_TICK] — (no tick within "
                            f"{self._wave_collect_timeout_s:.1f}s)"
                        ),
                    )
            if parts:
                self._print_merged_wave(parts)


def _best_bid_from_book(book: Any) -> float:
    """Extract best (highest) bid price from order book. Returns 0.0 if empty."""
    if not book:
        return 0.0
    bids = getattr(book, "bids", None)
    if not bids or len(bids) == 0:
        return 0.0
    prices: list[float] = []
    for b in bids:
        p = getattr(b, "price", None) or (b.get("price") if isinstance(b, dict) else None)
        if p is not None and 0 < float(p) <= 1:
            prices.append(float(p))
    return max(prices) if prices else 0.0


def _best_ask_from_book(book: Any) -> float:
    """Extract best (lowest) ask price from order book. Returns 0.0 if empty."""
    if not book:
        return 0.0
    asks = getattr(book, "asks", None)
    if not asks or len(asks) == 0:
        return 0.0
    prices: list[float] = []
    for a in asks:
        p = getattr(a, "price", None) or (a.get("price") if isinstance(a, dict) else None)
        if p is not None and 0 < float(p) <= 1:
            prices.append(float(p))
    return min(prices) if prices else 0.0


def _format_yes_no_book_prices(
    best_ask_yes: float,
    best_bid_yes: float,
    best_ask_no: float,
    best_bid_no: float,
) -> str:
    """YES/NO top-of-book for monitor stdout (ask + bid on one line)."""
    return (
        f"🟢YES best_ask={best_ask_yes:.4f} best_bid={best_bid_yes:.4f} "
        f"🔴NO best_ask={best_ask_no:.4f} best_bid={best_bid_no:.4f}"
    )


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _rem_inventory_suffix(
    rem_yes: float,
    rem_no: float,
    *,
    show_inventory: bool,
) -> str:
    """Format remaining YES/NO shares for monitor logs."""
    if not show_inventory:
        return ""
    return f" rem_YES={float(rem_yes):.4f} rem_NO={float(rem_no):.4f}"


def _rem_inventory_market_tick_suffix(
    symbol: str,
    rem_yes: float,
    rem_no: float,
    *,
    show_inventory: bool,
) -> str:
    """Per-asset suffix for wave [MARKET_TICK], e.g. 🟢rem_YES_btc=1.0000 🔴rem_NO_btc=0.0000."""
    sym = symbol.lower().strip()
    if not sym or not show_inventory:
        return ""
    return f" 🟢rem_YES_{sym}={float(rem_yes):.4f} 🔴rem_NO_{sym}={float(rem_no):.4f}"


def _chainlink_feed_id_for_symbol(feed_ids: dict[str, str], symbol: str) -> str | None:
    k = symbol.lower().strip()
    v = feed_ids.get(k)
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _spot_for_strike_compare(raw: float | None) -> float | None:
    """Raw Chainlink spot → value used for spot_minus_strike and sell logic."""
    if raw is None or raw <= 0:
        return None
    return float(raw)


@dataclass
class MonitorBookBundle:
    book_store: InMemoryOrderbookStore
    user_store: UserChannelStore | None = None


def resolve_clob_ws_auth(
    *,
    api_key: str = "",
    api_secret: str = "",
    api_passphrase: str = "",
    clob_client: Any | None = None,
) -> ClobWsCredentials | None:
    """CLOB API creds for user-channel WebSocket — must match the signing wallet."""
    from_client = credentials_from_clob_client(clob_client)
    if from_client is not None:
        return from_client
    key = (api_key or "").strip()
    secret = (api_secret or "").strip()
    passphrase = (api_passphrase or "").strip()
    auth = ClobWsCredentials(api_key=key, api_secret=secret, api_passphrase=passphrase)
    return auth if auth.valid() else None


def _log_user_trade_event(
    *,
    tag: str,
    trading_process_path: Path | None,
    trading_journal: TradingCycleJournal | None,
    user_store: UserChannelStore,
    data: dict[str, Any],
    remaining_s: float | None = None,
) -> None:
    row = user_store.trade_row_for_log(data)
    row["tag"] = tag
    row = enrich_strategy_row_t_minus(row, remaining_s=remaining_s)
    if trading_journal is not None:
        trading_journal.log_user_trade(row)
    elif trading_process_path is not None:
        row["ts_utc"] = utc_iso_z()
        append_trading_jsonl(trading_process_path, row)
    print(
        f"  {tag} [USER_TRADE] {row.get('outcome')} {row.get('side')} "
        f"px={row.get('price')} sz={row.get('size')} "
        f"status={row.get('status')} trader_side={row.get('trader_side')}"
        f"{format_t_minus_suffix(remaining_s)}",
    )


@contextlib.asynccontextmanager
async def clob_monitor_session(
    *,
    clob_ws_url: str | None,
    yes_token_id: str,
    no_token_id: str,
    condition_id: str = "",
    user_ws_auth: ClobWsCredentials | None = None,
    user_ws_enabled: bool = True,
    trading_process_path: Path | None = None,
    trading_journal: TradingCycleJournal | None = None,
    tag: str = "",
    remaining_s_fn: Callable[[], float] | None = None,
):
    """Book store for REST polling; optional authenticated user WS for fills on this condition."""
    _ws = (clob_ws_url or "").strip() or WS_URL
    store = InMemoryOrderbookStore()
    user_store: UserChannelStore | None = None
    user_client: ClobUserWebSocket | None = None
    cid = normalize_condition_id(condition_id)

    try:
        if user_ws_enabled and user_ws_auth is not None and user_ws_auth.valid() and cid:
            user_store = UserChannelStore(
                condition_id=cid,
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
            )

            async def _on_trade(data: dict[str, Any]) -> None:
                user_store.apply_trade(data)
                rem = remaining_s_fn() if remaining_s_fn is not None else None
                _log_user_trade_event(
                    tag=tag,
                    trading_process_path=trading_process_path,
                    trading_journal=trading_journal,
                    user_store=user_store,
                    data=data,
                    remaining_s=rem,
                )

            user_client = ClobUserWebSocket(_ws, auth=user_ws_auth, on_trade=_on_trade)
            try:
                await user_client.connect([cid])
                print(f"  {tag} MONITOR user WS {_ws}/ws/user condition={cid[:24]}...")
            except UserWsAuthError as e:
                print(f"  {tag} MONITOR user WS auth failed: {e}")
                user_store = None
                user_client = None

        yield MonitorBookBundle(book_store=store, user_store=user_store)
    finally:
        if user_client is not None:
            await user_client.disconnect()


async def post_redeem_monitor_orderbooks(
    clob_base_url: str,
    yes_token_id: str,
    no_token_id: str,
    duration_s: float,
    tag: str,
    poll_interval_s: float,
) -> None:
    """After redeem: poll public order books for `duration_s` seconds."""
    if duration_s <= 0:
        return
    from polybot5m.data.clob_rest import fetch_order_book

    end = time.monotonic() + duration_s
    print(f"  {tag} POST_REDEEM monitor {duration_s:g}s")
    while time.monotonic() < end:
        try:
            book_yes, book_no = await asyncio.gather(
                fetch_order_book(yes_token_id, clob_base_url),
                fetch_order_book(no_token_id, clob_base_url),
            )
            ay = _best_ask_from_book(book_yes)
            an = _best_ask_from_book(book_no)
            line = f"  {tag} [POST_REDEEM] 🟢YES best_ask={ay:.4f} 🔴NO best_ask={an:.4f} sum={ay + an:.4f}"
            print(MONITOR_LOG_SEPARATOR)
            print(line)
        except Exception as e:
            print(f"  {tag} [POST_REDEEM] book error: {e}")
        await asyncio.sleep(poll_interval_s)


async def monitor_orderbook_until_epoch_end(
    clob_base_url: str,
    yes_token_id: str,
    no_token_id: str,
    epoch_end: datetime,
    tag: str = "",
    poll_interval_s: float = 0.5,
    market_log_interval_s: float = 1.0,
    monitor_verbose_seconds_before_end: float = 5.0,
    *,
    strike_spot_feed: StrikeSpotContext | None = None,
    log_strike_spot_interval_s: float = 0.0,
    run_strike_spot_oracle: bool = False,
    trading_process_path: Path | None = None,
    trading_journal: TradingCycleJournal | None = None,
    trading_process_log_mode: str = "trades",
    trading_process_log_interval_s: float = 0.0,
    trading_process_log_stdout: bool = False,
    monitor_wave_gate: MonitorWavePrintGate | None = None,
    monitor_gate_symbol: str = "",
    spot_minus_strike_difference_rate_lookback_s: float = 0.0,
    clob_client: Any | None = None,
    paper_account: Any | None = None,
    balance_poll_interval_s: float | None = None,
    condition_id: str = "",
    split_inventory_yes: float = 0.0,
    split_inventory_no: float = 0.0,
    balance_rpc_url: str = "",
    balance_wallet_address: str = "",
    clob_ws_url: str | None = None,
    clob_api_key: str = "",
    clob_api_secret: str = "",
    clob_api_passphrase: str = "",
    monitor_user_ws_enabled: bool = True,
    monitor_context: EntryStrategyCoordinator | None = None,
    entry_symbol: str = "",
    rest_book_timeout_s: float = 3.0,
    balance_refresh_timeout_s: float = 3.0,
    balance_force_refresh_min_s: float = 1.0,
) -> dict[str, Any]:
    """
    Poll YES/NO CLOB books via REST until `epoch_end` (UTC).
    Trading logic runs every ``poll_interval_s`` using the latest polled snapshot.
    """
    _tp_mode = str(trading_process_log_mode or "trades").strip().lower()
    if _tp_mode not in ("full", "trades"):
        _tp_mode = "trades"
    _tp_trades_only = _tp_mode == "trades"

    last_market_log = 0.0
    rem_yes = max(0.0, float(split_inventory_yes))
    rem_no = max(0.0, float(split_inventory_no))
    rem_state: dict[str, float] = {"yes": rem_yes, "no": rem_no}

    og_line = ""
    if strike_spot_feed is not None:
        og_line = (
            f"; strike_feed={strike_spot_feed.strike_provider}+spot={strike_spot_feed.spot_provider} "
            f"({strike_spot_feed.symbol.upper()}-USD)"
        )
    user_auth = resolve_clob_ws_auth(
        api_key=clob_api_key,
        api_secret=clob_api_secret,
        api_passphrase=clob_api_passphrase,
        clob_client=clob_client,
    )
    user_ws_line = ""
    if monitor_user_ws_enabled and user_auth and (condition_id or "").strip():
        user_ws_line = f" + user WS (api_key={user_auth.api_key[:8]}...)"
    elif monitor_user_ws_enabled and not user_auth:
        user_ws_line = " (user WS off: no valid CLOB API_KEY/SECRET/PASSPHRASE)"
    print(
        f"  {tag} MONITOR (REST /book every {poll_interval_s:g}s{user_ws_line}{og_line}) until epoch end",
    )

    sms_epoch_avg = SpotMinusStrikeEpochAverage()

    last_tp_mono = 0.0

    stop_oracle: asyncio.Event | None = None
    oracle_task: asyncio.Task | None = None
    price_store: dict[str, float] = {}
    strike = 0.0
    product_id: str | None = None
    last_strike_spot_log = 0.0
    spot_log_key = "spot"
    need_oracle = strike_spot_feed is not None and (
        log_strike_spot_interval_s > 0 or run_strike_spot_oracle
    )
    if need_oracle:
        stop_oracle = asyncio.Event()
        product_id = f"{strike_spot_feed.symbol.upper()}-USD"
        fid = _chainlink_feed_id_for_symbol(
            dict(strike_spot_feed.chainlink_feed_ids),
            strike_spot_feed.symbol,
        )
        use_chainlink_spot = (
            bool(strike_spot_feed.chainlink_user_id)
            and bool(strike_spot_feed.chainlink_secret)
            and fid is not None
        )
        spot_log_key = "chainlink_spot"
        if use_chainlink_spot:
            oracle_task = asyncio.create_task(
                run_chainlink_spot_loop(
                    strike_spot_feed.symbol,
                    fid,
                    strike_spot_feed.chainlink_user_id,
                    strike_spot_feed.chainlink_secret,
                    product_id,
                    price_store,
                    stop_oracle,
                    strike_spot_feed.chainlink_spot_poll_interval_s,
                ),
            )
        else:
            print(
                f"  {tag} spot: chainlink missing user/secret or feed_id for "
                f"{strike_spot_feed.symbol}",
            )
        for _ in range(50):
            if price_store.get(product_id):
                break
            await asyncio.sleep(0.1)
        strike = await fetch_epoch_strike(
            strike_spot_feed.symbol.lower(),
            strike_spot_feed.epoch_start_unix,
            price_store,
            strike_spot_feed.strike_provider,
            strike_spot_feed.interval_secs,
            strike_spot_feed.chainlink_user_id,
            strike_spot_feed.chainlink_secret,
            dict(strike_spot_feed.chainlink_feed_ids),
            market_slug=strike_spot_feed.market_slug or "",
        )
        spot0 = price_store.get(product_id)
        sk = _fmt_strike_spot_price(float(strike)) if strike and strike > 0 else "—"
        if spot0 and spot0 > 0:
            sp = _fmt_strike_spot_price(spot0)
        else:
            sp = "—"
        _sep_top = MONITOR_WAVE_SEP_OUTER if monitor_wave_gate else MONITOR_LOG_SEPARATOR
        print(_sep_top)
        print(
            f"  {tag} strike_spot init target={sk} {spot_log_key}={sp} provider={strike_spot_feed.strike_provider}",
        )

    _bal_poll_cfg = float(balance_poll_interval_s) if balance_poll_interval_s is not None else 0.0
    balance_poll_iv = max(0.05, _bal_poll_cfg) if _bal_poll_cfg > 0 else 0.0
    show_rem_inventory = bool(
        paper_account is not None
        or (clob_client is not None and balance_poll_iv > 0)
        or rem_yes > 0
        or rem_no > 0
    )
    last_balance_poll_mono = 0.0

    _balance_log_once = True

    async def _refresh_rem_balances() -> None:
        nonlocal rem_yes, rem_no, _balance_log_once
        if paper_account is not None:
            paper_account.advance_monitor_tick(poll_interval_s=poll_interval_s)
            paper_account.settle_pending()
            ry, rn = paper_account.balances()
            rem_yes, rem_no = float(ry), float(rn)
        elif clob_client is not None:
            bal = await clob_client.fetch_conditional_outcome_balances_shares(
                yes_token_id,
                no_token_id,
                min_poll_s=balance_poll_iv,
                sync=False,
                condition_id=condition_id,
                log_tag=tag.strip() if _balance_log_once else "",
                rpc_url=balance_rpc_url or None,
                wallet_address=balance_wallet_address or None,
            )
            _balance_log_once = False
            if bal is not None:
                rem_yes, rem_no = float(bal[0]), float(bal[1])
        rem_state["yes"] = rem_yes
        rem_state["no"] = rem_no

    if show_rem_inventory:
        await _refresh_rem_balances()
        last_balance_poll_mono = time.monotonic()

    rest_timeout_s = max(0.5, float(rest_book_timeout_s or 3.0))
    bal_refresh_timeout_s = max(0.5, float(balance_refresh_timeout_s or 3.0))
    bal_force_min_s = max(0.0, float(balance_force_refresh_min_s or 1.0))
    last_force_balance_mono = 0.0
    _balance_timeout_logged = False

    try:
        async with clob_monitor_session(
            clob_ws_url=clob_ws_url,
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            condition_id=condition_id,
            user_ws_auth=user_auth,
            user_ws_enabled=monitor_user_ws_enabled,
            trading_process_path=trading_process_path,
            trading_journal=trading_journal,
            tag=tag,
            remaining_s_fn=lambda: max(0.0, (epoch_end - _utc_now()).total_seconds()),
        ) as monitor_bundle:
            book_store = monitor_bundle.book_store
            user_store = monitor_bundle.user_store
            async with aiohttp.ClientSession() as rest_session:
                _entry_sym = str(entry_symbol or monitor_gate_symbol).lower().strip()
                if monitor_context is not None and _entry_sym:

                    async def _entry_refresh_balances() -> None:
                        await _refresh_rem_balances()
                        sl = monitor_context._slots.get(_entry_sym)
                        if sl is not None:
                            sl.rem_yes = rem_yes
                            sl.rem_no = rem_no

                    monitor_context.register_market(
                        _entry_sym,
                        tag=tag,
                        yes_token_id=yes_token_id,
                        no_token_id=no_token_id,
                        condition_id=condition_id,
                        clob_client=clob_client,
                        paper_account=paper_account,
                        user_store=user_store,
                        refresh_balances=_entry_refresh_balances,
                        trading_journal=trading_journal,
                    )

                while _utc_now() < epoch_end:
                    remaining_s = max(0.0, (epoch_end - _utc_now()).total_seconds())
                    now_mono = time.monotonic()

                    if not await poll_books_into_store(
                        rest_session,
                        book_store,
                        yes_token_id,
                        no_token_id,
                        base_url=clob_base_url,
                        timeout_s=rest_timeout_s,
                    ):
                        await asyncio.sleep(poll_interval_s)
                        continue

                    book_yes = book_store.book_as_executor_view(yes_token_id)
                    book_no = book_store.book_as_executor_view(no_token_id)

                    best_bid_yes = _best_bid_from_book(book_yes)
                    best_bid_no = _best_bid_from_book(book_no)
                    best_ask_yes = _best_ask_from_book(book_yes)
                    best_ask_no = _best_ask_from_book(book_no)
                    depth_metrics = pair_depth_metrics_for_monitor(book_yes, book_no, top_n=5)
    
                    spot_this_raw: float | None = None
                    if strike_spot_feed is not None and product_id is not None:
                        spot_this_raw = price_store.get(product_id)
        
                    tick_strike = float(strike) if strike and strike > 0 else None
                    tick_spot = _spot_for_strike_compare(spot_this_raw)
                    sms_value_tick = spot_minus_strike_usd(tick_strike, tick_spot)
                    sym_for_sms = monitor_gate_symbol or (
                        str(strike_spot_feed.symbol).lower().strip() if strike_spot_feed else ""
                    )
                    epoch_avg_value = sms_epoch_avg.record(sms_value_tick)
                    avg_suffix = format_average_spot_minus_for_log(sym_for_sms, epoch_avg_value)

                    ir = float(depth_metrics["influence_rate"])
                    yir = float(depth_metrics["yes_influence_rate"])
                    nir = float(depth_metrics["no_influence_rate"])
                    tp_line_for_stdout: str | None = None
                    if not _tp_trades_only and trading_process_path is not None:
                        _inf_row: dict[str, Any] = {
                            "event": "INFLUENCE_TICK",
                            "ts_utc": utc_iso_z(),
                            "tag": tag,
                            "t_minus_s": remaining_s,
                            **depth_metrics,
                        }
                        if monitor_context is not None:
                            mx, mn = monitor_context.btc_wave_extrema()
                            if mx is not None:
                                _inf_row["max_spot_minus_strike_btc"] = round(float(mx), 4)
                            if mn is not None:
                                _inf_row["min_spot_minus_strike_btc"] = round(float(mn), 4)
                        append_trading_jsonl(trading_process_path, _inf_row)
                        now_tp = time.monotonic()
                        if trading_process_log_interval_s <= 0 or (
                            now_tp - last_tp_mono >= trading_process_log_interval_s
                        ):
                            last_tp_mono = now_tp
                            _tick_row: dict[str, Any] = {
                                "event": "MONITOR_TICK",
                                "ts_utc": utc_iso_z(),
                                "tag": tag,
                                "bid_yes": best_bid_yes,
                                "bid_no": best_bid_no,
                                "sum_bids": best_bid_yes + best_bid_no,
                                "t_minus_s": remaining_s,
                                "rem_yes": rem_yes,
                                "rem_no": rem_no,
                                "strike": tick_strike,
                                "spot": tick_spot,
                                "spot_minus_strike": spot_minus_strike_usd(tick_strike, tick_spot),
                                **depth_metrics,
                            }

                            if monitor_context is not None:
                                mx, mn = monitor_context.btc_wave_extrema()
                                if mx is not None:
                                    _tick_row["max_spot_minus_strike_btc"] = round(float(mx), 4)
                                if mn is not None:
                                    _tick_row["min_spot_minus_strike_btc"] = round(float(mn), 4)
                            append_trading_jsonl(trading_process_path, _tick_row)
                            if trading_process_log_stdout and not monitor_wave_gate:
                                sms = format_spot_minus_strike_for_log(tick_strike, tick_spot)
                                tp_line_for_stdout = (
                                    f"  {tag} [TRADING_PROCESS] t={remaining_s:.1f}s YES={best_bid_yes:.4f} "
                                    f"NO={best_bid_no:.4f} sum={best_bid_yes + best_bid_no:.4f} "
                                    f"inf={ir:.4f} yes_ir={yir:.4f} no_ir={nir:.4f}{sms}"
                                )
    
                    rem_suffix = _rem_inventory_suffix(
                        rem_yes,
                        rem_no,
                        show_inventory=show_rem_inventory,
                    )
                    if paper_account is not None and paper_account._session.pending_count() > 0:
                        rem_suffix += f" paper_pending={paper_account._session.pending_count()}"
                    rem_tick_sym_suffix = _rem_inventory_market_tick_suffix(
                        monitor_gate_symbol,
                        rem_yes,
                        rem_no,
                        show_inventory=show_rem_inventory,
                    )
        
                    in_verbose = (
                        monitor_verbose_seconds_before_end > 0
                        and 0 < remaining_s <= monitor_verbose_seconds_before_end
                    )
        
                    if monitor_wave_gate and monitor_gate_symbol:
                        strike_line: str | None = None
                        influence_value: float | None = None
                        if strike_spot_feed is not None and product_id is not None and log_strike_spot_interval_s > 0:
                            spot_cur = price_store.get(product_id)
                            spot_cur_cal = _spot_for_strike_compare(spot_cur)
                            ts = _fmt_strike_spot_price(float(strike)) if strike and strike > 0 else "—"
                            if spot_cur and spot_cur > 0:
                                ss = _fmt_strike_spot_price(spot_cur)
                            else:
                                ss = "—"
                            sms_value = spot_minus_strike_usd(
                                strike if strike and strike > 0 else None,
                                spot_cur_cal,
                            )
                            sms = "—" if sms_value is None else f"{float(sms_value):.6f}"
                            strike_line = (
                                f"{tag} [STRIKE_SPOT] target={ts} {spot_log_key}={ss} "
                                f"spot_minus_strike_{monitor_gate_symbol}={sms}{avg_suffix}"
                            )
                        influence_value = ir
        
                        market_line = ""
                        book_px = _format_yes_no_book_prices(
                            best_ask_yes, best_bid_yes, best_ask_no, best_bid_no
                        )
                        if in_verbose:
                            market_line = f"  {tag} [MARKET_TICK] {book_px}{rem_tick_sym_suffix}"
                        elif now_mono - last_market_log >= market_log_interval_s:
                            market_line = f"  {tag} [MARKET_TICK] {book_px}{rem_tick_sym_suffix}"
                            last_market_log = now_mono
                        elif rem_suffix:
                            market_line = (
                                f"  {tag} [REM]{rem_suffix} ⏰t_minus={remaining_s:.2f}s"
                            )
        
                        max_sms: float | None = None
                        min_sms: float | None = None
                        if monitor_gate_symbol == "btc" and monitor_context is not None:
                            monitor_context.update_btc_strategy_context(
                                spot_minus_strike_btc=sms_value_tick,
                                averge_spot_minus_btc=epoch_avg_value,
                                best_bid_yes=best_bid_yes,
                                best_bid_no=best_bid_no,
                                best_ask_yes=best_ask_yes,
                                best_ask_no=best_ask_no,
                            )
                            max_sms, min_sms = monitor_context.btc_wave_extrema()
                        await monitor_wave_gate.submit_wave(
                            monitor_gate_symbol,
                            MonitorWavePart(
                                symbol=monitor_gate_symbol,
                                t_minus_s=remaining_s,
                                max_spot_minus_strike_btc=max_sms,
                                min_spot_minus_strike_btc=min_sms,
                                strike_spot_line=strike_line,
                                influence_value=influence_value,
                                status_line=None,
                                market_line=market_line,
                            ),
                        )
                    else:
                        tick_lines: list[str] = [MONITOR_LOG_SEPARATOR]
                        if strike_spot_feed is not None and product_id is not None and log_strike_spot_interval_s > 0:
                            tick_mono = time.monotonic()
                            if tick_mono - last_strike_spot_log >= log_strike_spot_interval_s:
                                spot_cur = price_store.get(product_id)
                                spot_cur_cal = _spot_for_strike_compare(spot_cur)
                                ts = _fmt_strike_spot_price(float(strike)) if strike and strike > 0 else "—"
                                if spot_cur and spot_cur > 0:
                                    ss = _fmt_strike_spot_price(spot_cur)
                                else:
                                    ss = "—"
                                sms = format_spot_minus_strike_for_log(
                                    strike if strike and strike > 0 else None,
                                    spot_cur_cal,
                                )
                                tick_lines.append(
                                    f"  {tag} [STRIKE_SPOT] target={ts} {spot_log_key}={ss}{sms}{avg_suffix}",
                                )
                                last_strike_spot_log = tick_mono
        
                        tick_lines.append(
                            f"  {tag} [INFLUENCE] ⏰t={remaining_s:.2f}s inf={ir:.6f} yes_ir={yir:.6f} no_ir={nir:.6f}",
                        )
                        if tp_line_for_stdout:
                            tick_lines.append(tp_line_for_stdout)
                        if in_verbose:
                            tick_extra = ""
                            if strike_spot_feed is not None and product_id is not None:
                                ts = _fmt_strike_spot_price(float(strike)) if strike and strike > 0 else "—"
                                sp_used = _fmt_strike_spot_price(tick_spot) if tick_spot is not None else "—"
                                tick_extra = f" strike={ts} spot={sp_used}"
                                tick_extra += format_spot_minus_strike_for_log(tick_strike, tick_spot)
                            book_px = _format_yes_no_book_prices(
                                best_ask_yes, best_bid_yes, best_ask_no, best_bid_no
                            )
                            tick_lines.append(
                                f"  {tag} [MARKET_TICK] {book_px} ⏰t_minus={remaining_s:.2f}s{tick_extra}"
                                f"{rem_suffix}",
                            )
                        elif now_mono - last_market_log >= market_log_interval_s:
                            sms_m = format_spot_minus_strike_for_log(tick_strike, tick_spot)
                            book_px = _format_yes_no_book_prices(
                                best_ask_yes, best_bid_yes, best_ask_no, best_bid_no
                            )
                            tick_lines.append(
                                f"  {tag} [MARKET] {book_px} ⏰t_minus={remaining_s:.1f}s{rem_suffix}{sms_m}",
                            )
                            last_market_log = now_mono
                        elif rem_suffix:
                            tick_lines.append(
                                f"  {tag} [REM]{rem_suffix} ⏰t_minus={remaining_s:.2f}s",
                            )
                        for ln in tick_lines:
                            print(ln)
    
                    if show_rem_inventory:
                        force_bal = False
                        if user_store is not None and user_store.consume_balance_refresh():
                            if (now_mono - last_force_balance_mono) >= bal_force_min_s:
                                force_bal = True
                                last_force_balance_mono = now_mono
                        if balance_poll_iv > 0 and (
                            force_bal or now_mono - last_balance_poll_mono >= balance_poll_iv
                        ):
                            try:
                                await asyncio.wait_for(
                                    _refresh_rem_balances(),
                                    timeout=bal_refresh_timeout_s,
                                )
                            except asyncio.TimeoutError:
                                if not _balance_timeout_logged:
                                    _balance_timeout_logged = True
                                    print(
                                        f"  {tag} [BALANCE] refresh timed out after "
                                        f"{bal_refresh_timeout_s:g}s (using last rem_*)",
                                        flush=True,
                                    )
                            last_balance_poll_mono = now_mono
    
                    if paper_account is not None:
                        paper_account.advance_monitor_tick(poll_interval_s=poll_interval_s)
                        paper_account.settle_pending()
                        ry, rn = paper_account.balances()
                        rem_yes, rem_no = float(ry), float(rn)
                        rem_state["yes"] = rem_yes
                        rem_state["no"] = rem_no
    
                    if monitor_context is not None and _entry_sym:
                        await monitor_context.on_monitor_tick(
                            _entry_sym,
                            remaining_s=remaining_s,
                            rem_yes=rem_yes,
                            rem_no=rem_no,
                            best_bid_yes=best_bid_yes,
                            best_bid_no=best_bid_no,
                            best_ask_yes=best_ask_yes,
                            best_ask_no=best_ask_no,
                            spot_minus_strike=sms_value_tick,
                            averge_spot_minus_btc=epoch_avg_value if _entry_sym == "btc" else None,
                        )
    
                    await asyncio.sleep(poll_interval_s)
                if monitor_context is not None and _entry_sym:
                    monitor_context.unregister_market(_entry_sym)
    finally:
        if stop_oracle is not None:
            stop_oracle.set()
        if oracle_task is not None:
            try:
                await asyncio.wait_for(oracle_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                oracle_task.cancel()

    print(f"  {tag} [MONITOR_END] epoch end reached")
    if trading_process_path is not None and not _tp_trades_only:
        append_trading_jsonl(
            trading_process_path,
            {"event": "MONITOR_END", "ts_utc": utc_iso_z(), "tag": tag},
        )
    out: dict[str, Any] = {}
    return out
