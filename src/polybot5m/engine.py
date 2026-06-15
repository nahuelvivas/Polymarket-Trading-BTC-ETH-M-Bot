"""Monitor orderbook -> redeem."""

from __future__ import annotations

import asyncio
import enum
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import aiohttp

from polybot5m.config import MarketTarget, Settings, StrikeSpotContext
from polybot5m.constants import INTERVAL_SECONDS, PUSD_ADDRESS
from polybot5m.data.chainlink_feed import run_chainlink_spot_loop
from polybot5m.data.gamma import GammaClient
from polybot5m.data.models import resolve_outcome_token_ids
from polybot5m.data.orderbook_influence import pair_depth_metrics_for_monitor
from polybot5m.data.clob_rest import poll_books_into_store
from polybot5m.data.slug_builder import compute_epoch_slugs
from polybot5m.data.strike_price import fetch_epoch_strike
from polybot5m.execution.executor import (
    MonitorWavePrintGate,
    MonitorWavePart,
    _best_ask_from_book,
    _best_bid_from_book,
    _chainlink_feed_id_for_symbol,
    _fmt_strike_spot_price,
    _spot_for_strike_compare,
    clob_monitor_session,
    monitor_orderbook_until_epoch_end,
    resolve_clob_ws_auth,
    post_redeem_monitor_orderbooks,
)
from polybot5m.execution.collateral_adapter import resolve_collateral_adapter
from polybot5m.execution.deposit_wallet import resolve_deposit_wallet_address
from polybot5m.execution.exit_strategy import EntryStrategyCoordinator
from polybot5m.execution.paper_exchange import PaperSessionLedger, PaperV2Account
from polybot5m.execution.redeem_scheduler import RedeemJob, RedeemScheduler
from polybot5m.trading_process_log import (
    SpotMinusStrikeEpochAverage,
    TradingCycleJournal,
    TradingCycleKey,
    append_trading_jsonl,
    format_average_spot_minus_for_log,
    resolve_trading_process_path,
    spot_minus_strike_usd,
    utc_iso_z,
)


class Phase(enum.Enum):
    IDLE = "IDLE"
    MONITOR = "MONITOR"
    REDEEM = "REDEEM"


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _tag(symbol: str, epoch: str) -> str:
    return f"[{symbol}/{epoch}]"


async def _run_btc_wave_monitor(
    settings: Settings,
    epoch: str,
    epoch_end: datetime,
    monitor_wave_gate: MonitorWavePrintGate,
    *,
    poll_interval_s: float,
    clob_ws_url: str | None = None,
    clob_client: Any | None = None,
    entry_coordinator: EntryStrategyCoordinator | None = None,
) -> None:
    """Sidecar BTC monitor for merged wave logs when BTC is not in active symbols."""
    tag = _tag("btc", epoch)
    lm = settings.liquidity_maker
    exe = settings.execution
    user_auth = resolve_clob_ws_auth(
        api_key=exe.api_key,
        api_secret=exe.api_secret,
        api_passphrase=exe.api_passphrase,
        clob_client=clob_client,
    )
    monitor_user_ws = bool(getattr(lm, "monitor_user_ws_enabled", True))
    rest_timeout_s = max(0.5, float(getattr(lm, "monitor_rest_book_timeout_s", 3.0) or 3.0))
    btc_sms_epoch_avg = SpotMinusStrikeEpochAverage()
    stop_oracle: asyncio.Event | None = None
    oracle_task: asyncio.Task[Any] | None = None
    try:
        slugs = compute_epoch_slugs("btc", epoch)
        async with aiohttp.ClientSession() as session:
            gamma = GammaClient(settings.api.gamma_url, session)
            try:
                event = await gamma.fetch_event_by_slug(slugs.current_slug)
            except ValueError as e:
                print(f"  {tag} BTC wave monitor skipped: {e}")
                return
        asset_ids = event.all_asset_ids()
        if len(asset_ids) != 2:
            print(f"  {tag} BTC wave monitor skipped: invalid asset IDs")
            return
        yes_token_id, no_token_id = asset_ids[0], asset_ids[1]
        btc_condition_id = ""
        if event.markets:
            btc_condition_id = (event.markets[0].condition_id or "").strip()

        pf = settings.price_feed
        strike_spot_feed = StrikeSpotContext(
            symbol="btc",
            epoch_start_unix=int(slugs.current_start.timestamp()),
            interval_secs=INTERVAL_SECONDS.get(epoch, 300),
            strike_provider=pf.provider,
            chainlink_user_id=pf.chainlink.streams_user_id,
            chainlink_secret=pf.chainlink.streams_secret,
            chainlink_feed_ids=dict(pf.chainlink.feed_ids),
            market_slug=slugs.current_slug,
            spot_provider=pf.spot_provider,
            chainlink_spot_poll_interval_s=float(pf.chainlink_spot_poll_interval_s or 1.0),
        )
        price_store: dict[str, float] = {}
        stop_oracle = asyncio.Event()
        product_id = "BTC-USD"
        spot_log_key = "chainlink_spot"
        fid = _chainlink_feed_id_for_symbol(
            dict(strike_spot_feed.chainlink_feed_ids),
            strike_spot_feed.symbol,
        )
        use_chainlink_spot = (
            bool(strike_spot_feed.chainlink_user_id)
            and bool(strike_spot_feed.chainlink_secret)
            and fid is not None
        )
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
            print(f"  {tag} spot: chainlink missing user/secret or feed_id for btc")
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

        async with clob_monitor_session(
            clob_ws_url=clob_ws_url,
            yes_token_id=yes_token_id,
            no_token_id=no_token_id,
            condition_id=btc_condition_id,
            user_ws_auth=user_auth,
            user_ws_enabled=monitor_user_ws and not settings.bot.paper_trading,
            tag=tag,
            remaining_s_fn=lambda: max(0.0, (epoch_end - _utc_now()).total_seconds()),
        ) as monitor_bundle:
            book_store = monitor_bundle.book_store
            async with aiohttp.ClientSession() as rest_session:
                while _utc_now() < epoch_end:
                        remaining_s = max(0.0, (epoch_end - _utc_now()).total_seconds())

                        if not await poll_books_into_store(
                            rest_session,
                            book_store,
                            yes_token_id,
                            no_token_id,
                            base_url=settings.api.clob_url,
                            timeout_s=rest_timeout_s,
                        ):
                            await asyncio.sleep(poll_interval_s)
                            continue

                        book_yes = book_store.book_as_executor_view(yes_token_id)
                        book_no = book_store.book_as_executor_view(no_token_id)
                        depth_metrics = pair_depth_metrics_for_monitor(book_yes, book_no, top_n=5)
                        spot_this_raw = price_store.get(product_id)
                        tick_strike = float(strike) if strike and strike > 0 else None
                        tick_spot = _spot_for_strike_compare(spot_this_raw)
                        ts = _fmt_strike_spot_price(float(strike)) if strike and strike > 0 else "—"
                        if spot_this_raw and spot_this_raw > 0:
                            ss = _fmt_strike_spot_price(spot_this_raw)
                        else:
                            ss = "—"
                        sms_value_btc = spot_minus_strike_usd(tick_strike, tick_spot)
                        sms = "—"
                        if sms_value_btc is not None:
                            sms = f"{float(sms_value_btc):.6f}"
                        avg_suffix = format_average_spot_minus_for_log(
                            "btc",
                            btc_sms_epoch_avg.record(sms_value_btc),
                        )
                        if entry_coordinator is not None:
                            entry_coordinator.update_btc_strategy_context(
                                spot_minus_strike_btc=sms_value_btc,
                                averge_spot_minus_btc=btc_sms_epoch_avg.average(),
                                best_bid_yes=_best_bid_from_book(book_yes),
                                best_bid_no=_best_bid_from_book(book_no),
                                best_ask_yes=_best_ask_from_book(book_yes),
                                best_ask_no=_best_ask_from_book(book_no),
                            )
                        strike_line = (
                            f"{tag} [STRIKE_SPOT] target={ts} {spot_log_key}={ss} "
                            f"spot_minus_strike_btc={sms}{avg_suffix}"
                        )
                        max_sms, min_sms = None, None
                        if entry_coordinator is not None:
                            max_sms, min_sms = entry_coordinator.btc_wave_extrema()
                        await monitor_wave_gate.submit_wave(
                            "btc",
                            MonitorWavePart(
                                symbol="btc",
                                t_minus_s=remaining_s,
                                max_spot_minus_strike_btc=max_sms,
                                min_spot_minus_strike_btc=min_sms,
                                strike_spot_line=strike_line,
                                influence_value=float(depth_metrics["influence_rate"]),
                                status_line=None,
                                market_line="",
                            ),
                        )
                        await asyncio.sleep(poll_interval_s)
    finally:
        if stop_oracle is not None:
            stop_oracle.set()
        if oracle_task is not None:
            try:
                await asyncio.wait_for(oracle_task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                oracle_task.cancel()
        await monitor_wave_gate.deactivate("btc")


async def run_market_cycle(
    target: MarketTarget,
    settings: Settings,
    export_path: Path | None = None,
    market_index: int = 0,
    *,
    monitor_wave_gate: MonitorWavePrintGate | None = None,
    redeem_scheduler: RedeemScheduler | None = None,
    clob_client: Any | None = None,
    paper_session: PaperSessionLedger | None = None,
    entry_coordinator: EntryStrategyCoordinator | None = None,
    run_cycle: int = 0,
) -> dict[str, Any]:
    lm = settings.liquidity_maker
    exe = settings.execution
    tag = _tag(target.symbol, target.epoch)
    sym_key = str(target.symbol).lower().strip()
    paper_trading = bool(getattr(settings.bot, "paper_trading", False))
    slugs = compute_epoch_slugs(target.symbol, target.epoch)
    epoch_start = slugs.current_start
    epoch_end = epoch_start + timedelta(seconds=INTERVAL_SECONDS.get(target.epoch, 300))

    summary: dict[str, Any] = {
        "symbol": target.symbol,
        "epoch": target.epoch,
        "slug": slugs.current_slug,
        "epoch_start": epoch_start.isoformat(),
        "epoch_end": epoch_end.isoformat(),
        "phase": Phase.IDLE.value,
        "redeem_tx": None,
        "error": None,
    }

    tp_path = resolve_trading_process_path(settings)
    epoch_start_unix = int(epoch_start.timestamp())
    epoch_end_unix = int(epoch_end.timestamp())
    trading_journal: TradingCycleJournal | None = None
    _tp_mode = str(getattr(lm, "trading_process_log_mode", "trades") or "trades").strip().lower()
    if _tp_mode not in ("full", "trades"):
        _tp_mode = "trades"
    _tp_full_log = _tp_mode == "full"
    poll_interval = float(getattr(lm, "monitor_poll_interval_s", 0.5) or 0.5)
    balance_poll_interval = float(getattr(lm, "monitor_balance_poll_interval_s", 0.3) or 0.3)
    log_interval = float(getattr(lm, "monitor_log_interval_s", 1.0) or 1.0)
    verbose_before = float(getattr(lm, "monitor_verbose_seconds_before_end", 5.0) or 5.0)
    log_strike_spot_iv = float(getattr(lm, "log_strike_spot_interval_s", 0.0) or 0.0)
    diff_rate_lookback_lm = float(
        getattr(lm, "spot_minus_strike_difference_rate_lookback_s", 0.0) or 0.0
    )
    print(f"  {tag} slug={slugs.current_slug}")

    async with aiohttp.ClientSession() as session:
        gamma = GammaClient(settings.api.gamma_url, session)
        try:
            event = await gamma.fetch_event_by_slug(slugs.current_slug)
        except (ValueError, aiohttp.ClientResponseError) as e:
            summary["error"] = f"No event for slug: {e}"
            if monitor_wave_gate:
                await monitor_wave_gate.deactivate(sym_key)
            return summary

    market = event.markets[0]
    if len(market.asset_ids) != 2:
        summary["error"] = f"Expected 2 asset IDs (YES/NO), got {len(market.asset_ids)}"
        if monitor_wave_gate:
            await monitor_wave_gate.deactivate(sym_key)
        return summary

    condition_id = market.condition_id
    try:
        yes_token, no_token = resolve_outcome_token_ids(market)
    except ValueError as e:
        summary["error"] = str(e)
        if monitor_wave_gate:
            await monitor_wave_gate.deactivate(sym_key)
        return summary
    print(f"  {tag} condition_id={condition_id[:24]}...")
    wallet_addr = ""
    if (exe.private_key or "").strip():
        wallet_addr = resolve_deposit_wallet_address(
            private_key=exe.private_key,
            chain_id=exe.chain_id,
            configured_funder=exe.funder or "",
        )

    strike_spot_feed: StrikeSpotContext | None = None
    _want_strike_spot = log_strike_spot_iv > 0 or diff_rate_lookback_lm > 0
    if _want_strike_spot:
        pf = settings.price_feed
        strike_spot_feed = StrikeSpotContext(
            symbol=target.symbol.lower(),
            epoch_start_unix=int(slugs.current_start.timestamp()),
            interval_secs=INTERVAL_SECONDS.get(target.epoch, 300),
            strike_provider=pf.provider,
            chainlink_user_id=pf.chainlink.streams_user_id,
            chainlink_secret=pf.chainlink.streams_secret,
            chainlink_feed_ids=dict(pf.chainlink.feed_ids),
            market_slug=slugs.current_slug,
            spot_provider=pf.spot_provider,
            chainlink_spot_poll_interval_s=float(pf.chainlink_spot_poll_interval_s or 1.0),
        )

    if tp_path is not None:
        trading_journal = TradingCycleJournal(
            tp_path,
            TradingCycleKey(
                run_cycle=int(run_cycle),
                symbol=sym_key,
                epoch=str(target.epoch),
                slug=slugs.current_slug,
                epoch_start_unix=epoch_start_unix,
                epoch_end_unix=epoch_end_unix,
                condition_id=condition_id,
                paper_trading=paper_trading,
                yes_token_id=yes_token,
                no_token_id=no_token,
            ),
            tag=tag,
        )
        trading_journal.log_cycle_start(dry_run=settings.bot.dry_run)

    adapter_address = await resolve_collateral_adapter(
        yes_token,
        clob_client,
        ctf_adapter_override=getattr(settings.execution, "ctf_collateral_adapter", "") or "",
        neg_risk_adapter_override=getattr(settings.execution, "neg_risk_ctf_collateral_adapter", "") or "",
    )
    collateral = (getattr(exe, "collateral_token", "") or "").strip() or PUSD_ADDRESS

    summary["phase"] = Phase.MONITOR.value
    paper_account: PaperV2Account | None = None
    if paper_trading and paper_session is not None:
        paper_account = PaperV2Account(
            yes_token,
            no_token,
            session=paper_session,
        )

    strike_log_iv = log_strike_spot_iv
    if _want_strike_spot and strike_log_iv <= 0:
        strike_log_iv = poll_interval
    monitor_summary = await monitor_orderbook_until_epoch_end(
        settings.api.clob_url,
        yes_token,
        no_token,
        epoch_end,
        tag=tag,
        poll_interval_s=poll_interval,
        market_log_interval_s=log_interval,
        monitor_verbose_seconds_before_end=verbose_before,
        strike_spot_feed=strike_spot_feed,
        log_strike_spot_interval_s=strike_log_iv,
        run_strike_spot_oracle=_want_strike_spot,
        trading_process_path=tp_path,
        trading_journal=trading_journal,
        trading_process_log_mode=_tp_mode,
        trading_process_log_interval_s=float(getattr(lm, "trading_process_log_interval_s", 0.0) or 0.0),
        trading_process_log_stdout=bool(getattr(lm, "trading_process_log_stdout", False))
        and monitor_wave_gate is None,
        monitor_wave_gate=monitor_wave_gate,
        monitor_gate_symbol=sym_key,
        spot_minus_strike_difference_rate_lookback_s=float(
            getattr(lm, "spot_minus_strike_difference_rate_lookback_s", 0.0) or 0.0
        ),
        clob_client=clob_client,
        paper_account=paper_account,
        balance_poll_interval_s=balance_poll_interval,
        condition_id=condition_id,
        split_inventory_yes=0.0,
        split_inventory_no=0.0,
        balance_rpc_url=exe.rpc_url,
        balance_wallet_address=wallet_addr,
        clob_ws_url=settings.api.ws_url,
        clob_api_key=exe.api_key,
        clob_api_secret=exe.api_secret,
        clob_api_passphrase=exe.api_passphrase,
        monitor_user_ws_enabled=bool(getattr(lm, "monitor_user_ws_enabled", True))
        and not paper_trading,
        monitor_context=entry_coordinator,
        entry_symbol=sym_key,
        rest_book_timeout_s=float(getattr(lm, "monitor_rest_book_timeout_s", 3.0) or 3.0),
        balance_refresh_timeout_s=float(
            getattr(lm, "monitor_balance_refresh_timeout_s", 3.0) or 3.0
        ),
        balance_force_refresh_min_s=float(
            getattr(lm, "monitor_balance_force_refresh_min_s", 1.0) or 1.0
        ),
    )
    summary["monitor"] = monitor_summary

    if monitor_wave_gate:
        await monitor_wave_gate.deactivate(sym_key)

    redeem_enabled = bool(getattr(lm, "redeem_enabled", True))
    redeem_async = bool(getattr(lm, "redeem_async_enabled", True))

    if redeem_enabled and redeem_scheduler is not None:
        job = RedeemJob(
            condition_id=condition_id,
            adapter_address=adapter_address,
            collateral=collateral,
            symbol=sym_key,
            epoch=str(target.epoch),
            tag=tag,
            market_index=market_index,
            epoch_end=epoch_end,
            slug=slugs.current_slug,
            summary=summary,
            export_path=export_path,
            yes_token=yes_token,
            no_token=no_token,
            poll_interval=poll_interval,
            clob_url=settings.api.clob_url,
            paper_trading=paper_trading,
            dry_run=settings.bot.dry_run,
        )
        await redeem_scheduler.schedule(job, wait=not redeem_async)
    elif not redeem_enabled:
        summary["phase"] = Phase.IDLE.value
        print(f"  {tag} REDEEM skipped (redeem_enabled=false)")

    if trading_journal is not None:
        trading_journal.log_cycle_end(
            error=summary.get("error"),
            redeem_tx=summary.get("redeem_tx"),
            phase=summary.get("phase"),
        )
    elif tp_path and _tp_full_log:
        append_trading_jsonl(
            tp_path,
            {
                "event": "MARKET_CYCLE_END",
                "ts_utc": utc_iso_z(),
                "tag": tag,
                "error": summary.get("error"),
                "redeem_tx": summary.get("redeem_tx"),
            },
        )
    return summary


async def run_all_markets(
    targets: list[MarketTarget],
    settings: Settings,
    export_path: Path | None = None,
    *,
    redeem_scheduler: RedeemScheduler | None = None,
    clob_client: Any | None = None,
    paper_session: PaperSessionLedger | None = None,
    run_cycle: int = 0,
) -> list[dict[str, Any]]:
    lm = settings.liquidity_maker
    stagger = int(getattr(lm, "stagger_delay_seconds", 0) or 0)
    redeem_enabled = bool(getattr(lm, "redeem_enabled", True))

    symbols = {str(t.symbol).lower().strip() for t in targets if t.symbol}
    symbols_for_wave = set(symbols)
    symbols_for_wave.add("btc")

    monitor_wave_gate = (
        MonitorWavePrintGate(
            symbols_for_wave,
            wave_collect_timeout_s=float(
                getattr(lm, "monitor_wave_collect_timeout_s", 3.0) or 3.0
            ),
        )
        if targets
        else None
    )
    if monitor_wave_gate:
        monitor_wave_gate.start()

    poll_iv = float(getattr(lm, "monitor_poll_interval_s", 0.5) or 0.5)

    entry_coordinator: EntryStrategyCoordinator | None = EntryStrategyCoordinator(settings)
    entry_coordinator.reset_btc_epoch_stats()

    btc_wave_task: asyncio.Task[Any] | None = None
    if monitor_wave_gate is not None and "btc" not in symbols:
        epoch_for_btc = targets[0].epoch if targets else "5m"
        epoch_end_btc = compute_epoch_slugs("btc", epoch_for_btc).current_start + timedelta(
            seconds=INTERVAL_SECONDS.get(epoch_for_btc, 300)
        )
        btc_wave_task = asyncio.create_task(
            _run_btc_wave_monitor(
                settings,
                epoch_for_btc,
                epoch_end_btc,
                monitor_wave_gate,
                poll_interval_s=poll_iv,
                clob_ws_url=settings.api.ws_url,
                clob_client=clob_client,
                entry_coordinator=entry_coordinator,
            )
        )

    async def _run_with_delay(i: int, target: MarketTarget) -> dict[str, Any]:
        if i > 0 and stagger > 0:
            delay = i * stagger
            print(f"  [{target.symbol}/{target.epoch}] waiting {delay}s before monitor...")
            await asyncio.sleep(delay)
        sym_key = str(target.symbol).lower().strip()
        return await run_market_cycle(
            target,
            settings,
            export_path=export_path,
            market_index=i,
            monitor_wave_gate=monitor_wave_gate,
            redeem_scheduler=redeem_scheduler if redeem_enabled else None,
            clob_client=clob_client,
            paper_session=paper_session,
            entry_coordinator=entry_coordinator,
            run_cycle=run_cycle,
        )

    try:
        results = await asyncio.gather(*[_run_with_delay(i, t) for i, t in enumerate(targets)], return_exceptions=True)
    finally:
        if btc_wave_task is not None:
            btc_wave_task.cancel()
            try:
                await btc_wave_task
            except asyncio.CancelledError:
                pass
        if monitor_wave_gate:
            await monitor_wave_gate.shutdown()

    summaries: list[dict[str, Any]] = []
    for i, result in enumerate(results):
        if isinstance(result, BaseException):
            summaries.append({"symbol": targets[i].symbol, "epoch": targets[i].epoch, "error": str(result)})
        else:
            summaries.append(result)

    return summaries
