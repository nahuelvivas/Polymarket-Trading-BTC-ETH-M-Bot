"""Replay PolyBackTest snapshots with monitor-only paper summaries."""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from polybot5m.config import Settings
from polybot5m.execution.paper_report import MarketPaperSummary, build_market_summary, write_paper_report

import aiohttp

from polybot5m.backtest.client import PolyBacktestClient
from polybot5m.backtest.simulation import (
    RealisticPaperExecutionHook,
    SimulationRunMetrics,
    SnapshotTimeline,
    make_rng,
    max_drawdown_from_series,
)


def _parse_dt(s: str | None) -> datetime | None:
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


def _best_bid_from_orderbook(ob: Any) -> float:
    if not ob or not isinstance(ob, dict):
        return 0.0
    bids = ob.get("bids") or []
    best = 0.0
    for b in bids:
        p = b.get("price") if isinstance(b, dict) else None
        if p is not None:
            x = float(p)
            if 0 < x <= 1:
                best = max(best, x)
    return best


def _bids_from_snapshot(snap: dict[str, Any], *, use_orderbook: bool) -> tuple[float, float]:
    """Map UP → YES bid, DOWN → NO bid (matches Polymarket up/down binary naming)."""
    if use_orderbook:
        bu = _best_bid_from_orderbook(snap.get("orderbook_up"))
        bd = _best_bid_from_orderbook(snap.get("orderbook_down"))
        if bu > 0 or bd > 0:
            return bu, bd
    pu = snap.get("price_up")
    pd = snap.get("price_down")
    return (float(pu) if pu is not None else 0.0, float(pd) if pd is not None else 0.0)


def _best_ask_from_orderbook(ob: Any) -> float:
    if not ob or not isinstance(ob, dict):
        return 0.0
    asks = ob.get("asks") or []
    best = 0.0
    for a in asks:
        p = a.get("price") if isinstance(a, dict) else None
        if p is not None:
            x = float(p)
            if 0 < x <= 1:
                if best <= 0 or x < best:
                    best = x
    return best


def _asks_from_snapshot(snap: dict[str, Any], *, use_orderbook: bool) -> tuple[float, float]:
    if use_orderbook:
        au = _best_ask_from_orderbook(snap.get("orderbook_up"))
        ad = _best_ask_from_orderbook(snap.get("orderbook_down"))
        if au > 0 or ad > 0:
            return au, ad
    return 0.0, 0.0


def _market_summary_from_stored_dict(d: dict[str, Any]) -> MarketPaperSummary:
    """Backward-compatible load when older backtest JSON lacks new fields."""
    m = dict(d)
    m.setdefault("yes_shares_bought", 0.0)
    m.setdefault("no_shares_bought", 0.0)
    return MarketPaperSummary(**m)


def _normalize_winner(w: str | None) -> str | None:
    if w is None or str(w).strip() == "":
        return None
    x = str(w).strip().lower()
    if x in ("up", "yes", "y", "1", "true"):
        return "yes"
    if x in ("down", "no", "n", "0", "false"):
        return "no"
    return None


def _realized_pnl(summary: MarketPaperSummary, winner: str | None) -> float | None:
    if winner == "yes":
        return summary.pnl_if_yes_wins_usdc
    if winner == "no":
        return summary.pnl_if_no_wins_usdc
    return None


@dataclass
class BacktestMarketResult:
    market_id: str
    slug: str
    ok: bool
    error: str | None
    snapshot_count: int
    paper_summary: dict[str, Any] | None
    realized_pnl_usdc: float | None
    simulation_metrics: dict[str, Any] | None = None


def _sim_metrics_to_dict(m: SimulationRunMetrics) -> dict[str, Any]:
    avg_lat = m.avg_latency_ms()
    fail_rate = (m.exit_failures / m.exit_attempts) if m.exit_attempts else None
    return {
        "slippage_cost_usdc": round(m.slippage_cost_usdc, 6),
        "fee_usdc_total": round(m.fee_usdc_total, 6),
        "avg_latency_ms": round(avg_lat, 4) if avg_lat is not None else None,
        "latency_samples": m.latency_ms_count,
        "failed_sell_orders": m.failed_sell_orders,
        "partial_fills": m.partial_fills,
        "exit_attempts": m.exit_attempts,
        "exit_failures": m.exit_failures,
        "failed_exit_rate": round(fail_rate, 6) if fail_rate is not None else None,
    }


def _aggregate_batch_metrics(results: list[BacktestMarketResult]) -> dict[str, Any] | None:
    rows = [r.simulation_metrics for r in results if r.ok and r.simulation_metrics]
    if not rows:
        return None
    slip = sum(float(x["slippage_cost_usdc"]) for x in rows)
    fee = sum(float(x["fee_usdc_total"]) for x in rows)
    failed = sum(int(x["failed_sell_orders"]) for x in rows)
    partial = sum(int(x["partial_fills"]) for x in rows)
    ex_att = sum(int(x["exit_attempts"]) for x in rows)
    ex_fail = sum(int(x["exit_failures"]) for x in rows)
    lat_n = sum(int(x["latency_samples"]) for x in rows)
    lat_sum = 0.0
    for x in rows:
        avg = x.get("avg_latency_ms")
        n = int(x["latency_samples"])
        if avg is not None and n > 0:
            lat_sum += float(avg) * n
    avg_lat_batch = (lat_sum / lat_n) if lat_n else None

    cum: list[float] = []
    s = 0.0
    for r in results:
        if r.ok and r.realized_pnl_usdc is not None:
            s += float(r.realized_pnl_usdc)
            cum.append(s)
    mdd = max_drawdown_from_series(cum) if cum else 0.0

    resolved = [r for r in results if r.ok and r.realized_pnl_usdc is not None]
    wins = sum(1 for r in resolved if float(r.realized_pnl_usdc or 0) > 0)
    win_rate = (wins / len(resolved)) if resolved else None

    return {
        "total_slippage_cost_usdc": round(slip, 6),
        "total_fees_usdc": round(fee, 6),
        "failed_sell_orders": failed,
        "partial_fills": partial,
        "exit_attempts": ex_att,
        "exit_failures": ex_fail,
        "failed_exit_rate": round(ex_fail / ex_att, 6) if ex_att else None,
        "avg_latency_ms_weighted": round(avg_lat_batch, 4) if avg_lat_batch is not None else None,
        "latency_sample_count": lat_n,
        "max_drawdown_usdc": round(mdd, 6),
        "win_rate": round(win_rate, 6) if win_rate is not None else None,
        "markets_resolved": len(resolved),
        "markets_wins": wins,
    }


async def replay_one_market(
    client: PolyBacktestClient,
    session: aiohttp.ClientSession,
    *,
    coin: str,
    market_id: str,
    market_type_label: str,
    settings: Settings,
    include_orderbook: bool,
    verbose: bool,
    page_delay_s: float = 1.0,
) -> BacktestMarketResult:
    # Split allocation is removed in monitor/redeem mode.
    allocation = 0.0
    epoch_label = market_type_label.strip() or "5m"
    symbol = coin.strip().lower()
    tag = f"[{symbol}/{epoch_label}]"

    try:
        raw = await client.fetch_all_snapshots(
            session,
            coin=coin,
            market_id=market_id,
            include_orderbook=include_orderbook,
            page_delay_s=page_delay_s,
        )
    except Exception as e:
        return BacktestMarketResult(
            market_id=market_id,
            slug="",
            ok=False,
            error=str(e),
            snapshot_count=0,
            paper_summary=None,
            realized_pnl_usdc=None,
            simulation_metrics=None,
        )

    market = raw.get("market") or {}
    slug = str(market.get("slug") or "")
    snaps: list[dict[str, Any]] = list(raw.get("snapshots") or [])
    end_dt = _parse_dt(market.get("end_time"))
    if end_dt is None:
        return BacktestMarketResult(
            market_id=market_id,
            slug=slug,
            ok=False,
            error="missing market.end_time",
            snapshot_count=len(snaps),
            paper_summary=None,
            realized_pnl_usdc=None,
            simulation_metrics=None,
        )

    yes_tok = str(market.get("clob_token_up") or "").strip()
    no_tok = str(market.get("clob_token_down") or "").strip()
    condition_id = str(market.get("condition_id") or "").strip()
    if not yes_tok or not no_tok:
        return BacktestMarketResult(
            market_id=market_id,
            slug=slug,
            ok=False,
            error="missing clob_token_up / clob_token_down",
            snapshot_count=len(snaps),
            paper_summary=None,
            realized_pnl_usdc=None,
            simulation_metrics=None,
        )

    snaps.sort(key=lambda s: str(s.get("time") or ""))

    sim_cfg = settings.backtest.simulation
    use_sim = bool(sim_cfg.enabled)
    hook: RealisticPaperExecutionHook | None = None
    sim_metrics: SimulationRunMetrics | None = None
    if use_sim:
        timeline = SnapshotTimeline.from_snapshots(snaps, end_dt)
        sim_metrics = SimulationRunMetrics()
        hook = RealisticPaperExecutionHook(
            timeline=timeline,
            end_dt=end_dt,
            cfg=sim_cfg,
            rng=make_rng(sim_cfg),
            metrics=sim_metrics,
            include_orderbook=include_orderbook,
        )

    ticks = 0
    for snap in snaps:
        t = _parse_dt(snap.get("time"))
        if t is None:
            continue
        if t >= end_dt:
            break
        remaining_s = max(0.0, (end_dt - t).total_seconds())
        bid_yes, bid_no = _bids_from_snapshot(snap, use_orderbook=include_orderbook)
        ask_yes, ask_no = _asks_from_snapshot(snap, use_orderbook=include_orderbook)
        ticks += 1
        _ = (bid_yes, bid_no, remaining_s, ask_yes, ask_no)

    winner_raw = market.get("winner")
    winner = _normalize_winner(str(winner_raw) if winner_raw is not None else None)

    paper_fills: list[dict[str, Any]] = []
    summ = build_market_summary(
        paper_fills,
        [],
        symbol,
        epoch_label,
        slug or market_id,
        condition_id or market_id,
        end_dt,
        allocation,
        split_yes_shares=allocation,
        split_no_shares=allocation,
    )
    realized = _realized_pnl(summ, winner)
    sim_out: dict[str, Any] | None = None
    if sim_metrics is not None:
        sim_out = _sim_metrics_to_dict(sim_metrics)

    if verbose:
        line = (
            f"  {tag} id={market_id} slug={slug[:48]}… snaps={len(snaps)} ticks={ticks} "
            f"fills={len(paper_fills)} pnl_yes={summ.pnl_if_yes_wins_usdc} "
            f"pnl_no={summ.pnl_if_no_wins_usdc}"
        )
        if realized is not None:
            line += f" realized={realized} (winner={winner_raw})"
        if sim_out:
            line += f" sim_slip={sim_out.get('slippage_cost_usdc')} avgLat={sim_out.get('avg_latency_ms')}"
        print(line)

    return BacktestMarketResult(
        market_id=market_id,
        slug=slug,
        ok=True,
        error=None,
        snapshot_count=len(snaps),
        paper_summary=asdict(summ),
        realized_pnl_usdc=realized,
        simulation_metrics=sim_out,
    )


async def run_backtest_batch(
    *,
    api_key: str,
    base_url: str,
    coin: str,
    market_type: str,
    last_n: int,
    settings: Settings,
    include_orderbook: bool,
    resolved_only: bool,
    verbose: bool,
    out_dir: Path,
    market_delay_s: float = 10.0,
    page_delay_s: float = 1.0,
) -> list[BacktestMarketResult]:
    client = PolyBacktestClient(api_key, base_url)
    out_dir.mkdir(parents=True, exist_ok=True)

    async with aiohttp.ClientSession() as session:
        collected: list[dict[str, Any]] = []
        offset = 0
        while len(collected) < last_n:
            page_limit = min(100, last_n - len(collected))
            listing = await client.list_markets(
                session,
                coin=coin,
                limit=page_limit,
                offset=offset,
                market_type=market_type or None,
                resolved=True if resolved_only else None,
            )
            batch = listing.get("markets") or []
            if not batch:
                break
            collected.extend(batch)
            offset += len(batch)
            if len(batch) < page_limit:
                break
        slice_m = collected[:last_n]
        type_label = (market_type or "5m").strip()

        results: list[BacktestMarketResult] = []
        summaries_for_report: list[MarketPaperSummary] = []

        for i, m in enumerate(slice_m):
            if i > 0 and market_delay_s > 0:
                if verbose:
                    print(f"  … waiting {market_delay_s:g}s before next market (rate limit spacing)")
                await asyncio.sleep(market_delay_s)
            mid = str(m.get("market_id") or m.get("marketId") or m.get("id") or "")
            if not mid:
                continue
            r = await replay_one_market(
                client,
                session,
                coin=coin,
                market_id=mid,
                market_type_label=type_label,
                settings=settings,
                include_orderbook=include_orderbook,
                verbose=verbose,
                page_delay_s=page_delay_s,
            )
            results.append(r)
            if r.ok and r.paper_summary:
                summaries_for_report.append(_market_summary_from_stored_dict(r.paper_summary))

        stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        json_path = out_dir / f"backtest_summary_{stamp}.json"
        batch_sim = _aggregate_batch_metrics(results)
        serializable: dict[str, Any] = {
            "markets": [
                {
                    "market_id": x.market_id,
                    "slug": x.slug,
                    "ok": x.ok,
                    "error": x.error,
                    "snapshot_count": x.snapshot_count,
                    "realized_pnl_usdc": x.realized_pnl_usdc,
                    "paper_summary": x.paper_summary,
                    "simulation_metrics": x.simulation_metrics,
                }
                for x in results
            ],
            "simulation_batch": batch_sim,
        }
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(serializable, f, indent=2, default=str)

        if summaries_for_report:
            write_paper_report(summaries_for_report, str(out_dir), datetime.now(UTC))

        if verbose:
            ok_ct = sum(1 for x in results if x.ok)
            pnl_vals = [x.realized_pnl_usdc for x in results if x.realized_pnl_usdc is not None]
            total_realized = sum(pnl_vals) if pnl_vals else None
            print(f"\nBacktest done: {ok_ct}/{len(results)} markets OK → {json_path}")
            if total_realized is not None:
                print(f"  Sum realized PnL (resolved markets): {total_realized:.4f} USDC")
            if batch_sim:
                print(
                    f"  Simulation (batch): max_dd={batch_sim.get('max_drawdown_usdc')} "
                    f"win_rate={batch_sim.get('win_rate')} slip={batch_sim.get('total_slippage_cost_usdc')} "
                    f"avgLat={batch_sim.get('avg_latency_ms_weighted')} fail_exit%={batch_sim.get('failed_exit_rate')}"
                )

        return results
