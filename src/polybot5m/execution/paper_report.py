"""Paper trading report: PNL per market and detailed output.

At epoch end computes allocation, optional fill proceeds, and PNL if YES vs if NO wins.
Outputs detailed trading data per market (JSON + CSV).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from polybot5m.time_utils import format_utc_iso_z


@dataclass
class PaperFill:
    """Paper fill: sell = positive usdc_proceeds; buy = negative usdc_proceeds (USDC spent)."""

    token_id: str
    side_label: str
    price: float
    size: float
    usdc_proceeds: float
    filled_at: datetime
    best_bid_at_fill: float
    reason: str | None = None
    is_buy: bool = False
    # Optional: realistic backtest / execution simulation
    fee_usdc: float = 0.0
    latency_ms: float | None = None
    vwap_price: float | None = None
    limit_price_at_decision: float | None = None
    slippage_vs_limit: float | None = None
    best_bid_at_decision: float | None = None
    execution_failed: bool = False


@dataclass
class MarketPaperSummary:
    """Per-market paper trading summary for one epoch."""

    symbol: str
    epoch: str
    slug: str
    condition_id: str
    epoch_end_utc: str
    # Orders
    orders_placed: int
    orders_filled: int
    orders_open: int
    # Allocation (simulated split)
    allocation_usdc: float
    # Fills
    fills: list[dict[str, Any]]
    # Aggregates (filled = sold shares; bought = secondary buy leg)
    total_proceeds_usdc: float
    yes_shares_filled: float
    no_shares_filled: float
    yes_shares_bought: float
    no_shares_bought: float
    yes_shares_unfilled: float
    no_shares_unfilled: float
    # PNL at resolution: winning side redeems at $1/share
    pnl_if_yes_wins_usdc: float
    pnl_if_no_wins_usdc: float


def build_market_summary(
    fills: list[PaperFill],
    open_orders: list[dict[str, Any]],
    symbol: str,
    epoch: str,
    slug: str,
    condition_id: str,
    epoch_end: datetime,
    allocation_usdc: float,
    *,
    inventory_yes: float | None = None,
    inventory_no: float | None = None,
    split_yes_shares: float | None = None,
    split_no_shares: float | None = None,
) -> MarketPaperSummary:
    """
    Build per-market paper summary.

    With split_yes_shares/split_no_shares: per-side held at resolution =
    split − sold + bought (buys: fills with is_buy=True, negative usdc_proceeds).
    If inventory_yes/inventory_no are set (no split_*), use as held counts (legacy).
    Else unfilled is derived from open_orders (typically empty when no simulated orders).
    """
    # Sells: positive usdc_proceeds; buys: negative usdc_proceeds (cash out).
    total_proceeds = sum(f.usdc_proceeds for f in fills)
    yes_sold = sum(f.size for f in fills if f.side_label == "YES" and not f.is_buy)
    no_sold = sum(f.size for f in fills if f.side_label == "NO" and not f.is_buy)
    yes_bought = sum(f.size for f in fills if f.side_label == "YES" and f.is_buy)
    no_bought = sum(f.size for f in fills if f.side_label == "NO" and f.is_buy)

    if split_yes_shares is not None and split_no_shares is not None:
        yes_held = float(split_yes_shares) - yes_sold + yes_bought
        no_held = float(split_no_shares) - no_sold + no_bought
        yes_unfilled = max(0.0, yes_held)
        no_unfilled = max(0.0, no_held)
    elif inventory_yes is not None and inventory_no is not None:
        yes_unfilled = float(inventory_yes)
        no_unfilled = float(inventory_no)
    else:
        yes_unfilled = sum(
            o.get("size", 0) for o in open_orders
            if o.get("status") == "open" and o.get("side_label") == "YES"
        )
        no_unfilled = sum(
            o.get("size", 0) for o in open_orders
            if o.get("status") == "open" and o.get("side_label") == "NO"
        )

    # Settlement: winning side $1/share, losing $0; trading cash net in total_proceeds.
    pnl_yes = round(
        total_proceeds + yes_unfilled * 1.0 + no_unfilled * 0.0 - allocation_usdc,
        6,
    )
    pnl_no = round(
        total_proceeds + yes_unfilled * 0.0 + no_unfilled * 1.0 - allocation_usdc,
        6,
    )

    fills_data = []
    for f in fills:
        row: dict[str, Any] = {
            "token_id": (f.token_id[:24] + "...") if len(f.token_id) > 24 else f.token_id,
            "side": f.side_label,
            "price": f.price,
            "size": f.size,
            "usdc_proceeds": f.usdc_proceeds,
            "best_bid_at_fill": f.best_bid_at_fill,
            "filled_at": format_utc_iso_z(f.filled_at),
        }
        if f.reason:
            row["reason"] = f.reason
        if f.is_buy:
            row["is_buy"] = True
        if f.fee_usdc:
            row["fee_usdc"] = f.fee_usdc
        if f.latency_ms is not None:
            row["latency_ms"] = f.latency_ms
        if f.vwap_price is not None:
            row["vwap_price"] = f.vwap_price
        if f.limit_price_at_decision is not None:
            row["limit_price_at_decision"] = f.limit_price_at_decision
        if f.slippage_vs_limit is not None:
            row["slippage_vs_limit"] = f.slippage_vs_limit
        if f.best_bid_at_decision is not None:
            row["best_bid_at_decision"] = f.best_bid_at_decision
        if f.execution_failed:
            row["execution_failed"] = True
        fills_data.append(row)

    open_ct = len([o for o in open_orders if o.get("status") == "open"])
    if split_yes_shares is not None and split_no_shares is not None:
        orders_placed_ct = len(fills) + open_ct
    elif inventory_yes is not None and inventory_no is not None:
        orders_placed_ct = len(fills)
    else:
        orders_placed_ct = len(fills) + open_ct

    return MarketPaperSummary(
        symbol=symbol,
        epoch=epoch,
        slug=slug,
        condition_id=condition_id,
        epoch_end_utc=format_utc_iso_z(epoch_end) if epoch_end else "",
        orders_placed=orders_placed_ct,
        orders_filled=len(fills),
        orders_open=open_ct,
        allocation_usdc=allocation_usdc,
        fills=fills_data,
        total_proceeds_usdc=round(total_proceeds, 6),
        yes_shares_filled=round(yes_sold, 6),
        no_shares_filled=round(no_sold, 6),
        yes_shares_bought=round(yes_bought, 6),
        no_shares_bought=round(no_bought, 6),
        yes_shares_unfilled=round(yes_unfilled, 6),
        no_shares_unfilled=round(no_unfilled, 6),
        pnl_if_yes_wins_usdc=pnl_yes,
        pnl_if_no_wins_usdc=pnl_no,
    )


def write_paper_report(
    summaries: list[MarketPaperSummary],
    export_dir: str | Path,
    run_ts: datetime | None = None,
) -> tuple[Path | None, Path | None]:
    """
    Write detailed paper trading data: one JSON report and one CSV summary.
    Returns (json_path, csv_path).
    """
    if not summaries:
        return None, None

    run_ts = run_ts or datetime.now(UTC)
    ts_str = run_ts.strftime("%Y%m%d_%H%M%S")
    export_dir = Path(export_dir)
    export_dir.mkdir(parents=True, exist_ok=True)

    json_path = export_dir / f"paper_summary_{ts_str}.json"
    csv_path = export_dir / f"paper_pnl_{ts_str}.csv"

    report = {
        "run_utc": format_utc_iso_z(run_ts),
        "markets": [asdict(s) for s in summaries],
        "totals": {
            "total_allocation_usdc": round(sum(s.allocation_usdc for s in summaries), 4),
            "total_proceeds_usdc": round(sum(s.total_proceeds_usdc for s in summaries), 4),
            "total_yes_filled": round(sum(s.yes_shares_filled for s in summaries), 6),
            "total_no_filled": round(sum(s.no_shares_filled for s in summaries), 6),
            "total_yes_bought": round(sum(s.yes_shares_bought for s in summaries), 6),
            "total_no_bought": round(sum(s.no_shares_bought for s in summaries), 6),
        },
    }
    try:
        with open(json_path, "w") as f:
            json.dump(report, f, indent=2)
    except OSError:
        json_path = None

    try:
        with open(csv_path, "w") as f:
            f.write(
                "symbol,epoch,slug,orders_placed,orders_filled,orders_open,allocation_usdc,"
                "total_proceeds_usdc,yes_sold,no_sold,yes_bought,no_bought,yes_held,no_held,"
                "pnl_if_yes_wins_usdc,pnl_if_no_wins_usdc,epoch_end_utc\n"
            )
            for s in summaries:
                f.write(
                    f"{s.symbol},{s.epoch},{s.slug},{s.orders_placed},{s.orders_filled},{s.orders_open},"
                    f"{s.allocation_usdc},{s.total_proceeds_usdc},{s.yes_shares_filled},{s.no_shares_filled},"
                    f"{s.yes_shares_bought},{s.no_shares_bought},"
                    f"{s.yes_shares_unfilled},{s.no_shares_unfilled},"
                    f"{s.pnl_if_yes_wins_usdc},{s.pnl_if_no_wins_usdc},{s.epoch_end_utc}\n"
                )
    except OSError:
        csv_path = None

    return json_path, csv_path
