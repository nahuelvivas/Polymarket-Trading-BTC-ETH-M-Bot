"""Top-of-book depth sums and OBI-style influence rate from public CLOB books."""

from __future__ import annotations

from typing import Any


def _book_rows(book: Any, side: str) -> list[Any]:
    key = "bids" if side == "bids" else "asks"
    if isinstance(book, dict):
        return list(book.get(key) or [])
    return list(getattr(book, key, None) or [])


def _aggregate_levels(book: Any, side: str) -> dict[float, float]:
    """price -> total size for bids or asks."""
    rows = _book_rows(book, side)
    out: dict[float, float] = {}
    for r in rows:
        p = getattr(r, "price", None) or (r.get("price") if isinstance(r, dict) else None)
        s = getattr(r, "size", None) or (r.get("size") if isinstance(r, dict) else None)
        if p is None or s is None:
            continue
        try:
            pf = float(p)
            sf = float(s)
        except (TypeError, ValueError):
            continue
        if not (0 < pf <= 1) or sf <= 0:
            continue
        out[pf] = out.get(pf, 0.0) + sf
    return out


def influence_from_book(book: Any | None, *, top_n: int = 5, eps: float = 1e-9) -> dict[str, float | int]:
    """
    Sum sizes at the best `top_n` bid price levels and best `top_n` ask price levels (aggregate per price).

    influence_rate = (bid_n_sum - ask_n_sum) / (bid_n_sum + ask_n_sum + eps) in roughly [-1, 1].
    """
    if not book:
        return {
            "bid_n_sum": 0.0,
            "ask_n_sum": 0.0,
            "bid_levels_used": 0,
            "ask_levels_used": 0,
            "influence_rate": 0.0,
        }
    bids = _aggregate_levels(book, "bids")
    asks = _aggregate_levels(book, "asks")
    bid_prices = sorted(bids.keys(), reverse=True)[:top_n]
    ask_prices = sorted(asks.keys())[:top_n]
    bsum = sum(bids[p] for p in bid_prices)
    asum = sum(asks[p] for p in ask_prices)
    den = bsum + asum + eps
    rate = (bsum - asum) / den if den > eps else 0.0
    return {
        "bid_n_sum": round(bsum, 8),
        "ask_n_sum": round(asum, 8),
        "bid_levels_used": len(bid_prices),
        "ask_levels_used": len(ask_prices),
        "influence_rate": round(float(rate), 8),
    }


def pair_depth_metrics_for_monitor(book_yes: Any, book_no: Any, *, top_n: int = 5) -> dict[str, float | int]:
    """Flat dict for JSONL / on_tick: YES book, NO book, and canonical `influence_rate` (= YES book)."""
    y = influence_from_book(book_yes, top_n=top_n)
    n = influence_from_book(book_no, top_n=top_n)
    return {
        "yes_bid5_sum": y["bid_n_sum"],
        "yes_ask5_sum": y["ask_n_sum"],
        "yes_bid5_levels": y["bid_levels_used"],
        "yes_ask5_levels": y["ask_levels_used"],
        "yes_influence_rate": y["influence_rate"],
        "no_bid5_sum": n["bid_n_sum"],
        "no_ask5_sum": n["ask_n_sum"],
        "no_bid5_levels": n["bid_levels_used"],
        "no_ask5_levels": n["ask_levels_used"],
        "no_influence_rate": n["influence_rate"],
        # Single directional knob: YES-token book imbalance (>0 → YES pressure, <0 → NO pressure).
        "influence_rate": float(y["influence_rate"]),
    }
