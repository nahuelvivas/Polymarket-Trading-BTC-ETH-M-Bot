"""Polymarket CLOB balance-allowance helpers (py-clob-client-v2 official flow)."""

from __future__ import annotations

from typing import Any

_CONDITIONAL_DECIMALS = 1_000_000.0


def _shares_from_balance_allowance_response(resp: Any) -> float:
    if not isinstance(resp, dict):
        return 0.0
    try:
        return int(str(resp.get("balance") or 0)) / _CONDITIONAL_DECIMALS
    except (TypeError, ValueError):
        return 0.0


def official_sync_and_get_conditional_pair(
    client: Any,
    *,
    yes_token_id: str,
    no_token_id: str,
    log: bool = True,
    tag: str = "",
    sync_update: bool = True,
) -> tuple[float, float, dict[str, Any]]:
    """
    Read conditional balances via get_balance_allowance.
    When sync_update is True, call update_balance_allowance first (post-split only — rate limited).
    Returns (yes_shares, no_shares, raw_responses).
    """
    try:
        from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams
    except ImportError as e:
        raise ImportError(
            "py-clob-client-v2 is required (Polymarket CLOB V2). "
            "pip install py-clob-client-v2"
        ) from e

    def one(token_id: str) -> tuple[float, Any]:
        params = BalanceAllowanceParams(
            asset_type=AssetType.CONDITIONAL,
            token_id=str(token_id),
        )
        if sync_update:
            try:
                client.update_balance_allowance(params)
            except Exception:
                pass
        try:
            resp = client.get_balance_allowance(params)
        except Exception:
            resp = None
        return _shares_from_balance_allowance_response(resp), resp

    yes_v, yes_raw = one(yes_token_id)
    no_v, no_raw = one(no_token_id)
    raw: dict[str, Any] = {"yes": yes_raw, "no": no_raw}

    if log:
        p = f"  {tag} " if tag else "  "
        print(
            f"{p}[BALANCE] CLOB conditional YES={yes_v:.4f} NO={no_v:.4f}",
            flush=True,
        )

    return float(yes_v), float(no_v), raw
