"""Resolve CtfCollateralAdapter address for redeem (neg-risk vs standard)."""

from __future__ import annotations

from typing import Any

from polybot5m.constants import collateral_adapter_address


async def resolve_collateral_adapter(
    yes_token_id: str,
    clob_client: Any | None,
    *,
    ctf_adapter_override: str = "",
    neg_risk_adapter_override: str = "",
) -> str:
    neg_risk = False
    if clob_client is not None:
        try:
            neg_risk = bool(await clob_client.get_neg_risk(yes_token_id))
        except Exception:
            neg_risk = False
    override = neg_risk_adapter_override if neg_risk else ctf_adapter_override
    return collateral_adapter_address(neg_risk=neg_risk, override=override)
