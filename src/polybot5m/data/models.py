"""Pydantic models for Gamma events and markets."""

from datetime import datetime

from pydantic import BaseModel


class CryptoMarketMeta(BaseModel):
    asset: str
    interval: str
    slug: str
    expiry: datetime


class Market(BaseModel):
    condition_id: str
    asset_ids: list[str]
    question: str
    outcomes: list[str]
    meta: CryptoMarketMeta


def resolve_outcome_token_ids(market: Market) -> tuple[str, str]:
    """
    Map Gamma clobTokenIds to (up_or_yes_id, down_or_no_id) using outcomes labels.
    Defaults to asset_ids order when outcomes are missing or ambiguous.
    """
    ids = [str(x).strip() for x in (market.asset_ids or []) if str(x).strip()]
    if len(ids) < 2:
        raise ValueError(f"market needs 2 asset ids, got {len(ids)}")
    outcomes = [str(o).strip().lower() for o in (market.outcomes or [])]
    up_words = {"up", "yes", "y"}
    down_words = {"down", "no", "n"}
    up_i, down_i = 0, 1
    if len(outcomes) >= 2:
        o0, o1 = outcomes[0], outcomes[1]
        if o0 in down_words and o1 in up_words:
            up_i, down_i = 1, 0
        elif o0 in up_words and o1 in down_words:
            up_i, down_i = 0, 1
        elif o1 in up_words and o0 not in up_words:
            up_i, down_i = 1, 0
    return ids[up_i], ids[down_i]


class Event(BaseModel):
    id: str
    slug: str
    title: str
    markets: list[Market]

    def all_asset_ids(self) -> list[str]:
        out: list[str] = []
        for m in self.markets:
            out.extend(m.asset_ids)
        return out

    def condition_id_for_asset(self, asset_id: str) -> str | None:
        """Return the condition_id of the market that contains this asset_id."""
        for m in self.markets:
            if asset_id in m.asset_ids:
                return m.condition_id
        return None
