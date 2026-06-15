"""UTC datetime formatting — avoid naive/local .timestamp() bugs."""

from __future__ import annotations

from datetime import UTC, datetime


def format_utc_iso_z(dt: datetime) -> str:
    """Serialize datetime as ISO-8601 UTC with Z suffix (RFC 3339 style)."""
    if dt.tzinfo is not None:
        return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return dt.isoformat() + "Z"
