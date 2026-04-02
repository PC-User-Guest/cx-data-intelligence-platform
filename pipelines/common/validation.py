from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


TIMESTAMP_CANDIDATES = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
)


def parse_timestamp(value: str) -> datetime:
    """Parse common timestamp formats from API or CSV payloads."""

    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        pass

    for fmt in TIMESTAMP_CANDIDATES:
        try:
            parsed = datetime.strptime(value, fmt)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue

    raise ValueError(f"Unsupported timestamp format: {value}")


def validate_ticket_record(record: dict[str, Any]) -> tuple[bool, str | None]:
    required_fields = ("ticket_id", "customer_id", "created_at", "sentiment_score")
    for field in required_fields:
        if field not in record or record[field] in (None, ""):
            return False, f"Missing required field: {field}"

    try:
        parse_timestamp(str(record["created_at"]))
    except ValueError as exc:
        return False, str(exc)

    return True, None


def validate_order_record(record: dict[str, Any]) -> tuple[bool, str | None]:
    required_fields = ("order_id", "customer_id", "order_purchase_timestamp")
    for field in required_fields:
        if field not in record or record[field] in (None, ""):
            return False, f"Missing required field: {field}"

    try:
        parse_timestamp(str(record["order_purchase_timestamp"]))
    except ValueError as exc:
        return False, str(exc)

    return True, None
