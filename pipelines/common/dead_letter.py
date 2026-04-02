from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any


def build_failed_record(
    pipeline_name: str,
    source_name: str,
    raw_record: dict[str, Any],
    error_message: str,
) -> dict[str, Any]:
    """Build a standardized failed record payload for dead-letter storage."""

    serialized = json.dumps(raw_record, sort_keys=True, default=str)
    record_hash = hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    return {
        "record_hash": record_hash,
        "pipeline_name": pipeline_name,
        "source_name": source_name,
        "raw_record": serialized,
        "error_message": error_message,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }
