from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipelines.common.logging_utils import get_logger

LOGGER = get_logger("audit")

AUDIT_ENABLED = os.getenv("AUDIT_ENABLED", "true").strip().lower() in {"1", "true", "yes"}
AUDIT_LOG_PATH = os.getenv("AUDIT_LOG_PATH", "logs/audit/audit_events.jsonl").strip()
AUDIT_CONTROL_SET = os.getenv("AUDIT_CONTROL_SET", "enterprise-baseline-v1").strip()


def new_run_id() -> str:
    """Return a unique run identifier that ties all audit events together."""

    return str(uuid.uuid4())


def emit_audit_event(
    *,
    pipeline_name: str,
    run_id: str,
    event_type: str,
    status: str,
    details: dict[str, Any] | None = None,
    compliance_tags: list[str] | None = None,
) -> None:
    """Emit a structured audit event to JSON logs and an append-only audit file."""

    if not AUDIT_ENABLED:
        return

    payload: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "event_type": event_type,
        "status": status,
        "control_set": AUDIT_CONTROL_SET,
        "compliance_tags": compliance_tags or [],
        "details": details or {},
    }

    LOGGER.info("Audit event", extra=payload)

    try:
        target = Path(AUDIT_LOG_PATH)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, default=str))
            handle.write("\n")
    except OSError as exc:
        LOGGER.warning(
            "Unable to persist audit event to file",
            extra={"audit_log_path": AUDIT_LOG_PATH, "error": str(exc)},
        )
