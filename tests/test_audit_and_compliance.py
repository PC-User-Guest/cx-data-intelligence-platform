from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pipelines.common.audit import emit_audit_event
from scripts.compliance_check import evaluate_controls


def test_emit_audit_event_writes_jsonl(tmp_path: Any, monkeypatch: Any) -> None:
    target = tmp_path / "audit" / "events.jsonl"
    monkeypatch.setattr("pipelines.common.audit.AUDIT_ENABLED", True)
    monkeypatch.setattr("pipelines.common.audit.AUDIT_LOG_PATH", str(target))

    emit_audit_event(
        pipeline_name="tickets_pipeline",
        run_id="run-123",
        event_type="pipeline_completed",
        status="success",
        details={"records_processed": 10},
        compliance_tags=["soc2-cc7"],
    )

    lines = target.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1

    payload = json.loads(lines[0])
    assert payload["pipeline_name"] == "tickets_pipeline"
    assert payload["run_id"] == "run-123"
    assert payload["status"] == "success"
    assert payload["details"]["records_processed"] == 10


def test_compliance_report_contains_expected_controls() -> None:
    report = evaluate_controls(Path("."))

    assert report["total_controls"] >= 7
    assert report["failed_controls"] == 0
    assert report["status"] == "pass"