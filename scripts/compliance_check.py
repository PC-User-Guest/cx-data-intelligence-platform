from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _contains(path: Path, needle: str) -> bool:
    if not path.exists():
        return False
    return needle in path.read_text(encoding="utf-8")


def evaluate_controls(repo_root: Path) -> dict[str, Any]:
    controls: list[dict[str, Any]] = []

    ci_workflow = repo_root / ".github" / "workflows" / "ci.yml"
    release_workflow = repo_root / ".github" / "workflows" / "release.yml"
    audit_module = repo_root / "pipelines" / "common" / "audit.py"
    synthetic_suite = repo_root / "scripts" / "synthetic_load_test.py"
    backend_hcl = repo_root / "terraform" / "backend.hcl.example"
    compose_file = repo_root / "docker-compose.yml"

    controls.append(
        {
            "control_id": "CI-001",
            "description": "Continuous integration workflow is present",
            "passed": ci_workflow.exists(),
            "evidence": str(ci_workflow.relative_to(repo_root)),
        }
    )
    controls.append(
        {
            "control_id": "CD-001",
            "description": "Release workflow automation is present",
            "passed": release_workflow.exists(),
            "evidence": str(release_workflow.relative_to(repo_root)),
        }
    )
    controls.append(
        {
            "control_id": "AUDIT-001",
            "description": "Centralized audit event module exists",
            "passed": audit_module.exists(),
            "evidence": str(audit_module.relative_to(repo_root)),
        }
    )
    controls.append(
        {
            "control_id": "AUDIT-002",
            "description": "Audit module persists append-only JSONL records",
            "passed": _contains(audit_module, '"a", encoding="utf-8"') and _contains(audit_module, "AUDIT_LOG_PATH"),
            "evidence": str(audit_module.relative_to(repo_root)),
        }
    )
    controls.append(
        {
            "control_id": "PERF-001",
            "description": "Synthetic load test suite exists",
            "passed": synthetic_suite.exists(),
            "evidence": str(synthetic_suite.relative_to(repo_root)),
        }
    )
    controls.append(
        {
            "control_id": "IAC-001",
            "description": "Terraform remote backend has governed configuration fields",
            "passed": _contains(backend_hcl, "bucket =") and _contains(backend_hcl, "prefix =") and _contains(backend_hcl, "kms_encryption_key ="),
            "evidence": str(backend_hcl.relative_to(repo_root)),
        }
    )
    controls.append(
        {
            "control_id": "SEC-001",
            "description": "Kestra basic auth remains enabled by default",
            "passed": _contains(compose_file, "basic-auth") and _contains(compose_file, "enabled: true"),
            "evidence": str(compose_file.relative_to(repo_root)),
        }
    )

    passed = [item for item in controls if item["passed"]]
    failed = [item for item in controls if not item["passed"]]

    return {
        "control_set": "enterprise-hardening-v1",
        "total_controls": len(controls),
        "passed_controls": len(passed),
        "failed_controls": len(failed),
        "status": "pass" if not failed else "fail",
        "controls": controls,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run enterprise hardening compliance checks.")
    parser.add_argument("--repo-root", default=".")
    parser.add_argument("--output", default="compliance_report.json")
    args = parser.parse_args()

    report = evaluate_controls(Path(args.repo_root).resolve())
    output_path = Path(args.output)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))

    if report["status"] != "pass":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
