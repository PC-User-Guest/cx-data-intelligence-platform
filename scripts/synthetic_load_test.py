from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.orders_pipeline import normalize_order_record, select_latest_orders
from pipelines.tickets_pipeline import normalize_ticket_record


def _build_ticket(index: int) -> dict[str, Any]:
    return {
        "ticket_id": f"ticket-{index}",
        "customer_id": f"customer-{index % 2000}",
        "created_at": "2026-04-01T10:00:00Z",
        "sentiment_score": -0.4,
        "customer": {"region": "eu", "tier": "gold"},
        "metadata": {"channel": "chat", "priority": "high"},
    }


def _build_order(index: int) -> dict[str, Any]:
    return {
        "order_id": f"order-{index // 2}",
        "customer_id": f"customer-{index % 2000}",
        "order_purchase_timestamp": f"2026-03-{(index % 28) + 1:02d}T10:00:00Z",
        "order_delivered_customer_date": f"2026-03-{(index % 28) + 2:02d}T10:00:00Z",
        "order_estimated_delivery_date": f"2026-03-{(index % 28) + 3:02d}T10:00:00Z",
        "price": "99.99",
        "freight_value": "10.50",
    }


def _benchmark_tickets(total: int) -> tuple[float, int]:
    started = time.perf_counter()
    valid = 0
    for idx in range(total):
        normalized, error = normalize_ticket_record(_build_ticket(idx))
        if normalized and error is None:
            valid += 1
    return time.perf_counter() - started, valid


def _benchmark_orders(total: int) -> tuple[float, float, int]:
    started_normalize = time.perf_counter()
    normalized_orders: list[dict[str, Any]] = []
    for idx in range(total):
        normalized, error = normalize_order_record(_build_order(idx))
        if normalized and error is None:
            normalized_orders.append(normalized)
    normalize_seconds = time.perf_counter() - started_normalize

    started_dedupe = time.perf_counter()
    deduped = select_latest_orders(normalized_orders)
    dedupe_seconds = time.perf_counter() - started_dedupe

    return normalize_seconds, dedupe_seconds, len(deduped)


def run_suite() -> tuple[bool, dict[str, Any]]:
    ticket_records = int(os.getenv("SYNTHETIC_TICKET_RECORDS", "20000"))
    order_records = int(os.getenv("SYNTHETIC_ORDER_RECORDS", "50000"))

    max_ticket_seconds = float(os.getenv("SYNTHETIC_MAX_TICKET_SECONDS", "8.0"))
    max_order_seconds = float(os.getenv("SYNTHETIC_MAX_ORDER_SECONDS", "15.0"))
    max_dedupe_seconds = float(os.getenv("SYNTHETIC_MAX_DEDUPE_SECONDS", "8.0"))

    ticket_seconds, valid_tickets = _benchmark_tickets(ticket_records)
    order_seconds, dedupe_seconds, deduped_orders = _benchmark_orders(order_records)

    checks = {
        "tickets_within_threshold": ticket_seconds <= max_ticket_seconds,
        "orders_within_threshold": order_seconds <= max_order_seconds,
        "dedupe_within_threshold": dedupe_seconds <= max_dedupe_seconds,
        "all_tickets_valid": valid_tickets == ticket_records,
    }

    report = {
        "ticket_records": ticket_records,
        "order_records": order_records,
        "ticket_seconds": round(ticket_seconds, 3),
        "order_normalize_seconds": round(order_seconds, 3),
        "order_dedupe_seconds": round(dedupe_seconds, 3),
        "valid_tickets": valid_tickets,
        "deduped_orders": deduped_orders,
        "thresholds": {
            "max_ticket_seconds": max_ticket_seconds,
            "max_order_seconds": max_order_seconds,
            "max_dedupe_seconds": max_dedupe_seconds,
        },
        "checks": checks,
    }

    return all(checks.values()), report


def main() -> None:
    parser = argparse.ArgumentParser(description="Run synthetic load tests with thresholds.")
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    passed, report = run_suite()
    serialized = json.dumps(report, indent=2)
    print(serialized)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write(serialized)
            handle.write("\n")

    if not passed:
        sys.exit(1)


if __name__ == "__main__":
    main()
