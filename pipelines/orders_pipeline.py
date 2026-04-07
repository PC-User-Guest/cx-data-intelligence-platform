from __future__ import annotations

import csv
import io
import os
import time
from pathlib import Path
from typing import Any

import dlt  # type: ignore[import-not-found]
import requests

from pipelines.common.bigquery_config import configure_dlt_bigquery_env
from pipelines.common.audit import emit_audit_event, new_run_id
from pipelines.common.dead_letter import build_failed_record
from pipelines.common.logging_utils import get_logger
from pipelines.common.retry import retry_with_backoff
from pipelines.common.validation import parse_timestamp, validate_order_record

LOGGER = get_logger(__name__)

RAW_DATASET = os.getenv("RAW_DATASET", "raw_zone")
ORDERS_CSV_SOURCE = os.getenv("ORDERS_CSV_SOURCE", "")
ORDERS_CSV_TIMEOUT_SECONDS = int(os.getenv("ORDERS_CSV_TIMEOUT_SECONDS", "60"))
INITIAL_ORDERS_CURSOR = os.getenv("INITIAL_ORDERS_CURSOR", "2016-01-01T00:00:00Z")


def fetch_csv_text(source: str) -> str:
    """Return CSV content from URL or local file path."""

    if source.startswith("http://") or source.startswith("https://"):
        def _request() -> requests.Response:
            response = requests.get(source, timeout=ORDERS_CSV_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response

        response = retry_with_backoff(
            operation=_request,
            retries=3,
            base_delay_seconds=1.0,
            max_delay_seconds=8.0,
            retry_exceptions=(requests.RequestException,),
            logger=LOGGER,
            operation_name="orders_csv_download",
        )
        return response.text

    source_path = Path(source)
    if not source_path.exists():
        raise FileNotFoundError(f"Orders CSV path does not exist: {source}")

    return source_path.read_text(encoding="utf-8")


def parse_orders_csv(csv_text: str) -> list[dict[str, Any]]:
    """Parse CSV text into row dictionaries."""

    reader = csv.DictReader(io.StringIO(csv_text))
    return [dict(row) for row in reader if row]


def normalize_order_record(record: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    is_valid, error = validate_order_record(record)
    if not is_valid:
        return None, error

    normalized = dict(record)
    normalized["order_id"] = str(normalized.get("order_id", "")).strip()
    normalized["customer_id"] = str(normalized.get("customer_id", "")).strip()
    normalized["order_purchase_timestamp"] = parse_timestamp(
        str(normalized["order_purchase_timestamp"])
    ).isoformat()

    price = normalized.get("price")
    freight_value = normalized.get("freight_value")
    normalized["price"] = float(price) if price not in (None, "") else None
    normalized["freight_value"] = float(freight_value) if freight_value not in (None, "") else None

    delivered_at = normalized.get("order_delivered_customer_date")
    estimated_at = normalized.get("order_estimated_delivery_date")
    delayed_order = None
    if delivered_at and estimated_at:
        delayed_order = parse_timestamp(str(delivered_at)) > parse_timestamp(str(estimated_at))
    normalized["is_delayed_order"] = delayed_order

    return normalized, None


def is_newer_than_cursor(candidate_timestamp: str, cursor_timestamp: str | None) -> bool:
    """Return True when candidate timestamp is newer than incremental cursor."""
    if not cursor_timestamp:
        return True
    return parse_timestamp(candidate_timestamp) > parse_timestamp(cursor_timestamp)


def select_latest_orders(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep only the latest order row per order_id and return in timestamp order."""
    latest_by_order_id: dict[str, dict[str, Any]] = {}

    for row in rows:
        order_id = str(row["order_id"])
        existing = latest_by_order_id.get(order_id)
        if existing is None:
            latest_by_order_id[order_id] = row
            continue

        current_ts = parse_timestamp(str(row["order_purchase_timestamp"]))
        existing_ts = parse_timestamp(str(existing["order_purchase_timestamp"]))
        if current_ts > existing_ts:
            latest_by_order_id[order_id] = row

    return sorted(
        latest_by_order_id.values(),
        key=lambda order: parse_timestamp(str(order["order_purchase_timestamp"])),
    )


def failed_records_resource(rows: list[dict[str, Any]]) -> Any:
    @dlt.resource(
        name="failed_records",
        write_disposition="merge",
        primary_key="record_hash",
    )
    def _resource() -> Any:
        yield from rows

    return _resource


def orders_resource(
    failed_records: list[dict[str, Any]],
    metrics: dict[str, int],
) -> Any:
    @dlt.resource(
        name="raw_orders",
        write_disposition="merge",
        primary_key="order_id",
        columns={
            "price": {"data_type": "double"},
            "freight_value": {"data_type": "double"},
        },
    )
    def _resource(
        order_purchase_timestamp=dlt.sources.incremental(
            "order_purchase_timestamp",
            initial_value=INITIAL_ORDERS_CURSOR,
        ),
    ) -> Any:
        if not ORDERS_CSV_SOURCE:
            raise ValueError("ORDERS_CSV_SOURCE must be configured")

        csv_text = fetch_csv_text(ORDERS_CSV_SOURCE)
        rows = parse_orders_csv(csv_text)

        last_cursor = order_purchase_timestamp.last_value

        LOGGER.info(
            "Orders CSV loaded",
            extra={
                "source": ORDERS_CSV_SOURCE,
                "rows_received": len(rows),
                "cursor": str(last_cursor),
            },
        )

        normalized_rows: list[dict[str, Any]] = []
        for raw_row in rows:
            normalized, error = normalize_order_record(raw_row)
            if error:
                failed_records.append(
                    build_failed_record(
                        pipeline_name="orders_pipeline",
                        source_name="olist_orders_csv",
                        raw_record=raw_row,
                        error_message=error,
                    )
                )
                metrics["rows_failed"] += 1
                continue

            if normalized is None:
                metrics["rows_failed"] += 1
                continue

            normalized_rows.append(normalized)

        for normalized in select_latest_orders(normalized_rows):

            purchase_ts = str(normalized["order_purchase_timestamp"])
            if not is_newer_than_cursor(purchase_ts, str(last_cursor) if last_cursor else None):
                continue

            metrics["rows_processed"] += 1
            yield normalized

    return _resource


def run_pipeline() -> None:
    started = time.perf_counter()
    failed_records: list[dict[str, Any]] = []
    metrics = {"rows_processed": 0, "rows_failed": 0}
    run_id = new_run_id()

    emit_audit_event(
        pipeline_name="orders_pipeline",
        run_id=run_id,
        event_type="pipeline_started",
        status="started",
        compliance_tags=["soc2-cc7", "iso27001-a12"],
        details={"dataset": RAW_DATASET, "source": ORDERS_CSV_SOURCE},
    )

    configure_dlt_bigquery_env()

    pipeline = dlt.pipeline(
        pipeline_name="orders_pipeline",
        destination="bigquery",
        dataset_name=RAW_DATASET,
    )

    try:
        load_info = pipeline.run(orders_resource(failed_records, metrics)())

        if failed_records:
            pipeline.run(failed_records_resource(failed_records)())

        elapsed_seconds = round(time.perf_counter() - started, 3)
        LOGGER.info(
            "Orders ingestion finished",
            extra={
                "pipeline": "orders_pipeline",
                "run_id": run_id,
                "rows_processed": metrics["rows_processed"],
                "rows_failed": metrics["rows_failed"],
                "elapsed_seconds": elapsed_seconds,
                "load_summary": str(load_info),
            },
        )

        emit_audit_event(
            pipeline_name="orders_pipeline",
            run_id=run_id,
            event_type="pipeline_completed",
            status="success",
            compliance_tags=["soc2-cc7", "iso27001-a12"],
            details={
                "rows_processed": metrics["rows_processed"],
                "rows_failed": metrics["rows_failed"],
                "elapsed_seconds": elapsed_seconds,
            },
        )
    except Exception as exc:
        emit_audit_event(
            pipeline_name="orders_pipeline",
            run_id=run_id,
            event_type="pipeline_completed",
            status="failure",
            compliance_tags=["soc2-cc7", "iso27001-a12"],
            details={"error": str(exc), "rows_failed": metrics["rows_failed"]},
        )
        raise


if __name__ == "__main__":
    run_pipeline()
