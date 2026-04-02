from __future__ import annotations

import os
import time
import json
from typing import Any

import dlt  # type: ignore[import-not-found]
import requests

from pipelines.common.bigquery_config import configure_dlt_bigquery_env
from pipelines.common.dead_letter import build_failed_record
from pipelines.common.logging_utils import get_logger
from pipelines.common.retry import retry_with_backoff
from pipelines.common.validation import parse_timestamp, validate_ticket_record

LOGGER = get_logger(__name__)

RAW_DATASET = os.getenv("RAW_DATASET", "raw_zone")
TICKETS_API_URL = os.getenv("TICKETS_API_URL", "http://localhost:9000/api/tickets")
TICKETS_API_TIMEOUT_SECONDS = int(os.getenv("TICKETS_API_TIMEOUT_SECONDS", "30"))
TICKETS_API_PAGE_LIMIT = int(os.getenv("TICKETS_API_PAGE_LIMIT", "1000"))
TICKETS_API_MAX_PAGES = int(os.getenv("TICKETS_API_MAX_PAGES", "50"))
INITIAL_TICKETS_CURSOR = os.getenv("INITIAL_TICKETS_CURSOR", "2026-01-01T00:00:00Z")
TICKETS_API_AUTH_TOKEN = os.getenv("TICKETS_API_AUTH_TOKEN", "").strip()
TICKETS_API_AUTH_HEADER = os.getenv("TICKETS_API_AUTH_HEADER", "Authorization")
TICKETS_API_KEY = os.getenv("TICKETS_API_KEY", "").strip()
TICKETS_API_KEY_HEADER = os.getenv("TICKETS_API_KEY_HEADER", "x-api-key")
TICKETS_API_EXTRA_HEADERS_JSON = os.getenv("TICKETS_API_EXTRA_HEADERS_JSON", "").strip()


def build_tickets_api_headers() -> dict[str, str]:
    """Build request headers for live ticket APIs with optional auth."""

    headers: dict[str, str] = {}
    if TICKETS_API_AUTH_TOKEN:
        headers[TICKETS_API_AUTH_HEADER] = f"Bearer {TICKETS_API_AUTH_TOKEN}"

    if TICKETS_API_KEY:
        headers[TICKETS_API_KEY_HEADER] = TICKETS_API_KEY

    if TICKETS_API_EXTRA_HEADERS_JSON:
        try:
            parsed_headers = json.loads(TICKETS_API_EXTRA_HEADERS_JSON)
            if isinstance(parsed_headers, dict):
                for key, value in parsed_headers.items():
                    headers[str(key)] = str(value)
            else:
                LOGGER.warning(
                    "Ignoring non-object TICKETS_API_EXTRA_HEADERS_JSON",
                    extra={"provided_type": type(parsed_headers).__name__},
                )
        except json.JSONDecodeError as exc:
            LOGGER.warning(
                "Ignoring invalid TICKETS_API_EXTRA_HEADERS_JSON",
                extra={"error": str(exc)},
            )

    return headers


def fetch_tickets_from_api(created_after: str | None) -> list[dict[str, Any]]:
    """Fetch ticket records from API with retry and bounded timeout."""

    params: dict[str, Any] = {"limit": TICKETS_API_PAGE_LIMIT}
    if created_after:
        params["created_after"] = created_after

    headers = build_tickets_api_headers()

    def _extract_page(payload: Any) -> tuple[list[dict[str, Any]], str | None, bool]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)], None, False

        if not isinstance(payload, dict):
            raise ValueError("Unsupported ticket API response shape")

        records_raw = payload.get("data")
        if not isinstance(records_raw, list):
            records_raw = payload.get("tickets")
        if not isinstance(records_raw, list):
            records_raw = payload.get("results")
        if not isinstance(records_raw, list):
            records_raw = payload.get("items")

        if not isinstance(records_raw, list):
            raise ValueError("Unsupported ticket API response shape")

        records = [item for item in records_raw if isinstance(item, dict)]

        next_token = payload.get("next_page_token") or payload.get("nextPageToken")
        if not next_token:
            next_token = payload.get("next_cursor") or payload.get("nextCursor")

        has_more = bool(payload.get("has_more") or payload.get("hasMore") or payload.get("more"))
        return records, str(next_token) if next_token else None, has_more

    all_records: list[dict[str, Any]] = []
    page_number = 1
    page_token: str | None = None

    while page_number <= TICKETS_API_MAX_PAGES:
        request_params = dict(params)
        if page_token:
            request_params["page_token"] = page_token
        elif page_number > 1:
            request_params["page"] = page_number

        def _request() -> requests.Response:
            response = requests.get(
                TICKETS_API_URL,
                params=request_params,
                headers=headers,
                timeout=TICKETS_API_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response

        response = retry_with_backoff(
            operation=_request,
            retries=3,
            base_delay_seconds=1.0,
            max_delay_seconds=8.0,
            retry_exceptions=(requests.RequestException,),
            logger=LOGGER,
            operation_name="tickets_api_request",
        )

        records, next_token, has_more = _extract_page(response.json())
        all_records.extend(records)

        if next_token:
            page_token = next_token
            page_number += 1
            continue

        if has_more and records:
            page_number += 1
            continue

        break

    if page_number > TICKETS_API_MAX_PAGES:
        LOGGER.warning(
            "Ticket API pagination reached configured cap",
            extra={"max_pages": TICKETS_API_MAX_PAGES, "records_collected": len(all_records)},
        )

    return all_records


def normalize_ticket_record(record: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    """Normalize nested API payload into warehouse-friendly columns."""

    is_valid, error = validate_ticket_record(record)
    if not is_valid:
        return None, error

    normalized = dict(record)
    normalized["ticket_id"] = str(normalized["ticket_id"])
    normalized["customer_id"] = str(normalized["customer_id"])
    normalized["created_at"] = parse_timestamp(str(normalized["created_at"])) .isoformat()
    normalized["sentiment_score"] = float(normalized["sentiment_score"])

    customer = normalized.pop("customer", None)
    if isinstance(customer, dict):
        normalized["customer_region"] = customer.get("region")
        normalized["customer_tier"] = customer.get("tier")

    metadata = normalized.pop("metadata", None)
    if isinstance(metadata, dict):
        normalized["channel"] = metadata.get("channel")
        normalized["priority"] = metadata.get("priority")

    return normalized, None


def failed_records_resource(rows: list[dict[str, Any]]) -> Any:
    @dlt.resource(
        name="failed_records",
        write_disposition="merge",
        primary_key="record_hash",
    )
    def _resource() -> Any:
        yield from rows

    return _resource


def tickets_resource(
    failed_records: list[dict[str, Any]],
    metrics: dict[str, int],
) -> Any:
    @dlt.resource(name="raw_tickets", write_disposition="merge", primary_key="ticket_id")
    def _resource(
        created_at=dlt.sources.incremental("created_at", initial_value=INITIAL_TICKETS_CURSOR),
    ) -> Any:
        created_after = created_at.last_value
        tickets = fetch_tickets_from_api(str(created_after) if created_after else None)

        LOGGER.info(
            "Ticket API response received",
            extra={
                "api_status": "success",
                "created_after": created_after,
                "records_received": len(tickets),
            },
        )

        for record in tickets:
            normalized, error = normalize_ticket_record(record)
            if error:
                failed_records.append(
                    build_failed_record(
                        pipeline_name="tickets_pipeline",
                        source_name="simulated_ticket_api",
                        raw_record=record,
                        error_message=error,
                    )
                )
                metrics["records_failed"] += 1
                continue

            if normalized is None:
                metrics["records_failed"] += 1
                continue

            metrics["records_processed"] += 1
            yield normalized

    return _resource


def run_pipeline() -> None:
    started = time.perf_counter()
    failed_records: list[dict[str, Any]] = []
    metrics = {"records_processed": 0, "records_failed": 0}

    configure_dlt_bigquery_env()

    pipeline = dlt.pipeline(
        pipeline_name="tickets_pipeline",
        destination="bigquery",
        dataset_name=RAW_DATASET,
    )

    load_info = pipeline.run(tickets_resource(failed_records, metrics)())

    if failed_records:
        pipeline.run(failed_records_resource(failed_records)())

    elapsed_seconds = round(time.perf_counter() - started, 3)
    LOGGER.info(
        "Ticket ingestion finished",
        extra={
            "pipeline": "tickets_pipeline",
            "records_processed": metrics["records_processed"],
            "records_failed": metrics["records_failed"],
            "elapsed_seconds": elapsed_seconds,
            "load_summary": str(load_info),
        },
    )


if __name__ == "__main__":
    run_pipeline()
