from __future__ import annotations

from typing import Any

from pipelines.common.retry import retry_with_backoff
from pipelines.orders_pipeline import (
    is_newer_than_cursor,
    normalize_order_record,
    select_latest_orders,
)
from pipelines.tickets_pipeline import (
    build_tickets_api_headers,
    fetch_tickets_from_api,
    normalize_ticket_record,
)


class MockResponse:
    def __init__(self, payload: Any, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise ValueError(f"HTTP {self.status_code}")


def test_normalize_ticket_record_success() -> None:
    record = {
        "ticket_id": 101,
        "customer_id": "cust-1",
        "created_at": "2026-03-01T10:05:00Z",
        "sentiment_score": "-0.75",
        "customer": {"region": "south", "tier": "gold"},
        "metadata": {"channel": "chat", "priority": "urgent"},
    }

    normalized, error = normalize_ticket_record(record)

    assert error is None
    assert normalized is not None
    assert normalized["ticket_id"] == "101"
    assert normalized["customer_region"] == "south"
    assert normalized["priority"] == "urgent"


def test_normalize_ticket_record_missing_required_field() -> None:
    normalized, error = normalize_ticket_record({"ticket_id": "1"})

    assert normalized is None
    assert error is not None
    assert "Missing required field" in error


def test_normalize_order_record_success() -> None:
    record = {
        "order_id": "o-1",
        "customer_id": "c-9",
        "order_purchase_timestamp": "2026-03-01 10:05:00",
        "order_delivered_customer_date": "2026-03-03 10:05:00",
        "order_estimated_delivery_date": "2026-03-02 10:05:00",
        "price": "100.40",
        "freight_value": "5.10",
    }

    normalized, error = normalize_order_record(record)

    assert error is None
    assert normalized is not None
    assert normalized["is_delayed_order"] is True
    assert normalized["price"] == 100.40


def test_normalize_order_record_malformed_timestamp() -> None:
    record = {
        "order_id": "o-2",
        "customer_id": "c-11",
        "order_purchase_timestamp": "invalid-ts",
    }

    normalized, error = normalize_order_record(record)

    assert normalized is None
    assert error is not None
    assert "Unsupported timestamp format" in error


def test_fetch_tickets_from_api_with_mocked_response(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}

    def _mock_get(
        url: str,
        params: dict[str, Any],
        headers: dict[str, str],
        timeout: int,
    ) -> MockResponse:
        captured["url"] = url
        captured["params"] = params
        captured["headers"] = headers
        captured["timeout"] = timeout
        return MockResponse(
            {
                "tickets": [
                    {
                        "ticket_id": "10",
                        "customer_id": "cust-1",
                        "created_at": "2026-03-01T10:05:00Z",
                        "sentiment_score": -0.45,
                    }
                ]
            }
        )

    monkeypatch.setattr("pipelines.tickets_pipeline.requests.get", _mock_get)
    result = fetch_tickets_from_api("2026-03-01T00:00:00Z")

    assert len(result) == 1
    assert captured["params"]["created_after"] == "2026-03-01T00:00:00Z"
    assert isinstance(captured["headers"], dict)


def test_fetch_tickets_from_api_paginates_until_exhausted(monkeypatch: Any) -> None:
    calls: list[dict[str, Any]] = []

    def _mock_get(
        url: str,
        params: dict[str, Any],
        headers: dict[str, str],
        timeout: int,
    ) -> MockResponse:
        calls.append({"url": url, "params": dict(params), "headers": dict(headers), "timeout": timeout})

        if len(calls) == 1:
            return MockResponse(
                {
                    "data": [
                        {
                            "ticket_id": "100",
                            "customer_id": "cust-1",
                            "created_at": "2026-03-01T10:05:00Z",
                            "sentiment_score": -0.2,
                        }
                    ],
                    "has_more": True,
                }
            )

        return MockResponse(
            {
                "data": [
                    {
                        "ticket_id": "101",
                        "customer_id": "cust-2",
                        "created_at": "2026-03-01T10:06:00Z",
                        "sentiment_score": -0.4,
                    }
                ]
            }
        )

    monkeypatch.setattr("pipelines.tickets_pipeline.requests.get", _mock_get)
    result = fetch_tickets_from_api("2026-03-01T00:00:00Z")

    assert len(result) == 2
    assert calls[0]["params"]["limit"] > 0
    assert calls[1]["params"]["page"] == 2


def test_build_tickets_api_headers_from_overrides(monkeypatch: Any) -> None:
    monkeypatch.setattr("pipelines.tickets_pipeline.TICKETS_API_AUTH_TOKEN", "token-123")
    monkeypatch.setattr("pipelines.tickets_pipeline.TICKETS_API_AUTH_HEADER", "Authorization")
    monkeypatch.setattr("pipelines.tickets_pipeline.TICKETS_API_KEY", "api-key-456")
    monkeypatch.setattr("pipelines.tickets_pipeline.TICKETS_API_KEY_HEADER", "x-api-key")
    monkeypatch.setattr(
        "pipelines.tickets_pipeline.TICKETS_API_EXTRA_HEADERS_JSON",
        '{"x-tenant-id": "tenant-a"}',
    )

    headers = build_tickets_api_headers()

    assert headers["Authorization"] == "Bearer token-123"
    assert headers["x-api-key"] == "api-key-456"
    assert headers["x-tenant-id"] == "tenant-a"


def test_is_newer_than_cursor() -> None:
    assert is_newer_than_cursor("2026-03-02T00:00:00Z", "2026-03-01T00:00:00Z") is True
    assert is_newer_than_cursor("2026-03-01T00:00:00Z", "2026-03-01T00:00:00Z") is False


def test_select_latest_orders_keeps_newest_per_order_id() -> None:
    deduped = select_latest_orders(
        [
            {
                "order_id": "o-1",
                "order_purchase_timestamp": "2026-03-01T10:00:00+00:00",
            },
            {
                "order_id": "o-2",
                "order_purchase_timestamp": "2026-03-01T08:00:00+00:00",
            },
            {
                "order_id": "o-1",
                "order_purchase_timestamp": "2026-03-01T11:00:00+00:00",
            },
        ]
    )

    assert len(deduped) == 2
    assert [row["order_id"] for row in deduped] == ["o-2", "o-1"]
    assert deduped[-1]["order_purchase_timestamp"] == "2026-03-01T11:00:00+00:00"


def test_retry_with_backoff_eventually_succeeds() -> None:
    state = {"attempt": 0}

    def flaky_operation() -> str:
        state["attempt"] += 1
        if state["attempt"] < 3:
            raise ValueError("temporary failure")
        return "ok"

    result = retry_with_backoff(
        operation=flaky_operation,
        retries=3,
        base_delay_seconds=0.001,
        max_delay_seconds=0.001,
        retry_exceptions=(ValueError,),
    )

    assert result == "ok"
    assert state["attempt"] == 3
