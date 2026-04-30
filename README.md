# Real-Time Customer Sentiment and Operational Efficiency Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This repository delivers a data platform that correlates customer sentiment with operational delivery performance. The solution implements deterministic orchestration, layered warehouse modeling, idempotent ingestion, infrastructure as code, and API contract validation.

---

## Snapshot of Live Dashboard
<img width="1186" height="807" alt="image" src="https://github.com/user-attachments/assets/e8e64c4a-0dea-4596-8792-0154710f7800" />

---
## Overview
**Problem Statement:**

In most organizations, customer sentiment and operational delivery data evolve in parallel but disconnected systems. Customer support platforms continuously generate interaction signals such as tickets, chat transcripts, and social media mentions, while operations systems independently track order fulfillment, delivery timelines, and logistics failures. These systems are optimized for their own workflows rather than for shared analysis and do not naturally provide a unified view of customer experience and operational performance.

This creates operational friction across multiple roles:

- **Customer Experience Managers** struggle to identify which customers are becoming unhappy due to delayed or failed deliveries until escalation occurs, because sentiment signals are not automatically linked to operational events.
- **Operations Managers** cannot reliably see downstream customer impact of fulfillment delays in real time, making it difficult to prioritize remediation beyond SLA-based metrics.
- **Support Teams** lack context when handling tickets, often without knowing whether the customer is part of a broader operational incident.
- **Analysts and Reporting Teams** spend significant time manually joining exports from multiple systems to reconstruct a partial view of impacted customers.
- **Engineering Teams** face silent data quality risks where schema changes or ingestion failures propagate downstream without immediate visibility.

This leads to:

- **Delayed correlation of issues** – Links between operational failures (e.g., late deliveries) and sentiment degradation are typically identified hours or days after the event window.
- **Reactive decision-making** – Customer outreach and operational escalation occur after dissatisfaction has already manifested, rather than during early risk signals.
- **Manual reconciliation overhead** – Teams repeatedly assemble and align data across systems to identify affected customers and root causes.
- **Fragile reporting pipelines** – Inconsistent upstream data formats or pipeline failures can degrade trust in dashboards without immediate detection.

The core problem is the absence of a single, continuously updated and reliable pipeline that deterministically connects customer sentiment signals with operational delivery events at the data layer.

**The Solution:**

This project implements an end-to-end deterministic data platform that continuously ingests, validates, and correlates customer sentiment and operational data into a unified analytical model:

- **Ingestion** – Incremental pipelines (dlt-based) load customer interaction data (API-driven tickets, chat-like events) and operational order data (CSV-based fulfillment records) into a centralized BigQuery lakehouse in raw form.
- **Orchestration** – Kestra manages scheduled and dependency-aware workflows, ensuring ingestion, transformation, and dashboard refreshes execute in a controlled and repeatable manner.
- **Validation** – API contract checks (Postman/Newman) run before ingestion to prevent schema drift from silently corrupting downstream datasets.
- **Transformation** – SQL-based transformation models (bruin) standardize, clean, and join sentiment and operational datasets to produce business-ready entities such as delayed-order customers with negative sentiment signals and aggregated daily risk metrics.
- **Serving layer** – A generated dashboard exposes derived datasets (e.g., high-risk customers and sentiment vs SLA correlations) for operational decision-making.

**The Outcome:**

- **Reduced detection time for CX and Operations teams** – Customer risk signals (negative sentiment + operational delay correlation) are surfaced within a consistent 15–30 minute ingestion and transformation window, reducing reliance on delayed reporting cycles.
- **Actionable customer intelligence** – Teams can directly identify high-risk customers requiring proactive outreach instead of reconstructing insights manually across systems.
- **Elimination of manual data stitching** – Analysts and reporting teams no longer need to merge exports from multiple systems to understand operational impact on customers.
- **Operational visibility for leadership** – CX and Operations leaders gain a continuously updated view of how fulfillment performance is affecting customer sentiment trends.
- **Deterministic and observable pipelines** – All ingestion and transformation steps are idempotent, retry-aware, and visible through orchestration logs, reducing silent failure risk and improving trust in outputs.

The system establishes a consistent operational layer that allows CX, Operations, and Analytics stakeholders to detect and act on customer-impacting operational issues within defined latency bounds. It provides a foundation for transitioning toward fully event-driven real-time architectures while maintaining the reliability, cost constraints, and simplicity of batch and micro-batch processing at MVP stage.

---

## Enterprise Capabilities

- **Incremental & idempotent ingestion** using `dlt` merge semantics
- **Dead-letter handling** with a standardized schema and non-blocking pipeline continuation
- **Structured JSON logging** for traceable execution diagnostics
- **Kestra orchestration** with retries, timeouts, and dependency management
- **Newman pre-ingestion contract gating** to enforce API compatibility
- **Terraform-backed reproducibility** for core infrastructure (BigQuery, IAM, datasets)
- **Bruin SQL DAG execution** for layered warehouse modeling (raw → staging → core → enrichment → dashboard)
- **GitHub Actions CI/CD automation** for repeatable release gates and artifact builds
- **Centralized audit event logging** with compliance baseline checks
- **Synthetic load testing with thresholds** for parser and dedupe performance hardening
- **Production remote Terraform state governance** with encrypted backend patterns and bootstrap templates

---

## Architecture

```mermaid

flowchart LR
  subgraph Sources
    A1[Ticket API]
    A2[Orders CSV]
  end

  subgraph ContractGate
    C0[Postman Collection + Newman]
  end

  subgraph Ingestion
    B1[tickets_pipeline.py]
    B2[orders_pipeline.py]
    B3[raw_zone.failed_records]
  end

  subgraph Warehouse
    C1[raw_zone]
    C2[staging]
    C3[core]
    C4[enrichment]
    C5[dashboard]
  end

  subgraph Transform
    D1[stg_tickets]
    D2[stg_orders]
    D3[fact_sentiment_daily]
    D4[high_risk_customers]
    D5[sentiment_heatmap]
    D6[high_risk_table]
  end

  subgraph Orchestrator
    E1[ticket_ingestion.yml]
    E2[orders_ingestion.yml]
    E3[transform_dashboard.yml]
  end

  subgraph Infra
    I1[Terraform APIs]
    I2[Terraform Datasets]
    I3[Terraform Runtime IAM]
  end

  subgraph Delivery
    F1[dashboard/index.html]
  end

  A1 --> C0
  C0 --> E1
  E1 --> B1
  E2 --> B2
  B1 --> C1
  B2 --> C1
  B1 --> B3
  B2 --> B3
  C1 --> D1
  C1 --> D2
  D1 --> C2
  D2 --> C2
  D1 --> D3
  D2 --> D4
  D3 --> C3
  D4 --> C4
  D1 --> D5
  D4 --> D6
  D5 --> C5
  D6 --> C5
  E3 --> D1
  E3 --> D2
  C5 --> F1
  I1 --> C1
  I2 --> C2
  I3 --> B1

```

---

## Release Lifecycle View

```mermaid

flowchart TD
    A([Start]) --> B[Implement feature]
    B --> C[Run local validation gates]
    C --> D[Review against PR checklist]
    D --> E{All blocking gates pass?}

    E -- Yes --> F[Prepare release artifacts]
    F --> G[Go/No-Go decision]
    G --> H{Approved?}

    H -- Yes --> I[Release v1.0.0]
    H -- No --> J[Return to remediation]

    E -- No --> K[Fix defects and rerun checks]

    I --> L([Stop])
    J --> L
    K --> L

```

---

## Getting Started

### Prerequisites

- Python 3.9+
- Docker and Docker Compose
- Terraform (≥1.8) – optional, for infrastructure provisioning
- Google Cloud project with BigQuery enabled (for production deployment)

### Installation

```bash
git clone <repository-url>
cd customer-sentiment-platform
pip install -r requirements.txt
```

### Basic Validation

Run the full validation suite locally:

```bash
# Unit tests
python -m pytest -q

# Docker Compose configuration
docker compose config -q

# Terraform format and validation (no backend)
cd terraform
docker run --rm -v "${PWD}:/workspace" -w /workspace hashicorp/terraform:1.8.5 fmt -check -recursive
docker run --rm -v "${PWD}:/workspace" -w /workspace hashicorp/terraform:1.8.5 init -backend=false
docker run --rm -v "${PWD}:/workspace" -w /workspace hashicorp/terraform:1.8.5 validate
cd ..

# Start all services
docker compose up --build -d
```

### Optional Pipeline Execution

Run ingestion pipelines directly (bypassing Kestra):

```bash
python -m pipelines.tickets_pipeline
python -m pipelines.orders_pipeline
```

Execute the Bruin transformation DAG:

```bash
python -m pipelines.prepare_gcp_credentials
cd bruin
bruin run --config-file .bruin.yml
```

---

## Workflow Execution

```mermaid
sequenceDiagram
  autonumber
  participant K as Kestra
  participant N as Newman
  participant T as tickets_pipeline
  participant O as orders_pipeline
  participant BQ as BigQuery
  participant BR as Bruin
  participant UI as Dashboard

  K->>N: Validate ticket API contract
  N-->>K: Pass/Fail
  K->>T: Every 15 minutes
  T->>BQ: Merge raw tickets
  T->>BQ: Merge dead-letter rows

  K->>O: Daily run
  O->>BQ: Merge raw orders
  O->>BQ: Merge dead-letter rows

  K->>BR: Trigger transform flow
  BR->>BQ: Build staging/core/enrichment/dashboard assets
  BQ->>UI: Serve latest dashboard outputs

```

---

## Project Structure

```
.
├── pipelines/                # Ingestion workloads and shared helpers
├── kestra/flows/             # Scheduled and dependency-triggered workflows
├── .github/workflows/        # CI/CD and release automation workflows
├── bruin/assets/             # SQL assets (source, staging, core, enrichment, dashboard)
├── postman/                  # Live API contract collection and environments
├── terraform/                # Infrastructure baseline (APIs, datasets, IAM)
│   └── state_bootstrap/      # Remote Terraform state bucket governance bootstrap
├── scripts/                  # Compliance checks, synthetic load suite, and mock API utilities
├── dashboard/                # Static enterprise-facing operational UI (Nginx)
├── tests/                    # Unit tests for ingestion and reliability helpers
├── docs/                     # Operational runbook
├── PR_tasks.md               # Release code review checklist
├── activity_tracking.md      # Expectation vs. actual tracking
└── requirements.txt
```

---

## Validation and Testing Commands

| Command | Purpose |
|---------|---------|
| `pip install -r requirements.txt` | Install dependencies |
| `python -m pytest -q` | Run unit test suite |
| `docker compose config -q` | Validate Compose configuration |
| `docker run ... terraform fmt -check -recursive` | Check Terraform formatting |
| `docker run ... terraform validate` | Validate Terraform configuration |
| `newman run postman/live_ticket_api.postman_collection.json --env-var ticketsApiUrl=<url>` | Run API contract checks |
| `python scripts/compliance_check.py --output compliance_report.json` | Verify hardening compliance controls |
| `python scripts/synthetic_load_test.py --output synthetic_load_report.json` | Run high-volume synthetic performance thresholds |
| `docker compose up --build -d` | Start all services |

> **Production Terraform backend:**  
> `docker run ... terraform init -backend-config=backend.hcl`

---

## Reliability and Enterprise Controls

- **Incremental & idempotent ingestion** – `dlt` merge semantics prevent duplicate processing.
- **Structured JSON logging** – Enables traceable execution diagnostics.
- **Dead-letter handling** – Non-blocking pipeline continuation with a standardized failure schema.
- **Kestra retries & timeouts** – Resilient orchestration for transient failures.
- **Newman pre-ingestion contract gating** – Blocks ingestion on API contract regressions.
- **Terraform-backed reproducibility** – Infrastructure dependencies (BigQuery datasets, IAM) are versioned and repeatable.
- **Centralized audit logging** – Pipelines emit append-only JSONL audit events with run-level correlation IDs.
- **Compliance integration** – Repository control baseline is machine-validated via scripts/compliance_check.py.

---

## Performance Metrics

Baseline measurements from release preparation runs:

| Metric | Target | Observed | Status |
| :--- | :---: | :---: | :--- |
| Unit tests | 100% pass | 8/8 passed in 8.80s | ✅ Pass |
| Compose validation | Valid config | `docker compose config` passed | ✅ Pass |
| Terraform format & validate | No errors | fmt + validate passed | ✅ Pass |
| API contract gate | 100% assertions | 3/3 passed, avg 314ms | ✅ Pass |
| Bruin transform DAG | 100% assets | 8/8 succeeded, 21.445s | ✅ Pass |
| Ticket ingestion runtime | < 5 min | ~49s | ✅ Pass |
| Orders ingestion runtime | < 10 min | ~29s | ✅ Pass |

Post-hardening validation snapshot (2026-04-07):

| Metric | Target | Observed | Status |
| :--- | :---: | :---: | :--- |
| Unit tests | 100% pass | 12/12 passed in 8.51s | ✅ Pass |
| Compliance controls | 100% pass | 7/7 controls passed | ✅ Pass |
| Synthetic ticket normalize | <= 8.0s | 0.411s for 20,000 records | ✅ Pass |
| Synthetic order normalize | <= 15.0s | 0.836s for 50,000 records | ✅ Pass |
| Synthetic order dedupe | <= 8.0s | 0.156s for 50,000 records | ✅ Pass |
| Runtime bootstrap | Services start cleanly | `docker compose up --build -d` passed locally | ✅ Pass |

**Warehouse snapshot during end-to-end validation:**

- `raw_zone.raw_tickets`: 52 rows
- `raw_zone.raw_orders`: 99,441 rows
- `staging.stg_tickets`: 52 rows
- `staging.stg_orders`: 99,441 rows
- `core.fact_sentiment_daily`: 33 rows

---

## Scaling and Performance Analysis

### Current Profile

- **Ticket flow** – 15-minute cadence, low-latency incremental pulls.
- **Orders flow** – Daily bulk batch with schema-safe casting and deduplication.
- **Transform flow** – Compact SQL DAG with sub-minute to low-minute execution at MVP volumes.

### Identified Bottlenecks

- API response latency and pagination limits on the ticket source.
- BigQuery query cost and slot contention under higher dashboard refresh frequency.
- Transform DAG window growth with increased retention and historical backfills.

### Scale-Up Strategy

1. **Source/API layer** – Introduce explicit pagination loops, backpressure controls, and rate-limit–aware retries.
2. **Warehouse layer** – Optimize partitioning/clustering for query predicates, adopt incremental model patterns for larger windows, and implement cost guardrails.
3. **Orchestration layer** – Separate high-frequency ingestion from heavier transforms, add SLA-based freshness/failure alerting.
4. **Infrastructure layer** – Extend Terraform modules for production deployment, promote environment-specific `tfvars` with peer-reviewed changes.

---

## Troubleshooting Runbook

Refer to the detailed operational runbook: [`docs/runbook.md`](docs/runbook.md)

| Incident | Symptoms | Resolution |
| :--- | :--- | :--- |
| Ticket API unavailable | Connection refused in tickets pipeline | Validate endpoint with Newman/Postman; restore availability |
| Newman contract failure | `validate_ticket_api_contract` task fails | Inspect response contract drift; resolve upstream API/schema mismatch |
| Bruin ADC credential failure | Bruin cannot locate default credentials | Run `pipelines.prepare_gcp_credentials` and map ADC path |
| Terraform init/validate failure | Provider or schema validation errors | Re-run `init -backend=false`, then `fmt` and `validate` |

---

## Infrastructure as Code (Terraform)

The `terraform/` directory provisions:

- **Required APIs** – BigQuery and IAM
- **Datasets** – `raw_zone`, `staging`, `core`, `enrichment`, `dashboard`
- **Optional runtime service account** and BigQuery project roles
- Typed variables and outputs for environment-safe usage
- `state_bootstrap/` templates to provision a governed remote state bucket (versioning, retention, public-access prevention)

Use `backend.hcl.example` for production-safe backend initialization with KMS encryption and service-account impersonation fields.

---

## CI/CD Automation

Implemented workflows:

- `.github/workflows/ci.yml`
  - Unit tests
  - Terraform fmt/init/validate
  - Newman API contract gating against a local synthetic endpoint
  - Compliance baseline checks
  - Synthetic load threshold suite
- `.github/workflows/release.yml`
  - Automated release build gates and versioned Docker artifact export

All workflows are configured to avoid paid cloud execution by default.

---

## API Contract Governance (Postman)

Implemented assets:

- `postman/live_ticket_api.postman_collection.json`
- `postman/environments/local.postman_environment.json`
- `postman/environments/production.template.postman_environment.json`

**Governance model:** Contract checks run as a mandatory pre-flight step in the `ticket_ingestion` flow. Ingestion is blocked if the API contract regresses.

---

## Release Readiness Artifacts

- `PR_tasks.md` – Pull request and release sign-off checklist
- `activity_tracking.md` – Expectation vs. actual mapping for implementation plan
- `Workflow_procecess.md` – Development and production operational governance

---

## Contributing

All contributions must follow the process and mandatory validation gates documented in [`CONTRIBUTING.md`](CONTRIBUTING.md). Every pull request should include:

- Scope summary
- Validation evidence
- Risk and rollback notes
- Documentation updates for any behavioral changes

---

## License

This project is released under the **MIT License** – see the [`LICENSE`](LICENSE) file for details.

---

## Version

**Current release:** `1.3.1`  
Version sources: `VERSION` file and the README header.

**Versioning scheme (Semantic Versioning):**

- **MAJOR** – Breaking architecture or contract changes
- **MINOR** – Backward-compatible feature additions
- **PATCH** – Fixes, hardening, and documentation maintenance

---

## Known Deferred Enterprise Hardening Items

Current architecture remains optimized for deterministic batch and micro-batch processing; streaming capabilities are deferred for a future iteration.

- **Event streaming backbone** – Introduce an event streaming platform such as Apache Kafka or Google Cloud Pub/Sub to support continuous, event-driven ingestion in place of scheduled polling.
- **Stream processing layer** – Integrate a processing engine such as Apache Flink or Apache Spark Structured Streaming for low-latency, stateful computations and windowed aggregations.
- **Event-driven orchestration** – Extend Kestra to support event-based triggers alongside existing time-based schedules.
- **Streaming observability and reliability** – Add consumer lag monitoring, checkpointing visibility, and dead-letter handling at the streaming layer.
- **Schema evolution and governance** – Introduce schema registry and compatibility validation for event payloads, extending current contract governance practices.
