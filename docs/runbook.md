# Troubleshooting Runbook

## Scope

This runbook covers common operational issues for ingestion, orchestration, transformations, IaC validation, and API contract checks.

## 1. Ticket Ingestion Fails with Connection Refused

Symptoms:

- tickets pipeline retries and exits with HTTP connection refused

Likely cause:

- TICKETS_API_URL endpoint unavailable

Actions:

- Verify endpoint health with curl or Postman
- Run Newman collection against the same endpoint
- Confirm host resolution from runtime context (host vs. container network)

## 2. Newman Contract Check Fails in Kestra

Symptoms:

- validate_ticket_api_contract task fails before ingestion

Likely cause:

- API contract changed or endpoint inaccessible

Actions:

- Run postman/live_ticket_api.postman_collection.json manually
- Compare response shape against expected keys
- Coordinate with API owner before bypassing contract gate
- Confirm Kestra basic-auth credentials are correctly configured in environment variables

## 3. Bruin Transform Fails with ADC Credentials Error

Symptoms:

- Bruin run indicates missing ADC credentials

Likely cause:

- GOOGLE_APPLICATION_CREDENTIALS not prepared in runtime context

Actions:

- Execute python -m pipelines.prepare_gcp_credentials
- Ensure credential file is mapped to expected ADC path in containerized runs
- Confirm service account fields are present in .env

## 4. Terraform Validate or Init Fails

Symptoms:

- Provider resolution or validation errors

Likely cause:

- Missing provider lock, version mismatch, or invalid variable set

Actions:

- For local validation only: terraform init -backend=false
- For shared environments: terraform init -backend-config=backend.hcl
- Run terraform fmt -check -recursive
- Run terraform validate
- Verify tfvars values and provider constraints
- Confirm backend.hcl uses approved state bucket and prefix

## 5. Dashboard Data Looks Stale

Symptoms:

- dashboard tables are not refreshed as expected

Likely cause:

- Upstream flow failure or delayed ingestion cadence

Actions:

- Check Kestra execution history for ingestion and transform flows
- Validate raw table freshness in BigQuery
- Re-run transform flow after upstream recovery

## 6. Elevated Failed Record Ratio

Symptoms:

- failed_records volume spikes unexpectedly

Likely cause:

- Upstream data contract drift or malformed source payloads

Actions:

- Inspect dead-letter raw_record and error_message fields
- Identify top validation failure patterns
- Patch validation mapping or coordinate source-side correction

## 7. Docker Build Instability During Bruin Install

Symptoms:

- Bruin binary download fails intermittently during build

Likely cause:

- Transient network/SSL interruption

Actions:

- Use retry-enabled Dockerfile download pattern
- Retry build after transient issue clears
- Confirm pinned BRUIN_VERSION remains available

## 8. Release Validation Sequence

- python -m pytest -q
- docker compose config -q
- Terraform fmt/init/validate
- Newman contract run
- python scripts/compliance_check.py --output compliance_report.json
- python scripts/synthetic_load_test.py --output synthetic_load_report.json
- Optional end-to-end data run and warehouse row count verification

## 9. Centralized Audit and Compliance

Controls:

- Pipelines emit append-only audit events to AUDIT_LOG_PATH in JSONL format
- Every run carries run_id correlation for traceability across start/completion/failure events
- Compliance baseline can be validated with scripts/compliance_check.py

Actions:

- Ensure AUDIT_ENABLED=true in environment
- Ensure AUDIT_LOG_PATH points to a persisted mount in containerized runtime
- Run python scripts/compliance_check.py --output compliance_report.json
- Review controls with failed status before promotion

## 10. Synthetic Load Threshold Suite

Purpose:

- Validate high-volume parser and dedupe behavior without paid infrastructure

Actions:

- Run python scripts/synthetic_load_test.py --output synthetic_load_report.json
- Override thresholds with environment variables when profiling hardware variability:
	- SYNTHETIC_TICKET_RECORDS
	- SYNTHETIC_ORDER_RECORDS
	- SYNTHETIC_MAX_TICKET_SECONDS
	- SYNTHETIC_MAX_ORDER_SECONDS
	- SYNTHETIC_MAX_DEDUPE_SECONDS

## 11. Remote Terraform State Governance

State bucket bootstrap:

- cd terraform/state_bootstrap
- terraform init
- terraform plan -var-file=terraform.tfvars
- terraform apply -var-file=terraform.tfvars

Runtime initialization of main Terraform stack:

- cd terraform
- cp backend.hcl.example backend.hcl
- Update bucket, prefix, kms_encryption_key, and impersonate_service_account
- terraform init -backend-config=backend.hcl

## Escalation Path

- Data pipeline logic issue: Data Engineering owner
- SQL model correctness issue: Analytics Engineering owner
- IaC/runtime issue: Platform/DevOps owner
- KPI or prioritization issue: Technical Product Lead
- Delivery/schedule issue: Project Manager
