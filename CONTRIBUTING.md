# Contributing Guide

## Purpose

This project follows a reliability-first workflow designed for data platform systems with orchestration, warehouse modeling, and infrastructure as code.

## 1. Branching and Change Scope

- Create feature branches from main using feature/<short-topic> naming
- Keep each pull request focused on one functional objective
- Do not mix infrastructure, data model, and UI changes without explicit rationale

## 2. Commit Standards

- Use clear, action-oriented commit messages
- Include context for risk-sensitive changes (ingestion logic, orchestration, IAM)
- Keep commit history readable and reviewable

## 3. Required Local Validation Before PR

- python -m pytest -q
- docker compose config -q
- terraform fmt -check -recursive (terraform folder)
- terraform validate (terraform folder)
- newman run for ticket API contract checks

## 4. Data and Security Rules

- Never commit .env, credentials, or service account keys
- Keep .gitignore protections intact for runtime artifacts and Terraform state
- Use environment variables for endpoint and auth configuration
- Preserve dead-letter and retry pathways in ingestion code

## 5. PR Content Requirements

- Clear problem statement and scope boundaries
- Summary of files changed and architectural impact
- Validation evidence (commands + outcomes)
- Rollback notes for risky changes
- Updated documentation if behavior or operations changed

## 6. Review Focus Areas

- Idempotency and incremental correctness
- Orchestration retries, triggers, and timeout safety
- SQL DAG dependency integrity
- Data quality and contract compatibility
- Security and secrets hygiene

## 7. Merge Policy

- At least one reviewer from data engineering
- Principal Data Engineer sign-off for architecture-impacting changes
- Technical Product Lead sign-off for KPI/business-rule changes
- Merge only when all blocking validations are green
