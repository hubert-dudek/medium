---
name: lakehouse-pipeline-design
description: Create a Databricks lakehouse pipeline design doc (bronze/silver/gold, DLT or Jobs), including SLAs, data quality, Unity Catalog governance, monitoring, and an implementation checklist. Use when designing or reviewing ETL/ELT pipelines, DLT pipelines, streaming ingestion, CDC, or batch jobs on Databricks.
license: Proprietary. For internal team use.
compatibility: Designed for Databricks Assistant agent mode in Databricks workspaces.
metadata:
  author: your-team
  version: "1.0"
---

# Lakehouse pipeline design (Databricks)

Use this skill when someone asks for a **pipeline design**, **DLT design**, **ETL plan**, **CDC ingestion**, or a **review** of an existing pipeline.

## Deliverables

When activated, produce **at least**:
1. A filled design doc based on `assets/pipeline-design-doc.md`
2. A short, actionable implementation checklist (you can reuse `references/pipeline-checklist.md`)

Optionally (only if asked): a code skeleton (PySpark / SQL / DLT) that matches the design.

## Minimal inputs (ask only what’s missing)

Ask up to 3 questions total. Prefer defaults.

- Source type: files / DB / API / Kafka / etc.
- Mode: batch / streaming / CDC
- Target: tables (catalog.schema.*) and consumers (dashboards, ML, downstream jobs)
- Volume + SLA: rows/day, latency/freshness SLO, cost constraints
- Governance: PII? UC catalogs/schemas, access groups

## Design guidance (what to include)

- **Architecture:** bronze → silver → gold; DLT vs Jobs; where to enforce quality
- **Incremental strategy:** watermarking, MERGE for CDC, idempotency
- **Delta table design:** partitioning, ZORDER, OPTIMIZE/VACUUM policy
- **Quality checks:** schema validation, null/unique, freshness, anomaly checks
- **Observability:** metrics, logs, expectations failures, alerts, runbooks
- **Backfills:** replay strategy, how to reprocess safely, versioning
- **Security:** UC permissions, row/column filtering if needed, secrets management
- **Operational:** retries, SLAs, escalation, deployment strategy

## Output rules

- Put concrete decisions in a “Decisions” section and unknowns in “Open questions”.
- If details are missing, keep placeholders like `{{...}}` and add an “Info needed” section.
- Keep the doc concise; link to `references/pipeline-checklist.md` when you need long checklists.

## Examples

**User:** “Design a DLT pipeline that ingests Salesforce accounts daily and publishes a gold table for dashboards.”  
**Output:** Design doc + checklist + optional DLT skeleton.

**User:** “Review our existing silver-to-gold job for performance and reliability.”  
**Output:** Review-style design doc: risks, improvements, and prioritized actions.

## Edge cases

- Streaming sources: include checkpointing, schema evolution handling, and late data policy.
- Regulated data: include classification, retention, and UC policy controls.
- Multi-tenant tables: call out tenant key, partitioning, and access controls.
