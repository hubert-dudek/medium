# Lakehouse pipeline design — {{pipeline_name}}

**Owner:** {{owner}}  
**Team:** {{team}}  
**Date:** {{date}}  
**Status:** {{status}} (Draft/Proposed/Approved)  

## 1) Overview
### Goal
{{goal}}

### Scope
- In scope: {{in_scope}}
- Out of scope: {{out_of_scope}}

### Consumers
- Primary consumers: {{consumers}}
- Downstream dependencies: {{downstream}}

## 2) Source(s)
| Source | Type | Connectivity | Cadence | Notes |
|---|---|---|---|---|
| {{source_1}} | {{source_type_1}} | {{connectivity_1}} | {{cadence_1}} | {{notes_1}} |

### Data characteristics
- Estimated volume: {{volume}}
- Late data: {{late_data_policy}}
- Schema evolution expectations: {{schema_evolution}}

## 3) Storage & governance
- Target catalog/schema: `{{catalog}}.{{schema}}`
- Tables:
  - Bronze: `{{catalog}}.{{schema}}.{{bronze_table}}`
  - Silver: `{{catalog}}.{{schema}}.{{silver_table}}`
  - Gold: `{{catalog}}.{{schema}}.{{gold_table}}`
- Data classification: {{classification}} (PII/PHI/PCI/None/Unknown)
- Retention: {{retention_policy}}
- Access groups:
  - Readers: {{read_groups}}
  - Writers: {{write_groups}}

## 4) Processing approach
### Orchestration choice
- [ ] DLT
- [ ] Databricks Workflows Jobs
- [ ] Other: {{other}}

**Rationale:** {{rationale}}

### Batch / streaming / CDC
- Mode: {{mode}}
- Incremental strategy:
  - Watermark column: {{watermark_column}}
  - CDC keys: {{primary_key}}
  - Merge strategy: {{merge_strategy}}

### Idempotency & replay
How re-runs/backfills behave and how duplicates are prevented.

## 5) Transformations
### Bronze (raw ingestion)
- Parsing/normalization: {{bronze_parsing}}
- Expectations (light): {{bronze_expectations}}

### Silver (clean + conformed)
- Business rules: {{silver_rules}}
- Dedup logic: {{dedup_logic}}
- Expectations (strict): {{silver_expectations}}

### Gold (serving)
- Aggregations/semantic layer: {{gold_model}}
- SLO for consumers: {{consumer_slo}}

## 6) Data quality plan
- Checks: nulls, uniqueness, referential integrity, freshness
- Failure policy: {{dq_failure_policy}} (fail pipeline / quarantine / warn)

## 7) Performance & cost
- Partitioning: {{partitioning}}
- ZORDER columns: {{zorder}}
- OPTIMIZE cadence: {{optimize_cadence}}
- Cluster/serverless choice: {{compute_choice}}
- Cost guardrails: {{cost_guardrails}}

## 8) Observability & operations
- Metrics: {{metrics}}
- Alerts: {{alerts}}
- Runbook link: {{runbook_link}}
- On-call / escalation: {{oncall}}

## 9) Deployment & change management
- Environments: dev / stage / prod
- Release process: {{release_process}}
- Backfill procedure: {{backfill_procedure}}
- Rollback plan: {{rollback_plan}}

## 10) Decisions
- {{decision_1}}
- {{decision_2}}

## 11) Open questions
- [ ] {{question_1}}
- [ ] {{question_2}}

## 12) Info needed (if any)
- {{missing_1}}
- {{missing_2}}
