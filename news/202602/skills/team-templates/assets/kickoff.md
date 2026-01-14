# {{project_name}} — Project kickoff (1‑pager)

**Owner:** {{owner}}  
**Team / Org:** {{team}}  
**Stakeholders:** {{stakeholders}}  
**Date:** {{date}}  

## 1) Problem statement
What problem are we solving, for whom, and why now?

## 2) Goals and non‑goals
### Goals
- [ ] {{goal_1}}
- [ ] {{goal_2}}

### Non‑goals (explicitly out of scope)
- [ ] {{non_goal_1}}

## 3) Users & success metrics
**Primary users:** {{primary_users}}  
**Success metrics / KPIs:**  
- {{kpi_1}} (definition + target)
- {{kpi_2}}

## 4) Data sources & ownership
List source systems, tables, and owners.

| Source | Location (catalog.schema.table) | Owner | Refresh | Notes |
|---|---|---|---|---|
| {{source_1}} | {{table_1}} | {{owner_1}} | {{freshness_1}} | {{notes_1}} |

## 5) Proposed solution (high level)
Describe the pipeline + storage + serving surface.

- Ingestion: {{ingestion_summary}}
- Transform: {{transform_summary}}
- Serve: {{serving_summary}} (Dashboard / API / Tables)

## 6) Security & compliance
- Data classification: {{classification}} (PII/PHI/PCI/None/Unknown)
- Unity Catalog: {{uc_scope}}
- Access model: {{access_model}} (roles/groups)
- Retention: {{retention}}

## 7) Operational plan
- SLA/SLO: {{slo}}
- Monitoring: {{monitoring}}
- On-call / escalation: {{oncall}}

## 8) Milestones
- **M0 (Design approved):** {{m0_date}}
- **M1 (MVP):** {{m1_date}}
- **M2 (GA):** {{m2_date}}

## 9) Risks & open questions
### Risks
- {{risk_1}}
- {{risk_2}}

### Open questions
- [ ] {{question_1}}
- [ ] {{question_2}}

## 10) Definition of done
- [ ] Data sources documented + owners confirmed
- [ ] Data quality checks implemented (nulls, uniqueness, freshness, referential integrity)
- [ ] Access controls configured + reviewed
- [ ] Monitoring + alerting in place
- [ ] Documentation + runbook published
- [ ] Stakeholder sign‑off received
