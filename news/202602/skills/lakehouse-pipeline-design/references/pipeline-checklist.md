# Pipeline implementation checklist (Databricks)

Use this as a final pass before launch.

## Governance
- [ ] Tables registered in Unity Catalog with owner/contact
- [ ] Least-privilege groups defined (readers/writers/admins)
- [ ] Data classification reviewed (PII/PHI/PCI) and retention policy set

## Data quality
- [ ] Primary key defined (or documented as “none”)
- [ ] Null/unique checks for key columns
- [ ] Freshness check aligned to SLA
- [ ] Expectations failures are surfaced (DLT expectations / logged + alerted)

## Reliability
- [ ] Idempotent re-runs (safe to retry)
- [ ] Backfill plan documented and tested
- [ ] Retry policy + alerting configured
- [ ] Runbook exists with escalation path

## Performance & cost
- [ ] Partitioning justified (avoid over-partitioning)
- [ ] ZORDER / clustering plan for key query patterns
- [ ] OPTIMIZE/VACUUM cadence set (and won’t break time travel requirements)
- [ ] Compute choice is justified (serverless vs clusters)

## Observability
- [ ] Pipeline/job metrics dashboard or logs exist
- [ ] Alerts route to a real on-call destination
- [ ] SLOs defined (freshness, success rate)

## Documentation
- [ ] Data contract / schema docs published
- [ ] Consumers and dependencies listed
- [ ] Ownership and support hours clear
