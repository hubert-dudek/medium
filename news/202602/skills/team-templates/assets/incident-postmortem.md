# Incident postmortem — {{incident_title}}

**Date:** {{date}}  
**Severity:** {{severity}} (Sev-1/2/3)  
**Status:** {{status}} (Draft/Final)  
**Incident commander:** {{ic}}  
**Owners:** {{owners}}  

## 1) Summary (executive)
In 3–6 sentences: what happened, impact, duration, and current status.

## 2) Customer / business impact
- **Who was impacted:** {{impacted_users}}
- **What was impacted:** {{impacted_systems}}
- **Impact window:** {{start_time}} – {{end_time}} ({{timezone}})
- **Quantified impact:** {{impact_quantified}} (failed jobs, delayed dashboards, $$, etc.)

## 3) Detection
- **How did we learn about it?** {{detection_method}} (alert, user report, etc.)
- **Time to detect:** {{ttd}}
- **What alert(s) fired / didn’t fire?** {{alerts}}

## 4) Timeline (local time)
| Time | Event |
|---|---|
| {{t1}} | {{e1}} |
| {{t2}} | {{e2}} |
| {{t3}} | {{e3}} |

## 5) Root cause analysis
### What happened (technical)
Explain the failure mode and contributing factors.

### Why it happened (causal chain)
- Primary cause: {{primary_cause}}
- Contributing factors: {{contrib_1}}, {{contrib_2}}

### Where we got lucky / defense-in-depth gaps
- {{gap_1}}

## 6) Resolution & recovery
- **Mitigation:** {{mitigation}}
- **Fix:** {{fix}}
- **Time to mitigate:** {{ttm}}
- **Time to recover:** {{ttr}}

## 7) Corrective actions
### Immediate (0–2 days)
- [ ] {{action_1}} — owner: {{owner_1}} — due: {{due_1}}

### Short term (this sprint)
- [ ] {{action_2}} — owner: {{owner_2}} — due: {{due_2}}

### Long term (roadmap)
- [ ] {{action_3}} — owner: {{owner_3}} — due: {{due_3}}

## 8) What went well / what didn’t
### Went well
- {{well_1}}

### Didn’t go well
- {{not_well_1}}

## 9) Appendices
- Links: runbooks, job runs, dashboards, tickets, PRs
- Logs / screenshots (if applicable)
