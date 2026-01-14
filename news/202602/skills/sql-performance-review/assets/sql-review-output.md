# SQL performance review

## 1) Summary
- Goal: {{goal}}
- Assumptions: {{assumptions}}

## 2) Key issues found
1. {{issue_1}}
2. {{issue_2}}

## 3) Suggested improvements (ranked)
### High impact
- {{suggestion_hi_1}}
- {{suggestion_hi_2}}

### Medium impact
- {{suggestion_med_1}}

### Low impact / style
- {{suggestion_low_1}}

## 4) Rewritten query (proposed)

```sql
-- explain the intent with comments
{{rewritten_query}}
```

## 5) Verification plan
- [ ] Compare results between old and new query (row counts, checksum on key cols)
- [ ] Run EXPLAIN / check query profile for bytes read + shuffle + spill
- [ ] If still slow: capture sample stats (table sizes, partitions, file sizes)

## 6) Optional table layout actions (Delta)
Only include if applicable and safe in the user environment:
- {{table_action_1}}
- {{table_action_2}}
