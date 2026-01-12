# SQL review checklist (Databricks SQL)

## Correctness
- [ ] Joins: are join keys correct and complete?
- [ ] Duplicates: does the join cardinality create fanout?
- [ ] Aggregations: are GROUP BY columns correct?
- [ ] Time filters: timezone and inclusive/exclusive boundaries correct?

## Performance (big wins)
- [ ] Filter early: predicates applied before joins/aggregations where possible
- [ ] Partition pruning: filters on partition columns present (if partitioned)
- [ ] Reduce scan: select only required columns; avoid SELECT *
- [ ] Join order: join smaller, filtered tables first
- [ ] Use semi-joins: EXISTS / IN for filtering instead of joining wide tables
- [ ] Avoid expensive UDFs in hot paths
- [ ] Avoid implicit casts on join/filter columns (can block optimizations)

## Table/layout suggestions (Delta)
- [ ] If query pattern is stable, consider ZORDER on common filters/joins
- [ ] Avoid over-partitioning; partition only when it prunes meaningfully
- [ ] OPTIMIZE cadence aligns with cost/latency needs

## Next-step diagnostics
- [ ] Run EXPLAIN and inspect shuffle/exchange nodes
- [ ] Check query profile: bytes read, spilled bytes, shuffle read/write, skew
- [ ] Validate row counts at each step (CTEs) to catch fanout/skew
