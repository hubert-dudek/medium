---
name: sql-performance-review
description: Review and improve Databricks SQL queries for correctness, readability, and performance (joins, filters, aggregations, partition pruning). Use when someone pastes a SQL query, asks why it is slow, or requests a rewrite/optimization in Databricks SQL.
license: Proprietary. For internal team use.
compatibility: Designed for Databricks Assistant agent mode. Works best when the user provides the SQL query and (optionally) table names, row counts, or query profile details.
metadata:
  author: your-team
  version: "1.0"
---

# Databricks SQL performance review

Use this skill when optimizing or reviewing SQL in Databricks SQL.

## What to ask for (only if missing)
Ask up to 3 questions total:
1) The query text (if not provided)
2) The table(s) involved + their sizes (rough order of magnitude) OR the query profile / execution plan
3) The desired result constraints (correctness, exactness, latency SLA)

If the user can’t provide sizes/plan, proceed with best-effort heuristics and call out assumptions.

## Output format
Use the structure in `assets/sql-review-output.md`.

## Checklist
Use `references/sql-checklist.md` to ensure you cover the common performance levers:
- predicate pushdown / partition pruning
- join strategy and join keys
- avoid `SELECT *`
- minimize shuffles / wide aggregations
- use correct data types and avoid implicit casts
- reduce data scanned (pre-filter, semi-joins, EXISTS)

## Examples

**User:** “This query is slow in Databricks SQL. Can you optimize it?” (pastes query)  
**Assistant:** Provide issues, suggestions, and a rewritten query, plus next steps (EXPLAIN, add ZORDER, etc.).

## Edge cases
- If the query is *logically wrong* (duplicates from joins, missing filters), fix correctness first.
- If tables are Delta: suggest partitioning/ZORDER/OPTIMIZE only if it matches query patterns.
- If the user is in a governed environment: avoid suggestions that require elevated permissions unless noted.
