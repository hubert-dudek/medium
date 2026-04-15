---
name: enterprise-naming-convention
description: Apply enterprise naming standards to Databricks SQL DDL, especially CREATE TABLE statements. Use when a user asks to standardize, fix, or review schema, table, or column names.
---

# Enterprise naming convention

Use this skill whenever the user asks to create or rewrite Databricks SQL so object names follow the enterprise naming convention.

## Rules

1. Use lowercase snake_case for schemas, tables, and columns.
2. Schema names must follow `<domain>_<layer>` where layer is one of `raw`, `refined`, or `serving`.
3. If a schema only contains a domain name, convert it to `<domain>_serving`.
4. Table names must start with `tbl_`.
5. Column naming rules:
   - identifiers end with `_id`
   - date columns end with `_dt`
   - timestamp columns end with `_ts`
   - boolean columns start with `is_`
   - monetary amount columns end with `_amt`
6. Remove spaces, hyphens, camelCase, and PascalCase from identifiers.
7. Preserve data types, nullability, comments, table properties, partitions, and business meaning.
8. Do not change SQL logic unless required to rename objects and columns.
9. Return only the corrected SQL unless the user explicitly asks for an explanation.
10. If a rename is ambiguous, make the safest business-preserving choice and add a one-line SQL comment explaining the assumption.

## Example

Input:

```sql
CREATE TABLE Sales.Orders (
  OrderID BIGINT,
  CustomerID BIGINT,
  OrderDate DATE,
  TotalAmount DECIMAL(12,2),
  IsActive BOOLEAN,
  CreatedAt TIMESTAMP
);
```
