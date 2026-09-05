# Simplified Databricks Tag Automations experiment

This bundle creates a deliberately small test estate for an article about governed **Tag Automations**.

## What changed from the larger version

- One schema only: `tags_experiment`.
- Five tiny Delta tables.
- Two custom governed tags only:
  - `ta_demo_pii` — key-only.
  - `ta_demo_documentation` — allowed value `missing`.
- Three automation scenarios:
  1. roll PII from columns up to their table,
  2. flag missing table descriptions,
  3. deprecate tables whose name contains `_legacy` or whose last query was more than 90 days ago.
- No volume scenario.
- One optional ABAC notebook.

## Why there is no volume/file cleanup test

Tag Automations target a Unity Catalog **volume object**, not individual files inside a volume. The available volume conditions are metadata conditions on the volume, such as its name, description, tags, owner, created date, and last updated date. Table-only usage conditions and column-tag conditions are not offered for volumes. Therefore this project does not pretend that Tag Automations can remove or tag old files one by one.

## Dataset

| Table | Description | Seeded `ta_demo_pii` columns | Expected table PII tag | Expected documentation tag | Expected deprecated |
|---|---|---|---:|---:|---:|
| `customers` | Yes | `full_name`, `email_address`, `phone_number` | Yes | No | No |
| `orders` | Yes | None | No | No | No |
| `products` | Yes | None | No | No | No |
| `support_cases` | No | `requester_email` | Yes | Yes | No |
| `customer_export_legacy` | No | `full_name`, `email_address` | Yes | Yes | Yes |

Deterministic expected counts:

- PII roll-up: **3** tables.
- Missing documentation: **2** tables.
- Deprecated by `_legacy` name: **1** table.

The inactivity branch of the deprecation rule is supported by the product, but a fresh bundle cannot manufacture historical query telemetry. Use an existing table that was last queried more than 90 days ago to prove that branch separately.

## Files

```text
.
├── databricks.yml
├── resources
│   └── tag_automation_jobs.yml
└── src
    ├── 00_setup.py
    ├── 01_inspect_and_verify.py
    ├── 02_abac_demo.py
    └── 99_cleanup.py
```

## Prerequisites

For the selected catalog, the automation creator needs `USE CATALOG`, `USE SCHEMA`, `APPLY TAG`, and `MANAGE`. The creator also needs `ASSIGN` on each governed tag used by an automation.

The setup notebook creates governed tags with SQL, which requires Databricks Runtime 18.1 or newer and account-level `CREATE` permission. A user who creates a governed tag receives `MANAGE`, but `ASSIGN` is separate. Running the setup as an account admin is the simplest test path.

For the deprecation action, grant the test principal `ASSIGN` on the built-in system governed tag `system.certification_status`.

If the Automations tab is absent, enable **Tag automations** under the workspace **Previews** page.

## Deploy and build the dataset

The default catalog is `main`; the default schema is `tags_experiment`.

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev tag_automation_setup
```

The setup job first creates the dataset and then runs the inspection notebook. Its first output is suitable for the baseline screenshot.

To use another catalog:

```bash
databricks bundle deploy -t dev --var="catalog_name=my_catalog"
databricks bundle run -t dev tag_automation_setup
```

> The default `reset_schema=true` drops and recreates `catalog.tags_experiment`. Use only a dedicated test schema.

# Automation 1 — roll PII up to the table

Tag Automations do not inspect data values to discover PII. This bundle seeds column tags first, representing classifications created manually or by Data Classification.

### Genie prompt

```text
In catalog main and schema tags_experiment, target tables. When any column has the governed tag ta_demo_pii, add the same key-only governed tag ta_demo_pii to the table. Use a manual schedule.
```

### Form to verify

- Scope: `main.tags_experiment`.
- Target: Tables.
- Condition: Column tag is `ta_demo_pii`.
- Action: Add key-only tag `ta_demo_pii`.
- Schedule: Manual.
- Expected dry-run matches: **3**.

Expected tables:

```text
customers
support_cases
customer_export_legacy
```

Take screenshots of the prompt, generated definition, and the three dry-run matches. Then enable and run the automation.

# Automation 2 — flag missing documentation

### Genie prompt

```text
In catalog main and schema tags_experiment, target tables. When the table description does not exist, add governed tag ta_demo_documentation with value missing. Use a manual schedule.
```

### Form to verify

- Condition: Description does not exist.
- Action: Add `ta_demo_documentation=missing`.
- Expected dry-run matches: **2**.

Expected tables:

```text
support_cases
customer_export_legacy
```

Enable and run the automation after reviewing the dry run.

# Automation 3 — deprecate legacy or stale tables

Yes, the current feature supports inactivity conditions for tables. The condition list includes both **Last queried** and read/write query counts.

### Genie prompt

```text
In catalog main and schema tags_experiment, target tables. Match any of these conditions: the table name contains _legacy, or the table was last queried more than 90 days ago. Add system.certification_status with value deprecated. Use a manual schedule.
```

### Form to verify

- Condition mode: Match any.
- Condition 1: Name contains `_legacy`.
- Condition 2: Last queried more than `90` days ago.
- Action: Add `system.certification_status=deprecated`.
- Expected deterministic match in this fresh schema: **1**.

Expected deterministic table:

```text
customer_export_legacy
```

The second branch needs real historical usage metadata. Do not backdate a data column and present it as proof: the automation evaluates table metadata, not dates stored in the rows.

# Verify after all three live runs

```bash
databricks bundle run -t dev tag_automation_inspect
```

The compact summary should show:

```text
PII roll-up                    3 / 3  PASS
Missing documentation         2 / 2  PASS
Deprecated by _legacy name    1 / 1  PASS
```

# Optional ABAC notebook

The ABAC example uses the same tag at two levels:

```text
PII column tags
      ↓
Tag Automation adds ta_demo_pii to the table
      ↓
ABAC WHEN has_tag('ta_demo_pii')
      ↓
ABAC MATCH COLUMNS has_tag('ta_demo_pii')
      ↓
Mask only the tagged columns
```

By default the job prints the SQL and reads `ABAC_POLICY_DEFINITIONS` without creating a policy:

```bash
databricks bundle run -t dev tag_automation_abac
```

To create the policy, set a test user or group in `databricks.yml` or override the variables when deploying:

```bash
databricks bundle deploy -t dev --var="apply_abac_policy=true,abac_principal=tag_automation_readers"

databricks bundle run -t dev tag_automation_abac
```

The policy is attached to the `tags_experiment` schema. It applies only to tables carrying the table-level `ta_demo_pii` tag and masks only columns carrying the column-level `ta_demo_pii` tag.

For a strong article screenshot, show these `ABAC_POLICY_DEFINITIONS` fields:

- `POLICY_NAME`
- `POLICY_TYPE`
- `ON_SECURABLE_TYPE`
- `TO_PRINCIPALS`
- `WHEN_CONDITION`
- `MATCH_COLUMNS`

# Suggested screenshot sequence

1. Tag Automations preview enabled.
2. Successful bundle setup job.
3. Five-table inventory in `tags_experiment`.
4. Seeded `ta_demo_pii` column tags.
5. PII Genie prompt and generated rule.
6. PII dry run with three matches.
7. Missing-documentation dry run with two matches.
8. Deprecation rule showing **Match any**, `_legacy`, and **Last queried > 90 days**.
9. Deprecated icon/tag on `customer_export_legacy`.
10. Verification summary showing `3/3`, `2/2`, and `1/1`.
11. Optional ABAC policy definition showing both `WHEN_CONDITION` and `MATCH_COLUMNS`.

# Cleanup

```bash
databricks bundle run -t dev tag_automation_cleanup
```

This drops the schema but retains the two account-level governed-tag definitions by default. To drop those definitions as well, deploy with `drop_governed_tags_on_cleanup=true` and rerun cleanup.

## Official documentation used

- Tag Automations: https://docs.databricks.com/aws/en/admin/governed-tags/automate-tag-assignment
- Governed tags: https://docs.databricks.com/aws/en/admin/governed-tags/
- Create governed tags: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-governed-tag
- Create ABAC policies: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-policy
- ABAC policy definitions: https://docs.databricks.com/aws/en/sql/language-manual/information-schema/abac_policy_definitions
