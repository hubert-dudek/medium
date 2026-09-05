# Screenshot plan

## Before creating automations

1. **Preview toggle** — show Tag Automations enabled in Workspace Settings > Previews.
2. **Setup job** — show the two successful tasks: `setup_dataset` and `inspect_baseline`.
3. **Dataset inventory** — capture the five rows from the first output in `01_inspect_and_verify.py`.
4. **PII column metadata** — capture the six seeded column-tag assignments.
5. **No table tags yet** — capture the empty or incomplete table-tag output and the `PENDING` summary.

## PII automation

6. Paste the Genie prompt.
7. Capture the generated scope, `Column tag` condition, and key-only `ta_demo_pii` action.
8. Capture the dry-run details with exactly three tables.
9. After enabling the automation, capture `customers` in Catalog Explorer with its new table-level PII tag.

## Documentation automation

10. Capture `Description does not exist` and action `ta_demo_documentation=missing`.
11. Capture the dry run with `support_cases` and `customer_export_legacy`.

## Deprecation automation

12. Capture **Match any** with:
    - Name contains `_legacy`.
    - Last queried more than 90 days ago.
13. Capture action `system.certification_status=deprecated`.
14. Capture the dry run. The fresh DAB dataset guarantees one match through the name condition.
15. Capture the deprecated marker on `customer_export_legacy`.

## Verification and ABAC

16. Run `tag_automation_inspect` and capture the summary with `3/3`, `2/2`, and `1/1`.
17. Run the ABAC notebook in read-only mode and capture the generated policy SQL.
18. After creating the policy for a test principal, capture `ABAC_POLICY_DEFINITIONS`, especially `WHEN_CONDITION` and `MATCH_COLUMNS`.
19. Run the sample customer query as the test principal and capture masked values.
