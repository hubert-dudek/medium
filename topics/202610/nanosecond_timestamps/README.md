# Nanosecond timestamps

Two short SQL examples compare the same value before and now:

| Type | Before: default precision | Now: precision 9 |
| --- | --- | --- |
| TIMESTAMP | 2026-10-07 09:30:00.123456 | 2026-10-07 09:30:00.123456789 |
| TIMESTAMP_NTZ | 2026-10-07 09:30:00.123456 | 2026-10-07 09:30:00.123456789 |

These are expected results on enabled compute. The default remains microsecond precision; specify `(9)` to retain nanoseconds. The example uses no tables.

Open [src/01_check_precision.sql](src/01_check_precision.sql), or run the included bundle:

```bash
databricks bundle validate -t dev -p DEFAULT
databricks bundle deploy -t dev -p DEFAULT
databricks bundle run nanosecond_timestamps -t dev -p DEFAULT
```

On 2026-10-07, bundle validation passed. The SQL warehouse probe returned `FEATURE_NOT_ENABLED`; a session SET of `spark.sql.timestampNanosTypes.enabled` returned `CONFIG_NOT_AVAILABLE`. Notebook execution is checked separately.

Sources: [TIMESTAMP](https://docs.databricks.com/aws/en/sql/language-manual/data-types/timestamp-type), [TIMESTAMP_NTZ](https://docs.databricks.com/aws/en/sql/language-manual/data-types/timestamp-ntz-type), [Beta release notes](https://docs.databricks.com/aws/en/release-notes/product/2026/august#nanosecond-precision-timestamps-beta).
