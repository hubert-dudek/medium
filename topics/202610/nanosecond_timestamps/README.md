# Nanosecond timestamps: check the news

Added to the October 2026 topics on **2026-10-07** from the supplied Kent Marten LinkedIn screenshot about `TIMESTAMP(9)` and `TIMESTAMP_NTZ(9)`.

**Status:** documentation checked; live SQL warehouse probe blocked by a feature gate. The notebook's expected outputs have not yet been verified on enabled compute. This is a research/demo topic, not a completed performance benchmark.

## What the sources confirm

- The [August 2026 release notes](https://docs.databricks.com/aws/en/release-notes/product/2026/august#nanosecond-precision-timestamps-beta) announce the Beta on August 30 for Databricks SQL and Databricks Runtime 19+. October is the month this topic was added, not the original release month.
- [`TIMESTAMP(p)`](https://docs.databricks.com/aws/en/sql/language-manual/data-types/timestamp-type) accepts precision 6 through 9; omitting precision still means 6. `TIMESTAMP(9)` preserves nine fractional digits and represents an instant displayed using the session time zone.
- [`TIMESTAMP_NTZ(p)`](https://docs.databricks.com/aws/en/sql/language-manual/data-types/timestamp-ntz-type) has the same precision range but operates without time-zone interpretation. Precision 7-9 is Beta for both types.
- Delta storage needs the `timestampPrecision` table feature; existing NTZ tables also need `timestampNtz`. Protocol upgrades affect client compatibility. The type references currently exclude Iceberg tables, generated columns, and liquid clustering columns for these nanosecond types, and limit Delta/Parquet writes to years 1677-2262.
- The [Databricks announcement article](https://community.databricks.com/t5/technical-blog/unlocking-high-frequency-workloads-with-nanosecond-precision/ba-p/170873) describes open-source work across Spark, Delta Lake, and Iceberg. That does not establish support in every released version or every Databricks table format. The [Apache Spark source documentation](https://github.com/apache/spark/blob/master/docs/sql-ref-datatypes.md) provides a concrete upstream reference; verify release tags before making a version-specific claim.

The screenshot's short link is [lnkd.in/g22tTWwV](https://lnkd.in/g22tTWwV). Its redirect was not resolved during this check; the directly located announcement article above is a related primary source.

## Run the demo

Open [`src/01_check_precision.sql`](src/01_check_precision.sql) in Databricks and attach compute with nanosecond timestamp support enabled. It contains only SELECT statements over synthetic values: no table creation, protocol changes, or data writes.

Alternatively, use the included serverless Jobs bundle with a configured CLI profile:

```bash
databricks bundle validate -t dev -p DEFAULT
databricks bundle deploy -t dev -p DEFAULT
databricks bundle run nanosecond_timestamps -t dev -p DEFAULT
```

Run these commands from this topic folder. The serverless environment setting does not itself enable the timestamp Beta. The run should fail visibly at the precision probe if that feature is unavailable.

Expected checks on enabled compute:

1. Both precision-9 casts retain `.123456789`; the default NTZ cast retains `.123456`.
2. Native timestamp sorting produces `sensor_before`, `trigger`, `sensor_after` despite shuffled input.
3. Three distinct nanosecond values become one distinct microsecond value.
4. SQL assertions validate string round trips and equality of the same instant expressed with two UTC offsets. Successful assertions return NULL.

## Live verification: 2026-10-07

`databricks bundle validate -t dev -p DEFAULT` passed. This validates the bundle configuration, not the SQL runtime behavior.

On an existing 2X-Small serverless SQL warehouse using the Preview channel, the precision probe returned `FEATURE_NOT_ENABLED` (SQLSTATE `56038`) for nanosecond timestamp types. The error suggested `spark.sql.timestampNanosTypes.enabled=true`.

A separate session-level SET attempt returned `CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION` (SQLSTATE `42K0I`). Warehouse and workspace configuration were not changed. Select compute where the Beta is available, or resolve enablement with the Databricks account team, then rerun the notebook. The suggested setting is not a verified workaround for this SQL warehouse.

## Follow-up checks before an article

- [ ] Run the notebook on enabled compute and record runtime/channel and actual results.
- [ ] Verify Delta write/read preservation in a disposable table and inspect its protocol; test the reader versions used by downstream consumers.
- [ ] Check CSV/JSON/Parquet ingestion, explicit schemas, and client export boundaries. [PySpark's in-progress type documentation](https://apache.github.io/spark/api/python/reference/pyspark.sql/api/pyspark.sql.types.TimestampNTZNanosType.html) warns that Python `datetime` conversions truncate to microseconds; the demo therefore checks strings within SQL.
- [ ] Identify the relevant released Spark/Delta/Iceberg versions and reconcile their support with Databricks' current table limitations.
- [ ] Benchmark native timestamps against string/integer workarounds on equivalent data and compute before repeating the performance claim.
