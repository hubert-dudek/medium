-- Databricks notebook source
-- DBTITLE 1,Cell 1
    -- Generate ONE-COLUMN synthetic text tables for FTS benchmark: 10b / 10,000,000,000 rows.
    -- Parameters: catalog, schema, run_id.
    -- The schema itself is managed as a DAB resource in schemas.yml.
    -- Tables created; each has exactly one user column: message STRING. The last row contains fixed tail markers for LIMIT 1 tests.
    --   fts_text_10b_scan   : no full-text index, baseline scan
    --   fts_text_10b_ngram  : ngram full-text index target for substring tests
    --   fts_text_10b_split  : split full-text index target for word tests

    USE CATALOG IDENTIFIER(:catalog);
        USE SCHEMA IDENTIFIER(:schema);



    CREATE OR REPLACE TABLE fts_text_10b_scan
    TBLPROPERTIES (
      'delta.enableRowTracking' = 'true',
      'delta.autoOptimize.optimizeWrite' = 'true'
    )
    AS
    WITH vocab AS (
      SELECT array(
  'authentication','authorization','billing','checkout','customer','databricks','delta','warehouse','serverless','sqlwarehouse',
  'request','response','timeout','latency','network','gateway','firewall','token','session','cookie',
  'spark','photon','cluster','notebook','dashboard','driver','connector','odbc','jdbc','sql',
  'scala','error','warning','information','debug','critical','security','audit','policy',
  'permission','catalog','schema','table','index','search','substring','word','tokenizer','ngram',
  'split','payload','json','variant','array','struct','ip','cidr','source','destination',
  'login','logout','refresh','incremental','full','manual','automatic','failed','success','retry',
  'throttled','queued','cached','remote','local','storage','compute','metastore','managed','iceberg',
  'delta_lake','event','trace','span','metric','alert','incident','probe','kafka',
  'streaming','batch','merge','optimize','vacuum','liquid','clustering','file','pruning',
  'selective','common','medium','rare','host','tenant','browser','mobile','linux','windows',
  'macos','safari','chrome','firefox','edge','bot','crawler','api','proxy','router',
  'orders','payments','profile','workflow','jobs','governance','lineage','quality','monitoring','observability'
) AS words
    ), generated AS (
      SELECT
        concat_ws(' ',
                    CASE WHEN id = 9999999999 THEN 'zztailneedlealpha zztailwordprobe' END,
CASE WHEN pmod(id, 100000) = 42 THEN 'zzqneedlealpha zztokenomega' END,
          CASE WHEN pmod(id, 1000) = 7 THEN 'zzmediumfalcon zzmarkersignal' END,
          CASE WHEN pmod(id, 20) = 0 THEN 'authentication failed token expired retry login' END,
          CASE WHEN pmod(id, 50000) = 99 THEN 'zzcidrprobe zzspecialipprobe network firewall gateway' END,
          CASE WHEN pmod(id, 25000) = 77 THEN 'zzauthcodeprobe zzsecuritysignal suspicious activity' END,
          array_join(
            transform(
              sequence(1, 8 + CAST(pmod(xxhash64(CAST(id AS STRING), 'len'), 72) AS INT)),
              i -> element_at(words, CAST(pmod(xxhash64(CAST(id AS STRING), CAST(i AS STRING)), size(words)) AS INT) + 1)
            ),
            ' '
          )
        ) AS message
      FROM range(0, 10000000000, 1, 8192) AS r
      CROSS JOIN vocab
    )
    SELECT message
    FROM generated;

    CREATE OR REPLACE TABLE fts_text_10b_ngram
    TBLPROPERTIES (
      'delta.enableRowTracking' = 'true',
      'delta.autoOptimize.optimizeWrite' = 'true'
    )
    AS SELECT message FROM fts_text_10b_scan;

    CREATE OR REPLACE TABLE fts_text_10b_split
    TBLPROPERTIES (
      'delta.enableRowTracking' = 'true',
      'delta.autoOptimize.optimizeWrite' = 'true'
    )
    AS SELECT message FROM fts_text_10b_scan;

    ANALYZE TABLE fts_text_10b_scan COMPUTE STATISTICS;
    ANALYZE TABLE fts_text_10b_ngram COMPUTE STATISTICS;
    ANALYZE TABLE fts_text_10b_split COMPUTE STATISTICS;

    INSERT INTO fts_benchmark_object_checks
    SELECT :run_id, current_timestamp(), '10b', 'fts_text_10b_scan', 'table', 'row_count', CAST(COUNT(*) AS STRING), '10000000000', CASE WHEN COUNT(*) = 10000000000 THEN 'PASS' ELSE 'FAIL' END
    FROM fts_text_10b_scan
    UNION ALL
    SELECT :run_id, current_timestamp(), '10b', 'fts_text_10b_ngram', 'table', 'row_count', CAST(COUNT(*) AS STRING), '10000000000', CASE WHEN COUNT(*) = 10000000000 THEN 'PASS' ELSE 'FAIL' END
    FROM fts_text_10b_ngram
    UNION ALL
    SELECT :run_id, current_timestamp(), '10b', 'fts_text_10b_split', 'table', 'row_count', CAST(COUNT(*) AS STRING), '10000000000', CASE WHEN COUNT(*) = 10000000000 THEN 'PASS' ELSE 'FAIL' END
    FROM fts_text_10b_split;

    SELECT 'fts_text_10b_scan' AS table_name, COUNT(*) AS row_count FROM fts_text_10b_scan
    UNION ALL SELECT 'fts_text_10b_ngram', COUNT(*) FROM fts_text_10b_ngram
    UNION ALL SELECT 'fts_text_10b_split', COUNT(*) FROM fts_text_10b_split;