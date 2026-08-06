-- Use case 6
-- A provider may reissue an entire daily settlement file. Replacing the selected
-- provider/date slice also removes transactions absent from the accepted version.

CREATE STREAMING TABLE settlement_canonical
TBLPROPERTIES (
  'pipelines.reset.allowed' = 'false'
)
FLOW REPLACE WHERE
  provider = :provider
  AND business_date >= DATE_SUB(CURRENT_DATE(), CAST(:restatement_lookback_days AS INT))
BY NAME
WITH ranked_manifests AS (
  SELECT
    provider,
    business_date,
    file_version,
    accepted_at,
    file_checksum,
    ROW_NUMBER() OVER (
      PARTITION BY provider, business_date
      ORDER BY accepted_at DESC, file_version DESC
    ) AS manifest_rank
  FROM IDENTIFIER(:source_catalog || '.' || :source_schema || '.settlement_manifest')
  WHERE status = 'ACCEPTED'
),
latest_manifest AS (
  SELECT
    provider,
    business_date,
    file_version,
    accepted_at,
    file_checksum
  FROM ranked_manifests
  WHERE manifest_rank = 1
)
SELECT
  s.provider,
  s.business_date,
  s.transaction_id,
  s.gross_amount,
  s.fee_amount,
  CAST(s.gross_amount - s.fee_amount AS DECIMAL(18, 2)) AS net_amount,
  TRY_VARIANT_GET(s.attributes, '$.status', 'STRING') AS transaction_status,
  TRY_VARIANT_GET(s.attributes, '$.card_country', 'STRING') AS card_country,
  s.attributes,
  m.file_version AS source_file_version,
  m.accepted_at AS source_file_accepted_at,
  m.file_checksum AS source_file_checksum
FROM IDENTIFIER(:source_catalog || '.' || :source_schema || '.settlement_rows') AS s
JOIN latest_manifest AS m
  ON s.provider = m.provider
 AND s.business_date = m.business_date
 AND s.file_version = m.file_version
WHERE s.provider = :provider;
