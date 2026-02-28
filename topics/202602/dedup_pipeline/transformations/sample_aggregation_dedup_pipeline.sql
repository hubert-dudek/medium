-- Create the target table in the pipeline
CREATE OR REFRESH STREAMING TABLE dim_country_cdc;

-- Apply CDC
CREATE FLOW dim_country_flow
AS AUTO CDC INTO dim_country_cdc
FROM stream(country_cdc)
KEYS (country_id)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, sequence_num)
STORED AS SCD TYPE 1;
