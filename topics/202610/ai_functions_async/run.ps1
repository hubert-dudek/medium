databricks fs cp requests.jsonl dbfs:/Volumes/sbx_test_data/news_ai_functions_20261113/batch_files/requests.jsonl --overwrite --profile AWS-DEV
$batch = databricks api post /api/2.0/ai-functions/batches --json '@create_batch.json' --profile AWS-DEV | ConvertFrom-Json
databricks api get "/api/2.0/ai-functions/batches/$($batch.name)" --profile AWS-DEV
