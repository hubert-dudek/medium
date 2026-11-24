param(
    [string]$Cli = 'C:/git/cli/databricks.exe',
    [string]$Profile = 'AWS-DEV',
    [switch]$UseSeedData
)

$tableName = 'samples.tpch.orders'
$schemaName = 'sbx_test_data.default'
if ($UseSeedData) {
    $schemaName = 'sbx_test_data.news_202621_quality'
    $tableName = "$schemaName.sales"
}

$table = & $Cli tables get $tableName --profile $Profile -o json
if ($LASTEXITCODE) { throw "Cannot read table $tableName" }
$schema = & $Cli schemas get $schemaName --profile $Profile -o json
if ($LASTEXITCODE) { throw "Cannot read schema $schemaName" }
$tableId = ($table | ConvertFrom-Json).table_id
$schemaId = ($schema | ConvertFrom-Json).schema_id
if (!$tableId -or !$schemaId) { throw 'The lookup did not return both Unity Catalog IDs.' }

Push-Location $PSScriptRoot
try {
    $bundle = & $Cli bundle validate --profile $Profile --var "table_id=$tableId,output_schema_id=$schemaId" -o json
    if ($LASTEXITCODE) { throw 'Bundle validation failed.' }
    Write-Output "Table: $tableName   Output schema: $schemaName"
    ($bundle | ConvertFrom-Json).resources.data_quality_monitors | ConvertTo-Json -Depth 6
} finally {
    Pop-Location
}
