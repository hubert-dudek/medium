param(
    [Parameter(Mandatory)][string]$WarehouseId,
    [ValidatePattern('^[A-Za-z_][A-Za-z0-9_]*$')]
    [string]$Catalog = 'sbx_test_data',
    [string]$Profile = 'AWS-DEV',
    [string]$Databricks = 'databricks'
)

$ErrorActionPreference = 'Stop'

function Invoke-Sql([string]$Statement) {
    $body = @{
        warehouse_id = $WarehouseId
        statement = $Statement
        wait_timeout = '50s'
        on_wait_timeout = 'CANCEL'
    } | ConvertTo-Json -Compress
    $json = & $Databricks api post /api/2.0/sql/statements `
        --json $body --profile $Profile --output json
    if ($LASTEXITCODE -ne 0) { throw 'SQL request failed' }
    $result = $json | ConvertFrom-Json
    if ($result.status.state -ne 'SUCCEEDED') {
        throw ($result.status | ConvertTo-Json -Compress)
    }
    $result
}

$schemaName = 'news_undrop_' + [guid]::NewGuid().ToString('N').Substring(0, 8)
$schemaFullName = "$Catalog.$schemaName"
$volumeName = "$schemaFullName.restore_me"
$schemaCreated = $false

try {
    $null = Invoke-Sql "CREATE SCHEMA $schemaFullName"
    $schemaCreated = $true
    $null = Invoke-Sql "CREATE VOLUME $volumeName"
    Write-Host "DROP VOLUME $volumeName"
    $null = Invoke-Sql "DROP VOLUME $volumeName"
    Write-Host "SHOW VOLUMES DROPPED IN $schemaFullName"
    $dropped = Invoke-Sql "SHOW VOLUMES DROPPED IN $schemaFullName"
    $idColumn = $dropped.manifest.schema.columns | Where-Object name -eq 'volume_id'
    if ($null -eq $idColumn -or $dropped.result.data_array.Count -ne 1) {
        throw 'Expected one dropped volume in this newly created demo schema.'
    }
    $volumeId = [guid]$dropped.result.data_array[0][$idColumn.position]
    Write-Host "UNDROP VOLUME WITH ID '$volumeId'"
    $null = Invoke-Sql "UNDROP VOLUME WITH ID '$volumeId'"
    $restored = Invoke-Sql "DESCRIBE VOLUME $volumeName"
    if ($restored.result.data_array[0][0] -ne 'restore_me') {
        throw 'Restored volume name did not match'
    }
    [pscustomobject]@{
        volume = $volumeName
        volume_id = $volumeId
        restored_name = $restored.result.data_array[0][0]
    }
} finally {
    if ($schemaCreated) {
        $null = Invoke-Sql "DROP VOLUME IF EXISTS $volumeName"
        $null = Invoke-Sql "DROP SCHEMA $schemaFullName"
    }
}
