param(
    [string]$WorkspaceProfile = 'AWS-DEV',
    [string]$AccountProfile = 'AWS-DEV-ACCOUNT',
    [string]$Databricks = 'databricks',
    [guid]$BudgetId,
    [switch]$Example
)

$ErrorActionPreference = 'Stop'
if ($Example) {
    Write-Host 'Offline example of a GET response (placeholder values)'
    Get-Content -LiteralPath (Join-Path $PSScriptRoot 'example_get_response.json') -Raw
    return
}

$workspace = & $Databricks auth describe --profile $WorkspaceProfile --output json | ConvertFrom-Json
$config = $workspace.details.configuration
$accountId = $config.account_id.value
$workspaceId = $config.workspace_id.value
if (-not $accountId -or -not $workspaceId) {
    throw "Set account_id and workspace_id in the $WorkspaceProfile CLI profile."
}

if ($BudgetId) {
    & $Databricks account budgets get-budget-spend-status "accounts/$accountId/budget-spend-statuses/$BudgetId" --profile $AccountProfile --output json
} else {
    & $Databricks account budgets list-budget-spend-statuses "accounts/$accountId" --filter "workspace_id = $workspaceId" --profile $AccountProfile --output json
}
exit $LASTEXITCODE
