param(
    [Parameter(Mandatory)]
    [string]$Profile,
    [Parameter(Mandatory)]
    [guid]$AccountId,
    [Parameter(Mandatory)]
    [long]$WorkspaceId,
    [string]$Databricks = 'databricks'
)

& $Databricks account budgets list-budget-spend-statuses "accounts/$AccountId" `
    --filter "workspace_id = $WorkspaceId" `
    --page-size 10 `
    --profile $Profile `
    --output json

exit $LASTEXITCODE
