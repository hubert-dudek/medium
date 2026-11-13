param(
    [Parameter(Mandatory)]
    [string]$Profile,
    [Parameter(Mandatory)]
    [guid]$AccountId,
    [Parameter(Mandatory)]
    [guid]$BudgetId,
    [string]$Databricks = 'databricks'
)

& $Databricks account budgets get-budget-spend-status `
    "accounts/$AccountId/budget-spend-statuses/$BudgetId" `
    --profile $Profile `
    --output json

exit $LASTEXITCODE
