param(
    [string]$Provider = 'sbx_test_data.default.chatgpt',
    [string]$Profile = 'AWS-DEV',
    [string]$Databricks = 'databricks',
    [string]$GatewayCli = 'ug',
    [switch]$Launch
)

$ErrorActionPreference = 'Stop'
$description = & $Databricks auth describe --profile $Profile --output json | ConvertFrom-Json
$workspace = $description.details.host
if (-not $workspace) { throw "Cannot find the workspace URL for $Profile." }

$providerName = [uri]::EscapeDataString($Provider)
$service = & $Databricks api get "/api/2.1/unity-catalog/model-provider-services/$providerName" --profile $Profile --output json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) { throw "Cannot read provider $Provider in $Profile." }
if ($service.config.provider_type -ne 'EXTERNAL_MODEL_PROVIDER_TYPE_OPENAI' -or
    $null -eq $service.config.openai.PSObject.Properties['relayed']) {
    throw "$Provider does not use ChatGPT subscription authentication."
}
Write-Host "Provider verified: $Provider (OpenAI subscription relay)"

if ($Launch) {
    & $GatewayCli codex --provider $Provider --workspace $workspace
    exit $LASTEXITCODE
}

"& '$GatewayCli' codex --provider '$Provider' --workspace '$workspace'"
