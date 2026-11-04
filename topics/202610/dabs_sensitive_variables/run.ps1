param(
    [ValidateSet('01_explicit', '02_automatic', '03_rejected')]
    [string]$Case = '01_explicit',
    [string]$Profile = 'AWS-DEV',
    [string]$ConfigFile = 'C:/git/cli/.databrickscfg',
    [string]$Cli = 'C:/git/cli/databricks.exe'
)

$previousConfig = $env:DATABRICKS_CONFIG_FILE
$previousEngine = $env:DATABRICKS_BUNDLE_ENGINE
$previousValue = $env:BUNDLE_VAR_api_key

try {
    $env:DATABRICKS_CONFIG_FILE = $ConfigFile
    $env:DATABRICKS_BUNDLE_ENGINE = 'direct'
    $env:BUNDLE_VAR_api_key = 'FAKE_NEWS_DEMO_NOT_A_SECRET_20261104'
    Push-Location (Join-Path $PSScriptRoot $Case)
    try {
        $json = & $Cli bundle validate --profile $Profile --output json
        $exitCode = $LASTEXITCODE
        if ($exitCode -eq 0) {
            ($json | ConvertFrom-Json).variables | ConvertTo-Json -Depth 5
        }
    } finally {
        Pop-Location
    }
} finally {
    $env:DATABRICKS_CONFIG_FILE = $previousConfig
    $env:DATABRICKS_BUNDLE_ENGINE = $previousEngine
    $env:BUNDLE_VAR_api_key = $previousValue
}

exit $exitCode
