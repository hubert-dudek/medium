param(
    [ValidateSet('all', '01_explicit', '02_automatic', '03_rejected')]
    [string]$Case = 'all',
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
    $env:BUNDLE_VAR_api_key = 'public-recording-example'
    $cases = if ($Case -eq 'all') { '01_explicit', '02_automatic', '03_rejected' } else { $Case }
    foreach ($example in $cases) {
        Write-Output $example
        Push-Location (Join-Path $PSScriptRoot $example)
        try {
            if ($example -eq '03_rejected') {
                $output = & $Cli bundle validate --profile $Profile 2>&1
                if ($LASTEXITCODE -eq 0 -or ($output -join "`n") -notmatch 'Sensitive variable "api_key" is used in a non-sensitive field') {
                    throw "Expected a sensitive-field validation error. CLI output: $output"
                }
                $output | ForEach-Object { $_.ToString() } | Where-Object { $_ -match '^Error:|^  at |^  in ' }
            } else {
                $config = & $Cli bundle validate --profile $Profile --output json
                if ($LASTEXITCODE) { throw "Validation failed for $example." }
                $variables = ($config | ConvertFrom-Json).variables
                if ($variables.api_key.value -ne '[redacted]') { throw 'Expected api_key to be redacted.' }
                $variables | ConvertTo-Json -Depth 5
            }
        } finally {
            Pop-Location
        }
    }
} finally {
    $env:DATABRICKS_CONFIG_FILE = $previousConfig
    $env:DATABRICKS_BUNDLE_ENGINE = $previousEngine
    $env:BUNDLE_VAR_api_key = $previousValue
}
