param(
    [ValidateRange(0, 26)][int]$Chapter = 0,
    [ValidateSet('list', 'validate', 'plan', 'deploy', 'run', 'summary', 'pipeline')][string]$Action = 'list',
    [ValidateSet('01_initial_setup', '02_every_deployment', '03_json_configuration', '04_validation_succeeds', '05_validation_fails')][string]$Example = '01_initial_setup',
    [ValidatePattern('^[A-Za-z0-9_]*$')][string]$Resource = '',
    [string]$Cli = 'C:\git\cli\databricks.exe',
    [string]$ConfigFile = 'C:\git\cli\.databrickscfg',
    [string]$WarehouseId = '8a19c545d43c9bc3'
)

$ErrorActionPreference = 'Stop'
$chapters = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'chapters.json') -Raw | ConvertFrom-Json
if ($Action -eq 'list' -or $Chapter -eq 0) {
    $chapters | Select-Object number, cloud, mode, title | Format-Table -AutoSize
    return
}

$selected = @($chapters | Where-Object number -eq $Chapter)[0]
if (-not $selected) { throw "Chapter $Chapter does not exist." }
$chapterDirectory = Join-Path $PSScriptRoot $selected.folder
if ($selected.mode -eq 'ui') {
    Write-Output (Join-Path $chapterDirectory '01_demo.py')
    Write-Output 'Open this notebook for the UI recording steps. No compute is needed.'
    return
}
if ($selected.mode -eq 'variants') { $chapterDirectory = Join-Path $chapterDirectory $Example }
if (-not (Test-Path -LiteralPath (Join-Path $chapterDirectory 'databricks.yml'))) { throw 'Bundle configuration is missing.' }

$savedEnvironment = @{}
$scopedNames = @('DATABRICKS_CONFIG_FILE', 'DATABRICKS_CONFIG_PROFILE', 'DATABRICKS_PROFILE', 'DATABRICKS_HOST', 'DATABRICKS_TOKEN', 'DATABRICKS_AUTH_TYPE', 'DATABRICKS_CLIENT_ID', 'DATABRICKS_CLIENT_SECRET')
foreach ($name in $scopedNames) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
    [Environment]::SetEnvironmentVariable($name, $null, 'Process')
}
$env:DATABRICKS_CONFIG_FILE = $ConfigFile
Push-Location -LiteralPath $chapterDirectory
try {
    $identityText = & $Cli current-user me --profile $selected.profile --output json
    if ($LASTEXITCODE -ne 0) { throw 'Workspace login check failed.' }
    $identity = $identityText | ConvertFrom-Json
    if ($identity.userName -ne $selected.identity) { throw "Unexpected workspace identity: $($identity.userName)" }
    if ($Action -eq 'pipeline') {
        if ($Chapter -ne 4) { throw 'The pipeline sequence is available only in chapter 04.' }
        & py .\run_pipeline_demo.py --warehouse-id $WarehouseId --profile $selected.profile --cli $Cli
    } elseif ($Chapter -eq 23 -and $Action -eq 'run') {
        & .\run_demo.ps1 -Cli $Cli -ConfigFile $ConfigFile
    } else {
        $arguments = @('bundle', $Action, '--profile', $selected.profile, '-t', $selected.target)
        if ($Action -eq 'run') {
            if (-not $Resource) { $Resource = $selected.resource }
            if (-not $Resource) { throw 'Choose a resource with -Resource, or deploy the job_runs example.' }
            $arguments += @($Resource)
        }
        & $Cli @arguments
    }
    if ($LASTEXITCODE -ne 0) { throw "Demo command failed with exit code $LASTEXITCODE." }
} finally {
    Pop-Location
    foreach ($name in $scopedNames) { [Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name], 'Process') }
}
