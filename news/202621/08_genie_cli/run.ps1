param([string]$CliDirectory = 'C:\git\cli')

$variableNames = @('DATABRICKS_CONFIG_FILE', 'DATABRICKS_CONFIG_PROFILE', 'DATABRICKS_HOST', 'GENIE_HOME')
$previousValues = @{}
foreach ($name in $variableNames) {
    $previousValues[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

Push-Location $PSScriptRoot
try {
    $env:DATABRICKS_CONFIG_FILE = Join-Path $CliDirectory '.databrickscfg'
    $env:DATABRICKS_CONFIG_PROFILE = 'AWS-DEV'
    $env:DATABRICKS_HOST = $null
    $env:GENIE_HOME = Join-Path $CliDirectory '.genie'

    $prompt = @(
        'Compare the two YAML files below. In two sentences, give the old and new job timeout and say whether deploying the bundle runs the job. Use the supplied text only.'
        'before.yml'
        (Get-Content -LiteralPath 'before.yml' -Raw)
        'after.yml'
        (Get-Content -LiteralPath 'after.yml' -Raw)
    ) -join [Environment]::NewLine
    $prompt | & (Join-Path $CliDirectory 'genie.cmd') exec --sandbox read-only --skip-git-repo-check --ephemeral -
    if ($LASTEXITCODE -ne 0) { throw 'Genie Code CLI failed.' }
} finally {
    Pop-Location
    foreach ($name in $variableNames) {
        [Environment]::SetEnvironmentVariable($name, $previousValues[$name], 'Process')
    }
}
