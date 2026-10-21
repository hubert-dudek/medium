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

    $prompt = 'Read before.yml and after.yml in this folder. Explain the single job setting that changed in two short sentences. Mention whether deploying this bundle runs the job. Only read these two local files. Do not edit files, run SQL, call workspace APIs or execute deployments. Do not include local paths, usernames, emails or resource IDs in the answer.'
    & (Join-Path $CliDirectory 'genie.cmd') exec --sandbox read-only --skip-git-repo-check --ephemeral $prompt
    if ($LASTEXITCODE -ne 0) { throw 'Genie Code CLI failed.' }
} finally {
    Pop-Location
    foreach ($name in $variableNames) {
        [Environment]::SetEnvironmentVariable($name, $previousValues[$name], 'Process')
    }
}
