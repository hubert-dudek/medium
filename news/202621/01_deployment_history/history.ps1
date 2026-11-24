param(
    [string]$Cli = 'C:/git/cli/databricks.exe',
    [string]$Profile = 'AWS-DEV'
)

$deployments = & $Cli bundle-deployments list-deployments --profile $Profile -o json
if ($LASTEXITCODE) { throw 'Could not list bundle deployments.' }
$history = ($deployments | ConvertFrom-Json) | Where-Object display_name -eq 'news-deployment-history-20261021'
if (@($history).Count -ne 1) { throw 'Expected the article deployment on AWS-DEV.' }

$versions = & $Cli bundle-deployments list-versions $history.name --profile $Profile -o json
if ($LASTEXITCODE) { throw 'Could not read the recorded versions.' }
($versions | ConvertFrom-Json) | Format-Table version_id, version_type, status, create_time, created_by

$resources = & $Cli bundle-deployments list-resources $history.name --profile $Profile -o json
if ($LASTEXITCODE) { throw 'Could not read the recorded resources.' }
($resources | ConvertFrom-Json) | Format-Table resource_type, resource_key, last_version_id, last_action_type
