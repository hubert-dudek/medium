param(
    [string]$Cli = 'C:\git\cli\databricks.exe',
    [string]$ConfigFile = 'C:\git\cli\.databrickscfg',
    [ValidateRange(1, 30)]
    [int]$TimeoutMinutes = 15
)

$ErrorActionPreference = 'Stop'
$profileName = 'AZURE-MVPLINK'
$clusterId = '0919-203238-dmcv9ho3'
$startedHere = $false
$runId = $null
$runFinished = $false

function Invoke-DemoCli {
    param([string[]]$Command, [int]$TimeoutSeconds = 120)

    $startInfo = [System.Diagnostics.ProcessStartInfo]::new()
    $startInfo.FileName = $Cli
    $startInfo.WorkingDirectory = $PSScriptRoot
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    foreach ($key in @('DATABRICKS_HOST', 'DATABRICKS_TOKEN', 'DATABRICKS_CLIENT_ID', 'DATABRICKS_CLIENT_SECRET', 'DATABRICKS_AUTH_TYPE', 'DATABRICKS_CONFIG_PROFILE', 'DATABRICKS_PROFILE')) {
        $startInfo.Environment.Remove($key) > $null
    }
    $startInfo.Environment['DATABRICKS_CONFIG_FILE'] = $ConfigFile
    foreach ($value in ($Command + @('--profile', $profileName, '--output', 'json'))) {
        $startInfo.ArgumentList.Add($value)
    }
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo = $startInfo
    [void]$process.Start()
    $stdout = $process.StandardOutput.ReadToEndAsync()
    $stderr = $process.StandardError.ReadToEndAsync()
    if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
        $process.Kill($true)
        throw "Databricks command exceeded $TimeoutSeconds seconds."
    }
    $result = [pscustomobject]@{ Stdout = $stdout.Result; Stderr = $stderr.Result }
    if ($process.ExitCode -ne 0) { throw $result.Stderr }
    return $result
}

try {
    $identity = (Invoke-DemoCli @('current-user', 'me')).Stdout | ConvertFrom-Json
    if ($identity.userName -ne 'hubert@mvplink.io' -or -not $identity.active) {
        throw 'The Azure workspace identity does not match this demo.'
    }
    $cluster = (Invoke-DemoCli @('clusters', 'get', $clusterId)).Stdout | ConvertFrom-Json
    if ($cluster.autotermination_minutes -ne 10 -or -not $cluster.is_single_node -or $cluster.num_workers -ne 0) {
        throw 'This demo requires the existing single-node compute with a 10-minute idle timeout.'
    }
    if ($cluster.dependency_mode -ne 'DEPENDENCY_MODE_ENVIRONMENTS') {
        throw 'Select Environments dependency mode before running this demo.'
    }
    if ($cluster.state -eq 'TERMINATED') {
        $startedHere = $true
        $null = Invoke-DemoCli @('clusters', 'start', $clusterId, '--no-wait')
    } elseif ($cluster.state -ne 'RUNNING') {
        throw "Wait until the compute is RUNNING or TERMINATED; current state: $($cluster.state)."
    } else {
        Write-Host 'Using already-running compute. Stop it immediately after recording when it is no longer needed.'
    }

    $summary = (Invoke-DemoCli @('bundle', 'summary', '-t', 'dev')).Stdout | ConvertFrom-Json
    $jobId = [string]$summary.resources.jobs.demo.id
    if (-not $jobId) { throw 'Deploy this chapter before running the demo.' }
    $launch = (Invoke-DemoCli @('jobs', 'run-now', $jobId, '--no-wait')).Stdout | ConvertFrom-Json
    $runId = [string]$launch.run_id
    if (-not $runId) { throw 'The job launch did not return a run identifier.' }
    Write-Host "Run: $runId"
    $deadline = [DateTime]::UtcNow.AddMinutes($TimeoutMinutes)
    do {
        $run = (Invoke-DemoCli @('jobs', 'get-run', $runId)).Stdout | ConvertFrom-Json
        if ($run.state.life_cycle_state -in @('TERMINATED', 'INTERNAL_ERROR', 'SKIPPED')) {
            $runFinished = $true
            if ($run.state.result_state -ne 'SUCCESS') { throw $run.state.state_message }
            Write-Host "SUCCESS: $($run.run_page_url)"
            break
        }
        Start-Sleep -Seconds 10
    } while ([DateTime]::UtcNow -lt $deadline)
    if (-not $runFinished) { throw "The demo exceeded $TimeoutMinutes minutes." }
} finally {
    try {
        if ($runId -and -not $runFinished) {
            $null = Invoke-DemoCli @('jobs', 'cancel-run', $runId, '--no-wait')
        }
    } finally {
        if ($startedHere) {
            $null = Invoke-DemoCli @('clusters', 'delete', $clusterId, '--timeout', '3m') 200
            Write-Host 'Demo compute terminated; its configuration is retained.'
        }
    }
}
