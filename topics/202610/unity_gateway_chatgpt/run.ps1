param(
    [Parameter(Mandatory = $true)][string]$Provider,
    [Parameter(Mandatory = $true)][string]$Workspace
)

ug codex --provider $Provider --workspace $Workspace
