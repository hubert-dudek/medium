$ErrorActionPreference = 'Stop'
$event = Get-Content -LiteralPath (Join-Path $PSScriptRoot 'example_event.json') -Raw | ConvertFrom-Json
$seen = [System.Collections.Generic.HashSet[string]]::new()

foreach ($delivery in $event, $event) {
    if ($seen.Add($delivery.event_id)) {
        [pscustomobject]@{
            Event     = $delivery.event_type
            Budget    = $delivery.budget.display_name
            LimitUSD  = $delivery.evaluation.quantity_threshold
            SpendUSD  = $delivery.evaluation.current_spend
            Outcome   = 'Processed'
        }
    } else {
        Write-Host "Skipped repeat delivery of $($delivery.event_id)"
    }
}
