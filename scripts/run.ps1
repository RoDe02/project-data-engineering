# Each trigger publishes ONE simulated historical daily batch.
# No -Date: oldest day without a verified Gold completion
# -Date: deterministic replay of a date.
[CmdletBinding(DefaultParameterSetName = 'Trigger')]
param(
    [Parameter(ParameterSetName = 'Trigger')][ValidatePattern('^(2013-01-1[2-8])?\z')][string]$Date = '',
    [Parameter(ParameterSetName = 'Trigger')][switch]$Unpause,
    [Parameter(Mandatory = $true, ParameterSetName = 'EnableOnly')][switch]$UnpauseOnly
)
. (Join-Path $PSScriptRoot 'common.ps1')
Push-Location $ProjectRoot
try {
    Assert-DockerDesktopContext
    $dag = 'twitter_geospatial_daily_batch'
    $scheduler = Get-ReadySchedulerPod
    if ($Unpause -or $UnpauseOnly) {
        & kubectl -n twitter-data exec $scheduler -c scheduler -- airflow dags unpause $dag
        Assert-NativeSuccess 'Enable the daily DAG'
        if ($UnpauseOnly) { return }
    }
    if ([string]::IsNullOrWhiteSpace($Date)) {
        & kubectl -n twitter-data exec $scheduler -c scheduler -- airflow dags trigger $dag
    } else {
        # Base64 keeps JSON quotes, BOMs and Windows line endings out of shell parsing.
        # Python builds the argument list and replaces itself with the Airflow CLI.
        $trigger = @"
import json
import os
os.execvp("airflow", ["airflow", "dags", "trigger", "--conf", json.dumps({"source_date": "$Date"}), "$dag"])
"@
        $encodedTrigger = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($trigger))
        & kubectl -n twitter-data exec $scheduler -c scheduler -- python -c "exec(__import__('base64').b64decode('$encodedTrigger'))"
    }
    Assert-NativeSuccess 'Airflow daily DAG trigger'
    Write-Host 'One historical source day will be ingested and processed (Bronze -> Silver -> Gold).'
} finally {
    Pop-Location
}
