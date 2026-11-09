# Non-destructive readiness / RBAC inspection.
param([switch]$Detailed)
. (Join-Path $PSScriptRoot 'common.ps1')
Push-Location $ProjectRoot
try {
    Assert-DockerDesktopContext
    & kubectl -n twitter-data get pods,pvc
    Assert-NativeSuccess 'data Pods/PVCs'
    & kubectl -n spark-operator get pods
    Assert-NativeSuccess 'Spark Operator Pods'
    & kubectl -n twitter-data get sparkapplications
    Assert-NativeSuccess 'SparkApplication CRD'
    $access = & kubectl -n twitter-data auth can-i create sparkapplications '--as=system:serviceaccount:twitter-data:airflow-orchestrator'
    Assert-NativeSuccess 'Airflow SparkApplication RBAC inspection'
    if (($access | Out-String).Trim() -ne 'yes') { throw 'Airflow cannot create SparkApplications.' }
    $access = & kubectl -n twitter-data auth can-i create pods '--as=system:serviceaccount:twitter-data:spark-runner'
    Assert-NativeSuccess 'Spark pod RBAC inspection'
    if (($access | Out-String).Trim() -ne 'yes') { throw 'Spark runner cannot create Pods.' }
    $raw = & kubectl -n twitter-data get statefulset airflow-scheduler -o json
    Assert-NativeSuccess 'Inspect scheduler readiness'
    $schedulerSet = ($raw -join [Environment]::NewLine) | ConvertFrom-Json
    $ready = 0
    if ($schedulerSet.status.PSObject.Properties['readyReplicas']) { $ready = $schedulerSet.status.readyReplicas }
    $desired = $schedulerSet.spec.replicas
    Write-Host "Airflow schedulers Ready: $ready/$desired"
    if ($Detailed -and $ready -gt 0) {
        $scheduler = Get-ReadySchedulerPod
        Get-Content -Raw (Join-Path $PSScriptRoot 'batch-status.py') |
            & kubectl -n twitter-data exec -i $scheduler -c scheduler -- python -
        Assert-NativeSuccess 'Read latest requested batch and release status'
    }
    if ($ready -lt $desired) { throw "Airflow scheduler readiness is degraded ($ready/$desired)." }
    Write-Host 'Scheduler readiness and RBAC checks passed.'
} finally {
    Pop-Location
}
