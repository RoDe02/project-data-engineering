# Native Windows PowerShell deployment to the EXISTING Docker Desktop Kubernetes cluster.
# Requires manually copied .\terraform.exe in the repository root (no system Terraform needed).
param(
    [switch]$PlanOnly,
    [ValidateSet('Project', 'OneExecutor', 'TwoExecutors', 'FourExecutors')]
    [string]$DemoProfile = 'Project'
)
. (Join-Path $PSScriptRoot 'common.ps1')
Push-Location $ProjectRoot
$previousKubeconfig = $env:TF_VAR_kubeconfig_path
$previousKubeContext = $env:TF_VAR_kube_context
try {
    Require-Tool 'docker'
    Require-Tool 'kubectl'
    Assert-DockerDesktopContext
    Assert-DefaultStorageClass
    $terraform = Get-LocalTerraform
    & $terraform version
    Assert-NativeSuccess 'terraform.exe version'
    Set-TerraformKubeconfig

    $overrides = @()
    switch ($DemoProfile) {
        'OneExecutor'   { $overrides = @('-var=spark_tuning={executor_instances=1}') }
        'TwoExecutors'  { $overrides = @('-var=spark_tuning={executor_instances=2}') }
        'FourExecutors'  { $overrides = @('-var=spark_tuning={executor_instances=4}') }
    }
    Invoke-LocalTerraform @('init', '-input=false', '-upgrade')
    Assert-ProjectNamespacesNotShared
    if ($PlanOnly) {
        Invoke-LocalTerraform (@('plan', '-input=false') + $overrides)
        return
    }

    & (Join-Path $PSScriptRoot 'build.ps1')
    if (-not $?) { throw 'Image build / Kubernetes availability probe failed.' }

    $apply = @('apply', '-input=false', '-auto-approve') + $overrides
    # Provision namespaces before shared settings and role-specific workload Secrets.
    Invoke-LocalTerraform ($apply + @('-target=kubernetes_namespace_v1.data', '-target=kubernetes_namespace_v1.spark'))
    & kubectl apply -f 'infra/k8s/platform.yaml'
    Assert-NativeSuccess 'kubectl apply platform resources'
    & (Join-Path $PSScriptRoot 'secrets.ps1')
    if (-not $?) { throw 'MinIO workload credential provisioning failed.' }

    # PostgreSQL must exist before reconciling credentials stored in old PVCs.
    Invoke-LocalTerraform ($apply + @('-target=helm_release.postgres'))
    . (Join-Path $PSScriptRoot 'prepare-airflow.ps1')
    Initialize-AirflowDatabase

    # Helm migrations can now complete and init containers will unblock.
    Invoke-LocalTerraform $apply
    & kubectl -n twitter-data rollout status statefulset/airflow-scheduler --timeout=600s
    Assert-NativeSuccess 'Airflow scheduler startup'
    & kubectl -n twitter-data rollout status deployment/airflow-webserver --timeout=600s
    Assert-NativeSuccess 'Airflow webserver startup'
    if (-not $?) { throw 'Airflow admin provisioning failed.' }
    Write-Host ''
    Write-Host 'Deployment submitted to EXISTING Docker Desktop Kubernetes (docker-desktop).'
    Write-Host 'Next: .\scripts\health.ps1'
    Write-Host 'Airflow UI: .\scripts\airflow-ui.ps1'
    Write-Host 'MinIO UI: .\scripts\minio-ui.ps1  (admin / adminadmin)'
} finally {
    if ($null -eq $previousKubeconfig) { Remove-Item Env:TF_VAR_kubeconfig_path -ErrorAction SilentlyContinue }
    else { $env:TF_VAR_kubeconfig_path = $previousKubeconfig }
    if ($null -eq $previousKubeContext) { Remove-Item Env:TF_VAR_kube_context -ErrorAction SilentlyContinue }
    else { $env:TF_VAR_kube_context = $previousKubeContext }
    Pop-Location
}
