# DANGEROUS: deletes only resources provisioned by this Terraform project in Docker Desktop.
# NEVER resets/stops/deletes the Docker Desktop Kubernetes cluster itself.
. (Join-Path $PSScriptRoot 'common.ps1')
Push-Location $ProjectRoot
$previousKubeconfig = $env:TF_VAR_kubeconfig_path
$previousKubeContext = $env:TF_VAR_kube_context
try {
    Assert-DockerDesktopContext
    $null = Get-LocalTerraform
    Write-Warning 'Terraform destroy will DELETE this project''s namespaces and their MinIO/PostgreSQL/Airflow PVC data and Secrets!'
    Write-Warning 'Other Kubernetes workloads and the Docker Desktop cluster will NOT be deleted.'
    $confirmation = Read-Host 'To confirm the loss of project data, type DELETE'
    if ($confirmation -cne 'DELETE') {
        Write-Host 'Cancelled: no resources deleted.'
        return
    }
    Set-TerraformKubeconfig
    Invoke-LocalTerraform @('destroy', '-input=false', '-auto-approve')
    # Do not call docker desktop kubernetes reset or kind delete cluster!
    Write-Host 'Terraform-managed project resources destroyed. Docker Desktop Kubernetes remains running.'
} finally {
    if ($null -eq $previousKubeconfig) { Remove-Item Env:TF_VAR_kubeconfig_path -ErrorAction SilentlyContinue }
    else { $env:TF_VAR_kubeconfig_path = $previousKubeconfig }
    if ($null -eq $previousKubeContext) { Remove-Item Env:TF_VAR_kube_context -ErrorAction SilentlyContinue }
    else { $env:TF_VAR_kube_context = $previousKubeContext }
    Pop-Location
}
