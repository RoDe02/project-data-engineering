# Validate and select the ALREADY ENABLED Kubernetes cluster inside Docker Desktop.
# This script NEVER creates, resets or deletes a Kubernetes cluster.
. (Join-Path $PSScriptRoot 'common.ps1')
Push-Location $ProjectRoot
try {
    Require-Tool 'docker'
    Require-Tool 'kubectl'
    $containerOS = & docker info --format '{{.OSType}}'
    Assert-NativeSuccess 'docker info (start Docker Desktop with Linux containers)'
    if (($containerOS | Out-String).Trim() -ne 'linux') {
        throw 'Docker Desktop must run Linux containers. Windows containers are unsupported.'
    }
    $contexts = @(& kubectl config get-contexts -o name)
    Assert-NativeSuccess 'kubectl config get-contexts'
    if ($contexts -notcontains $ExpectedKubernetesContext) {
        throw "Kubernetes context '$ExpectedKubernetesContext' is missing. Enable/start Kubernetes in Docker Desktop > Kubernetes first. No cluster will be created by this project."
    }
    & kubectl config use-context $ExpectedKubernetesContext
    Assert-NativeSuccess 'kubectl config use-context docker-desktop'
    Assert-DockerDesktopContext
    $raw = & kubectl get nodes -o json
    Assert-NativeSuccess 'kubectl get nodes'
    $nodes = ($raw -join [Environment]::NewLine) | ConvertFrom-Json
    if (@($nodes.items).Count -eq 0) { throw 'Docker Desktop Kubernetes has no nodes.' }
    foreach ($node in $nodes.items) {
        $ready = @($node.status.conditions | Where-Object { $_.type -eq 'Ready' -and $_.status -eq 'True' }).Count -gt 0
        if (-not $ready) { throw "Kubernetes node '$($node.metadata.name)' is not Ready. Wait for Docker Desktop Kubernetes to finish starting." }
    }
    Assert-DefaultStorageClass
    & kubectl get nodes -o wide
    Assert-NativeSuccess 'kubectl get nodes -o wide'
    Write-Host ''
    Write-Host 'Docker Desktop cluster is ready. No cluster has been created or modified.'
    Write-Host 'Next: .\scripts\deploy.ps1  (requires terraform.exe in project root)'
} finally {
    Pop-Location
}
