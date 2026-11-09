# Windows PowerShell 5.1+ helpers for an EXISTING Docker Desktop Kubernetes cluster.
# Dot-source from a script in this directory, do not run directly.
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$ExpectedKubernetesContext = 'docker-desktop'

function Require-Tool {
    param([Parameter(Mandatory = $true)][string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Missing '$Name'. Install it and add it to PATH before continuing."
    }
}

function Assert-NativeSuccess {
    param([Parameter(Mandatory = $true)][string]$Action)
    if ($LASTEXITCODE -ne 0) {
        throw "$Action failed with exit code $LASTEXITCODE."
    }
}

function Assert-DockerDesktopContext {
    Require-Tool 'kubectl'
    $context = & kubectl config current-context
    Assert-NativeSuccess 'kubectl config current-context'
    if (($context | Out-String).Trim() -cne $ExpectedKubernetesContext) {
        throw "Refusing operation in Kubernetes context '$context'. Expected '$ExpectedKubernetesContext'. Run .\scripts\bootstrap.ps1 to select Docker Desktop."
    }
    & kubectl --context $ExpectedKubernetesContext cluster-info | Out-Null
    Assert-NativeSuccess 'kubectl cluster-info (enable Kubernetes in Docker Desktop)'
}

function Get-ReadySchedulerPod {
    $raw = & kubectl -n twitter-data get pods -l 'component=scheduler,release=airflow' -o json
    Assert-NativeSuccess 'List Airflow schedulers'
    $ready = @(foreach ($pod in (($raw -join [Environment]::NewLine) | ConvertFrom-Json).items) {
        if ($pod.metadata.PSObject.Properties['deletionTimestamp']) { continue }
        $conditions = $pod.status.PSObject.Properties['conditions']
        if ($conditions -and @($conditions.Value | Where-Object { $_.type -eq 'Ready' -and $_.status -eq 'True' }).Count -gt 0) {
            $pod.metadata.name
        }
    })
    if ($ready.Count -eq 0) { throw 'No Ready Airflow scheduler pod is available.' }
    return $ready[0]
}

function Assert-DefaultStorageClass {
    # MinIO, PostgreSQL and Airflow create persistent volumes using the cluster default.
    $raw = & kubectl get storageclasses.storage.k8s.io -o json
    Assert-NativeSuccess 'kubectl get storageclass'
    $classes = ($raw -join [Environment]::NewLine) | ConvertFrom-Json
    $defaults = @($classes.items | Where-Object {
        $annotations = $_.metadata.PSObject.Properties['annotations']
        if ($null -eq $annotations) { return $false }
        ($annotations.Value.'storageclass.kubernetes.io/is-default-class' -eq 'true') -or ($annotations.Value.'storageclass.beta.kubernetes.io/is-default-class' -eq 'true')
    })
    if ($defaults.Count -eq 0) {
        throw 'No default Kubernetes StorageClass. Configure one in Docker Desktop before deploying PVC-backed MinIO, PostgreSQL and Airflow.'
    }
    Write-Host "Default StorageClass: $(($defaults | ForEach-Object { $_.metadata.name }) -join ', ')"
}

function Get-LocalTerraform {
    $exe = Join-Path $ProjectRoot 'terraform.exe'
    if (-not (Test-Path -LiteralPath $exe -PathType Leaf)) {
        throw "terraform.exe not found at '$exe'. Copy the Windows terraform.exe into the project root first. The binary is never included in this repository."
    }
    return $exe
}

function Set-TerraformKubeconfig {
    # Providers need a real, absolute file path. Docker Desktop normally writes ~/.kube/config.
    if ($env:KUBECONFIG) {
        if ($env:KUBECONFIG.Contains(';')) {
            throw 'Set KUBECONFIG to a single kubeconfig file for Terraform (multiple files are not supported by this script).'
        }
        $path = $env:KUBECONFIG
    } else {
        $path = Join-Path $HOME '.kube\config'
    }
    $path = [System.IO.Path]::GetFullPath($path)
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "Kubernetes config '$path' does not exist. Run .\scripts\bootstrap.ps1 first."
    }
    $env:TF_VAR_kubeconfig_path = $path.Replace('\', '/')
    $env:TF_VAR_kube_context = $ExpectedKubernetesContext
}

function Invoke-LocalTerraform {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)
    $exe = Get-LocalTerraform
    & $exe '-chdir=infra/terraform' @Arguments
    Assert-NativeSuccess "terraform $($Arguments -join ' ')"
}

function Assert-ProjectNamespacesNotShared {
    # Protect pre-existing Docker Desktop namespaces that are not owned by THIS local Terraform state.
    # Namespace destruction deletes everything inside the namespace, including objects not in TF state.
    $stateFile = Join-Path $ProjectRoot 'infra\terraform\terraform.tfstate'
    $tracked = @()
    if (Test-Path -LiteralPath $stateFile -PathType Leaf) {
        $exe = Get-LocalTerraform
        $tracked = @(& $exe '-chdir=infra/terraform' state list)
        Assert-NativeSuccess 'terraform state list'
    }
    foreach ($entry in @(
        @{ Namespace = 'twitter-data'; Address = 'kubernetes_namespace_v1.data' },
        @{ Namespace = 'spark-operator'; Address = 'kubernetes_namespace_v1.spark' }
    )) {
        $existing = & kubectl get namespace $entry.Namespace '--ignore-not-found' '-o=name'
        Assert-NativeSuccess "kubectl get namespace $($entry.Namespace)"
        if (-not [string]::IsNullOrWhiteSpace(($existing | Out-String).Trim()) -and $tracked -notcontains $entry.Address) {
            throw "Namespace '$($entry.Namespace)' already exists but is NOT tracked by this project's Terraform state. Refusing to adopt or destroy a shared namespace. Use a clean dedicated Docker Desktop cluster/namespace or explicitly import after review."
        }
    }
}

function New-RandomHex {
    param([int]$Bytes = 24)
    $data = New-Object 'System.Byte[]' $Bytes
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try { $generator.GetBytes($data) }
    finally { $generator.Dispose() }
    return ([System.BitConverter]::ToString($data)).Replace('-', '').ToLowerInvariant()
}
