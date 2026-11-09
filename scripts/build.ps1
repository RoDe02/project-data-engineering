# Build images on Docker Desktop and probe their visibility in the existing Kubernetes cluster.
. (Join-Path $PSScriptRoot 'common.ps1')

function Test-ImageAvailableOnNode {
    param([string]$Image, [string]$Node, [int]$Index)
    $name = "twitter-image-probe-$Index-$([guid]::NewGuid().ToString('N').Substring(0,8))"
    $manifest = @{
        apiVersion = 'v1'
        kind = 'Pod'
        metadata = @{ name = $name; namespace = 'default'; labels = @{ 'app.kubernetes.io/part-of' = 'twitter-batch-image-probe' } }
        spec = @{
            nodeName = $Node
            restartPolicy = 'Never'
            containers = @(@{
                name = 'probe'
                image = $Image
                imagePullPolicy = 'Never'
                command = @('/bin/sh', '-c', 'echo image-ready')
            })
        }
    }
    try {
        $manifest | ConvertTo-Json -Depth 12 -Compress | & kubectl apply -f - | Out-Null
        Assert-NativeSuccess "create image probe on $Node"
        for ($i = 0; $i -lt 35; $i++) {
            $phase = (& kubectl -n default get pod $name -o 'jsonpath={.status.phase}' 2>$null | Out-String).Trim()
            if ($phase -eq 'Succeeded') { return }
            $reason = (& kubectl -n default get pod $name -o 'jsonpath={.status.containerStatuses[0].state.waiting.reason}' 2>$null | Out-String).Trim()
            if ($phase -eq 'Failed' -or $reason -eq 'ErrImageNeverPull' -or $reason -eq 'ImagePullBackOff') {
                & kubectl -n default describe pod $name
                throw "Local image '$Image' not accessible on Kubernetes node '$Node' ($reason)."
            }
            Start-Sleep -Seconds 2
        }
        & kubectl -n default describe pod $name
        throw "Timed out checking '$Image' on '$Node'."
    } finally {
        & kubectl -n default delete pod $name --ignore-not-found '--wait=false' 2>$null | Out-Null
    }
}

Push-Location $ProjectRoot
try {
    Require-Tool 'docker'
    Assert-DockerDesktopContext
    $containerOS = & docker info --format '{{.OSType}}'
    Assert-NativeSuccess 'docker info'
    if (($containerOS | Out-String).Trim() -ne 'linux') { throw 'Build requires Docker Desktop Linux containers.' }

    $builds = @(
        @{ Dockerfile = 'docker/ingest.Dockerfile'; Image = 'twitter-ingest:local' },
        @{ Dockerfile = 'docker/spark.Dockerfile'; Image = 'twitter-spark:local' },
        @{ Dockerfile = 'docker/airflow.Dockerfile'; Image = 'twitter-airflow:local' }
    )
    foreach ($build in $builds) {
        & docker build -f $build.Dockerfile -t $build.Image .
        Assert-NativeSuccess "docker build $($build.Image)"
    }

    # Probe only Ready, schedulable nodes. In multi-node Desktop clusters image caches may differ.
    $nodeJson = & kubectl get nodes -o json
    Assert-NativeSuccess 'kubectl get nodes'
    $nodes = (($nodeJson -join [Environment]::NewLine) | ConvertFrom-Json).items
    $targets = @($nodes | Where-Object {
        $ready = @($_.status.conditions | Where-Object { $_.type -eq 'Ready' -and $_.status -eq 'True' }).Count -gt 0
        $unschedulable = $_.spec.PSObject.Properties['unschedulable']
        $taints = $_.spec.PSObject.Properties['taints']
        $blocked = ($null -ne $taints) -and (@($taints.Value | Where-Object { $_.effect -eq 'NoSchedule' -or $_.effect -eq 'NoExecute' }).Count -gt 0)
        $ready -and (-not ($null -ne $unschedulable -and $unschedulable.Value)) -and (-not $blocked)
    })
    if ($targets.Count -eq 0) { throw 'No schedulable Ready nodes in Docker Desktop Kubernetes.' }
    $n = 0
    try {
        foreach ($node in $targets) {
            foreach ($build in $builds) {
                $n++
                Test-ImageAvailableOnNode -Image $build.Image -Node $node.metadata.name -Index $n
            }
        }
    } catch {
        throw "Container images were built but aren't usable by Docker Desktop Kubernetes. Depending on the Docker Desktop Kubernetes provisioner and image store, locally built images may not be visible to node runtimes. Configure a compatible Docker Desktop containerd image store or a registry accessible to all nodes, then retry. Root cause: $($_.Exception.Message)"
    }
    Write-Host 'All three application images are available on every schedulable Docker Desktop Kubernetes node.'
} finally {
    Pop-Location
}
