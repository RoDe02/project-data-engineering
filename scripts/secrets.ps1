# MinIO service credentials are created once per namespace and reused on redeploy.
param([switch]$Reconcile)
. (Join-Path $PSScriptRoot 'common.ps1')
Assert-DockerDesktopContext
if ($Reconcile) { Require-Tool 'helm' }
$users = @{ 'ingestion' = 'twitteringestion'; 'processing' = 'twitterprocessing'; 'ml-reader' = 'twittermlreader' }
foreach ($role in $users.Keys) {
    $name = "minio-$role"
    $existing = & kubectl -n twitter-data get secret $name --ignore-not-found -o name
    Assert-NativeSuccess "Look up $name"
    if ($existing) { continue }
    $secret = @{
        apiVersion = 'v1'
        kind = 'Secret'
        metadata = @{ name = $name; namespace = 'twitter-data' }
        type = 'Opaque'
        stringData = @{ AWS_ACCESS_KEY_ID = $users[$role]; AWS_SECRET_ACCESS_KEY = (New-RandomHex) }
    }
    # Create (never replace) using stdin so credentials do not appear in CLI arguments.
    $secret | ConvertTo-Json -Depth 5 -Compress | & kubectl create -f - -o name
    Assert-NativeSuccess "Create $name"
}

if ($Reconcile) {
    $chart = Join-Path $ProjectRoot 'infra/charts/minio'
    & helm upgrade twitter-minio $chart --kube-context $ExpectedKubernetesContext -n twitter-data --reuse-values --wait
    Assert-NativeSuccess 'Reconcile MinIO workload users with their Secrets'
}
