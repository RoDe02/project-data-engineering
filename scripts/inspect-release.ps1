# Launch a short-lived ML-reader Pod to validate the latest release.
param([switch]$CheckAccess)
. (Join-Path $PSScriptRoot 'common.ps1')
Push-Location $ProjectRoot
$name = 'gold-verify-' + [guid]::NewGuid().ToString('N').Substring(0, 10)
try {
    Assert-DockerDesktopContext
    $verifyArgs = if ($CheckAccess) { '["--check-reader-access"]' } else { '[]' }
    $manifest = @"
apiVersion: v1
kind: Pod
metadata:
  name: $name
  namespace: twitter-data
spec:
  restartPolicy: Never
  serviceAccountName: data-ingest
  automountServiceAccountToken: false
  containers:
  - name: verify
    image: twitter-ingest:local
    imagePullPolicy: IfNotPresent
    securityContext:
      runAsNonRoot: true
      runAsUser: 10001
      allowPrivilegeEscalation: false
      capabilities: {drop: ["ALL"]}
      seccompProfile: {type: RuntimeDefault}
    command: [python, -m, dataflow.verify]
    args: $verifyArgs
    envFrom:
    - configMapRef: {name: platform-settings}
    - secretRef: {name: minio-ml-reader}
"@
    $manifest | & kubectl apply -f -
    Assert-NativeSuccess 'kubectl create release verification pod'
    $phase = ''
    for ($i = 0; $i -lt 90; $i++) {
        $phase = (& kubectl -n twitter-data get pod $name -o 'jsonpath={.status.phase}' 2>$null | Out-String).Trim()
        if ($phase -eq 'Succeeded' -or $phase -eq 'Failed') { break }
        Start-Sleep -Seconds 2
    }
    & kubectl -n twitter-data logs $name
    Assert-NativeSuccess 'fetch release verifier logs'
    if ($phase -ne 'Succeeded') { throw "Release verifier did not succeed (phase=$phase)." }
    Write-Host 'Latest Gold release verified via read-only MinIO identity.'
} finally {
    & kubectl -n twitter-data delete pod $name --ignore-not-found '--wait=false' 2>$null | Out-Null
    Pop-Location
}
