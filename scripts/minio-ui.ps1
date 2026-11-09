# Opens the local MinIO Console. Existing persistent data are never reset.
. (Join-Path $PSScriptRoot 'common.ps1')
Assert-DockerDesktopContext
& kubectl -n twitter-data rollout status statefulset/minio --timeout=180s
Assert-NativeSuccess 'MinIO readiness'
$root = (& kubectl -n twitter-data exec minio-0 -c minio -- printenv MINIO_ROOT_USER | Out-String).Trim()
Assert-NativeSuccess 'Read MinIO root user'
$rootAccess = (& kubectl -n twitter-data exec minio-0 -c minio -- printenv MINIO_API_ROOT_ACCESS | Out-String).Trim()
Assert-NativeSuccess 'Read MinIO root access setting'
if ($root -ne 'admin' -or $rootAccess -ne 'on') { throw "MinIO runtime differs from PoC Helm values (user=$root, rootAccess=$rootAccess). Run .\scripts\deploy.ps1." }
Write-Host 'MinIO Console: http://localhost:9001 | username admin | default password adminadmin'
Write-Host 'Port 9001 is the Console, port 9000 is the S3 API. Press Ctrl+C to stop forwarding.'
& kubectl -n twitter-data port-forward svc/minio 9001:9001
