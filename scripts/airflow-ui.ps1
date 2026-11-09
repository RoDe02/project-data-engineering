# Forward the Airflow UI to this computer until the operator presses Ctrl+C.
. (Join-Path $PSScriptRoot 'common.ps1')
Assert-DockerDesktopContext
& kubectl -n twitter-data rollout status deployment/airflow-webserver --timeout=180s
Assert-NativeSuccess 'Airflow webserver readiness'
Write-Host 'Open http://localhost:8080 in your browser. Press Ctrl+C to stop forwarding.'
& kubectl -n twitter-data port-forward --address 127.0.0.1 svc/airflow-webserver 8080:8080
Assert-NativeSuccess 'Forward Airflow UI'
