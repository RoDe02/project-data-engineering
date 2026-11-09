. (Join-Path $PSScriptRoot 'common.ps1')

function Initialize-AirflowDatabase {
    Assert-DockerDesktopContext
    $namespace = 'twitter-data'
    & kubectl -n $namespace rollout status statefulset/airflow-postgres --timeout=300s
    Assert-NativeSuccess 'Wait for PostgreSQL'
    
    $sql = @'
DO $poc$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'admin') THEN
    CREATE ROLE admin WITH LOGIN SUPERUSER PASSWORD 'admin';
  ELSE
    ALTER ROLE admin WITH LOGIN SUPERUSER PASSWORD 'admin';
  END IF;
END
$poc$;
'@
    $done = $false
    foreach ($localSuperuser in @('admin', 'airflow', 'postgres')) {
        $output = & kubectl -n $namespace exec airflow-postgres-0 -c postgres -- psql -X -v ON_ERROR_STOP=1 -U $localSuperuser -d airflow -c $sql 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "PostgreSQL local PoC role reconciled (via $localSuperuser)."
            $done = $true
            break
        }
    }
    if (-not $done) {
        throw 'Could not update PostgreSQL admin role via local socket. Existing PGDATA may use custom authentication. PVCs were preserved.'
    }

    # Verify actual TCP password authentication by exit code only. Avoid
    # examining psql stdout: kubectl/PowerShell may wrap output differently.
    # `\conninfo` requires no SQL quotes and fails if login fails.
    & kubectl -n $namespace exec airflow-postgres-0 -c postgres -- `
        env PGPASSWORD=admin psql -X -w -h 127.0.0.1 -U admin -d airflow `
        -c '\conninfo' | Out-Host
    Assert-NativeSuccess 'PostgreSQL TCP admin credential check'
    Write-Host 'PostgreSQL TCP admin authentication verified.'

    # Run migrations independently of Airflow Helm hook semantics. This makes
    # upgrades and re-deploys idempotent and releases init-container waiters.
    $job = @'
apiVersion: batch/v1
kind: Job
metadata:
  name: airflow-db-migrate-poc
  namespace: twitter-data
  labels:
    app.kubernetes.io/part-of: twitter-batch-platform
spec:
  backoffLimit: 2
  ttlSecondsAfterFinished: 86400
  template:
    metadata:
      labels:
        app: airflow-db-migrate-poc
    spec:
      restartPolicy: Never
      automountServiceAccountToken: false
      containers:
        - name: migrate
          image: twitter-airflow:local
          imagePullPolicy: IfNotPresent
          securityContext:
            runAsNonRoot: true
            runAsUser: 50000
            runAsGroup: 0
            allowPrivilegeEscalation: false
            capabilities: {drop: ["ALL"]}
            seccompProfile: {type: RuntimeDefault}
          command: ["/bin/bash", "-ec"]
          args: ["exec airflow db migrate"]
          env:
            - name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
              value: "postgresql+psycopg2://admin:admin@airflow-postgres:5432/airflow"
            - name: AIRFLOW__CORE__LOAD_EXAMPLES
              value: "False"
          resources:
            requests: {cpu: 100m, memory: 256Mi}
            limits: {cpu: "1", memory: 1Gi}
'@
    & kubectl -n $namespace delete job airflow-db-migrate-poc --ignore-not-found=true --wait=true
    Assert-NativeSuccess 'Remove previous one-shot migration job'
    $job | & kubectl apply -f -
    Assert-NativeSuccess 'Create database migration job'
    & kubectl -n $namespace wait --for=condition=complete job/airflow-db-migrate-poc --timeout=600s
    if ($LASTEXITCODE -ne 0) {
        & kubectl -n $namespace describe job airflow-db-migrate-poc
        & kubectl -n $namespace get pods -l job-name=airflow-db-migrate-poc -o wide
        & kubectl -n $namespace logs job/airflow-db-migrate-poc --all-containers=true --tail=120 --prefix=true
        throw 'Airflow metadata migration failed. Check the migration logs above. No database volumes were removed.'
    }
    Write-Host 'Airflow metadata database migration completed.'
}
