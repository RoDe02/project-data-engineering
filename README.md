# Twitter Geospatial Batch Platform

**Pipeline:** UCI archive → Python ingestion → MinIO Bronze → Spark Silver → Spark Gold. Airflow orchestrates batches, PostgreSQL stores Airflow metadata. Terraform and Helm provision the workloads.

## Prerequisites

- Windows PowerShell.
- Docker Desktop with Linux containers, Kubernetes enabled (single node cluster created with kubeadm)
- `docker` and `kubectl` on `PATH`.
- Terraform `>= 1.16.0, < 2.0.0`: place the Windows `terraform.exe` in the repository root.
- Internet access for images, providers, charts and the initial dataset download.

Run all commands from the repository root. 

## Deploy

Enable unsigned scripts in each new PowerShell window:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
```

This setting expires with the session. Organization Group Policy can override it.

```powershell
# Select docker-desktop and check nodes, Linux containers and storage.
.\scripts\bootstrap.ps1

.\scripts\deploy.ps1

# Report pod readiness, storage, permissions and the latest batch status.
.\scripts\health.ps1 -Detailed
```

`deploy.ps1` builds images, provisions resources and credentials, migrates the Airflow database, and auto-applies Terraform changes.


```powershell
# Enable the DAG and submit one batch for the next unfinished source day.
.\scripts\run.ps1 -Unpause

# Report current task states and batch progress. Rerun to refresh.
.\scripts\health.ps1 -Detailed
```

The trigger returns before processing finishes. Wait until `run_state`, `release_validation` and `day_completion` are all `success`, then verify the output:

```powershell
# Validate the latest Gold release and verify reader permissions in a temporary pod.
.\scripts\inspect-release.ps1 -CheckAccess
```

`-CheckAccess` demonstrates the access restrictions for downstream applications such as ML consumers using the `minio-ml-reader` credentials: they can read only aggregated Gold data, while the policy grants no access to raw Bronze or detailed Silver data. The check reads a Gold Parquet header, attempts a Silver read and a Gold write, and prints the actual MinIO errors for the expected denials. Bronze access is restricted by the policy but is not explicitly tested by this command.

Gold reduces location detail to hourly counts in 0.25° grid cells and suppresses groups below the minimum count (default: 5). This reduces exposure of individual locations; it is not a guarantee of full anonymization.

| Operation | Command |
| --- | --- |
| Process next unfinished day | `.\scripts\run.ps1` |
| Replay a specific day | `.\scripts\run.ps1 -Date 2013-01-15` |
| Unpause an already queued run | `.\scripts\run.ps1 -UnpauseOnly` |
| Inspect latest release | `.\scripts\inspect-release.ps1` |



## Local interfaces

Run each forward in a separate PowerShell window after setting its execution policy. Keep it open. Ctrl+C stops forwarding.

| Service | Command | URL | Login |
| --- | --- | --- | --- |
| Airflow | `.\scripts\airflow-ui.ps1` | http://localhost:8080 | `admin / admin` |
| MinIO | `.\scripts\minio-ui.ps1` | http://localhost:9001 | `admin / adminadmin` |



### Restart the Airflow UI forward after deployment changes

Redeployment can replace the webserver pod and interrupt an existing port-forward. If the UI stops responding, press Ctrl+C in its forwarding window, then restart it and refresh the browser:

```powershell
.\scripts\airflow-ui.ps1
```

## Scalability example: one vs. two Spark executors
 vs.
Use the same source day and fixed Docker Desktop CPU/memory allocation. Finish active batches before switching profiles. Both profiles use the same per-executor resources and partition settings.

```powershell
# Deploy one Spark executor and process the comparison day.
.\scripts\deploy.ps1 -DemoProfile OneExecutor
.\scripts\run.ps1 -Date 2013-01-15 -Unpause
.\scripts\health.ps1 -Detailed
```

Wait for the batch to succeed, then run `inspect-release.ps1` and record its `run_id`. Use `minio-ui.ps1` to read `silver/daily/date=2013-01-15/runs/<run_id>/manifest.json`. Record `source_version`, source/valid/invalid counts, and `write_metrics`.

```powershell
# After recording the first result, replay the same day with two executors.
.\scripts\deploy.ps1 -DemoProfile TwoExecutors # Alternatively: -DemoProfile -FourExecutors
.\scripts\run.ps1 -Date 2013-01-15
.\scripts\health.ps1 -Detailed
```

Wait for success and record the second manifest in the same way. Confirm identical source versions/counts and `write_partitions`, and executor counts of 1 and 2. Compare `elapsed_seconds` and `parquet_files`.The timer covers Spark session startup through Silver output listing, excluding Bronze download. Repeat runs if needed to account for caching and runtime variation. Two executors may be slower on a resource-constrained host.

## Example run files

The [_example-runs](./_example-runs/) folder contains saved outputs for source day `2013-01-15`:

- `manifest_one-executor.json`, `manifest_two-executor.json`, and `manifest_four-executor.json`: Silver manifests with record counts and `write_metrics` for each executor count.
- `latest.json`: the Gold release metadata from the four-executor run. It contains no runtime measurements.

These are static examples, not the live MinIO release pointer. Compare `write_metrics.elapsed_seconds` across the Silver manifests after checking matching source versions, record counts and partition settings.
Since all examples were executed on the same hardware, the performance differences between varying numbers of executors are noticeable but relatively small.
## Teardown

```powershell
# After DELETE confirmation, remove managed namespaces and persistent data.
.\scripts\destroy.ps1
```

Requires confirmation `DELETE`. Removes managed namespaces and persistent data, retains the Docker Desktop cluster.

Dataset: [UCI Twitter Geospatial Data](https://archive.ics.uci.edu/dataset/1050/twitter+geospatial+data)
