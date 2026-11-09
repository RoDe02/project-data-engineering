output "namespace" {
  value = kubernetes_namespace_v1.data.metadata[0].name
}

output "minio_service" {
  value = "minio.${var.namespace}.svc.cluster.local:9000"
}

output "airflow_forward" {
  value = "kubectl -n ${var.namespace} port-forward svc/airflow-webserver 8080:8080"
}
