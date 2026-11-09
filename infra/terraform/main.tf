resource "kubernetes_namespace_v1" "data" {
  metadata {
    name = var.namespace
  }
}

resource "kubernetes_namespace_v1" "spark" {
  metadata {
    name = "spark-operator"
  }
}

resource "helm_release" "minio" {
  name      = "twitter-minio"
  chart     = "${path.module}/../charts/minio"
  version   = yamldecode(file("${path.module}/../charts/minio/Chart.yaml")).version
  namespace = kubernetes_namespace_v1.data.metadata[0].name
  wait      = true
  timeout   = 900
}

resource "helm_release" "postgres" {
  name      = "twitter-postgres"
  chart     = "${path.module}/../charts/postgres"
  namespace = kubernetes_namespace_v1.data.metadata[0].name
  wait      = true
  timeout   = 900
}

resource "helm_release" "spark_operator" {
  name       = "spark-operator"
  repository = "https://kubeflow.github.io/spark-operator"
  chart      = "spark-operator"
  version    = var.spark_operator_version
  namespace  = kubernetes_namespace_v1.spark.metadata[0].name
  wait       = true
  timeout    = 900

  set = [
    {
      name  = "spark.jobNamespaces[0]"
      value = var.namespace
    },
    {
      name  = "webhook.enable"
      value = "true"
    },
    {
      name  = "spark.serviceAccount.name"
      value = "spark-runner"
    }
  ]

  depends_on = [
    kubernetes_namespace_v1.data
  ]
}

resource "helm_release" "airflow" {
  name       = "airflow"
  chart      = "https://github.com/apache/airflow/releases/download/helm-chart/1.16.0/airflow-1.16.0.tgz"
  namespace  = kubernetes_namespace_v1.data.metadata[0].name
  values = [
    file("${path.module}/airflow-values.yaml"),
    yamlencode({
      env = [for name, value in var.spark_tuning : {
        name  = "TWITTER_SPARK_${upper(name)}"
        value = tostring(value)
      }]
    })
  ]
  wait    = true
  timeout = 1200

  depends_on = [
    helm_release.minio,
    helm_release.spark_operator,
    helm_release.postgres
  ]
}
