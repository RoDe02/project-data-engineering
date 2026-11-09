variable "kubeconfig_path" {
  description = "Absolute path to kubeconfig, set by Windows deployment script."
  type        = string
  default     = "~/.kube/config"
}

variable "kube_context" {
  description = "Existing Docker Desktop Kubernetes context."
  type        = string
  default     = "docker-desktop"
}

variable "namespace" {
  description = "Namespace for the data platform."
  type        = string
  default     = "twitter-data"
}

variable "spark_operator_version" {
  description = "Spark Operator Helm chart version."
  type        = string
  default     = "2.2.0"
}

variable "spark_tuning" {
  description = "SparkApplication resources and partition counts for each manually triggered batch."
  type = object({
    driver_cores            = optional(number, 1)
    driver_memory_mib       = optional(number, 2048)
    executor_instances      = optional(number, 2)
    executor_cores          = optional(number, 1)
    executor_memory_mib     = optional(number, 2048)
    shuffle_partitions      = optional(number, 64)
    silver_write_partitions = optional(number, 64)
  })
  default  = {}
  nullable = false

  validation {
    condition     = alltrue([for n in values(var.spark_tuning) : n >= 1 && floor(n) == n])
    error_message = "Spark resource and partition settings must be positive whole numbers."
  }
}
