variable "project_id" {
  description = "GCP project ID where platform resources are created."
  type        = string
}

variable "region" {
  description = "Default region for regional resources."
  type        = string
  default     = "europe-west1"
}

variable "location" {
  description = "BigQuery dataset location."
  type        = string
  default     = "EU"
}

variable "environment" {
  description = "Deployment environment label (dev, stg, prod)."
  type        = string
  default     = "dev"
}

variable "labels" {
  description = "Additional labels applied to managed resources."
  type        = map(string)
  default     = {}
}

variable "bigquery_datasets" {
  description = "Datasets required for bronze-to-serving architecture."
  type        = list(string)
  default = [
    "raw_zone",
    "staging",
    "core",
    "enrichment",
    "dashboard"
  ]
}

variable "create_runtime_service_account" {
  description = "Whether to create a runtime service account for ingestion and transforms."
  type        = bool
  default     = true
}

variable "runtime_service_account_id" {
  description = "Service account id (account_id) for runtime workload identity."
  type        = string
  default     = "sentiment-platform-runtime"
}

variable "runtime_project_roles" {
  description = "Project-level IAM roles assigned to runtime service account."
  type        = list(string)
  default = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser"
  ]
}
