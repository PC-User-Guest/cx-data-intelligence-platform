variable "project_id" {
  description = "GCP project that owns the Terraform state bucket."
  type        = string
}

variable "region" {
  description = "Region used for provider operations."
  type        = string
  default     = "europe-west1"
}

variable "bucket_name" {
  description = "Unique GCS bucket name for remote Terraform state."
  type        = string
}

variable "bucket_location" {
  description = "Location for the Terraform state bucket."
  type        = string
  default     = "EU"
}

variable "retention_period_seconds" {
  description = "Minimum retention period for state objects."
  type        = number
  default     = 604800
}

variable "kms_key_name" {
  description = "Optional KMS key resource name for bucket encryption."
  type        = string
  default     = ""
}

variable "labels" {
  description = "Additional labels on the state bucket."
  type        = map(string)
  default     = {}
}
