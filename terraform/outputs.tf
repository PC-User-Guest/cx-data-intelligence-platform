output "dataset_ids" {
  description = "BigQuery datasets created for the platform."
  value       = [for dataset in google_bigquery_dataset.platform : dataset.id]
}

output "runtime_service_account_email" {
  description = "Runtime service account email used by pipelines and transforms."
  value       = var.create_runtime_service_account ? google_service_account.runtime[0].email : null
}
