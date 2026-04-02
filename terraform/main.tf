locals {
  common_labels = merge(
    {
      environment = var.environment
      managed_by  = "terraform"
      platform    = "realtime-customer-sentiment"
    },
    var.labels
  )

  required_apis = toset([
    "bigquery.googleapis.com",
    "iam.googleapis.com"
  ])
}

resource "google_project_service" "required" {
  for_each = local.required_apis

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_bigquery_dataset" "platform" {
  for_each = toset(var.bigquery_datasets)

  project                    = var.project_id
  dataset_id                 = each.value
  location                   = var.location
  delete_contents_on_destroy = false
  labels                     = local.common_labels

  depends_on = [google_project_service.required]
}

resource "google_service_account" "runtime" {
  count = var.create_runtime_service_account ? 1 : 0

  account_id   = var.runtime_service_account_id
  display_name = "Runtime SA for Real-Time Sentiment Platform"
  description  = "Used by ingestion, orchestration and transformation execution runtimes"
}

resource "google_project_iam_member" "runtime_roles" {
  for_each = var.create_runtime_service_account ? toset(var.runtime_project_roles) : toset([])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.runtime[0].email}"
}
