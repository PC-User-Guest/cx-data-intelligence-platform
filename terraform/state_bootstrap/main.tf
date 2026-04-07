provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  common_labels = merge(
    {
      managed_by  = "terraform"
      environment = "shared"
      purpose     = "terraform-state"
    },
    var.labels
  )
}

resource "google_storage_bucket" "terraform_state" {
  name                        = var.bucket_name
  location                    = var.bucket_location
  force_destroy               = false
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  labels                      = local.common_labels

  versioning {
    enabled = true
  }

  retention_policy {
    retention_period = var.retention_period_seconds
    is_locked        = false
  }

  dynamic "encryption" {
    for_each = var.kms_key_name != "" ? [1] : []
    content {
      default_kms_key_name = var.kms_key_name
    }
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }

    condition {
      age                = 180
      with_state         = "ARCHIVED"
      send_age_if_zero   = false
      num_newer_versions = 20
    }
  }
}
