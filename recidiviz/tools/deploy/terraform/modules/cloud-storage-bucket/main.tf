# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================

# The project id associated with the buckets and service accounts (e.g. "recidiviz-123").
variable "project_id" {
  type = string
}

# Combined with the project id to create the name (ex: "dashboard-data" becomes "recidiviz-123-dashboard-data")
variable "name_suffix" {
  type = string
}

# The location for the bucket, can be regional or multiregion (ex: "us-east1" or "us").
variable "location" {
  type    = string
  default = "us"
}

# The storage class of the bucket.
variable "storage_class" {
  type    = string
  default = "STANDARD"
}

# When true, access control to the bucket is controlled by IAM rather than
# object-level ACLs.
variable "uniform_bucket_level_access" {
  type    = bool
  default = true
}

# A map of key/value label pairs to assign to the bucket.
variable "labels" {
  type = map(string)
  default = {
    "vanta-owner"       = "joshua",
    "vanta-description" = "terraform-managed-gcs-bucket"
  }
}

# See https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket#soft_delete_policy-1
variable "soft_delete_policy" {
  type = object({
    retention_duration_seconds = number
  })
  default = null
}

# See https://cloud.google.com/storage/docs/lifecycle
variable "lifecycle_rules" {
  type = list(object({
    action = object({
      type = string
    }),
    condition = object({
      age                = optional(number)
      num_newer_versions = optional(number)
    })
  }))
  default = [
    {
      action = {
        type = "Delete"
      }
      condition = {
        num_newer_versions = 3
      }
    }
  ]
}

# When true, creates a CMEK crypto key for this bucket under the gcs-cmek
# key ring in the shared CMEK project and uses it to encrypt the bucket.
variable "use_cmek" {
  type    = bool
  default = false
}

variable "cmek_project_id" {
  type        = string
  description = "GCP project ID that holds the gcs-cmek key ring. Only used when use_cmek is true."
  default     = "cmek-82ade411-5705-4461-b2cb-9"
}


# CMEK resources: when use_cmek is true, create a per-bucket crypto key under
# the shared gcs-cmek key ring in the CMEK project.
data "google_kms_key_ring" "gcs_cmek" {
  count    = var.use_cmek ? 1 : 0
  project  = var.cmek_project_id
  name     = "gcs-cmek"
  location = var.location
}

resource "google_kms_crypto_key" "gcs_cmek" {
  count           = var.use_cmek ? 1 : 0
  name            = "${var.project_id}-${var.name_suffix}"
  key_ring        = data.google_kms_key_ring.gcs_cmek[0].id
  rotation_period = "7776000s" # 90 days

  lifecycle {
    prevent_destroy = true
  }
}

data "google_project" "bucket_project" {
  count      = var.use_cmek ? 1 : 0
  project_id = var.project_id
}

# Grant the GCS service agent permission to use the key.
resource "google_kms_crypto_key_iam_member" "gcs_sa_cmek_user" {
  count         = var.use_cmek ? 1 : 0
  crypto_key_id = google_kms_crypto_key.gcs_cmek[0].id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:service-${data.google_project.bucket_project[0].number}@gs-project-accounts.iam.gserviceaccount.com"
}


resource "google_storage_bucket" "bucket" {
  name                        = "${var.project_id}-${var.name_suffix}"
  location                    = var.location
  storage_class               = var.storage_class
  uniform_bucket_level_access = var.uniform_bucket_level_access

  depends_on = [google_kms_crypto_key_iam_member.gcs_sa_cmek_user]

  labels = var.labels

  logging {
    log_bucket = "${var.project_id}-gcs-object-logs"
  }

  dynamic "lifecycle_rule" {
    for_each = var.lifecycle_rules
    content {
      action {
        type = lifecycle_rule.value["action"].type
      }
      condition {
        age                = lifecycle_rule.value["condition"].age
        num_newer_versions = lifecycle_rule.value["condition"].num_newer_versions
      }
    }
  }

  versioning {
    enabled = true
  }

  dynamic "soft_delete_policy" {
    for_each = var.soft_delete_policy == null ? [] : [var.soft_delete_policy]
    content {
      retention_duration_seconds = soft_delete_policy.value.retention_duration_seconds
    }
  }

  dynamic "encryption" {
    for_each = var.use_cmek ? [1] : []
    content {
      default_kms_key_name = google_kms_crypto_key.gcs_cmek[0].id
    }
  }
}
