# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
# Dataflow encrypts worker disks, Shuffle state, and Streaming Engine state with
# the key that the launch body names (recidiviz/pipelines/pipeline_parameters.py).
# The temp and staging buckets carry their own bucket-level keys (cloud-storage.tf);
# the job key does not reach them. Dataflow only accepts a key in the job's own
# region, so every region in
# DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE (recidiviz/pipelines/ingest/activity/
# pipeline_utils.py) needs its own key ring and key below; a pipeline launched in a
# region with no key fails at launch. Keyed by project id so every block below,
# including the imports, is empty in production until it migrates into the CJIS
# folder (Recidiviz/zenhub-tasks#2335).
locals {
  dataflow_cmek_regions = toset(var.project_id == "recidiviz-staging" ? [
    "us-central1",
    "us-east1",
    "us-east4",
    "us-east5",
    "us-south1",
    "us-west1",
    "us-west2",
    "us-west4",
  ] : [])
}

resource "google_kms_key_ring" "dataflow_cmek" {
  for_each = local.dataflow_cmek_regions
  project  = local.cmek_project_id
  name     = "dataflow-cmek"
  location = each.key
}

resource "google_kms_crypto_key" "dataflow_cmek" {
  for_each        = local.dataflow_cmek_regions
  name            = "${var.project_id}-dataflow"
  key_ring        = google_kms_key_ring.dataflow_cmek[each.key].id
  rotation_period = "7776000s" # 90 days

  lifecycle {
    prevent_destroy = true
  }
}

# The key rings and keys already exist in the CMEK project; the registry adopts
# them. These blocks can be removed once staging has applied them.
import {
  for_each = local.dataflow_cmek_regions
  to       = google_kms_key_ring.dataflow_cmek[each.key]
  id       = "projects/${local.cmek_project_id}/locations/${each.key}/keyRings/dataflow-cmek"
}

import {
  for_each = local.dataflow_cmek_regions
  to       = google_kms_crypto_key.dataflow_cmek[each.key]
  id       = "projects/${local.cmek_project_id}/locations/${each.key}/keyRings/dataflow-cmek/cryptoKeys/${var.project_id}-dataflow"
}

# Dataflow attaches the key through its own service agent, and Compute Engine
# encrypts the worker disks through its service agent, so both need the key.
resource "google_kms_crypto_key_iam_member" "dataflow_sa_cmek_user" {
  for_each      = local.dataflow_cmek_regions
  crypto_key_id = google_kms_crypto_key.dataflow_cmek[each.key].id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:service-${data.google_project.current.number}@dataflow-service-producer-prod.iam.gserviceaccount.com"
}

resource "google_kms_crypto_key_iam_member" "compute_sa_dataflow_cmek_user" {
  for_each      = local.dataflow_cmek_regions
  crypto_key_id = google_kms_crypto_key.dataflow_cmek[each.key].id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:service-${data.google_project.current.number}@compute-system.iam.gserviceaccount.com"
}
