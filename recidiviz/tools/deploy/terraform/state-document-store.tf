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

module "state_document_store_resources" {
  for_each = local.document_collection_states
  source   = "./modules/state-document-store-resources"

  state_code = each.key
  project_id = var.project_id
  region     = var.direct_ingest_region
}

# Import CMEK keys that were created by the oneoff script (PR #74881) before
# Terraform knew about them. Remove these blocks after the first successful apply.
import {
  for_each = local.document_collection_states
  id       = "projects/cmek-82ade411-5705-4461-b2cb-9/locations/us-east1/keyRings/gcs-cmek/cryptoKeys/${var.project_id}-${replace(lower(each.value), "_", "-")}-temp-document-store-output"
  to       = module.state_document_store_resources[each.value].module.document-store-upload-results-bucket.google_kms_crypto_key.gcs_cmek[0]
}

import {
  for_each = local.document_collection_states
  id       = "projects/cmek-82ade411-5705-4461-b2cb-9/locations/us-east1/keyRings/gcs-cmek/cryptoKeys/${var.project_id}-${replace(lower(each.value), "_", "-")}-document-blob-storage"
  to       = module.state_document_store_resources[each.value].module.document-blob-storage-bucket.google_kms_crypto_key.gcs_cmek[0]
}
