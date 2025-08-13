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

# Creates an "ingest bucket" for each state and instance, which serve as the 
# initial depository for data that is sent to Recidiviz (through SFTP or directly pushing).
# The raw data DAG will import these files' contents into BigQuery and then move the files to 
# the "storage bucket" defined by the module "direct-ingest-state-storage" in cloud-storage.tf

# Note that var.direct_ingest_region is a GCP region not to be confused with 
# a "direct ingest region" comprised of a state code and instance elsewhere in the codebase.

# TODO(#6160): Rename this module to better reflect what it actually is doing for us.
module "state_direct_ingest_buckets_and_accounts" {
  for_each = toset(keys(local.direct_ingest_region_manifests_to_deploy))
  source   = "./modules/state-direct-ingest-resources"

  state_code                             = each.key
  region                                 = var.direct_ingest_region
  is_production                          = local.is_production
  project_id                             = var.project_id
  state_admin_role                       = google_project_iam_custom_role.state-admin-role.name
  region_manifest                        = local.direct_ingest_region_manifests_to_deploy[each.key]
  raw_data_storage_notification_topic_id = google_pubsub_topic.raw_data_storage_notification_topic.id
}
