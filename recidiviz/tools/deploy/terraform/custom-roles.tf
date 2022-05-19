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

resource "google_project_iam_custom_role" "state-admin-role" {
  role_id     = "stateAdminRole"
  title       = "State Admin Role"
  description = "Role that gives external state agencies permissions to upload files with state data to GCS."
  permissions = ["storage.objects.create", "storage.objects.get", "storage.objects.list"]
}

resource "google_project_iam_custom_role" "gcs-object-and-bucket-viewer" {
  role_id     = "gcsObjectAndBucketViewer"
  title       = "GCS Object and Bucket Viewer"
  description = "Role that lets service accounts view GCS buckets as well as the objects within them. This is a superset of the storage.objectViewer role."
  permissions = [
    "storage.buckets.get",
    "storage.buckets.list",
    "storage.objects.get",
    "storage.objects.list",
  ]
}

resource "google_project_iam_custom_role" "sql-importer" {
  role_id     = "sqlImporter"
  title       = "Recidiviz SQL Importer"
  description = "Role that lets service accounts import data into a SQL database, without needing to have Cloud SQL Admin"
  permissions = [
    "cloudsql.instances.import",
  ]
}
