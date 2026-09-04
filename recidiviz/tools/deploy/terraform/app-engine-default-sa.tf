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

# The App Engine default service account (<project>@appspot) is created by Google, not by
# Terraform, and Google grants it roles/editor automatically. That grant is being removed in
# favour of the narrow set below, so the roles this account is meant to have are visible in
# code and reviewable in a PR rather than inherited silently.
#
# These are additive: declaring them here does NOT remove roles/editor, which is deleted
# out-of-band with gcloud because Terraform does not manage it. See AUR-43.
#
# The account also carries five other bindings that predate this file and remain unmanaged:
# bigquery.admin, datastore.importExportAdmin, secretmanager.secretAccessor,
# iap.httpsResourceAccessor, and the gcsObjectAndBucketViewer custom role. Bringing those
# under Terraform is deliberately out of scope here.
locals {
  # Suggested by IAM Recommender as the replacement for roles/editor, based on 90 days of
  # measured usage. Observed activity in audit logs is monitoring writes only
  # (telemetry.writer covers those); the other two are kept because the App Engine runtime
  # does work that audit logs do not capture.
  app_engine_default_sa_roles = [
    "roles/appengine.deployer",
    "roles/backupdr.cloudSqlOperator",
    "roles/telemetry.writer",
  ]
}

resource "google_project_iam_member" "app_engine_default_sa" {
  for_each = toset(local.app_engine_default_sa_roles)
  project  = var.project_id
  role     = each.key
  member   = "serviceAccount:${var.project_id}@appspot.gserviceaccount.com"
}
