# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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


resource "google_pubsub_subscription" "export_metric_view_data" {
  name  = "export-metric-view-data"
  topic = "projects/${var.project_id}/topics/v1.export.view.data"

  push_config {
    push_endpoint = "${local.app_engine_url}/export/create_metric_view_data_export_tasks"
    oidc_token {
      service_account_email = data.google_app_engine_default_service_account.default.email
      # https://cloud.google.com/pubsub/docs/push#configure_for_push_authentication
      audience = local.app_engine_iap_client
    }
  }
}
