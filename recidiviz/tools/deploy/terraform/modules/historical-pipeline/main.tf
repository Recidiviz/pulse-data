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

resource "google_pubsub_topic" "start_historical_pipeline" {
  name = "v1.calculator.${local.lower_state_code}_historical_${var.pipeline_type}"
}

resource "google_cloudfunctions_function" "trigger_historical_pipeline" {
  name    = "trigger_calculation_pipeline_historical_${var.pipeline_type}_${local.lower_state_code}"
  runtime = "python38"
  labels = {
    "deployment-tool" = "terraform"
  }

  event_trigger {
    event_type = "google.pubsub.topic.publish"
    resource   = google_pubsub_topic.start_historical_pipeline.id
  }


  entry_point = "start_calculation_pipeline"
  environment_variables = {
    "TEMPLATE_NAME" = local.template_name
    "JOB_NAME"      = local.template_name
    "REGION"        = var.region
  }

  source_repository {
    url = "https://source.developers.google.com/projects/${var.project_id}/repos/github_Recidiviz_pulse-data/revisions/${var.git_hash}/paths/recidiviz/cloud_functions"
  }
}
