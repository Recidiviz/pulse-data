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

resource "google_cloud_scheduler_job" "schedule_incremental_calculation_pipeline_topic" {
  name             = "schedule_incremental_calculation_pipeline_cloud_function"
  schedule         = "0 6 * * *" # Every day at 6 am
  description      = "Schedules the running of the incremental calculation pipeline topic"
  time_zone        = "America/Los_Angeles"

  pubsub_target {
    # topic's full resource name.
    topic_name = "projects/${var.project_id}/topics/v1.calculator.trigger_calculation_pipelines"
    data       = base64encode("DAILY")
  }
}
