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
"""Utils for the cloud function that triggers that CloudSQL to BQ refresh."""

PIPELINE_RUN_TYPE_REQUEST_ARG = "pipeline_run_type"
PIPELINE_RUN_TYPE_NONE_VALUE = "no_pipelines"
# This value must be the same as the MetricPipelineRunType.HISTORICAL value,
# but lowercase
PIPELINE_RUN_TYPE_HISTORICAL_VALUE = "historical"

UPDATE_MANAGED_VIEWS_REQUEST_ARG = "update_managed_views"

TRIGGER_HISTORICAL_DAG_FLAG = "TRIGGER_HISTORICAL_DAG"
NO_HISTORICAL_DAG_FLAG = "NO_HISTORICAL_DAG"
