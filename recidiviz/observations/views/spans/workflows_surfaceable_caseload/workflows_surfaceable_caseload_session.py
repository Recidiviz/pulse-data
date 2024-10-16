# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""View with spans of time over which a workflows caseload was surfaceable (
    had one or more eligible or almost eligible clients)"""

from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = """View with spans of time over which a workflows caseload was surfaceable (
    had one or more eligible or almost eligible clients)"""

_SOURCE_DATA_QUERY_TEMPLATE = """
    SELECT
        caseloads.state_code,
        caseloads.caseload_id,
        caseloads.start_date,
        caseloads.end_date_exclusive,
        caseloads.completion_event_type AS task_type,
        caseloads.system_type,
        launches.first_access_date IS NOT NULL AS task_type_is_live,
        IFNULL(launches.is_fully_launched, FALSE) AS task_type_is_fully_launched,
    FROM
        `{project_id}.analyst_data.workflows_record_archive_surfaceable_caseload_sessions_materialized` caseloads
    LEFT JOIN
        `{project_id}.analyst_data.workflows_live_completion_event_types_by_state_materialized` launches
    USING
        (state_code, completion_event_type)
"""

VIEW_BUILDER: SpanObservationBigQueryViewBuilder = SpanObservationBigQueryViewBuilder(
    span_type=SpanType.WORKFLOWS_SURFACEABLE_CASELOAD_SESSION,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "task_type",
        "system_type",
        "task_type_is_live",
        "task_type_is_fully_launched",
    ],
    span_start_date_col="start_date",
    span_end_date_col="end_date_exclusive",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
