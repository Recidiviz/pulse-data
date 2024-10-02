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
"""View with task eligibility spans"""

from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Task eligibility spans"

_SOURCE_DATA_QUERY_TEMPLATE = """
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    task_name,
    completion_event_type AS task_type,
    system_type,
    launches.first_access_date IS NOT NULL AS task_type_is_live,
    IFNULL(launches.is_fully_launched, FALSE) AS task_type_is_fully_launched,
    is_eligible,
    ARRAY_TO_STRING(ineligible_criteria, ",") AS ineligible_criteria,
    is_almost_eligible,
    -- Used for pulling in custody level from relevant criteria to create custom metric of downgrades from a given level
    -- TODO(#27010): Remove this addition of custody level when custom unit of analysis is supported
    ARRAY(
        SELECT JSON_VALUE(criteria_reason, '$.reason.custody_level')
        FROM UNNEST(JSON_QUERY_ARRAY(reasons_v2)) AS criteria_reason
        WHERE JSON_VALUE(criteria_reason, '$.criteria_name') = 'CUSTODY_LEVEL_HIGHER_THAN_RECOMMENDED' 
    )[SAFE_OFFSET(0)] AS custody_level,
FROM
    `{project_id}.task_eligibility.all_tasks_materialized`
INNER JOIN
    `{project_id}.reference_views.task_to_completion_event`
USING
    (state_code, task_name)
INNER JOIN
    `{project_id}.reference_views.completion_event_type_metadata_materialized` metadata
USING
    (completion_event_type)
LEFT JOIN
    `{project_id}.analyst_data.workflows_live_completion_event_types_by_state_materialized` launches
USING
    (state_code, completion_event_type)
"""

VIEW_BUILDER: SpanObservationBigQueryViewBuilder = SpanObservationBigQueryViewBuilder(
    span_type=SpanType.TASK_ELIGIBILITY_SESSION,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "task_name",
        "task_type",
        "system_type",
        "task_type_is_live",
        "task_type_is_fully_launched",
        "is_eligible",
        "ineligible_criteria",
        "is_almost_eligible",
        "custody_level",
    ],
    span_start_date_col="start_date",
    span_end_date_col="end_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
