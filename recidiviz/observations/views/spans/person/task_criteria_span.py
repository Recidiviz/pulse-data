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
"""View with spans of time over which a person is eligible for a given criteria"""

from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Spans of time over which a person is eligible for a given criteria"

_SOURCE_DATA_QUERY_TEMPLATE = """
WITH relevant_criteria AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        "INCARCERATION_PAST_FULL_TERM_RELEASE_DATE" AS criteria,
        meets_criteria,
    FROM
        `{project_id}.task_eligibility_criteria_general.incarceration_past_full_term_completion_date_materialized`
    
    UNION ALL
    
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        "SUPERVISION_PAST_FULL_TERM_RELEASE_DATE" AS criteria,
        meets_criteria,
    FROM
        `{project_id}.task_eligibility_criteria_general.supervision_past_full_term_completion_date_materialized`
    
    UNION ALL
    
    SELECT
        state_code,
        person_id,
        start_date,
        end_date,
        "INCARCERATION_PAST_PAROLE_ELIGIBILITY_DATE" AS criteria,
        meets_criteria,
    FROM
        `{project_id}.task_eligibility_criteria_general.incarceration_past_parole_eligibility_date_materialized`
)
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    criteria,
    meets_criteria
FROM relevant_criteria
"""

VIEW_BUILDER: SpanObservationBigQueryViewBuilder = SpanObservationBigQueryViewBuilder(
    span_type=SpanType.TASK_CRITERIA_SPAN,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "criteria",
        "meets_criteria",
    ],
    span_start_date_col="start_date",
    span_end_date_col="end_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
