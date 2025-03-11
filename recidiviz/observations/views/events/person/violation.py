# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""View with violations"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_exclusive_clause
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_DESCRIPTION = "Violations"

_SOURCE_DATA_QUERY_TEMPLATE = f"""
SELECT
    v.state_code,
    v.person_id,
    IFNULL(violation_date, response_date) AS event_date,
    COALESCE(violation_type, "INTERNAL_UNKNOWN") AS violation_type,
    violation_type_subtype,
    violation_type_subtype_raw_text,
    most_severe_response_decision,
    is_most_severe_violation_type_of_all_violations AS is_most_severe_violation_type,
    -- Indicates that event_date is hydrated with response_date because violation_date is NULL
    violation_date IS NULL AS is_inferred_violation_date,
    -- Deduplicates to the earliest response date associated with the violation date
    #TODO(#27010) remove once support for custom units of analysis are implemented 
    v.gender,
    v.prioritized_race_or_ethnicity,
    MIN(response_date) AS response_date,
    -- These should already be unique since we're pulling from sub-sessions
    ANY_VALUE(css.correctional_level) AS supervision_level,
    ANY_VALUE(css.correctional_level_raw_text) AS supervision_level_raw_text,
FROM
    `{{project_id}}.dataflow_metrics_materialized.most_recent_violation_with_response_metrics_materialized` v
LEFT JOIN
    `{{project_id}}.sessions.compartment_sub_sessions_materialized` css
ON v.person_id = css.person_id
AND IFNULL(violation_date, response_date) BETWEEN css.start_date AND {nonnull_end_date_exclusive_clause("css.end_date_exclusive")}
WHERE
    IFNULL(violation_date, response_date) IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
"""

VIEW_BUILDER: EventObservationBigQueryViewBuilder = EventObservationBigQueryViewBuilder(
    event_type=EventType.VIOLATION,
    description=_VIEW_DESCRIPTION,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=[
        "violation_type",
        "violation_type_subtype",
        "violation_type_subtype_raw_text",
        "most_severe_response_decision",
        "is_most_severe_violation_type",
        "response_date",
        "is_inferred_violation_date",
        "gender",
        "prioritized_race_or_ethnicity",
        "supervision_level",
        "supervision_level_raw_text",
    ],
    event_date_col="event_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
