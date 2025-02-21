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
"""An officer-level view of the task eligibility sessions person spans, used to identify
the sessions when an officer had any clients considered eligible or almost eligible.
"""
from recidiviz.calculator.query.bq_utils import nonnull_end_date_clause
from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_intersection_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.observations.views.spans.person import task_eligibility_session
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# Re-use the task-specific attribute columns from the task eligibility session spans,
# but drop any attributes that are only relevant at the person-level
_OFFICER_SPAN_ATTRIBUTE_COLUMNS = [
    col
    for col in task_eligibility_session.VIEW_BUILDER.attribute_cols
    # drop attributes that cannot easily be aggregate to the officer-level
    if col not in ["ineligible_criteria", "custody_level"]
]

# Aggregate the eligibility attribute columns from person-level to officer-level
_OFFICER_LEVEL_AGGREGATION_COLUMNS = [
    "is_eligible",
    "is_almost_eligible",
    "is_eligible_or_almost_eligible",
]

# Separate out the attribute columns that apply to all rows with the same
# state_code and task_type to help with a GROUP BY below
_TASK_TYPE_ATTRIBUTE_COLUMNS = [
    col
    for col in _OFFICER_SPAN_ATTRIBUTE_COLUMNS
    # drop attribute columns that need to be aggregated at the officer-level
    if col not in _OFFICER_LEVEL_AGGREGATION_COLUMNS
]

_SOURCE_DATA_QUERY_TEMPLATE = f"""
WITH
-- Pull all person-level task eligibility sessions through TES in order to
-- support look backs that can extend pre-launch (required to support zero
-- grants for opportunities launched within the last 12 months)
task_eligibility_sessions AS (
    SELECT
        state_code,
        person_id,
        start_date,
        end_date AS end_date_exclusive,
        task_name,
        is_eligible,
        is_almost_eligible,
    FROM
        `{{project_id}}.observations__person_span.task_eligibility_session_materialized`
    WHERE
        system_type = "SUPERVISION"
        -- Optimize this query by dropping eligibility spans from more than 10 years ago
        -- since that eligibility & officer data is likely not very relevant/accurate
        AND {nonnull_end_date_clause("end_date")} >= DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 10 YEAR)
)
,
-- Use `supervision_officer_sessions` for the person <> supervision officer mapping view
client_officer_assignments AS (
    SELECT * FROM `{{project_id}}.sessions.supervision_officer_sessions_materialized`
)
,
-- Create intersection spans between the person-level eligibility spans
-- and any overlapping client supervision officer assignments, use
-- |include_zero_day_intersections = True| in order to match the task <> officer
-- attribution logic used for the rest of aggregated metrics
person_officer_eligibility_sessions AS (
{create_intersection_spans(
    table_1_name="task_eligibility_sessions",
    table_2_name="client_officer_assignments",
    index_columns=["state_code", "person_id"],
    table_1_columns=["task_name", "is_eligible", "is_almost_eligible"],
    table_2_columns=["supervising_officer_external_id"],
    include_zero_day_intersections=True,
)}
)
,
-- Create eligibility sub-sessions at the officer-level, creating a sub-session
-- boundary for every caseload change OR eligibility status change
{create_sub_sessions_with_attributes(
    table_name="person_officer_eligibility_sessions",
    index_columns=["state_code", "supervising_officer_external_id"],
    end_date_field_name="end_date_exclusive",
)}
,
-- Keep 1 sub-session per officer and time period, aggregating the relevant
-- person-level eligibility attributes up to the officer-level
distinct_officer_task_eligibility_sessions AS (
    SELECT
        state_code,
        supervising_officer_external_id AS officer_id,
        start_date,
        end_date_exclusive,
        task_name,
        -- Set the eligibility flags to TRUE if at least 1 client
        -- falls into that category during the span
        LOGICAL_OR(is_eligible = "true") AS is_eligible,
        LOGICAL_OR(is_almost_eligible = "true") AS is_almost_eligible,
        LOGICAL_OR(is_eligible = "true" OR is_almost_eligible = "true") AS is_eligible_or_almost_eligible,
    FROM sub_sessions_with_attributes
    GROUP BY
        state_code,
        supervising_officer_external_id,
        start_date,
        end_date_exclusive,
        task_name
),
-- Aggregate any adjacent spans that share the same eligibility attributes
-- for the same task type
aggregated_spans AS (
{aggregate_adjacent_spans(
    table_name="distinct_officer_task_eligibility_sessions",
    index_columns=["state_code", "officer_id", "task_name"],
    attribute=["is_eligible", "is_almost_eligible", "is_eligible_or_almost_eligible"],
    end_date_field_name="end_date_exclusive",
)}
)
SELECT
    state_code,
    officer_id,
    start_date,
    end_date_exclusive,
    task_name,
    completion_event_type AS task_type,
    system_type,
    decarceral_impact_type,
    is_jii_decarceral_transition,
    has_mandatory_due_date,
    launches.first_access_date IS NOT NULL AS task_type_is_live,
    IFNULL(launches.is_fully_launched, FALSE) AS task_type_is_fully_launched,
    is_eligible,
    is_almost_eligible,
    is_eligible_or_almost_eligible
FROM aggregated_spans
INNER JOIN
    `{{project_id}}.reference_views.task_to_completion_event`
USING
    (state_code, task_name)
INNER JOIN
    `{{project_id}}.reference_views.completion_event_type_metadata_materialized` metadata
USING
    (completion_event_type)
LEFT JOIN
    `{{project_id}}.analyst_data.workflows_live_completion_event_types_by_state_materialized` launches
USING
    (state_code, completion_event_type)
"""

VIEW_BUILDER: SpanObservationBigQueryViewBuilder = SpanObservationBigQueryViewBuilder(
    span_type=SpanType.SUPERVISION_OFFICER_ELIGIBILITY_SESSIONS,
    description=__doc__,
    sql_source=_SOURCE_DATA_QUERY_TEMPLATE,
    attribute_cols=_TASK_TYPE_ATTRIBUTE_COLUMNS + _OFFICER_LEVEL_AGGREGATION_COLUMNS,
    span_start_date_col="start_date",
    span_end_date_col="end_date_exclusive",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
