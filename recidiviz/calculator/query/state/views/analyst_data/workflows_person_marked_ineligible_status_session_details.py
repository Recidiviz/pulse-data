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
"""
View that enriches marked ineligible funnel status sessions with additional 
information from snooze events.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "workflows_person_marked_ineligible_status_session_details"

_VIEW_DESCRIPTION = """
View that enriches marked ineligible funnel status sessions with additional 
information from snooze events.
"""

_QUERY_TEMPLATE = f"""
-- We need to use the funnel sessions instead of just the snooze spans because the funnel sessions 
-- have additional logic in place to close out snooze spans properly.
WITH marked_ineligible_funnel_status_sessions AS (
    SELECT
        s.state_code,
        person_id,
        INITCAP(JSON_EXTRACT_SCALAR(full_name, "$.given_names")) AS first_name,
        INITCAP(JSON_EXTRACT_SCALAR(full_name, "$.surname")) AS last_name,
        task_type,
        start_date,
        end_date_exclusive,
        is_eligible,
        is_almost_eligible
    FROM
        `{{project_id}}.analyst_data.workflows_person_impact_funnel_status_sessions_materialized` AS s
    LEFT JOIN 
        `{{project_id}}.normalized_state.state_person`
    USING 
        (state_code, person_id)
    WHERE
        marked_ineligible
)
,
-- Aggregate adjacent marked ineligible status sessions such that we would expect
-- the beginning of each resulting session to correspond with a snooze event
marked_ineligible_funnel_status_sessions_aggregated AS (
{aggregate_adjacent_spans(table_name = "marked_ineligible_funnel_status_sessions",
                          attribute = ["first_name","last_name","task_type"],
                          end_date_field_name = "end_date_exclusive")}
)
,
clients_snooze_spans AS (
    SELECT 
        snoozes.state_code,
        person_id,
        completion_event_type AS task_type,
        staff_id AS snoozed_by_staff_id,
        LOWER(snoozed_by) AS snoozed_by_email,
        INITCAP(JSON_EXTRACT_SCALAR(full_name, "$.given_names")) AS snoozed_by_first_name,
        INITCAP(JSON_EXTRACT_SCALAR(full_name, "$.surname")) AS snoozed_by_last_name,
        denial_reasons,
        other_reason,
        start_date AS snooze_start_date,
        end_date_scheduled AS snooze_end_date_scheduled,
        end_date_actual AS snooze_end_date_actual
    FROM 
        `{{project_id}}.workflows_views.clients_snooze_spans_materialized` AS snoozes
    INNER JOIN 
        `{{project_id}}.reference_views.workflows_opportunity_configs_materialized`
    USING 
        (state_code, opportunity_type)
    LEFT JOIN 
        `{{project_id}}.normalized_state.state_staff` AS staff
    ON 
        snoozes.state_code = staff.state_code 
        AND LOWER(snoozes.snoozed_by) = LOWER(staff.email)
),
-- See https://github.com/Recidiviz/pulse-data/issues/35535 for explanation of why deduping
-- is necessary
clients_snooze_spans_deduped AS (
    SELECT
        state_code,
        person_id,
        task_type,
        snoozed_by_staff_id,
        snoozed_by_email,
        snoozed_by_first_name,
        snoozed_by_last_name,
        ARRAY_AGG(DISTINCT denial_reasons_unnested ORDER BY denial_reasons_unnested) AS denial_reasons,
        ARRAY_AGG(other_reason IGNORE NULLS ORDER BY other_reason) AS other_reason,
        CAST(snooze_start_date AS DATE) as snooze_start_date,
        ARRAY_AGG(snooze_start_date ORDER BY snooze_start_date) AS snooze_start_timestamps,
        ARRAY_AGG(snooze_end_date_scheduled IGNORE NULLS ORDER BY snooze_end_date_scheduled) AS snooze_end_timestamps_scheduled,
        ARRAY_AGG(snooze_end_date_actual IGNORE NULLS ORDER BY snooze_end_date_actual) AS snooze_end_timestamps_actual
    FROM
        clients_snooze_spans, UNNEST(denial_reasons) AS denial_reasons_unnested
    GROUP BY
        state_code,
        person_id,
        task_type,
        snoozed_by_staff_id,
        snoozed_by_email,
        snoozed_by_first_name,
        snoozed_by_last_name,
        CAST(snooze_start_date AS DATE)
)

SELECT
    status_sessions_aggregated.person_id,
    status_sessions_aggregated.state_code,
    status_sessions_aggregated.start_date AS marked_ineligible_status_session_start_date,
    status_sessions_aggregated.end_date_exclusive AS marked_ineligible_status_session_end_date,
    status_sessions_aggregated.first_name,
    status_sessions_aggregated.last_name,
    status_sessions_aggregated.task_type,
    is_eligible,
    is_almost_eligible,
    snoozed_by_staff_id,
    snoozed_by_email,
    snoozed_by_first_name,
    snoozed_by_last_name,
    denial_reasons,
    other_reason,
    snooze_start_date,
    snooze_start_timestamps,
    snooze_end_timestamps_scheduled,
    snooze_end_timestamps_actual
FROM 
    marked_ineligible_funnel_status_sessions_aggregated AS status_sessions_aggregated
-- Join with snoozes to include info from the snooze that happened at the beginning
-- of the marked ineligible funnel status session
LEFT JOIN 
    clients_snooze_spans_deduped AS snoozes
ON 
    status_sessions_aggregated.person_id = snoozes.person_id
    AND status_sessions_aggregated.state_code = snoozes.state_code
    AND status_sessions_aggregated.task_type = snoozes.task_type
    -- In the resulting view, we will only have snooze details for the snooze that 
    -- began on the start date of the marked ineligible status session
    AND (CAST(status_sessions_aggregated.start_date AS DATE) = snoozes.snooze_start_date)
    -- Join on the original marked ineligible status sessions to retrieve the eligibility
    -- flags that applied at the beginning of the status session. We can't include these
    -- flags prior to aggregating adjacent spans because we want to aggregate adjacent
    -- marked ineligible spans regardless of eligibility changes mid-span.
LEFT JOIN 
    marked_ineligible_funnel_status_sessions AS status_sessions
ON 
    status_sessions_aggregated.person_id = status_sessions.person_id
    AND status_sessions_aggregated.state_code = status_sessions.state_code
    AND status_sessions_aggregated.task_type = status_sessions.task_type
    AND status_sessions_aggregated.start_date = status_sessions.start_date
"""

WORKFLOWS_PERSON_MARKED_INELIGIBLE_STATUS_SESSION_DETAILS_VIEW_BUILDER = (
    SimpleBigQueryViewBuilder(
        dataset_id=ANALYST_VIEWS_DATASET,
        view_id=_VIEW_NAME,
        description=_VIEW_DESCRIPTION,
        view_query_template=_QUERY_TEMPLATE,
        clustering_fields=["state_code", "task_type"],
        should_materialize=True,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        WORKFLOWS_PERSON_MARKED_INELIGIBLE_STATUS_SESSION_DETAILS_VIEW_BUILDER.build_and_print()
