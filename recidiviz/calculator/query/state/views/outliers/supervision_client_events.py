# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Information about individual-level events for supervision clients."""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import get_pseudonymized_id_query_str
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    state_specific_external_id_type,
)
from recidiviz.calculator.query.state.views.outliers.utils import (
    format_state_specific_person_events_filters,
)
from recidiviz.outliers.constants import VIOLATION_RESPONSES, VIOLATIONS
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "supervision_client_events"

_DESCRIPTION = """Information about individual-level events for supervision clients."""

# NOTE: When adding new queries to this, if the event can be queried from person_events or similar,
# the list of event types corresponding to the event in question can be found in
# event.aggregated_metric.event_types where event is an OutliersClientEvent
#
# In order to be queried from the frontend, the relevant OutliersClientEvent must also be listed
# in the OutliersConfig for the given state.

_QUERY_TEMPLATE = f"""
WITH
latest_year_time_period AS (
    SELECT
        population_start_date,
        population_end_date,
    FROM
        `{{project_id}}.aggregated_metrics.metric_time_periods_materialized`
    WHERE 
        period = "YEAR"
    QUALIFY ROW_NUMBER() OVER (ORDER BY population_start_date DESC) = 1
),
events_with_metric_id AS (
  {format_state_specific_person_events_filters()}
),
violations AS (
    -- TODO(#24491): Add real query
    SELECT
        "US_MI" AS state_code,
        "{VIOLATIONS.name}" AS metric_id,
        DATE(9999, 12, 31) AS event_date,
        0 AS person_id,
        TO_JSON_STRING(
            STRUCT(
                "TEST" AS code,
                "TEST" AS description
            )
        ) AS attributes
    
    UNION ALL

    -- TODO(#24489): Add real query
    SELECT
        "US_TN" AS state_code,
        "{VIOLATIONS.name}" AS metric_id,
        DATE(9999, 12, 31) AS event_date,
        0 AS person_id,
        TO_JSON_STRING(
            STRUCT(
                "TEST" AS code,
                "TEST" AS description
            )
        ) AS attributes
    
),
sanctions AS (
    -- TODO(#24492): Add real query
    SELECT
        "US_MI" AS state_code,
        "{VIOLATION_RESPONSES.name}" AS metric_id,
        DATE(9999, 12, 31) AS event_date,
        0 AS person_id,
        TO_JSON_STRING(
            STRUCT(
                "TEST" AS code,
                "TEST" AS description
            )
        ) AS attributes
    
    UNION ALL

    -- TODO(#24490): Add real query
    SELECT
        "US_TN" AS state_code,
        "{VIOLATION_RESPONSES.name}" AS metric_id,
        DATE(9999, 12, 31) AS event_date,
        0 AS person_id,
        TO_JSON_STRING(
            STRUCT(
                "TEST" AS code,
                "TEST" AS description
            )
        ) AS attributes
    
),
all_events AS (
    SELECT * FROM events_with_metric_id
        UNION ALL
    SELECT * FROM violations
        UNION ALL
    SELECT * FROM sanctions
),
supervision_client_events AS (
    SELECT 
        e.state_code, 
        e.metric_id,
        e.event_date,
        pid.external_id AS client_id,
        p.full_name AS client_name,
        a.officer_id,
        a.assignment_date AS officer_assignment_date,
        a.end_date AS officer_assignment_end_date,
        c.start_date AS supervision_start_date,
        c.end_date AS supervision_end_date,
        c.compartment_level_2 AS supervision_type, 
        e.attributes,
        {get_pseudonymized_id_query_str("e.state_code || pid.external_id")} AS pseudonymized_client_id,
        -- This pseudonymized_id will match the one for the user in the auth0 roster. Hashed
        -- attributes must be kept in sync with recidiviz.auth.helpers.generate_pseudonymized_id.
        {get_pseudonymized_id_query_str("e.state_code || a.officer_id")} AS pseudonymized_officer_id,
    FROM 
        `{{project_id}}.aggregated_metrics.supervision_officer_metrics_person_assignment_sessions_materialized` a
    CROSS JOIN
        latest_year_time_period period
    INNER JOIN `{{project_id}}.normalized_state.state_person` p 
        USING (state_code, person_id)
    INNER JOIN all_events e
        USING (state_code, person_id)
    INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` pid
        USING (state_code, person_id)
    INNER JOIN `{{project_id}}.sessions.compartment_sessions_materialized` c  
        USING (state_code, person_id)
    WHERE
        -- Get events for the latest year period only
        event_date >= population_start_date
        AND event_date < population_end_date
        -- Get the officer assignment information at the time of the event
        AND event_date BETWEEN assignment_date AND COALESCE(a.end_date, CURRENT_DATE("US/Eastern"))
        -- Get the supervision period information at the time of the event
        AND c.compartment_level_1 = 'SUPERVISION'
        AND pid.id_type = {{state_id_type}}
    -- selects the first period among two periods that overlap with the same event date; 
    -- allow us to not prioritize a session that starts on the same day as the event (unless there is no other overlapping period) when multiple officer sessions end on the same day as the event date
    QUALIFY RANK() OVER (PARTITION BY person_id, event_date ORDER BY COALESCE(c.end_date_exclusive, "9999-01-01")) = 1
)

SELECT 
    {{columns}}
FROM supervision_client_events
"""

SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_DESCRIPTION,
    should_materialize=True,
    state_id_type=state_specific_external_id_type("pid"),
    columns=[
        "state_code",
        "metric_id",
        "event_date",
        "client_id",
        "client_name",
        "officer_id",
        "officer_assignment_date",
        "officer_assignment_end_date",
        "supervision_start_date",
        "supervision_end_date",
        "supervision_type",
        "attributes",
        "pseudonymized_client_id",
        "pseudonymized_officer_id",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER.build_and_print()
