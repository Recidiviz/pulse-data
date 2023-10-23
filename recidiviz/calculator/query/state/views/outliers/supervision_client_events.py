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
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "supervision_client_events"

_DESCRIPTION = """Information about individual-level events for supervision clients."""


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
supervision_client_events AS (
    SELECT 
        e.state_code, 
        e.metric_id,
        e.event_date,
        pid.external_id AS client_id,
        p.full_name AS client_name,
        a.officer_id,
        NULL AS attributes,
        {get_pseudonymized_id_query_str("e.state_code || pid.external_id")} AS pseudonymized_client_id,
        {get_pseudonymized_id_query_str("e.state_code || a.officer_id")} AS pseudonymized_officer_id,
    FROM 
        `{{project_id}}.aggregated_metrics.supervision_officer_metrics_assignment_sessions_materialized` a
    CROSS JOIN
        latest_year_time_period period
    INNER JOIN `{{project_id}}.normalized_state.state_person` p 
        USING (state_code, person_id)
    INNER JOIN events_with_metric_id e
        USING (state_code, person_id)
    INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` pid
        USING (state_code, person_id)
    WHERE
        event_date BETWEEN assignment_date AND COALESCE(end_date, CURRENT_DATE("US/Eastern"))
        AND event_date >= population_start_date
        AND event_date < population_end_date
        AND pid.id_type = {{state_id_type}}
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
        "attributes",
        "pseudonymized_client_id",
        "pseudonymized_officer_id",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_CLIENT_EVENTS_VIEW_BUILDER.build_and_print()
