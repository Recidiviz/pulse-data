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
"""Supervision population count time series.

To generate the BQ view, run:
python -m recidiviz.calculator.query.state.views.dashboard.pathways.event_level.supervision_population
"""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import (
    filter_to_enabled_states,
    get_binned_time_period_months,
)
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.calculator.query.state.state_specific_query_strings import (
    pathways_state_specific_supervision_level,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_metric_big_query_view import (
    PathwaysMetricBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_POPULATION_VIEW_NAME = "supervision_population"

SUPERVISION_POPULATION_VIEW_DESCRIPTION = """Supervision population count.
    This query outputs one row per session, per month.
"""

SUPERVISION_POPULATION_VIEW_QUERY_TEMPLATE = """
    /*{description}*/
    WITH sessions_by_month AS (
        SELECT 
            sessions.state_code,
            sessions.person_id,
            sessions.start_date,
            sessions.end_date,
            sessions.dataflow_session_id,
            date_in_population,
            EXTRACT(YEAR FROM date_in_population) AS year,
            EXTRACT(MONTH FROM date_in_population) AS month,
            {time_period_months} AS time_period,            
            COALESCE(name_map.location_name, session_attributes.supervision_office, "UNKNOWN") AS supervision_district,
            COALESCE(CASE
                WHEN sessions.state_code="US_ND" THEN NULL
                ELSE {state_specific_supervision_level}
            END, "UNKNOWN") AS supervision_level,
            COALESCE(compartment_sessions.prioritized_race_or_ethnicity, "UNKNOWN") as race
        FROM `{project_id}.{sessions_dataset}.dataflow_sessions_materialized` sessions,
            UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 5 YEAR), MONTH), 
                CURRENT_DATE('US/Eastern'), INTERVAL 1 MONTH)) as date_in_population,
            UNNEST (session_attributes) session_attributes
        LEFT JOIN `{project_id}.{sessions_dataset}.compartment_sessions_materialized` compartment_sessions
            ON sessions.state_code = compartment_sessions.state_code
            AND sessions.person_id = compartment_sessions.person_id
            AND sessions.dataflow_session_id <= compartment_sessions.dataflow_session_id_end
            AND sessions.dataflow_session_id >= compartment_sessions.dataflow_session_id_start
        LEFT JOIN `{project_id}.{dashboards_dataset}.pathways_supervision_location_name_map` name_map
            ON sessions.state_code = name_map.state_code
            AND session_attributes.supervision_office = name_map.location_id
        WHERE session_attributes.compartment_level_1 = 'SUPERVISION' 
            AND date_in_population BETWEEN sessions.start_date AND COALESCE(sessions.end_date, CURRENT_DATE('US/Eastern'))
    ),
    coalesced AS (
        SELECT
            state_code,
            person_id,
            date_in_population,
            year,
            month,
            time_period,
            start_date,
            end_date,
            {dimensions_clause}
        FROM sessions_by_month
    )
    SELECT {columns}
    FROM coalesced
    {filter_to_enabled_states} AND {state_specific_district_filter}
    AND time_period IS NOT NULL
    ORDER BY state_code, person_id, year, month
    """

SUPERVISION_POPULATION_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    columns=[
        "state_code",
        "person_id",
        "start_date",
        "end_date",
        "date_in_population",
        "year",
        "month",
        "time_period",
        "supervision_district",
        "supervision_level",
        "race",
    ],
    dimensions_clause=PathwaysMetricBigQueryViewBuilder.replace_unknowns(
        ["supervision_district", "supervision_level", "race"]
    ),
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_POPULATION_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_VIEW_QUERY_TEMPLATE,
    description=SUPERVISION_POPULATION_VIEW_DESCRIPTION,
    dashboards_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    filter_to_enabled_states=filter_to_enabled_states(
        state_code_column="state_code", enabled_states=get_pathways_enabled_states()
    ),
    state_specific_supervision_level=pathways_state_specific_supervision_level(
        "sessions.state_code",
        "session_attributes.correctional_level",
    ),
    state_specific_district_filter=state_specific_query_strings.pathways_state_specific_supervision_district_filter(
        district_column_name="supervision_district"
    ),
    time_period_months=get_binned_time_period_months(
        "date_in_population",
        current_date_expr="DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH)",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_VIEW_BUILDER.build_and_print()
