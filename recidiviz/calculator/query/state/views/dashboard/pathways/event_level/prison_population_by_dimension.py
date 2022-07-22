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
"""Prison population count time series.

To generate the BQ view, run:
python -m recidiviz.calculator.query.state.views.dashboard.pathways.event_level.prison_population_by_dimension
"""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import (
    add_age_groups,
    filter_to_enabled_states,
    length_of_stay_month_groups,
)
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_metric_big_query_view import (
    PathwaysMetricBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PRISON_POPULATION_BY_DIMENSION_VIEW_NAME = "prison_population_by_dimension"

PRISON_POPULATION_BY_DIMENSION_VIEW_DESCRIPTION = """Prison population by dimension.
    This query outputs at least one row per person.
"""

PRISON_POPULATION_BY_DIMENSION_VIEW_QUERY_TEMPLATE = """
       /*{description}*/
    WITH length_of_stay_bins AS (
        SELECT 
        person_id,
        {length_of_stay_months_grouped} AS length_of_stay,
        FROM (
            SELECT
                person_id,
                DATE_DIFF(CURRENT_DATE('US/Eastern'), start_date, MONTH) AS length_of_stay_months,
            FROM `{project_id}.{sessions_dataset}.compartment_level_1_super_sessions_materialized`
            WHERE compartment_level_1 = 'INCARCERATION'
                AND end_date IS NULL
        )
    ),
    all_dimensions AS (
        SELECT
            metrics.state_code,
            person_id,
            gender,
            admission_reason,
            COALESCE(aggregating_location_id, metrics.facility) AS facility,
            {add_age_groups}
            length_of_stay,
            prioritized_race_or_ethnicity as race,
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_single_day_incarceration_population_metrics_included_in_state_population_materialized` metrics
        LEFT JOIN length_of_stay_bins USING (person_id)
        LEFT JOIN `{project_id}.{dashboards_dataset}.pathways_incarceration_location_name_map` name_map
            ON metrics.state_code = name_map.state_code
            AND metrics.facility = name_map.location_id
    ), data AS (
        SELECT DISTINCT
            state_code,
            person_id,
            length_of_stay,
            {dimensions_clause}
        FROM all_dimensions
        {filter_to_enabled_states} AND {facility_filter} 
    )
    SELECT {columns} FROM data
"""

PRISON_POPULATION_BY_DIMENSION_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    columns=[
        "state_code",
        "person_id",
        "gender",
        "admission_reason",
        "facility",
        "age_group",
        "length_of_stay",
        "race",
    ],
    dimensions_clause=PathwaysMetricBigQueryViewBuilder.replace_unknowns(
        ["gender", "admission_reason", "facility", "age_group", "race"]
    ),
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=PRISON_POPULATION_BY_DIMENSION_VIEW_NAME,
    view_query_template=PRISON_POPULATION_BY_DIMENSION_VIEW_QUERY_TEMPLATE,
    description=PRISON_POPULATION_BY_DIMENSION_VIEW_DESCRIPTION,
    dashboards_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    filter_to_enabled_states=filter_to_enabled_states(
        state_code_column="state_code", enabled_states=get_pathways_enabled_states()
    ),
    add_age_groups=add_age_groups(),
    length_of_stay_months_grouped=length_of_stay_month_groups(),
    facility_filter=state_specific_query_strings.pathways_state_specific_facility_filter(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRISON_POPULATION_BY_DIMENSION_VIEW_BUILDER.build_and_print()
