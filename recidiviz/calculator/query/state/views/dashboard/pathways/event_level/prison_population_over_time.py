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
python -m recidiviz.calculator.query.state.views.dashboard.pathways.event_level.prison_population_over_time
"""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import (
    add_age_groups,
    filter_to_enabled_states,
    get_binned_time_period_months,
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

PRISON_POPULATION_OVER_TIME_VIEW_NAME = "prison_population_over_time"

PRISON_POPULATION_OVER_TIME_VIEW_DESCRIPTION = """Prison population count.
    This query outputs at least one row per person, per month.
    Additional rows are included for distinct attribute combinations.
"""

PRISON_POPULATION_OVER_TIME_VIEW_QUERY_TEMPLATE = """
    /*{description}*/
    WITH 
    add_mapped_dimensions AS (
        SELECT 
            pop.state_code,
            year,
            month,
            {time_period_months} AS time_period,
            gender,
            admission_reason,
            IFNULL(aggregating_location_id, pop.facility) AS facility,
            {add_age_groups}
            prioritized_race_or_ethnicity AS race,
            person_id
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_included_in_state_population_materialized` pop
        LEFT JOIN `{project_id}.{dashboards_dataset}.pathways_incarceration_location_name_map` name_map
            ON pop.state_code = name_map.state_code
            AND pop.facility = name_map.location_id
        WHERE date_of_stay >= DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 5 YEAR), MONTH)
        AND EXTRACT(DAY FROM date_of_stay) = 1
    )
    , filtered_rows AS (
        SELECT 
            state_code,
            year,
            month,
            time_period,
            person_id,
            {dimensions_clause},
        FROM add_mapped_dimensions
        {filter_to_enabled_states}
        AND {facility_filter}
        AND DATE(year, month, 1) <= CURRENT_DATE('US/Eastern')
        AND time_period IS NOT NULL
        # TODO(#13850) Remove age_group filter for US_MI when Pathways is on the new backend
        AND CASE
            WHEN state_code IN ("US_MI", "US_CO") THEN
                age_group = "ALL"
            ELSE true
        END
    )
    SELECT DISTINCT
        {columns}
    FROM filtered_rows
    ORDER BY state_code, year, month, person_id
"""

PRISON_POPULATION_OVER_TIME_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    columns=[
        "state_code",
        "person_id",
        "year",
        "month",
        "time_period",
        "gender",
        "admission_reason",
        "facility",
        "age_group",
        "race",
    ],
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=PRISON_POPULATION_OVER_TIME_VIEW_NAME,
    view_query_template=PRISON_POPULATION_OVER_TIME_VIEW_QUERY_TEMPLATE,
    description=PRISON_POPULATION_OVER_TIME_VIEW_DESCRIPTION,
    dashboards_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    add_age_groups=add_age_groups(),
    filter_to_enabled_states=filter_to_enabled_states(
        state_code_column="state_code", enabled_states=get_pathways_enabled_states()
    ),
    dimensions_clause=PathwaysMetricBigQueryViewBuilder.replace_unknowns(
        [
            "gender",
            "admission_reason",
            "facility",
            "age_group",
            "race",
        ]
    ),
    facility_filter=state_specific_query_strings.pathways_state_specific_facility_filter(),
    time_period_months=get_binned_time_period_months(
        "DATE(year, month, 1)",
        current_date_expr="DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH)",
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRISON_POPULATION_OVER_TIME_VIEW_BUILDER.build_and_print()
