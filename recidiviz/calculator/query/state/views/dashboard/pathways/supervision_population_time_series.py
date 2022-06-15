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
python -m recidiviz.calculator.query.state.views.dashboard.pathways.supervision_population_time_series
"""
from recidiviz.calculator.query.bq_utils import filter_to_enabled_states
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
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_supervision_dimension_combinations import (
    PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_VIEW_NAME,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_POPULATION_TIME_SERIES_VIEW_NAME = "supervision_population_time_series"

SUPERVISION_POPULATION_TIME_SERIES_VIEW_DESCRIPTION = (
    """Supervision population count time series."""
)

SUPERVISION_POPULATION_TIME_SERIES_VIEW_QUERY_TEMPLATE = """
    /*{description}*/

    WITH filtered_rows AS (
        SELECT
            state_code,
            year,
            month,
            district,
            supervision_level,
            race,
            person_id
        FROM  `{project_id}.{dashboard_views_dataset}.pathways_deduped_supervision_sessions`
        WHERE {state_specific_district_filter}
    )
    , full_time_series as (
        SELECT 
            state_code,
            year,
            month,
            district,
            supervision_level,
            race,
            COUNT(person_id) AS person_count
        FROM filtered_rows
        FULL OUTER JOIN
        (
            SELECT DISTINCT
                state_code,
                year,
                month,
                district,
                supervision_level,
                race
            FROM `{project_id}.{dashboard_views_dataset}.{dimension_combination_view}`
        ) USING (state_code, year, month, district, supervision_level, race)
        GROUP BY 1,2,3,4,5, 6
    )
    SELECT
        state_code,
        year,
        month,
        district,
        supervision_level,
        race,
        SUM(person_count) AS person_count,
    FROM full_time_series,
    UNNEST([district, "ALL"]) AS district,
    UNNEST([supervision_level, "ALL"]) AS supervision_level,
    UNNEST([race, "ALL"]) AS race
    {filter_to_enabled_states}
    AND DATE(year, month, 1) <= CURRENT_DATE('US/Eastern')
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY year, month
    """

SUPERVISION_POPULATION_TIME_SERIES_VIEW_BUILDER = PathwaysMetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_POPULATION_TIME_SERIES_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_TIME_SERIES_VIEW_QUERY_TEMPLATE,
    # year must come before month to export correctly
    dimensions=("state_code", "year", "month", "district", "supervision_level", "race"),
    metric_stats=("person_count",),
    description=SUPERVISION_POPULATION_TIME_SERIES_VIEW_DESCRIPTION,
    filter_to_enabled_states=filter_to_enabled_states(
        state_code_column="state_code", enabled_states=get_pathways_enabled_states()
    ),
    dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    dimension_combination_view=PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_VIEW_NAME,
    state_specific_district_filter=state_specific_query_strings.pathways_state_specific_supervision_district_filter(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_TIME_SERIES_VIEW_BUILDER.build_and_print()
