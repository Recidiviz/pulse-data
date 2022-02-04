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
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    ENABLED_STATES,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_supervision_dimension_combinations import (
    PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_VIEW_NAME,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_POPULATION_TIME_SERIES_VIEW_NAME = "supervision_population_time_series"

SUPERVISION_POPULATION_TIME_SERIES_VIEW_DESCRIPTION = (
    """Supervision population count time series."""
)

SUPERVISION_POPULATION_TIME_SERIES_VIEW_QUERY_TEMPLATE = """
    /*{description}*/
    WITH
    mapped_district_names AS (
        SELECT
            metrics.state_code,
            year,
            month,
            IFNULL(location_name, level_1_supervision_location_external_id) AS district,
            supervision_level,
            COUNT(DISTINCT person_id) AS person_count
        FROM `{project_id}.{metrics_dataset}.most_recent_supervision_population_metrics_materialized` metrics
        LEFT JOIN `{project_id}.{dashboards_dataset}.pathways_supervision_location_name_map` name_map
            ON metrics.state_code = name_map.state_code
            AND metrics.level_1_supervision_location_external_id = name_map.location_id
        WHERE date_of_supervision >= DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 5 YEAR), MONTH)
        and date_of_stay = DATE(year, month, 1)
        group by 1,2,3,4,5
    ), full_time_series as (
        SELECT
            state_code,
            year,
            month,
            district,
            supervision_level,
            IFNULL(person_count, 0) AS person_count
        FROM mapped_district_names
        FULL OUTER JOIN
        (
            SELECT DISTINCT
                state_code,
                year,
                month,
                district,
                supervision_level
            FROM `{project_id}.{dashboard_views_dataset}.{dimension_combination_view}`
        ) USING (state_code, year, month, district, supervision_level)
    )
    SELECT
        state_code,
        year,
        month,
        district,
        supervision_level,
        SUM(person_count) AS person_count,
    FROM full_time_series,
    UNNEST([district, "ALL"]) AS district,
    UNNEST([supervision_level, "ALL"]) AS supervision_level
    {filter_to_enabled_states}
    AND DATE(year, month, 1) <= CURRENT_DATE('US/Eastern')
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY year, month
    """

SUPERVISION_POPULATION_TIME_SERIES_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_POPULATION_TIME_SERIES_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_TIME_SERIES_VIEW_QUERY_TEMPLATE,
    dimensions=("state_code", "year", "month", "district", "supervision_level"),
    description=SUPERVISION_POPULATION_TIME_SERIES_VIEW_DESCRIPTION,
    dashboards_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    filter_to_enabled_states=filter_to_enabled_states(
        state_code_column="state_code", enabled_states=ENABLED_STATES
    ),
    dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    dimension_combination_view=PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_VIEW_NAME,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_TIME_SERIES_VIEW_BUILDER.build_and_print()
