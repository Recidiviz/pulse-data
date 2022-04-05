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
    WITH cte AS (
        /*
        Use equivalent logic from compartment_sessions to deduplicate individuals who have more than
        one supervision location on a given day.
        */
        SELECT 
            s.state_code,
            s.person_id,
            date_of_supervision,
            EXTRACT(YEAR FROM date_of_supervision) AS year,
            EXTRACT(MONTH FROM date_of_supervision) AS month,
            IFNULL(name_map.location_name,session_attributes.supervision_office) AS district,
            CASE WHEN s.state_code="US_ND" THEN NULL 
                ELSE IFNULL(session_attributes.correctional_level, "EXTERNAL_UNKNOWN") END AS supervision_level,
        FROM `{project_id}.{sessions_dataset}.dataflow_sessions_materialized` s,
        UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 5 YEAR), MONTH), 
            CURRENT_DATE('US/Eastern'), INTERVAL 1 MONTH)) as date_of_supervision,
        UNNEST (session_attributes) session_attributes
        LEFT JOIN `{project_id}.{dashboards_dataset}.pathways_supervision_location_name_map` name_map
            ON s.state_code = name_map.state_code
            AND session_attributes.supervision_office = name_map.location_id
        LEFT JOIN `{project_id}.{sessions_dataset}.compartment_level_2_dedup_priority` cl2_dedup
            ON "SUPERVISION" = cl2_dedup.compartment_level_1
            AND session_attributes.compartment_level_2=cl2_dedup.compartment_level_2
        LEFT JOIN `{project_id}.{sessions_dataset}.supervision_level_dedup_priority` sl_dedup
            ON session_attributes.correctional_level=sl_dedup.correctional_level
        WHERE session_attributes.compartment_level_1 = 'SUPERVISION' 
            AND date_of_supervision BETWEEN s.start_date AND COALESCE(s.end_date, CURRENT_DATE('US/Eastern'))
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, date_of_supervision
            ORDER BY COALESCE(cl2_dedup.priority, 999),
            COALESCE(sl_dedup.correctional_level_priority, 999),
            NULLIF(session_attributes.supervising_officer_external_id, 'EXTERNAL_UNKNOWN') NULLS LAST,
            NULLIF(session_attributes.case_type, 'EXTERNAL_UNKNOWN') NULLS LAST,
            NULLIF(session_attributes.judicial_district_code, 'EXTERNAL_UNKNOWN') NULLS LAST
        ) = 1
    ),
    full_time_series as (
        SELECT 
            state_code,
            year,
            month,
            district,
            supervision_level,
            COUNT(person_id) AS person_count
        FROM cte
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
        GROUP BY 1,2,3,4,5
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

SUPERVISION_POPULATION_TIME_SERIES_VIEW_BUILDER = PathwaysMetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_POPULATION_TIME_SERIES_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_TIME_SERIES_VIEW_QUERY_TEMPLATE,
    dimensions=("state_code", "year", "month", "district", "supervision_level"),
    description=SUPERVISION_POPULATION_TIME_SERIES_VIEW_DESCRIPTION,
    dashboards_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
    filter_to_enabled_states=filter_to_enabled_states(
        state_code_column="state_code", enabled_states=ENABLED_STATES
    ),
    dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    dimension_combination_view=PATHWAYS_SUPERVISION_DIMENSION_COMBINATIONS_VIEW_NAME,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_TIME_SERIES_VIEW_BUILDER.build_and_print()
