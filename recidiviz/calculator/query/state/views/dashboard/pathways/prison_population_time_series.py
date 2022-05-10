#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Prison population count time series."""
from recidiviz.calculator.query.bq_utils import add_age_groups, filter_to_enabled_states
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_incarceration_last_updated_date,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    get_pathways_enabled_states,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_metric_big_query_view import (
    PathwaysMetricBigQueryViewBuilder,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_prison_dimension_combinations import (
    PATHWAYS_PRISON_DIMENSION_COMBINATIONS_VIEW_NAME,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PRISON_POPULATION_TIME_SERIES_VIEW_NAME = "prison_population_time_series"

PRISON_POPULATION_TIME_SERIES_DESCRIPTION = """Prison population time series """

PRISON_POPULATION_TIME_SERIES_QUERY_TEMPLATE = """
    /*{description}*/
    WITH get_last_updated AS ({get_pathways_incarceration_last_updated_date}),
    add_mapped_dimensions AS (
        SELECT 
            pop.state_code,
            year,
            month,
            gender,
            admission_reason,
            IFNULL(aggregating_location_id, pop.facility) AS facility,
            {add_age_groups}
            COUNT(DISTINCT person_id) AS person_count
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_included_in_state_population_materialized` pop
        LEFT JOIN `{project_id}.{dashboard_views_dataset}.pathways_incarceration_location_name_map` name_map
            ON pop.state_code = name_map.state_code
            AND pop.facility = name_map.location_id
        WHERE date_of_stay >= DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 5 YEAR), MONTH)
        AND date_of_stay = DATE(year, month, 1)
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    )
    , filtered_rows AS (
        SELECT *
        FROM add_mapped_dimensions
        WHERE {facility_filter}
    )
    , full_time_series as (
        SELECT
            state_code,
            year,
            month,
            gender,
            admission_reason as legal_status,
            facility,
            age_group,
            IFNULL(person_count, 0) as person_count
        FROM filtered_rows
        FULL OUTER JOIN
        (
            SELECT DISTINCT
                state_code,
                year,
                month,
                gender,
                facility,
                age_group,
                admission_reason,
            FROM `{project_id}.{dashboard_views_dataset}.{dimension_combination_view}`
        ) USING (state_code, year, month, gender, facility, age_group, admission_reason)
    )
    SELECT
        state_code,
        get_last_updated.last_updated,
        year,
        month,
        gender,
        legal_status,
        facility,
        age_group,
        SUM(person_count) as person_count,
    FROM full_time_series,
    UNNEST ([age_group, 'ALL']) as age_group,
    UNNEST ([legal_status, 'ALL']) as legal_status,
    UNNEST ([facility, 'ALL']) as facility,
    UNNEST ([gender, 'ALL']) as gender
    LEFT JOIN get_last_updated  USING (state_code)
    {filter_to_enabled_states}
    AND DATE(year, month, 1) <= CURRENT_DATE('US/Eastern')
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    ORDER BY year, month
"""

PRISON_POPULATION_TIME_SERIES_VIEW_BUILDER = PathwaysMetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=PRISON_POPULATION_TIME_SERIES_VIEW_NAME,
    view_query_template=PRISON_POPULATION_TIME_SERIES_QUERY_TEMPLATE,
    description=PRISON_POPULATION_TIME_SERIES_DESCRIPTION,
    dimensions=(
        "state_code",
        "year",
        "month",
        "gender",
        "legal_status",
        "facility",
        "age_group",
    ),
    dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    add_age_groups=add_age_groups(),
    get_pathways_incarceration_last_updated_date=get_pathways_incarceration_last_updated_date(),
    filter_to_enabled_states=filter_to_enabled_states(
        state_code_column="state_code", enabled_states=get_pathways_enabled_states()
    ),
    dimension_combination_view=PATHWAYS_PRISON_DIMENSION_COMBINATIONS_VIEW_NAME,
    facility_filter=state_specific_query_strings.pathways_state_specific_facility_filter(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRISON_POPULATION_TIME_SERIES_VIEW_BUILDER.build_and_print()
