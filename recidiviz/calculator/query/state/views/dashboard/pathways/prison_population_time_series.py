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
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_incarceration_last_updated_date,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    ENABLED_STATES,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
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
            admission_reason AS legal_status,
            IFNULL(location_name, pop.facility) AS facility,
            {add_age_groups}
            COUNT(DISTINCT person_id) AS person_count
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_included_in_state_population_materialized` pop
        LEFT JOIN `{project_id}.{dashboard_views_dataset}.pathways_incarceration_location_name_map` name_map
            ON pop.state_code = name_map.state_code
            AND pop.facility = name_map.location_id
        WHERE date_of_stay >= DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 5 YEAR), MONTH)
        GROUP BY 1, 2, 3, 4, 5, 6, 7
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
    FROM add_mapped_dimensions,
    UNNEST ([age_group, 'ALL']) as age_group,
    UNNEST ([legal_status, 'ALL']) as legal_status,
    UNNEST ([facility, 'ALL']) as facility,
    UNNEST ([gender, 'ALL']) as gender
    LEFT JOIN get_last_updated  USING (state_code)

    {filter_to_enabled_states}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    ORDER BY year, month
"""

PRISON_POPULATION_TIME_SERIES_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=PRISON_POPULATION_TIME_SERIES_VIEW_NAME,
    view_query_template=PRISON_POPULATION_TIME_SERIES_QUERY_TEMPLATE,
    description=PRISON_POPULATION_TIME_SERIES_DESCRIPTION,
    dimensions=(
        "state_code",
        "last_updated",
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
        state_code_column="state_code", enabled_states=ENABLED_STATES
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRISON_POPULATION_TIME_SERIES_VIEW_BUILDER.build_and_print()
