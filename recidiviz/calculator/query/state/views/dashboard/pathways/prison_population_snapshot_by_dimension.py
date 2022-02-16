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
"""Prison population snapshot by dimension."""
from recidiviz.calculator.query.bq_utils import (
    add_age_groups,
    filter_to_enabled_states,
    length_of_stay_month_groups,
)
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.calculator.query.state.dataset_config import (
    DASHBOARD_VIEWS_DATASET,
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_incarceration_last_updated_date,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_enabled_states import (
    ENABLED_STATES,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_metric_big_query_view import (
    PathwaysMetricBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_NAME = (
    "prison_population_snapshot_by_dimension"
)

PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_DESCRIPTION = (
    """Prison population snapshot by dimension"""
)

PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_QUERY_TEMPLATE = """
    /*{description}*/
    WITH get_last_updated AS ({get_pathways_incarceration_last_updated_date}),
    length_of_stay_bins AS (
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
            gender,
            admission_reason AS legal_status,
            IFNULL(aggregating_location_id, metrics.facility) AS facility,
            {add_age_groups}
            length_of_stay,
            COUNT(DISTINCT person_id) as person_count
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_single_day_incarceration_population_metrics_included_in_state_population_materialized` metrics
        LEFT JOIN length_of_stay_bins USING (person_id)
        LEFT JOIN `{project_id}.{dashboard_views_dataset}.pathways_incarceration_location_name_map` name_map
            ON metrics.state_code = name_map.state_code
            AND metrics.facility = name_map.location_id
        group by 1, 2, 3, 4, 5, 6
    ),
    filtered_rows AS (
        SELECT *
        FROM all_dimensions
        WHERE {facility_filter}
    )
    SELECT
        state_code,
        get_last_updated.last_updated,
        gender,
        legal_status,
        facility,
        age_group,
        length_of_stay,
        SUM(person_count) as person_count
    FROM filtered_rows,
    UNNEST ([age_group, 'ALL']) as age_group,
    UNNEST ([legal_status, 'ALL']) as legal_status,
    UNNEST ([facility, 'ALL']) as facility,
    UNNEST ([gender, 'ALL']) as gender,
    UNNEST ([length_of_stay, 'ALL']) as length_of_stay
    LEFT JOIN get_last_updated  USING (state_code)
    {filter_to_enabled_states}
    group by 1, 2, 3, 4, 5, 6, 7
"""

PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER = PathwaysMetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_NAME,
    view_query_template=PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_QUERY_TEMPLATE,
    description=PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_DESCRIPTION,
    dimensions=(
        "state_code",
        "gender",
        "legal_status",
        "facility",
        "age_group",
        "length_of_stay",
    ),
    dashboard_views_dataset=DASHBOARD_VIEWS_DATASET,
    materialized_metrics_dataset=DATAFLOW_METRICS_MATERIALIZED_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    add_age_groups=add_age_groups(),
    get_pathways_incarceration_last_updated_date=get_pathways_incarceration_last_updated_date(),
    filter_to_enabled_states=filter_to_enabled_states(
        state_code_column="state_code", enabled_states=ENABLED_STATES
    ),
    length_of_stay_months_grouped=length_of_stay_month_groups(),
    facility_filter=state_specific_query_strings.pathways_state_specific_facility_filter(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER.build_and_print()
