#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
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
"""Supervision to liberty population snapshot by dimension.

To generate the BQ view, run:
python -m recidiviz.calculator.query.state.views.dashboard.pathways.supervision_to_liberty_population_snapshot_by_dimension
"""
from recidiviz.calculator.query.bq_utils import (
    add_age_groups,
    get_binned_time_period_months,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_supervision_last_updated_date,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_TO_LIBERTY_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_NAME = (
    "supervision_to_liberty_population_snapshot_by_dimension"
)

SUPERVISION_TO_LIBERTY_POPULATION_SNAPSHOT_BY_DIMENSION_DESCRIPTION = (
    """Supervision to liberty population snapshot by dimension"""
)

SUPERVISION_TO_LIBERTY_POPULATION_SNAPSHOT_BY_DIMENSION_QUERY_TEMPLATE = """
    /*{description}*/
    WITH transitions AS (
        SELECT
            state_code,
            person_id,
            gender,
            {add_age_groups}
            supervision_type,
            district_id,
            prioritized_race_or_ethnicity AS race,
            {binned_time_periods} AS time_period,
        FROM `{project_id}.{reference_views_dataset}.supervision_to_liberty_transitions`
        WHERE state_code = 'US_ND'
            AND transition_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 60 MONTH)
    ),
    get_last_updated AS ({get_pathways_supervision_last_updated_date})

    SELECT
        transitions.state_code,
        get_last_updated.last_updated,
        time_period,
        gender,
        age_group,
        race,
        supervision_type,
        IFNULL(location_name, district_id) AS district,
        COUNT(1) AS event_count,
    FROM transitions,
    UNNEST([gender, 'ALL']) AS gender,
    UNNEST([race, 'ALL']) AS race,
    UNNEST([age_group, 'ALL']) AS age_group,
    UNNEST([supervision_type, 'ALL']) AS supervision_type,
    UNNEST([district_id, 'ALL']) AS district_id
    LEFT JOIN get_last_updated
        USING (state_code)
    LEFT JOIN `{project_id}.{dashboards_dataset}.pathways_supervision_location_name_map` locations
        ON district_id = location_id
        AND transitions.state_code = locations.state_code
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
"""

SUPERVISION_TO_LIBERTY_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_TO_LIBERTY_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_NAME,
    view_query_template=SUPERVISION_TO_LIBERTY_POPULATION_SNAPSHOT_BY_DIMENSION_QUERY_TEMPLATE,
    description=SUPERVISION_TO_LIBERTY_POPULATION_SNAPSHOT_BY_DIMENSION_DESCRIPTION,
    dimensions=(
        "state_code",
        "last_updated",
        "gender",
        "district",
        "supervision_level",
        "age_group",
        "race",
    ),
    dashboards_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    binned_time_periods=get_binned_time_period_months("transition_date"),
    get_pathways_supervision_last_updated_date=get_pathways_supervision_last_updated_date(),
    add_age_groups=add_age_groups("age"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TO_LIBERTY_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER.build_and_print()
