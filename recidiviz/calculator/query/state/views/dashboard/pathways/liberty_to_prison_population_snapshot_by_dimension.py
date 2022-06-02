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
"""Liberty to prison population snapshot by dimension.

To generate the BQ view, run:
python -m recidiviz.calculator.query.state.views.dashboard.pathways.liberty_to_prison_population_snapshot_by_dimension
"""
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_incarceration_last_updated_date,
)
from recidiviz.calculator.query.state.views.dashboard.pathways.pathways_metric_big_query_view import (
    PathwaysMetricBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

LIBERTY_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_NAME = (
    "liberty_to_prison_population_snapshot_by_dimension"
)

LIBERTY_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_DESCRIPTION = (
    """Admissions to prison from liberty population snapshot by dimension"""
)

LIBERTY_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_QUERY_TEMPLATE = """
    /*{description}*/
    WITH transitions AS (
        SELECT
            state_code,
            gender,
            age_group,
            judicial_district,
            race,
            time_period,
            prior_length_of_incarceration
        FROM `{project_id}.{dashboard_views_dataset}.liberty_to_prison_transitions`
        WHERE transition_date >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 60 MONTH)
    ),
    get_last_updated AS ({get_pathways_incarceration_last_updated_date})

    SELECT
        transitions.state_code,
        get_last_updated.last_updated,
        time_period,
        gender,
        age_group,
        race,
        judicial_district,
        prior_length_of_incarceration,
        COUNT(1) AS event_count,
    FROM transitions,
    UNNEST([gender, 'ALL']) AS gender,
    UNNEST([race, 'ALL']) AS race,
    UNNEST([age_group, 'ALL']) AS age_group,
    UNNEST([judicial_district, 'ALL']) AS judicial_district,
    UNNEST([prior_length_of_incarceration, 'ALL']) AS prior_length_of_incarceration
    LEFT JOIN get_last_updated
        USING (state_code)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
"""

LIBERTY_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER = PathwaysMetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=LIBERTY_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_NAME,
    view_query_template=LIBERTY_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_QUERY_TEMPLATE,
    description=LIBERTY_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_DESCRIPTION,
    dimensions=(
        "state_code",
        "time_period",
        "gender",
        "age_group",
        "race",
        "judicial_district",
        "prior_length_of_incarceration",
    ),
    metric_stats=(
        "last_updated",
        "event_count",
    ),
    dashboard_views_dataset=dataset_config.DASHBOARD_VIEWS_DATASET,
    get_pathways_incarceration_last_updated_date=get_pathways_incarceration_last_updated_date(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LIBERTY_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER.build_and_print()
