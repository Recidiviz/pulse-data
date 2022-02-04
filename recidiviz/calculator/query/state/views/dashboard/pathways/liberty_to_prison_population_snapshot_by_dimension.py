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
from recidiviz.calculator.query.bq_utils import (
    convert_days_to_years,
    create_buckets_with_cap,
    get_binned_time_period_months,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.state_specific_query_strings import (
    get_pathways_incarceration_last_updated_date,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

LIBERTY_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_NAME = (
    "liberty_to_prison_population_snapshot_by_dimension"
)

LIBERTY_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_DESCRIPTION = (
    """Supervision to liberty population snapshot by dimension"""
)

LIBERTY_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_QUERY_TEMPLATE = """
    /*{description}*/
    WITH transitions AS (
        SELECT
            state_code,
            gender,
            age_group,
            intake_district AS judicial_district,
            prioritized_race_or_ethnicity AS race,
            {binned_time_periods} AS time_period,
            {length_of_stay} AS prior_length_of_incarceration,
        FROM `{project_id}.{reference_views_dataset}.liberty_to_prison_transitions`
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

LIBERTY_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=LIBERTY_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_NAME,
    view_query_template=LIBERTY_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_QUERY_TEMPLATE,
    description=LIBERTY_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_DESCRIPTION,
    dimensions=(
        "state_code",
        "last_updated",
        "time_period",
        "gender",
        "age_group",
        "race",
        "judicial_district",
        "prior_length_of_incarceration",
    ),
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    binned_time_periods=get_binned_time_period_months("transition_date"),
    get_pathways_incarceration_last_updated_date=get_pathways_incarceration_last_updated_date(),
    length_of_stay=create_buckets_with_cap(
        convert_days_to_years("prior_length_of_incarceration"), 11
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        LIBERTY_TO_PRISON_POPULATION_SNAPSHOT_BY_DIMENSION_VIEW_BUILDER.build_and_print()
