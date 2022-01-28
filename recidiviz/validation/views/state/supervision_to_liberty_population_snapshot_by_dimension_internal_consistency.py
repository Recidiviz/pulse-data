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
"""Validation query to make sure all data included in the ALL rows are in the table."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import DASHBOARD_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

SUPERVISION_TO_LIBERTY_POPULATION_SNAPSHOT_BY_DIMENSION_INTERNAL_CONSISTENCY = (
    "supervision_to_liberty_population_snapshot_by_dimension_internal_consistency"
)

SUPERVISION_TO_LIBERTY_POPULATION_SNAPSHOT_BY_DIMENSION_INTERNAL_CONSISTENCY_DESCRIPTION = (
    "Consistency check to make sure all included dimensions sum to the ALL rows."
)

SUPERVISION_TO_LIBERTY_POPULATION_SNAPSHOT_BY_DIMENSION_INTERNAL_CONSISTENCY_QUERY = """
    /*{description}*/
    WITH all_count AS (
        SELECT state_code, time_period, event_count
        FROM `{project_id}.{dashboard_dataset}.supervision_to_liberty_population_snapshot_by_dimension`
        WHERE gender = 'ALL'
            AND age_group = 'ALL'
            AND race = 'ALL'
            AND supervision_type = 'ALL'
            AND district  = 'ALL'
    ), summed_count AS (
        SELECT state_code, time_period, sum(event_count) AS sum_count
        FROM `{project_id}.{dashboard_dataset}.supervision_to_liberty_population_snapshot_by_dimension`
        WHERE gender != 'ALL'
            AND age_group != 'ALL'
            AND race != 'ALL'
            AND supervision_type != 'ALL'
            AND district  != 'ALL'
        GROUP BY 1, 2
    )

    SELECT *
    FROM all_count
    JOIN summed_count
    USING (state_code, time_period)
    WHERE event_count != sum_count
"""

SUPERVISION_TO_LIBERTY_POPULATION_SNAPSHOT_BY_DIMENSION_INTERNAL_CONSISTENCY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=SUPERVISION_TO_LIBERTY_POPULATION_SNAPSHOT_BY_DIMENSION_INTERNAL_CONSISTENCY,
    view_query_template=SUPERVISION_TO_LIBERTY_POPULATION_SNAPSHOT_BY_DIMENSION_INTERNAL_CONSISTENCY_QUERY,
    description=SUPERVISION_TO_LIBERTY_POPULATION_SNAPSHOT_BY_DIMENSION_INTERNAL_CONSISTENCY_DESCRIPTION,
    dashboard_dataset=DASHBOARD_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TO_LIBERTY_POPULATION_SNAPSHOT_BY_DIMENSION_INTERNAL_CONSISTENCY_VIEW_BUILDER.build_and_print()
