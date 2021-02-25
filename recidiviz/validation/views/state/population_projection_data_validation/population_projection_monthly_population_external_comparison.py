# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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

"""A view comparing values from internal population projection historical monthly population to the aggregate values
from external metrics provided by the state.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

POPULATION_PROJECTION_MONTHLY_POPULATION_EXTERNAL_COMPARISON_VIEW_NAME = \
    'population_projection_monthly_population_external_comparison'

POPULATION_PROJECTION_MONTHLY_POPULATION_EXTERNAL_COMPARISON_DESCRIPTION = """
Comparison of values between internal population projection historical population and external monthly
incarceration/supervision populations.
"""

# TODO(#5998): add the supervision population to this validation
POPULATION_PROJECTION_MONTHLY_POPULATION_EXTERNAL_COMPARISON_QUERY_TEMPLATE = \
    """
/*{description}*/
WITH external_data AS (
    SELECT
        region_code,
        compartment,
        year,
        month,
        total_population
    -- Compare against the data from the state's population projection inputs or budget data
    FROM `{project_id}.{external_accuracy_dataset}.population_projection_monthly_population`
),
internal_metrics AS (
    SELECT
        simulation_tag AS region_code,
        compartment,
        year,
        month,
        SUM(total_population) AS total_population
    FROM `{project_id}.{population_projection_dataset}.microsim_projection`
    GROUP BY region_code, year, month, compartment
),
internal_metrics_for_valid_regions_and_dates AS (
    SELECT * FROM
    -- Only compare regions, date_of_stay, and legal statuses for which we have external validation data
    (SELECT DISTINCT region_code, compartment, year, month FROM external_data)
    LEFT JOIN internal_metrics
    USING (region_code, compartment, year, month)
)
SELECT
    region_code,
    compartment,
    year,
    month,
    external_data.total_population AS external_total_population,
    internal_data.total_population AS internal_total_population
FROM
    external_data
FULL OUTER JOIN
    internal_metrics_for_valid_regions_and_dates internal_data
USING (region_code, compartment, year, month)
ORDER BY region_code, compartment, year, month
"""

POPULATION_PROJECTION_MONTHLY_POPULATION_EXTERNAL_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=POPULATION_PROJECTION_MONTHLY_POPULATION_EXTERNAL_COMPARISON_VIEW_NAME,
    view_query_template=POPULATION_PROJECTION_MONTHLY_POPULATION_EXTERNAL_COMPARISON_QUERY_TEMPLATE,
    description=POPULATION_PROJECTION_MONTHLY_POPULATION_EXTERNAL_COMPARISON_DESCRIPTION,
    base_dataset=state_dataset_config.STATE_BASE_DATASET,
    external_accuracy_dataset=dataset_config.EXTERNAL_ACCURACY_DATASET,
    materialized_metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    population_projection_dataset=state_dataset_config.POPULATION_PROJECTION_DATASET,
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        POPULATION_PROJECTION_MONTHLY_POPULATION_EXTERNAL_COMPARISON_VIEW_BUILDER.build_and_print()
