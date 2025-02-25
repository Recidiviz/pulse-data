# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""A view which provides a comparison of internal incarceration population counts by custody level to external counts
provided by the state."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

INCARCERATION_POPULATION_BY_CUSTODY_LEVEL_EXTERNAL_COMPARISON_VIEW_NAME = (
    "incarceration_population_by_custody_level_external_comparison"
)

INCARCERATION_POPULATION_BY_CUSTODY_LEVEL_EXTERNAL_COMPARISON_DESCRIPTION = """ Comparison of internal and external incarceration population counts by facility """


INCARCERATION_POPULATION_BY_CUSTODY_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE = """
    WITH external_validation_dates AS (
        -- Only compare states and months for which we have external validation data
        SELECT DISTINCT region_code, date_of_stay FROM
        `{project_id}.{external_accuracy_dataset}.incarceration_population_by_custody_level_materialized`
    ), internal_incarceration_population AS (
        SELECT
            state_code as region_code, date_of_stay,
            CASE WHEN state_code='US_AZ' THEN
              CASE
                WHEN custody_level_raw_text LIKE "%INTAKE%" THEN 'RECEPTION'
                WHEN custody_level_raw_text LIKE "%MINIMUM%" THEN 'MINIMUM CUSTODY'
                WHEN custody_level_raw_text LIKE "%MEDIUM%" THEN 'MEDIUM CUSTODY'
                WHEN custody_level_raw_text LIKE "%MAXIMUM%" THEN 'MAXIMUM CUSTODY'
                WHEN custody_level_raw_text LIKE "%CLOSE%" THEN 'CLOSE CUSTODY'
                WHEN custody_level_raw_text = "DETENTION" THEN 'MAXIMUM CUSTODY'
                ELSE CONCAT(CUSTODY_LEVEL, ' CUSTODY')
              END
            END AS custody_level,
            COUNT(DISTINCT(person_id)) as internal_population_count
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_span_to_single_day_metrics_materialized`
        WHERE included_in_state_population
        GROUP BY state_code, date_of_stay, custody_level
    ), relevant_internal_incarceration_population AS (
        SELECT * FROM
          external_validation_dates
        LEFT JOIN
          internal_incarceration_population
        USING(region_code, date_of_stay)
    ),
    comparison AS (
        SELECT
          region_code,
          date_of_stay,
          custody_level,
          IFNULL(population_count, 0) as external_population_count,
          IFNULL(internal_population_count, 0) as internal_population_count
        FROM
          `{project_id}.{external_accuracy_dataset}.incarceration_population_by_custody_level_materialized`
        FULL OUTER JOIN
          relevant_internal_incarceration_population
        USING (region_code, date_of_stay, custody_level)
    )
    SELECT *
    FROM comparison
    -- We filter out populations where the custody level has fewer than 10 because an 
    -- off by one error can cause a huge error percentage.
    WHERE external_population_count >= 10
    ORDER BY region_code, date_of_stay, custody_level
"""

INCARCERATION_POPULATION_BY_CUSTODY_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INCARCERATION_POPULATION_BY_CUSTODY_LEVEL_EXTERNAL_COMPARISON_VIEW_NAME,
    view_query_template=INCARCERATION_POPULATION_BY_CUSTODY_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE,
    description=INCARCERATION_POPULATION_BY_CUSTODY_LEVEL_EXTERNAL_COMPARISON_DESCRIPTION,
    external_accuracy_dataset=dataset_config.EXTERNAL_ACCURACY_DATASET,
    materialized_metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_POPULATION_BY_CUSTODY_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER.build_and_print()
