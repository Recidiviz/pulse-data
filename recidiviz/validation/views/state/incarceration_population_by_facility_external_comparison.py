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

"""A view which provides a comparison of internal incarceration population counts by facility to external counts
provided by the state."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.utils.state_specific_query_strings import (
    state_specific_dataflow_facility_name_transformation,
)

INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_VIEW_NAME = (
    "incarceration_population_by_facility_external_comparison"
)

INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_DESCRIPTION = """ Comparison of internal and external incarceration population counts by facility """


INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_QUERY_TEMPLATE = """
    /*{description}*/
    WITH external_validation_dates AS (
        -- Only compare states and months for which we have external validation data
        SELECT DISTINCT region_code, date_of_stay FROM
        `{project_id}.{external_accuracy_dataset}.incarceration_population_by_facility`
    ), internal_incarceration_population AS (
        SELECT
            state_code as region_code, date_of_stay,
            {state_specific_dataflow_facility_name_transformation},
            COUNT(DISTINCT(person_id)) as internal_population_count
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_included_in_state_population_materialized`
        GROUP BY state_code, date_of_stay, facility
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
          facility,
          IFNULL(population_count, 0) as external_population_count,
          IFNULL(internal_population_count, 0) as internal_population_count
        FROM
          `{project_id}.{external_accuracy_dataset}.incarceration_population_by_facility`
        FULL OUTER JOIN
          relevant_internal_incarceration_population
        USING (region_code, date_of_stay, facility)
    )
    SELECT *
    FROM comparison
    -- We filter out populations where the facility has fewer than 10 because an off by one error can cause a huge error
    -- percentage.
    WHERE external_population_count >= 10
    ORDER BY region_code, date_of_stay, facility
"""

INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_VIEW_NAME,
    view_query_template=INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_QUERY_TEMPLATE,
    description=INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_DESCRIPTION,
    external_accuracy_dataset=dataset_config.EXTERNAL_ACCURACY_DATASET,
    materialized_metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    state_specific_dataflow_facility_name_transformation=state_specific_dataflow_facility_name_transformation(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_VIEW_BUILDER.build_and_print()
