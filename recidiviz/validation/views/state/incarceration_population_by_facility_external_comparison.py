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

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.validation.views import dataset_config

INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_VIEW_NAME = \
    'incarceration_population_by_facility_external_comparison'

INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_DESCRIPTION = """ 
Comparison of internal and external incarceration population counts by facility """


INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      state_code as region_code,
      year,
      month,
      facility,
      IFNULL(population_count, 0) as external_population_count,
      IFNULL(internal_population_count, 0) as internal_population_count
    FROM
      `{project_id}.{external_accuracy_dataset}.incarceration_population_by_facility`
        FULL OUTER JOIN
      (SELECT * FROM
         -- Only compare states and months for which we have external validation data
        (SELECT
          state_code, year, month
         FROM
            `{project_id}.{external_accuracy_dataset}.incarceration_population_by_facility`
         GROUP BY state_code, year, month)
       LEFT JOIN
          (SELECT state_code, year, month, facility, COUNT(DISTINCT(person_id)) as internal_population_count
          FROM `{project_id}.{metrics_dataset}.incarceration_population_metrics`
          WHERE metric_period_months = 1
          AND methodology = 'PERSON'
          GROUP BY state_code, year, month, facility)
      USING(state_code, year, month))
    USING (state_code, year, month, facility)
    ORDER BY region_code, year, month, facility
"""

INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_VIEW = BigQueryView(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_VIEW_NAME,
    view_query_template=INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_QUERY_TEMPLATE,
    description=INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_DESCRIPTION,
    external_accuracy_dataset=dataset_config.EXTERNAL_ACCURACY_DATASET,
    metrics_dataset=state_dataset_config.DATAFLOW_METRICS_DATASET,
    reference_dataset=state_dataset_config.REFERENCE_TABLES_DATASET
)

if __name__ == '__main__':
    print(INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_VIEW.view_id)
    print(INCARCERATION_POPULATION_BY_FACILITY_EXTERNAL_COMPARISON_VIEW.view_query)
