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

"""A view which provides a comparison of various internal incarceration population counts by demographic."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

INCARCERATION_POPULATION_BY_DEMOGRAPHIC_INTERNAL_COMPARISON_VIEW_NAME = (
    "incarceration_population_by_demographic_internal_comparison"
)

INCARCERATION_POPULATION_BY_DEMOGRAPHIC_INTERNAL_COMPARISON_DESCRIPTION = (
    """ Comparison of various internal incarceration population counts by facility """
)

INCARCERATION_POPULATION_BY_DEMOGRAPHIC_INTERNAL_COMPARISON_QUERY_TEMPLATE = """WITH
incarceration_population_by_admission_reason AS (
  SELECT 
    state_code, date_of_stay, race_or_ethnicity, gender, age_bucket,
    SUM(total_population) AS population_by_admission_reason_total_population
  FROM `{project_id}.{public_dashboard_dataset}.incarceration_population_by_admission_reason`
  GROUP BY state_code, date_of_stay, race_or_ethnicity, gender, age_bucket
),
incarceration_population_by_facility_by_demographics AS (
  SELECT state_code, date_of_stay, race_or_ethnicity, gender, age_bucket,
  SUM(total_population) AS population_by_facility_by_demographics_total_population
  FROM `{project_id}.{public_dashboard_dataset}.incarceration_population_by_facility_by_demographics`
  WHERE facility = 'ALL'
  GROUP BY state_code, date_of_stay, race_or_ethnicity, gender, age_bucket
)
SELECT 
  state_code AS region_code, date_of_stay, race_or_ethnicity, gender, age_bucket, 
  IFNULL(population_by_admission_reason_total_population, 0) AS population_by_admission_reason_total_population, 
  IFNULL(population_by_facility_by_demographics_total_population, 0) AS population_by_facility_by_demographics_total_population
FROM
  incarceration_population_by_admission_reason
FULL OUTER JOIN
  incarceration_population_by_facility_by_demographics
USING (state_code, date_of_stay, race_or_ethnicity, gender, age_bucket)
ORDER BY state_code, date_of_stay, race_or_ethnicity, gender, age_bucket
"""

INCARCERATION_POPULATION_BY_DEMOGRAPHIC_INTERNAL_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INCARCERATION_POPULATION_BY_DEMOGRAPHIC_INTERNAL_COMPARISON_VIEW_NAME,
    view_query_template=INCARCERATION_POPULATION_BY_DEMOGRAPHIC_INTERNAL_COMPARISON_QUERY_TEMPLATE,
    description=INCARCERATION_POPULATION_BY_DEMOGRAPHIC_INTERNAL_COMPARISON_DESCRIPTION,
    public_dashboard_dataset=state_dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_POPULATION_BY_DEMOGRAPHIC_INTERNAL_COMPARISON_VIEW_BUILDER.build_and_print()
