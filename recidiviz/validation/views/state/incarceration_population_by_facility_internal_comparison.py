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

"""A view which provides a comparison of various internal incarceration population counts by facility."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

INCARCERATION_POPULATION_BY_FACILITY_INTERNAL_COMPARISON_VIEW_NAME = (
    "incarceration_population_by_facility_internal_comparison"
)

INCARCERATION_POPULATION_BY_FACILITY_INTERNAL_COMPARISON_DESCRIPTION = (
    """ Comparison of various internal incarceration population counts by facility """
)


INCARCERATION_POPULATION_BY_FACILITY_INTERNAL_COMPARISON_QUERY_TEMPLATE = """
    /*{description}*/
    WITH covid_report_facility_counts AS (
      SELECT * FROM `{project_id}.{covid_report_dataset}.facility_population_by_age_with_capacity_by_day`
    ), public_dashboard_facility_counts AS (
      SELECT * FROM `{project_id}.{public_dashboard_dataset}.incarceration_population_by_facility_by_demographics`
      WHERE age_bucket = 'ALL'
      AND race_or_ethnicity = 'ALL'
      AND gender = 'ALL'
    ), public_dashboard_date_of_stay AS (
      SELECT DISTINCT state_code, date_of_stay FROM `{project_id}.{public_dashboard_dataset}.incarceration_population_by_facility_by_demographics`
    )
    
    SELECT
      state_code as region_code,
      date_of_stay,
      facility,
      covid_report_facility_counts.total_population as covid_report_facility_population,
      public_dashboard_facility_counts.total_population as public_dashboard_facility_population
    FROM
      public_dashboard_date_of_stay
    LEFT JOIN
      covid_report_facility_counts 
    USING (state_code, date_of_stay)
    FULL OUTER JOIN
      public_dashboard_facility_counts
    USING (state_code, date_of_stay, facility)
    ORDER BY state_code, date_of_stay, facility 
"""

INCARCERATION_POPULATION_BY_FACILITY_INTERNAL_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INCARCERATION_POPULATION_BY_FACILITY_INTERNAL_COMPARISON_VIEW_NAME,
    view_query_template=INCARCERATION_POPULATION_BY_FACILITY_INTERNAL_COMPARISON_QUERY_TEMPLATE,
    description=INCARCERATION_POPULATION_BY_FACILITY_INTERNAL_COMPARISON_DESCRIPTION,
    covid_report_dataset=state_dataset_config.COVID_REPORT_DATASET,
    public_dashboard_dataset=state_dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_POPULATION_BY_FACILITY_INTERNAL_COMPARISON_VIEW_BUILDER.build_and_print()
