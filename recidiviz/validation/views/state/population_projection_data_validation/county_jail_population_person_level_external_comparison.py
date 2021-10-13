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

"""A view comparing values from internal person-level county jail population to the person-level values from external
metrics provided by the state.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_NAME = (
    "county_jail_population_person_level_external_comparison_matching_people"
)

COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_NAME = (
    "county_jail_population_person_level_external_comparison"
)

COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_DESCRIPTION = """
Comparison of values between internal and external lists of start of month person-level county jail incarceration
populations.
"""

COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_DATE = "2020-09-01"

COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE = """
/*{description}*/
WITH external_data AS (
  SELECT
    region_code,
    person_external_id,
    facility,
    legal_status,
    date_of_stay,
  FROM `{project_id}.{external_accuracy_dataset}.us_id_county_jail_09_2020_incarceration_population`
  WHERE date_of_stay = '{comparison_date}'
),
internal_metrics AS (
  SELECT
    state_code AS region_code,
    report_month AS date_of_stay,
    external_id AS person_external_id,
    facility,
    compartment_level_2 AS legal_status,
  FROM `{project_id}.{population_projection_dataset}.us_id_monthly_paid_incarceration_population`
  LEFT JOIN `{project_id}.{base_dataset}.state_person_external_id`
  USING (state_code, person_id)
  WHERE report_month = '{comparison_date}'
    AND (facility = 'COUNTY JAIL'
        -- TODO(#5931): use the US_ID_INCARCERATION_DISAGGREGATED_COUNTY_JAILS list once the Jefferson validation data
        -- is available
        OR (state_code = 'US_ID' AND facility = 'BONNEVILLE COUNTY SHERIFF DEPARTMENT')
    )
),
internal_metrics_for_valid_regions_and_dates AS (
  SELECT * FROM
  -- Only compare regions, date_of_stay, and legal statuses for which we have external validation data
  (SELECT DISTINCT region_code, date_of_stay, legal_status FROM external_data)
  LEFT JOIN internal_metrics
  USING (region_code, date_of_stay, legal_status)
)
SELECT
  region_code,
  date_of_stay,
  external_data.person_external_id AS external_person_external_id,
  internal_data.person_external_id AS internal_person_external_id,
  external_data.facility AS external_facility,
  internal_data.facility AS internal_facility,
  external_data.legal_status AS external_legal_status,
  internal_data.legal_status AS internal_legal_status,
FROM
  external_data
FULL OUTER JOIN
  internal_metrics_for_valid_regions_and_dates internal_data
USING(region_code, date_of_stay, person_external_id)
{filter_clause}
ORDER BY region_code, date_of_stay, person_external_id
"""

COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_FILTER = "WHERE external_data.person_external_id IS NOT NULL AND internal_data.person_external_id IS NOT NULL"

COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_NAME,
    view_query_template=COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE,
    description=COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_DESCRIPTION,
    base_dataset=state_dataset_config.STATE_BASE_DATASET,
    external_accuracy_dataset=dataset_config.EXTERNAL_ACCURACY_DATASET,
    population_projection_dataset=state_dataset_config.POPULATION_PROJECTION_DATASET,
    comparison_date=COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_DATE,
    filter_clause=COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_FILTER,
)

COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_NAME,
    view_query_template=COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_QUERY_TEMPLATE,
    description=COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_DESCRIPTION,
    base_dataset=state_dataset_config.STATE_BASE_DATASET,
    external_accuracy_dataset=dataset_config.EXTERNAL_ACCURACY_DATASET,
    population_projection_dataset=state_dataset_config.POPULATION_PROJECTION_DATASET,
    comparison_date=COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_DATE,
    filter_clause="",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_MATCHING_PEOPLE_VIEW_BUILDER.build_and_print()
        COUNTY_JAIL_POPULATION_PERSON_LEVEL_EXTERNAL_COMPARISON_VIEW_BUILDER.build_and_print()
