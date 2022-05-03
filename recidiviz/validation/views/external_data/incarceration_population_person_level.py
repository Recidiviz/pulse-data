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
"""A view containing external data for person level incarceration population to validate against."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

_QUERY_TEMPLATE = """
-- Don't use facility from this ID data as it groups most of the jails together.
SELECT region_code, person_external_id, date_of_stay, NULL as facility
FROM `{project_id}.{us_id_validation_dataset}.incarceration_population_person_level_raw`
UNION ALL
SELECT region_code, person_external_id, date_of_stay, facility
FROM `{project_id}.{us_id_validation_dataset}.daily_summary_incarceration`
UNION ALL 
SELECT region_code, person_external_id, date_of_stay, facility
FROM `{project_id}.{us_me_validation_dataset}.incarceration_population_person_level_view`
UNION ALL
SELECT region_code, person_external_id, date_of_stay, facility
FROM `{project_id}.{us_pa_validation_dataset}.incarceration_population_person_level_raw`
-- TODO(#10883): Ignoring this ND data for now because we are not sure that it is correct.
-- UNION ALL 
-- SELECT
--   'US_ND' as region_code,
--   Offender_ID as person_external_id,
--   DATE('2021-11-01') as date_of_stay,
--   Facility as facility
-- FROM `{project_id}.{us_nd_validation_dataset}.incarcerated_individuals_2021_11_01`
UNION ALL
SELECT
  'US_TN' as region_code,
  Offender_ID as person_external_id,
  DATE(2022, 3, 14) as date_of_stay,
  Site as facility
FROM `{project_id}.{us_tn_validation_dataset}.TDPOP_20220314`
UNION ALL
SELECT
  'US_PA' as region_code,
  person_external_id,
  date_of_stay,
  facility
FROM (
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-03-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_03_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-04-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_04_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-05-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_05_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-06-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_06_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-07-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_07_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-08-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_08_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-09-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_09_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-10-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_10_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-11-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_11_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2021-12-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2021_12_incarceration_population`
  UNION ALL
  SELECT control_number as person_external_id, LAST_DAY(DATE '2022-01-01', MONTH) as date_of_stay, CurrLoc_Cd as facility from `{project_id}.{us_pa_validation_dataset}.2022_01_incarceration_population`
)
"""

INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.EXTERNAL_ACCURACY_DATASET,
    view_id="incarceration_population_person_level",
    view_query_template=_QUERY_TEMPLATE,
    description="Contains external data for person level incarceration population to "
    "validate against. See http://go/external-validations for instructions on adding "
    "new data.",
    us_id_validation_dataset=dataset_config.validation_dataset_for_state(
        StateCode.US_ID
    ),
    us_me_validation_dataset=dataset_config.validation_dataset_for_state(
        StateCode.US_ME
    ),
    us_nd_validation_dataset=dataset_config.validation_dataset_for_state(
        StateCode.US_ND
    ),
    us_pa_validation_dataset=dataset_config.validation_dataset_for_state(
        StateCode.US_PA
    ),
    us_tn_validation_dataset=dataset_config.validation_dataset_for_state(
        StateCode.US_TN
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
