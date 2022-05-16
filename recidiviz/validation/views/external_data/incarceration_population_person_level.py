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
UNION ALL
SELECT
  'US_MI' as region_code,
  person_external_id,
  date_of_stay,
  facility
FROM (
  SELECT Offender_Number AS person_external_id, DATE('2020-01-08') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200108` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-01-15') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200115` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-01-22') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200122` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-01-29') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200129` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-02-05') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200205` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-02-12') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200212` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-02-19') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200219` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-02-26') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200226` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-02-27') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200227` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-03-11') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200311` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-03-18') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200318` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-03-25') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200325` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-04-01') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200401` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-04-07') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200407` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-04-08') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200408` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-04-15') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200415` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-04-22') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200422` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-05-13') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200513` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-05-14') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200514` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-05-15') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200515` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-05-27') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200527` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-06-03') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200603` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-06-10') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200610` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-06-17') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200617` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-06-24') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200624` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-07-01') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200701` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-07-08') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200708` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-07-15') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200715` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-07-22') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200722` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-07-29') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200729` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-08-05') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200805` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-08-12') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200812` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-08-19') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200819` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-08-26') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200826` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-09-02') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200902` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-09-09') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200909` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-09-16') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200916` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-09-23') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200923` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-09-30') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20200930` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-10-09') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20201009` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-10-14') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20201014` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-10-21') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20201021` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-10-28') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20201028` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-11-04') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20201104` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-11-13') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20201113` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-11-18') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20201118` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-11-25') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20201125` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-12-02') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20201202` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-12-04') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20201204` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-12-09') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20201209` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-12-16') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20201216` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-12-18') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20201218` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-12-23') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20201223` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-12-30') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20201230` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2020-01-08') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210106` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-01-13') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210113` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-01-20') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210120` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-01-27') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210127` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-02-03') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210203` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-02-10') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210210` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-02-11') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210211` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-02-17') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210217` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-02-24') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210224` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-03-03') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210303` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-03-17') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210317` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-03-19') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210319` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-03-24') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210324` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-03-31') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210331` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-04-07') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210407` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-04-14') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210414` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-04-21') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210421` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-04-28') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210428` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-04-29') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210429` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-05-05') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210505` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-05-12') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210512` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-05-19') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210519` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-05-26') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210526` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-06-02') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210602` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-06-11') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210611` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-06-16') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210616` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-06-23') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210623` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-07-01') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210701` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-07-07') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210707` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-07-14') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210714` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-07-21') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210721` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-07-28') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210728` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-08-04') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210804` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-08-11') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210811` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-09-09') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210909` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-09-15') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210915` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-09-16') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210916` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-09-22') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210922` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-09-29') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20210929` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-10-06') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20211006` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-10-13') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20211013` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-10-27') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20211027` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-11-10') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20211110` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-11-17') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20211117` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-12-01') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20211201` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-12-08') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20211208` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-12-15') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20211215` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2021-12-22') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20211222` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2022-01-05') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20220105` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2022-01-13') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20220113` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2022-01-19') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20220119` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2022-01-28') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20220128` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2022-02-03') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20220203` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2022-02-23') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20220223` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2022-03-02') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20220302` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2022-03-14') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20220314` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2022-03-16') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20220316` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2022-03-23') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20220323` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2022-03-30') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20220330` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2022-04-06') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20220406` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2022-05-06') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20220506` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2022-05-10') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20220510` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2022-05-11') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20220511` WHERE Unique = 'Yes'
  UNION ALL
  SELECT Offender_Number AS person_external_id, DATE('2022-05-13') AS date_of_stay, Location AS facility FROM `{project_id}.{us_mi_validation_dataset}.orc_report_20220513` WHERE Unique = 'Yes'
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
    us_mi_validation_dataset=dataset_config.validation_dataset_for_state(
        StateCode.US_MI
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
