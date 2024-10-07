# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""A view containing the incarceration population person-level for Idaho.

Note: This currently pulls data that was produced from the pre-Atlas data system. Once
we get reliable validation data from Atlas we should add it (or replace what is here).
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

VIEW_QUERY_TEMPLATE = """
WITH legacy_data AS (
  SELECT
    region_code AS state_code,
    person_external_id,
    'US_IX_DOC' AS external_id_type,
    date_of_stay,
    -- Don't use facility from this ID data as it groups most of the jails together.
    CAST(NULL AS STRING) AS facility
  FROM `{project_id}.{us_ix_validation_oneoff_dataset}.incarceration_population_person_level_raw`
), daily_summary AS (
  SELECT
    'US_IX' AS state_code,
    ofndr_num AS person_external_id,
    'US_IX_DOC' AS external_id_type,
    EXTRACT(DATE FROM CAST(popdate AS DATETIME)) AS date_of_stay,
    CASE
      WHEN Facility = 'ISCC' THEN 'IDAHO CORRECTIONAL CENTER - BOISE'
      WHEN Facility = 'ISCI' THEN 'IDAHO STATE CORRECTIONAL INSTITUTION, BOISE'
      WHEN Facility IN ('SICI Male', 'SICI Female') THEN 'SOUTH IDAHO CORRECTIONAL INSTITUTION, BOISE'
      WHEN Facility = 'ICIO' THEN 'IDAHO CORRECTIONAL INSTITUTION, OROFINO'
      WHEN Facility = 'IMSI' THEN 'IDAHO MAXIMUM SECURITY INSTITUTION, BOISE'
      WHEN Facility = 'Saguaro Corr Center' THEN 'SAGUARO CORRECTIONAL CENTER, ARIZONA'
      WHEN Facility = 'CAPP' THEN 'CORRECTIONAL ALTERNATIVE PLACEMENT PROGRAM - BOISE'
      WHEN Facility = 'NICI' THEN 'NORTH IDAHO CORRECTIONAL INSTITUTION, COTTONWOOD'
      WHEN Facility = 'PWCC' THEN "POCATELLO WOMAN'S CORRECTIONAL CENTER, POCATELLO"
      WHEN Facility = 'SAWC' THEN 'ST. ANTHONY WORK CENTER, ST. ANTHONY'
      WHEN Facility = 'SBWCC' THEN "SOUTH BOISE WOMEN'S CORRECTIONAL CENTER"
      WHEN Facility IN ('CRC East Boise', 'Expanded CRC East Boise') THEN 'EAST BOISE COMMUNITY WORK CENTER, BOISE'
      WHEN Facility IN ('CRC Idaho Falls', 'Expanded CRC Idaho Falls') THEN 'IDAHO FALLS COMMUNITY WORK CENTER, IDAHO FALLS'
      WHEN Facility IN ('CRC Nampa', 'Expanded CRC Nampa') THEN 'NAMPA COMMUNITY WORK CENTER, NAMPA'
      WHEN Facility IN ('CRC Treasure Valley', 'Expanded CRC Treasure Valley') THEN 'SICI COMMUNITY WORK CENTER'
      WHEN Facility IN ('CRC Twin Falls', 'Expanded CRC Twin Falls') THEN 'TWIN FALLS COMMUNITY WORK CENTER, TWIN FALLS'
      WHEN body_loc_desc = 'FEDERAL CUSTODY' THEN 'FEDERAL BUREAU OF PRISONS'
      WHEN body_loc_desc = 'US MARSHAL CUSTODY' THEN 'U.S. MARSHALL CUSTODY'
      WHEN body_loc_desc = 'SECURE LOCATION' THEN assgn_rsn_desc
      WHEN ENDS_WITH(body_loc_desc, 'SHERIFF') THEN REPLACE(body_loc_desc, 'SHERIFF', 'COUNTY SHERIFF DEPARTMENT')
      -- Other states
      ELSE body_loc_desc
    END AS facility
  FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.current_day_daily_summary_latest`
  WHERE
    IDOCPopulationGrouping NOT IN ('Community Supervision') AND
    (Facility != 'Other' OR fac_cd = 'RT')
)

SELECT * FROM legacy_data
UNION ALL
SELECT * FROM daily_summary;
"""

US_IX_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_IX),
    view_id="incarceration_population_person_level",
    description="A view detailing the incarceration population at the person level for Idaho.",
    view_query_template=VIEW_QUERY_TEMPLATE,
    should_materialize=True,
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
    us_ix_validation_oneoff_dataset=dataset_config.validation_oneoff_dataset_for_state(
        StateCode.US_IX
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
