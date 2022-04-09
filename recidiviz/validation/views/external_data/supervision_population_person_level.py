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
"""A view containing external data for person level supervision population to validate against."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

_QUERY_TEMPLATE = """
SELECT region_code, person_external_id, date_of_supervision, district, supervising_officer, supervision_level
FROM `{project_id}.{us_id_validation_dataset}.supervision_population_person_level_raw`
UNION ALL
SELECT region_code, person_external_id, date_of_supervision, district, supervising_officer, supervision_level
FROM `{project_id}.{us_id_validation_dataset}.daily_summary_supervision`
UNION ALL
SELECT region_code, person_external_id, date_of_supervision, district, supervising_officer, supervision_level
FROM `{project_id}.{us_mo_validation_dataset}.supervision_population_person_level_raw`
UNION ALL
SELECT region_code, person_external_id, date_of_supervision, district, supervising_officer, supervision_level
FROM `{project_id}.{us_pa_validation_dataset}.supervision_population_person_level_raw`
UNION ALL
SELECT region_code, person_external_id, date_of_supervision, district, supervising_officer, CAST(supervision_level AS STRING) AS supervision_level
FROM `{project_id}.{us_me_validation_dataset}.supervision_by_person_by_officer_view`
UNION ALL
SELECT
  'US_TN' as region_code,
  OffenderID as person_external_id,
  EXTRACT(DATE FROM CAST(ReportingDate AS DATETIME)) as date_of_supervision,
  SiteID as district,
  LTRIM(StaffID, '*') as supervising_officer,
  NULL as supervision_level
FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.DailyCommunitySupervisionForRecidiviz_latest`
-- TODO(#10884): This validation data seems incorrect, so excluding it for now.
-- UNION ALL 
-- (SELECT
--     'US_ND' as region_code,
--     TRIM(SID) as person_external_id,
--     DATE('2020-06-01') as date_of_supervision,
--     MIN(supervising_district_external_id) as district,
--     NULL as supervising_officer,
--     NULL as supervision_level
-- FROM `{project_id}.{us_nd_validation_dataset}.open_supervision_2020_06_01`
-- LEFT JOIN `{project_id}.{us_nd_raw_data_up_to_date_dataset}.RECIDIVIZ_REFERENCE_supervision_district_id_to_name_latest`
--     ON SITE_NAME = supervising_district_name
-- GROUP BY SID)
"""

SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.EXTERNAL_ACCURACY_DATASET,
    view_id="supervision_population_person_level",
    view_query_template=_QUERY_TEMPLATE,
    description="Contains external data for person level supervision population to "
    "validate against. See http://go/external-validations for instructions on adding "
    "new data.",
    us_id_validation_dataset=dataset_config.validation_dataset_for_state(
        StateCode.US_ID
    ),
    us_me_validation_dataset=dataset_config.validation_dataset_for_state(
        StateCode.US_ME
    ),
    us_mo_validation_dataset=dataset_config.validation_dataset_for_state(
        StateCode.US_MO
    ),
    us_nd_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        StateCode.US_ND.value
    ),
    us_nd_validation_dataset=dataset_config.validation_dataset_for_state(
        StateCode.US_ND
    ),
    us_pa_validation_dataset=dataset_config.validation_dataset_for_state(
        StateCode.US_PA
    ),
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        StateCode.US_TN.value
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
