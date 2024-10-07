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
"""A view containing the supervision population person-level for Idaho.

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
    date_of_supervision,
    district,
    COALESCE(ref.EmployeeId, supervising_officer) as supervising_officer,
    supervising_officer as supervising_officer_staff_id,
    supervision_level
  FROM `{project_id}.{us_ix_validation_oneoff_dataset}.supervision_population_person_level_raw` v
  LEFT JOIN `{project_id}.{us_ix_raw_data_up_to_date_dataset}.ref_Employee_latest` ref 
    on UPPER(v.supervising_officer) = UPPER(COALESCE(ref.StaffId, split(ref.Email, "@")[offset(0)]))
), daily_summary AS (
  SELECT
    'US_IX' AS state_code,
    ofndr_num AS person_external_id,
    'US_IX_DOC' AS external_id_type,
    EXTRACT(DATE FROM CAST(popdate AS DATETIME)) AS date_of_supervision,
    CASE
      WHEN Facility = 'D0' THEN 'DISTRICT 0'
      WHEN Facility = 'D1' THEN "DISTRICT OFFICE 1, COEUR D'ALENE"
      WHEN Facility = 'D2' THEN 'DISTRICT OFFICE 2, LEWISTON'
      WHEN Facility = 'D3' THEN 'DISTRICT OFFICE 3, CALDWELL'
      WHEN Facility = 'D4' THEN 'DISTRICT OFFICE 4, BOISE'
      WHEN Facility = 'D5' THEN 'DISTRICT OFFICE 5, TWIN FALLS'
      WHEN Facility = 'D6' THEN 'DISTRICT OFFICE 6, POCATELLO'
      WHEN Facility = 'D7' THEN 'DISTRICT OFFICE 7, IDAHO FALLS'
      ELSE body_loc_desc
    END AS district,
    CAST(NULL AS string) AS supervising_officer,
    CAST(NULL AS STRING) AS supervising_officer_staff_id,
    CAST(NULL AS string) AS supervision_level
  FROM `{project_id}.{us_ix_raw_data_up_to_date_dataset}.current_day_daily_summary_latest`
  WHERE
    IDOCPopulationGrouping = 'Community Supervision'
)

SELECT * FROM legacy_data
UNION ALL
SELECT * FROM daily_summary;
"""

US_IX_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_IX),
    view_id="supervision_population_person_level",
    description="A view detailing the supervision population at the person level for Idaho.",
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
        US_IX_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
