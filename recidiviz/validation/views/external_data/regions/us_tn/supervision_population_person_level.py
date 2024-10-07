# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""A view containing supervision population at the person level for Tennessee."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

VIEW_QUERY_TEMPLATE = """
SELECT
  'US_TN' AS state_code,
  OffenderID as person_external_id,
  'US_TN_DOC' as external_id_type,
  EXTRACT(DATE FROM CAST(ReportingDate AS DATETIME)) as date_of_supervision,
  SiteID as district,
  LTRIM(StaffID, '*') as supervising_officer,
  CAST(NULL as STRING) as supervision_level
FROM `{project_id}.{us_tn_raw_data_up_to_date_dataset}.DailyCommunitySupervisionForRecidiviz_latest`
"""

US_TN_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_TN),
    view_id="supervision_population_person_level",
    description="A view detailing supervision population at the person level for Tennessee",
    view_query_template=VIEW_QUERY_TEMPLATE,
    should_materialize=True,
    us_tn_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
