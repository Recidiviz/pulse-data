# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""A view detailing the incarceration population at the person level for Utah."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

VIEW_QUERY = """
SELECT
  'US_UT' AS state_code,
  ofndr_num AS person_external_id, 
  'US_UT_DOC' AS external_id_type,
  DATE(update_datetime) AS date_of_stay, 
  location AS facility 
FROM `{project_id}.{us_ut_raw_data_dataset}.validation_dpo_listing`
"""


US_UT_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_UT),
    view_id="incarceration_population_person_level",
    description="A view detailing the incarceration population at the person level for Utah",
    view_query_template=VIEW_QUERY,
    should_materialize=True,
    us_ut_raw_data_dataset=raw_tables_dataset_for_region(
        StateCode.US_UT, DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_UT_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
