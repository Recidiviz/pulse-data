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
"""A view containing the incarceration population person-level for CODOC"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_tables_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

VIEW_QUERY_TEMPLATE = """
  SELECT 
    'US_CO' as region_code,
    offenderid AS person_external_id, 
    update_datetime AS date_of_stay, 
    FAC_LDESC AS facility, 
  FROM `{project_id}.{us_co_raw_data_dataset}.Base_Curr_Off_Pop`
"""

US_CO_INCARCERATION_POPULATION_PERSON_LEVEL_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_CO),
    view_id="incarceration_population_person_level",
    description="A view detailing the incarceration population at the person level for CODOC.",
    view_query_template=VIEW_QUERY_TEMPLATE,
    us_co_raw_data_dataset=raw_tables_dataset_for_region(StateCode.US_CO.value),
    # us_co_raw_data_dataset="us_co_raw_data",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_CO_INCARCERATION_POPULATION_PERSON_LEVEL_BUILDER.build_and_print()
