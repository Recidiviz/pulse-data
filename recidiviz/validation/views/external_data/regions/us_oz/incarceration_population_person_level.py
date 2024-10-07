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
"""A view containing the incarceration population person-level for Oz

Raw validation data arises from "raw_validation_incarceration_population.csv"
that has individuals incarcerated in Oz on 2020-01-01 or 2021-01-01.
That data is currently:

    date,data_system,person_id
    2021-01-01,eg,1
    2020-01-01,eg,1
    2020-01-01,lotr,8
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

VIEW_QUERY_TEMPLATE = """
  SELECT 
    'US_OZ' AS state_code,
    CAST(PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', Date) AS DATE) as date_of_stay,
    CASE
      WHEN data_system = 'vfds' THEN 'US_OZ_VFDS'
      WHEN data_system = 'eg' THEN 'US_OZ_EG'
      WHEN data_system = 'lotr' THEN 'US_OZ_LOTR_ID'
      WHEN data_system = 'sm' THEN 'US_OZ_SM'
      WHEN data_system = 'hg' THEN 'US_OZ_HG_ID'
      WHEN data_system = 'egt' THEN 'US_OZ_EGT'
    END AS external_id_type,
    person_id AS person_external_id, 
    CAST(NULL AS STRING) AS facility,
  FROM `{project_id}.{us_oz_raw_data_up_to_date_dataset}.validation_incarceration_population_latest`
"""

US_OZ_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_OZ),
    view_id="incarceration_population_person_level",
    description="A view containing the incarceration population at the person level for Oz.",
    view_query_template=VIEW_QUERY_TEMPLATE,
    should_materialize=True,
    us_oz_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_OZ, instance=DirectIngestInstance.PRIMARY
    ),
    projects_to_deploy={GCP_PROJECT_STAGING},
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_OZ_INCARCERATION_POPULATION_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
