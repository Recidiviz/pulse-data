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
"""A view collating data for population releases at the person level for Maine"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

VIEW_QUERY_TEMPLATE = """
SELECT 
    "US_ME" AS state_code,
    CIS_CLIENT_ID AS person_external_id,
    CURRENT_CLIENT_STATUS AS current_status,
    current_housing AS released_from_housing_unit,
    DATE(release_date) AS release_date,
    release_reason,
    released_from,
    location_from.Cis_9080_Ccs_Location_Type_Cd AS released_from_location_type,
    released_to,
    location_to.Cis_9080_Ccs_Location_Type_Cd AS released_to_location_type,
    DATE(admission_date) AS original_intake_date,
    admission_reason AS original_admission_reason,
    admitted_from AS admitted_from,
    admit_from.Cis_9080_Ccs_Location_Type_Cd AS admitted_from_location_type
FROM `{project_id}.{us_me_raw_data_dataset}.INCARCERATION_POPULATION_RELEASES`

LEFT JOIN `{project_id}.{us_me_raw_data_up_to_date_dataset}.CIS_908_CCS_LOCATION_latest` location_from
ON location_from.Location_Name = released_from 

LEFT JOIN `{project_id}.{us_me_raw_data_up_to_date_dataset}.CIS_908_CCS_LOCATION_latest` location_to
ON location_to.Location_Name = released_to 

LEFT JOIN `{project_id}.{us_me_raw_data_up_to_date_dataset}.CIS_908_CCS_LOCATION_latest` admit_from
ON admit_from.Location_Name = admitted_from 
"""

US_ME_POPULATION_RELEASES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_ME),
    view_id="population_releases",
    description="A view collating data for population releases at the person level for Maine",
    should_materialize=True,
    view_query_template=VIEW_QUERY_TEMPLATE,
    us_me_raw_data_dataset=raw_tables_dataset_for_region(
        StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
    us_me_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        StateCode.US_ME, instance=DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_POPULATION_RELEASES_VIEW_BUILDER.build_and_print()
