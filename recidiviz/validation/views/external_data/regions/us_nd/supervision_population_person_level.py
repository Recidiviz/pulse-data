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
"""A view containing supervision population at the person level for North Datokta."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

# TODO(#10884): This validation data seems incorrect, so excluding it for now.
# remove the LIMIT 0 when we are sure it is correct.
VIEW_QUERY_TEMPLATE = """
SELECT
    'US_ND' AS state_code,
    TRIM(SID) as person_external_id,
    'US_ND_SID' as external_id_type,
    DATE('2020-06-01') as date_of_supervision,
    MIN(level_1_supervision_location_external_id) as district,
    CAST(NULL as STRING) as supervising_officer,
    CAST(NULL as STRING) supervision_level
FROM `{project_id}.{us_nd_validation_oneoffs_dataset}.open_supervision_2020_06_01`
LEFT JOIN `{project_id}.{us_nd_raw_data_up_to_date_dataset}.RECIDIVIZ_REFERENCE_supervision_location_ids_latest`
    ON SITE_NAME = level_1_supervision_location_name
GROUP BY SID
--- TODO(#10884): remove the LIMIT 0 when we are sure it is correct.
LIMIT 0
"""

US_ND_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_ND),
    view_id="supervision_population_person_level",
    description="A view detailing supervision population at the person level for North Datokta",
    view_query_template=VIEW_QUERY_TEMPLATE,
    should_materialize=True,
    us_nd_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
    ),
    us_nd_validation_oneoffs_dataset=dataset_config.validation_oneoff_dataset_for_state(
        StateCode.US_ND
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_SUPERVISION_POPULATION_PERSON_LEVEL_VIEW_BUILDER.build_and_print()
