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
"""A view containing supervision population by supervision type for Arizona."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

VIEW_QUERY_TEMPLATE = """
SELECT DISTINCT 
        'US_AZ' AS region_code, 
        CAST(date_of_supervision AS DATE) AS date_of_supervision, 
        'PAROLE' AS supervision_type,
        SUM(CAST(population_count AS INT64)) OVER (PARTITION BY date_of_supervision) AS population_count
FROM `{project_id}.{us_az_raw_data_up_to_date_dataset}.validation_daily_count_sheet_by_supervision_type_latest`
"""

US_AZ_SUPERVISION_POPULATION_BY_TYPE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.validation_dataset_for_state(StateCode.US_AZ),
    view_id="supervision_population_by_type",
    description="A unified view of AZ supervision population counts by type based on Daily Count Sheets available on the ADCRR Reports webpage.",
    view_query_template=VIEW_QUERY_TEMPLATE,
    should_materialize=True,
    us_az_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        StateCode.US_AZ, instance=DirectIngestInstance.PRIMARY
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AZ_SUPERVISION_POPULATION_BY_TYPE_VIEW_BUILDER.build_and_print()
