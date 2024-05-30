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
"""A view detailing the incarceration population at the facility level for Arizona."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.dataset_config import validation_dataset_for_state

VIEW_QUERY_TEMPLATE = """
SELECT 
    'US_AZ' AS state_code,
    EXTRACT(YEAR FROM CAST(count_date AS DATE)) AS year,
    EXTRACT(MONTH FROM CAST(count_date AS DATE)) AS month,
    CAST(COUNT_DATE AS DATE) AS date_of_stay,
    UPPER(facility) AS facility,
    CAST(population_count AS INT64) AS population_count,
    'US_AZ' AS region_code
FROM `{project_id}.{us_az_raw_data_up_to_date_dataset}.validation_daily_count_sheet_by_facility_latest`
"""

US_AZ_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=validation_dataset_for_state(StateCode.US_AZ),
    view_id="incarceration_population_by_facility",
    description="A view detailing the incarceration population at the facility level for Arizona",
    view_query_template=VIEW_QUERY_TEMPLATE,
    us_az_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        StateCode.US_AZ, instance=DirectIngestInstance.PRIMARY
    ),
)
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AZ_INCARCERATION_POPULATION_BY_FACILITY_VIEW_BUILDER.build_and_print()
