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
"""BQ view containing US_MI additional metadata for housing units in facilities."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_MI_HOUSING_UNIT_METADATA_VIEW_NAME = "us_mi_housing_unit_metadata"

US_MI_HOUSING_UNIT_METADATA_DESCRIPTION = (
    """Provides additional metadata for housing units in US_MI facilities."""
)

US_MI_HOUSING_UNIT_METADATA_VIEW_QUERY = """
    /*{description}*/
    SELECT DISTINCT
        person_id,
        housing_unit,
        facility,
        r.reporting_station_id,
        r.name AS reporting_station_name
    FROM `{project_id}.{base_dataset}.state_incarceration_period` i 
    JOIN `{project_id}.{us_mi_raw_data_up_to_date_dataset}.ADH_UNIT_LOCK_latest` u
    ON u.unit_lock_id = i.housing_unit
    JOIN `{project_id}.{us_mi_raw_data_up_to_date_dataset}.ADH_REPORTING_STATION_latest` r
    ON r.reporting_station_id = u.reporting_station_id
    WHERE i.state_code = 'US_MI'
"""

US_MI_HOUSING_UNIT_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_MI_HOUSING_UNIT_METADATA_VIEW_NAME,
    view_query_template=US_MI_HOUSING_UNIT_METADATA_VIEW_QUERY,
    description=US_MI_HOUSING_UNIT_METADATA_DESCRIPTION,
    base_dataset=dataset_config.STATE_BASE_DATASET,
    us_mi_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region("us_mi"),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_HOUSING_UNIT_METADATA_VIEW_BUILDER.build_and_print()
