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
"""Helpers for collecting raw data datasets"""
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    raw_data_views_dataset_for_region,
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


def get_raw_data_table_and_view_datasets() -> dict[str, StateCode]:
    """Returns all all datasets for raw data tables (i.e. us_xx_raw_data), latest
    views (i.e. us_xx_up_to_date_views), and raw data views (i.e. us_xx_raw_data_views)
    across all states.
    """
    raw_datasets: dict[str, StateCode] = {}
    for state_code in get_existing_direct_ingest_states():
        for instance in DirectIngestInstance:
            raw_tables_dataset = raw_tables_dataset_for_region(state_code, instance)
            raw_latest_views_dataset = raw_latest_views_dataset_for_region(
                state_code, instance
            )
            raw_table_views_dataset = raw_data_views_dataset_for_region(
                state_code, instance
            )
            raw_datasets[raw_tables_dataset] = state_code
            raw_datasets[raw_latest_views_dataset] = state_code
            raw_datasets[raw_table_views_dataset] = state_code
    return raw_datasets
