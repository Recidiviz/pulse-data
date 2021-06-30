# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Store used to maintain all admin panel related stores"""
from typing import Dict, List

from recidiviz.admin_panel.dataset_metadata_store import DatasetMetadataCountsStore
from recidiviz.admin_panel.ingest_metadata_store import IngestDataFreshnessStore
from recidiviz.admin_panel.ingest_operations_store import IngestOperationsStore
from recidiviz.admin_panel.validation_metadata_store import ValidationStatusStore
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING, in_development, in_gcp
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.timer import RepeatedTimer

_INGEST_METADATA_NICKNAME = "ingest"
_INGEST_METADATA_PREFIX = "ingest_state_metadata"
_VALIDATION_METADATA_NICKNAME = "validation"
_VALIDATION_METADATA_PREFIX = "validation_metadata"


class AdminStores:
    """
    A wrapper around all stores needed for the admin panel.
    """

    def __init__(self) -> None:
        if in_development():
            with local_project_id_override(GCP_PROJECT_STAGING):
                self._initialize_stores()
        elif in_gcp():
            self._initialize_stores()

    def _initialize_stores(self) -> None:
        self.ingest_metadata_store = DatasetMetadataCountsStore(
            _INGEST_METADATA_NICKNAME,
            _INGEST_METADATA_PREFIX,
        )

        self.validation_metadata_store = DatasetMetadataCountsStore(
            _VALIDATION_METADATA_NICKNAME,
            _VALIDATION_METADATA_PREFIX,
        )

        self.ingest_data_freshness_store = IngestDataFreshnessStore()

        self.ingest_operations_store = IngestOperationsStore()

        self.validation_status_store = ValidationStatusStore()

    def start_timers(self) -> None:
        """Starts store refresh timers for all stores that are a subclass of the AdminPanelStore class."""
        if in_gcp() or in_development():
            stores_with_timers = [
                self.ingest_metadata_store,
                self.validation_metadata_store,
                self.ingest_data_freshness_store,
                self.validation_status_store,
            ]

            for store in stores_with_timers:
                RepeatedTimer(
                    15 * 60, store.recalculate_store, run_immediately=True
                ).start()


def fetch_state_codes(state_codes: List[StateCode]) -> List[Dict[str, str]]:
    return [
        {
            "code": state_code.value,
            "name": state_code.get_state().name,
        }
        for state_code in state_codes
    ]
