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
from typing import Dict, Iterable, List, Optional

from recidiviz.admin_panel.admin_panel_store import AdminPanelStore
from recidiviz.admin_panel.dataset_metadata_store import DatasetMetadataCountsStore
from recidiviz.admin_panel.ingest_operations_store import IngestOperationsStore
from recidiviz.admin_panel.lineage_store import LineageStore
from recidiviz.admin_panel.validation_metadata_store import ValidationStatusStore
from recidiviz.common.constants.states import StateCode

_INGEST_METADATA_NICKNAME = "ingest"
_INGEST_METADATA_PREFIX = "ingest_state_metadata"
_VALIDATION_METADATA_NICKNAME = "validation"
_VALIDATION_METADATA_PREFIX = "validation_metadata"


class _AdminStores:
    """
    A wrapper around all stores needed for the admin panel.
    """

    def __init__(self) -> None:
        self.all_stores = self._initialize_stores()

    def _initialize_stores(self) -> List[AdminPanelStore]:
        admin_panel_stores: List[AdminPanelStore] = []
        self.ingest_metadata_store = DatasetMetadataCountsStore(
            _INGEST_METADATA_NICKNAME,
            _INGEST_METADATA_PREFIX,
        )
        admin_panel_stores.append(self.ingest_metadata_store)

        self.validation_metadata_store = DatasetMetadataCountsStore(
            _VALIDATION_METADATA_NICKNAME,
            _VALIDATION_METADATA_PREFIX,
        )
        admin_panel_stores.append(self.validation_metadata_store)

        self.ingest_operations_store = IngestOperationsStore()
        admin_panel_stores.append(self.ingest_operations_store)

        self.validation_status_store = ValidationStatusStore()
        admin_panel_stores.append(self.validation_status_store)

        self.lineage_store = LineageStore()
        admin_panel_stores.append(self.lineage_store)

        return admin_panel_stores


_admin_stores: Optional[_AdminStores] = None


def initialize_admin_stores() -> _AdminStores:
    global _admin_stores
    _admin_stores = _AdminStores()
    return _admin_stores


def get_ingest_operations_store() -> IngestOperationsStore:
    if _admin_stores is None:
        raise ValueError(
            "Admin stores not initialized, must first call initialize_admin_stores()."
        )
    return _admin_stores.ingest_operations_store


def get_ingest_metadata_store() -> DatasetMetadataCountsStore:
    if _admin_stores is None:
        raise ValueError(
            "Admin stores not initialized, must first call initialize_admin_stores()."
        )
    return _admin_stores.ingest_metadata_store


def get_validation_metadata_store() -> DatasetMetadataCountsStore:
    if _admin_stores is None:
        raise ValueError(
            "Admin stores not initialized, must first call initialize_admin_stores()."
        )
    return _admin_stores.validation_metadata_store


def get_validation_status_store() -> ValidationStatusStore:
    if _admin_stores is None:
        raise ValueError(
            "Admin stores not initialized, must first call initialize_admin_stores()."
        )
    return _admin_stores.validation_status_store


def get_lineage_store() -> LineageStore:
    if _admin_stores is None:
        raise ValueError(
            "Admin stores not initialized, must first call initialize_admin_stores()."
        )
    return _admin_stores.lineage_store


def fetch_state_codes(state_codes: Iterable[StateCode]) -> List[Dict[str, str]]:
    return [
        {
            "code": state_code.value,
            "name": state_code.get_state().name,
        }
        for state_code in state_codes
    ]
