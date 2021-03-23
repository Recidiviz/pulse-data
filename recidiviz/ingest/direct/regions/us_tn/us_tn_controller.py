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
"""Direct ingest controller implementation for US_TN."""

from typing import List, Dict, Optional, Callable

from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.common.constants.enum_overrides import (
    EnumOverrides,
    EnumMapper,
    EnumIgnorePredicate,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller import (
    CsvGcsfsDirectIngestController,
)
from recidiviz.ingest.direct.direct_ingest_controller_utils import (
    update_overrides_from_maps,
)
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


class UsTnController(CsvGcsfsDirectIngestController):
    """Direct ingest controller implementation for US_TN."""

    @classmethod
    def region_code(cls) -> str:
        return StateCode.US_TN.value.lower()

    @classmethod
    def system_level(cls) -> SystemLevel:
        return SystemLevel.STATE

    def __init__(
        self,
        ingest_directory_path: GcsfsDirectoryPath,
        storage_directory_path: GcsfsDirectoryPath,
        ingest_database_key: SQLAlchemyDatabaseKey,
    ):
        super().__init__(
            ingest_directory_path, storage_directory_path, ingest_database_key
        )
        self.enum_overrides = self.generate_enum_overrides()

        self.row_post_processors_by_file: Dict[str, List[Callable]] = {}

        self.file_post_processors_by_file: Dict[str, List[Callable]] = {}

        self.primary_key_override_hook_by_file: Dict[str, Callable] = {}

        self.ancestor_chain_overrides_callback_by_file: Dict[str, Callable] = {}

    ENUM_OVERRIDES: Dict[EntityEnum, List[str]] = {}
    ENUM_MAPPERS: Dict[EntityEnumMeta, EnumMapper] = {}
    ENUM_IGNORES: Dict[EntityEnumMeta, List[str]] = {}
    ENUM_IGNORE_PREDICATES: Dict[EntityEnumMeta, EnumIgnorePredicate] = {}

    @classmethod
    def get_file_tag_rank_list(cls) -> List[str]:
        return []

    def generate_enum_overrides(self) -> EnumOverrides:
        """Provides US_TN-specific overrides for enum mappings."""
        base_overrides = super().get_enum_overrides()
        return update_overrides_from_maps(
            base_overrides,
            self.ENUM_OVERRIDES,
            self.ENUM_IGNORES,
            self.ENUM_MAPPERS,
            self.ENUM_IGNORE_PREDICATES,
        )

    def get_enum_overrides(self) -> EnumOverrides:
        return self.enum_overrides

    def _get_row_post_processors_for_file(self, file_tag: str) -> List[Callable]:
        return self.row_post_processors_by_file.get(file_tag, [])

    def _get_file_post_processors_for_file(self, file_tag: str) -> List[Callable]:
        return self.file_post_processors_by_file.get(file_tag, [])

    def _get_primary_key_override_for_file(self, file: str) -> Optional[Callable]:
        return self.primary_key_override_hook_by_file.get(file, None)

    def _get_ancestor_chain_overrides_callback_for_file(
        self, file: str
    ) -> Optional[Callable]:
        return self.ancestor_chain_overrides_callback_by_file.get(file, None)
