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
"""Direct ingest controller implementation for US_MI."""

from typing import Dict, List, Optional

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.common.constants.enum_overrides import (
    EnumIgnorePredicate,
    EnumMapper,
    EnumOverrides,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.controllers.legacy_ingest_view_processor import (
    IngestAncestorChainOverridesCallable,
    IngestFilePostprocessorCallable,
    IngestPrimaryKeyOverrideCallable,
    IngestRowPosthookCallable,
    IngestRowPrehookCallable,
    LegacyIngestViewProcessorDelegate,
)
from recidiviz.ingest.direct.direct_ingest_controller_utils import (
    update_overrides_from_maps,
)


# TODO(#8904): Delete LegacyIngestViewProcessorDelegate superclass when we have fully
#  migrated this state to new ingest mappings version.
class UsMiController(BaseDirectIngestController, LegacyIngestViewProcessorDelegate):
    """Direct ingest controller implementation for US_MI."""

    @classmethod
    def region_code(cls) -> str:
        return StateCode.US_MI.value.lower()

    def __init__(self, ingest_bucket_path: GcsfsBucketPath):
        super().__init__(ingest_bucket_path)
        self.enum_overrides = self.generate_enum_overrides()

        self.row_post_processors_by_file: Dict[
            str, List[IngestRowPosthookCallable]
        ] = {}

        self.file_post_processors_by_file: Dict[
            str, List[IngestFilePostprocessorCallable]
        ] = {}

        self.primary_key_override_hook_by_file: Dict[
            str, IngestPrimaryKeyOverrideCallable
        ] = {}

        self.ancestor_chain_overrides_callback_by_file: Dict[
            str, IngestAncestorChainOverridesCallable
        ] = {}

    ENUM_OVERRIDES: Dict[EntityEnum, List[str]] = {}
    ENUM_MAPPERS: Dict[EntityEnumMeta, EnumMapper] = {}
    ENUM_IGNORES: Dict[EntityEnumMeta, List[str]] = {}
    ENUM_IGNORE_PREDICATES: Dict[EntityEnumMeta, EnumIgnorePredicate] = {}

    def get_file_tag_rank_list(self) -> List[str]:
        return []

    def generate_enum_overrides(self) -> EnumOverrides:
        """Provides US_MI-specific overrides for enum mappings."""
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

    # TODO(#8904): Delete LegacyIngestViewProcessorDelegate methods when we have fully
    #  migrated this state to new ingest mappings version.
    def get_row_post_processors_for_file(
        self, file_tag: str
    ) -> List[IngestRowPosthookCallable]:
        return self.row_post_processors_by_file.get(file_tag, [])

    def get_file_post_processors_for_file(
        self, file_tag: str
    ) -> List[IngestFilePostprocessorCallable]:
        return self.file_post_processors_by_file.get(file_tag, [])

    def get_primary_key_override_for_file(
        self, file: str
    ) -> Optional[IngestPrimaryKeyOverrideCallable]:
        return self.primary_key_override_hook_by_file.get(file, None)

    def get_ancestor_chain_overrides_callback_for_file(
        self, file: str
    ) -> Optional[IngestAncestorChainOverridesCallable]:
        return self.ancestor_chain_overrides_callback_by_file.get(file, None)

    def get_row_pre_processors_for_file(
        self, _file_tag: str
    ) -> List[IngestRowPrehookCallable]:
        return []

    def get_files_to_set_with_empty_values(self) -> List[str]:
        return []
