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
"""Direct ingest controller implementation for US_ME."""
from enum import Enum
from typing import Dict, List, Optional, Type

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.constants.enum_overrides import (
    EnumIgnorePredicate,
    EnumMapperFn,
    EnumOverrides,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.controllers.legacy_ingest_view_processor import (
    IngestRowPrehookCallable,
    LegacyIngestViewProcessorDelegate,
)
from recidiviz.ingest.direct.direct_ingest_controller_utils import (
    update_overrides_from_maps,
)
from recidiviz.ingest.extractor.csv_data_extractor import (
    AncestorChainOverridesCallable,
    FilePostprocessorCallable,
    PrimaryKeyOverrideCallable,
    RowPosthookCallable,
)


# TODO(#8908): Delete LegacyIngestViewProcessorDelegate superclass once ingest mappings
#   overhaul is ready for new states going forward.
class UsMeController(BaseDirectIngestController, LegacyIngestViewProcessorDelegate):
    """Direct ingest controller implementation for US_ME."""

    @classmethod
    def region_code(cls) -> str:
        return StateCode.US_ME.value.lower()

    def __init__(self, ingest_bucket_path: GcsfsBucketPath):
        super().__init__(ingest_bucket_path)
        self.enum_overrides = self.generate_enum_overrides()

        self.row_post_processors_by_file: Dict[str, List[RowPosthookCallable]] = {}

        self.file_post_processors_by_file: Dict[
            str, List[FilePostprocessorCallable]
        ] = {}

        self.primary_key_override_hook_by_file: Dict[
            str, PrimaryKeyOverrideCallable
        ] = {}

        self.ancestor_chain_overrides_callback_by_file: Dict[
            str, AncestorChainOverridesCallable
        ] = {}

    ENUM_OVERRIDES: Dict[Enum, List[str]] = {}
    ENUM_MAPPER_FUNCTIONS: Dict[Type[Enum], EnumMapperFn] = {}
    ENUM_IGNORES: Dict[Type[Enum], List[str]] = {}
    ENUM_IGNORE_PREDICATES: Dict[Type[Enum], EnumIgnorePredicate] = {}

    def get_file_tag_rank_list(self) -> List[str]:
        return []

    def generate_enum_overrides(self) -> EnumOverrides:
        """Provides US_ME-specific overrides for enum mappings."""
        base_overrides = super().get_enum_overrides()
        return update_overrides_from_maps(
            base_overrides,
            self.ENUM_OVERRIDES,
            self.ENUM_IGNORES,
            self.ENUM_MAPPER_FUNCTIONS,
            self.ENUM_IGNORE_PREDICATES,
        )

    def get_enum_overrides(self) -> EnumOverrides:
        return self.enum_overrides

    # TODO(#8908): Delete LegacyIngestViewProcessorDelegate methods once ingest mappings
    #   overhaul is ready for new states going forward.
    def get_row_post_processors_for_file(
        self, file_tag: str
    ) -> List[RowPosthookCallable]:
        return self.row_post_processors_by_file.get(file_tag, [])

    def get_file_post_processors_for_file(
        self, file_tag: str
    ) -> List[FilePostprocessorCallable]:
        return self.file_post_processors_by_file.get(file_tag, [])

    def get_primary_key_override_for_file(
        self, file: str
    ) -> Optional[PrimaryKeyOverrideCallable]:
        return self.primary_key_override_hook_by_file.get(file, None)

    def get_ancestor_chain_overrides_callback_for_file(
        self, file: str
    ) -> Optional[AncestorChainOverridesCallable]:
        return self.ancestor_chain_overrides_callback_by_file.get(file, None)

    def get_row_pre_processors_for_file(
        self, _file_tag: str
    ) -> List[IngestRowPrehookCallable]:
        return []

    def get_files_to_set_with_empty_values(self) -> List[str]:
        return []
