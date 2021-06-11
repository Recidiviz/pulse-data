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

from typing import Dict, List, Optional

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.common.constants.enum_overrides import (
    EnumIgnorePredicate,
    EnumMapper,
    EnumOverrides,
)
from recidiviz.common.constants.person_characteristics import Ethnicity
from recidiviz.common.constants.state.external_id_types import US_TN_DOC
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller import (
    CsvGcsfsDirectIngestController,
    IngestAncestorChainOverridesCallable,
    IngestFilePostprocessorCallable,
    IngestPrimaryKeyOverrideCallable,
    IngestRowPosthookCallable,
)
from recidiviz.ingest.direct.direct_ingest_controller_utils import create_if_not_exists
from recidiviz.ingest.direct.regions.us_tn.us_tn_enum_helpers import (
    generate_enum_overrides,
)
from recidiviz.ingest.direct.state_shared_row_posthooks import IngestGatingContext
from recidiviz.ingest.models.ingest_info import (
    IngestObject,
    StatePerson,
    StatePersonEthnicity,
    StatePersonExternalId,
)
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache


class UsTnController(CsvGcsfsDirectIngestController):
    """Direct ingest controller implementation for US_TN."""

    @classmethod
    def region_code(cls) -> str:
        return StateCode.US_TN.value.lower()

    def __init__(self, ingest_bucket_path: GcsfsBucketPath):
        super().__init__(ingest_bucket_path)
        self.enum_overrides = generate_enum_overrides()

        self.row_post_processors_by_file: Dict[str, List[IngestRowPosthookCallable]] = {
            "OffenderName": [
                self._normalize_external_id_type,
                self._set_state_person_ethnicity,
            ],
        }

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
        return ["OffenderName"]

    def get_enum_overrides(self) -> EnumOverrides:
        return self.enum_overrides

    def _get_row_post_processors_for_file(
        self, file_tag: str
    ) -> List[IngestRowPosthookCallable]:
        return self.row_post_processors_by_file.get(file_tag, [])

    def _get_file_post_processors_for_file(
        self, file_tag: str
    ) -> List[IngestFilePostprocessorCallable]:
        return self.file_post_processors_by_file.get(file_tag, [])

    def _get_primary_key_override_for_file(
        self, file: str
    ) -> Optional[IngestPrimaryKeyOverrideCallable]:
        return self.primary_key_override_hook_by_file.get(file, None)

    def _get_ancestor_chain_overrides_callback_for_file(
        self, file: str
    ) -> Optional[IngestAncestorChainOverridesCallable]:
        return self.ancestor_chain_overrides_callback_by_file.get(file, None)

    @staticmethod
    def _normalize_external_id_type(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Set the external id type."""
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StatePersonExternalId):
                extracted_object.__setattr__("id_type", US_TN_DOC)

    def _set_state_person_ethnicity(
        self,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Set the person's ethnicity, given their race.

        In TN, `Hispanic` is included in their race columns. So parse the race to get the corresponding
        ethnicity value."""
        race = row.get("Race", None)
        if race:
            ethnicity = self.get_enum_overrides().parse(race, Ethnicity)
            if ethnicity:
                ethnicity_to_create = StatePersonEthnicity(ethnicity=ethnicity.value)
                for extracted_object in extracted_objects:
                    if isinstance(extracted_object, StatePerson):
                        create_if_not_exists(
                            ethnicity_to_create,
                            extracted_object,
                            "state_person_ethnicities",
                        )
                        # If the parsed ethnicity is `HISPANIC`, then clear the race. There is no ethnicity column in
                        # TN, and one can have only 1 race in their system. So if the raw race value corresponds with
                        # `HISPANIC`, then there no other race that can be associated with the person.
                        if ethnicity == Ethnicity.HISPANIC:
                            extracted_object.state_person_races.clear()
