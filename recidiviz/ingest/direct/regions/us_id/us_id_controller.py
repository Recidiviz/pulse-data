# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Direct ingest controller implementation for US_ID."""
from typing import List, Dict, Optional, Callable

from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.common.constants.enum_overrides import EnumOverrides, EnumMapper, EnumIgnorePredicate
from recidiviz.common.constants.person_characteristics import Race, Ethnicity, Gender
from recidiviz.common.constants.state.external_id_types import US_ID_DOC
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller import CsvGcsfsDirectIngestController
from recidiviz.ingest.direct.direct_ingest_controller_utils import update_overrides_from_maps, create_if_not_exists
from recidiviz.ingest.direct.state_shared_row_posthooks import copy_name_to_alias, gen_label_single_external_id_hook
from recidiviz.ingest.models.ingest_info import StatePersonEthnicity, StatePerson, IngestObject
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache


class UsIdController(CsvGcsfsDirectIngestController):
    """Direct ingest controller implementation for US_ID."""

    def __init__(self,
                 ingest_directory_path: Optional[str] = None,
                 storage_directory_path: Optional[str] = None,
                 max_delay_sec_between_files: Optional[int] = None):
        super(UsIdController, self).__init__(
            'us_id',
            SystemLevel.STATE,
            ingest_directory_path,
            storage_directory_path,
            max_delay_sec_between_files=max_delay_sec_between_files)
        self.enum_overrides = self.generate_enum_overrides()
        self.row_post_processors_by_file: Dict[str, List[Callable]] = {
            'offender': [
                copy_name_to_alias,
                # When first parsed, the info object just has a single external id - the DOC id.
                gen_label_single_external_id_hook(US_ID_DOC),
                self._rationalize_race_and_ethnicity,
            ],
        }
        self.file_post_processors_by_file: Dict[str, List[Callable]] = {
            'offender': []
        }

    FILE_TAGS = ['offender']

    ENUM_OVERRIDES: Dict[EntityEnum, List[str]] = {
        Race.ASIAN: ['A'],
        Race.BLACK: ['B'],
        Race.AMERICAN_INDIAN_ALASKAN_NATIVE: ['I'],
        Race.OTHER: ['O'],
        Race.EXTERNAL_UNKNOWN: ['U'],
        Race.WHITE: ['W'],

        Ethnicity.HISPANIC: ['H'],

        Gender.MALE: ['M'],
        Gender.FEMALE: ['F'],
        Gender.TRANS_MALE: ['X'],
        Gender.TRANS_FEMALE: ['Y'],
        Gender.EXTERNAL_UNKNOWN: ['U'],
    }
    ENUM_IGNORES: Dict[EntityEnumMeta, List[str]] = {}
    ENUM_MAPPERS: Dict[EntityEnumMeta, EnumMapper] = {}
    ENUM_IGNORE_PREDICATES: Dict[EntityEnumMeta, EnumIgnorePredicate] = {}

    def _get_file_tag_rank_list(self) -> List[str]:
        return self.FILE_TAGS

    def generate_enum_overrides(self) -> EnumOverrides:
        """Provides Missouri-specific overrides for enum mappings."""
        base_overrides = super(UsIdController, self).get_enum_overrides()
        return update_overrides_from_maps(
            base_overrides, self.ENUM_OVERRIDES, self.ENUM_IGNORES, self.ENUM_MAPPERS, self.ENUM_IGNORE_PREDICATES)

    def get_enum_overrides(self) -> EnumOverrides:
        return self.enum_overrides

    def _get_row_post_processors_for_file(self, file_tag: str) -> List[Callable]:
        return self.row_post_processors_by_file.get(file_tag, [])

    def _get_file_post_processors_for_file(self, file_tag: str) -> List[Callable]:
        return self.file_post_processors_by_file.get(file_tag, [])

    def _rationalize_race_and_ethnicity(
            self,
            _file_tag: str,
            _row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """For a person whose provided race is actually an ethnicity, we record it as an ethnicity instead of a race."""
        ethnicity_override_values = []
        for ethnicity in Ethnicity:
            ethnicity_override_values.extend(self.ENUM_OVERRIDES.get(ethnicity, []))

        for obj in extracted_objects:
            if isinstance(obj, StatePerson):
                updated_person_races = []
                for person_race in obj.state_person_races:
                    if person_race.race in ethnicity_override_values:
                        ethnicity_to_create = StatePersonEthnicity(ethnicity=person_race.race)
                        create_if_not_exists(ethnicity_to_create, obj, 'state_person_ethnicities')
                    else:
                        updated_person_races.append(person_race)
                obj.state_person_races = updated_person_races
