# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Direct ingest controller implementation for US_PA."""

from typing import List, Dict, Optional, Callable

from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.common.constants.enum_overrides import EnumOverrides, EnumMapper, EnumIgnorePredicate
from recidiviz.common.constants.person_characteristics import Race, Gender, Ethnicity
from recidiviz.common.constants.state.external_id_types import US_PA_SID, US_PA_INMATE, US_PA_CONT, US_PA_DOC, \
    US_PA_PBPP
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller import CsvGcsfsDirectIngestController
from recidiviz.ingest.direct.direct_ingest_controller_utils import update_overrides_from_maps, create_if_not_exists
from recidiviz.ingest.direct.state_shared_row_posthooks import copy_name_to_alias, gen_label_single_external_id_hook, \
    gen_rationalize_race_and_ethnicity
from recidiviz.ingest.models.ingest_info import IngestObject, StatePerson, StatePersonExternalId
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache


class UsPaController(CsvGcsfsDirectIngestController):
    """Direct ingest controller implementation for US_PA."""

    def __init__(self,
                 ingest_directory_path: Optional[str] = None,
                 storage_directory_path: Optional[str] = None,
                 max_delay_sec_between_files: Optional[int] = None):
        super(UsPaController, self).__init__(
            'us_pa',
            SystemLevel.STATE,
            ingest_directory_path,
            storage_directory_path,
            max_delay_sec_between_files=max_delay_sec_between_files)
        self.enum_overrides = self.generate_enum_overrides()

        self.row_post_processors_by_file: Dict[str, List[Callable]] = {
            'dbo_IcsDoc': [
                gen_label_single_external_id_hook(US_PA_DOC),
                self.gen_hydrate_alternate_external_ids({
                    'Cont_Num': US_PA_CONT,
                    'Inmate_Num': US_PA_INMATE,
                    'PBPP_Num': US_PA_PBPP,
                    'SID_Num': US_PA_SID,
                }),
                copy_name_to_alias,
                gen_rationalize_race_and_ethnicity(self.ENUM_OVERRIDES),
                self._compose_current_address,
            ],
            'dbo_Offender': [
                gen_label_single_external_id_hook(US_PA_PBPP),
                self.gen_hydrate_alternate_external_ids({
                    'OffSID': US_PA_SID,
                }),
                gen_rationalize_race_and_ethnicity(self.ENUM_OVERRIDES),
            ],
        }

        self.file_post_processors_by_file: Dict[str, List[Callable]] = {
            'dbo_IcsDoc': [],
            'dbo_Offender': [],
        }

    FILE_TAGS = [
        # Data source: DOC
        'dbo_IcsDoc',
        # Data source: PBPP
        'dbo_Offender',
    ]

    ENUM_OVERRIDES: Dict[EntityEnum, List[str]] = {
        Race.ASIAN: ['1', 'A'],
        Race.BLACK: ['2', 'B'],
        Race.AMERICAN_INDIAN_ALASKAN_NATIVE: ['4', 'I'],
        Race.OTHER: ['5', 'N'],
        Race.WHITE: ['6', 'W'],

        Ethnicity.HISPANIC: ['3', 'H'],

        Gender.FEMALE: ['1', 'F'],
        Gender.MALE: ['2', 'M'],
    }
    ENUM_IGNORES: Dict[EntityEnumMeta, List[str]] = {}
    ENUM_MAPPERS: Dict[EntityEnumMeta, EnumMapper] = {}
    ENUM_IGNORE_PREDICATES: Dict[EntityEnumMeta, EnumIgnorePredicate] = {}

    def _get_file_tag_rank_list(self) -> List[str]:
        return self.FILE_TAGS

    def generate_enum_overrides(self) -> EnumOverrides:
        """Provides Pennsylvania-specific overrides for enum mappings."""
        base_overrides = super(UsPaController, self).get_enum_overrides()
        return update_overrides_from_maps(
            base_overrides, self.ENUM_OVERRIDES, self.ENUM_IGNORES, self.ENUM_MAPPERS, self.ENUM_IGNORE_PREDICATES)

    def get_enum_overrides(self) -> EnumOverrides:
        return self.enum_overrides

    def _get_row_post_processors_for_file(self, file_tag: str) -> List[Callable]:
        return self.row_post_processors_by_file.get(file_tag, [])

    def _get_file_post_processors_for_file(self, file_tag: str) -> List[Callable]:
        return self.file_post_processors_by_file.get(file_tag, [])

    @staticmethod
    def gen_hydrate_alternate_external_ids(columns_to_id_types: Dict[str, str]) -> Callable:
        """Generates a row post-hook that will hydrate alternate external ids than the "main" external id in a row, for
        rows which have multiple external ids to be hydrated.

        TODO(1882): If yaml format supported raw values and multiple children of the same type,
        then this would be no-longer necessary.
        """

        def _hydrate_external_id(_file_tag: str,
                                 row: Dict[str, str],
                                 extracted_objects: List[IngestObject],
                                 _cache: IngestObjectCache):
            for obj in extracted_objects:
                if isinstance(obj, StatePerson):
                    external_ids_to_create = []
                    for column, id_type in columns_to_id_types.items():
                        value = row.get(column, '').strip()

                        if value:
                            external_ids_to_create.append(
                                StatePersonExternalId(state_person_external_id_id=value, id_type=id_type))

                    for id_to_create in external_ids_to_create:
                        create_if_not_exists(id_to_create, obj, 'state_person_external_ids')

        return _hydrate_external_id

    @staticmethod
    def _compose_current_address(
            _file_tag: str, row: Dict[str, str], extracted_objects: List[IngestObject], _cache: IngestObjectCache):
        """Composes all of the address-related fields into a single address."""
        line_1 = row['OfndrLegal_AddrLn1']
        line_2 = row['OfndrLegal_AddrLn2']
        city = row['OfndrLegal_AddrCity']
        state = row['OfndrLegalAddr_StateCd']
        zip_code = row['OfndrLegalAddr_ZipCd']
        state_and_zip = f"{state} {zip_code}" if zip_code else state
        country = row['OfndrLegalAddrCntry_Id']

        address = ', '.join(filter(None, (line_1, line_2, city, state_and_zip, country)))

        for obj in extracted_objects:
            if isinstance(obj, StatePerson):
                obj.current_address = address
