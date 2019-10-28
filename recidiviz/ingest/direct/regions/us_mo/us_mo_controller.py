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
"""Direct ingest controller implementation for us_mo."""

from typing import Optional, List, Callable, Dict

from recidiviz.common.constants.state.external_id_types import US_MO_DOC, \
    US_MO_SID, US_MO_FBI, US_MO_OLN
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller \
    import CsvGcsfsDirectIngestController
from recidiviz.ingest.direct.direct_ingest_controller_utils import \
    create_if_not_exists
from recidiviz.ingest.direct.state_shared_row_posthooks import \
    copy_name_to_alias, gen_label_single_external_id_hook
from recidiviz.ingest.models.ingest_info import IngestObject, StatePerson, \
    StatePersonExternalId
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache


class UsMoController(CsvGcsfsDirectIngestController):
    """Direct ingest controller implementation for us_mo."""

    def __init__(self,
                 ingest_directory_path: Optional[str] = None,
                 storage_directory_path: Optional[str] = None,
                 max_delay_sec_between_files: Optional[int] = None):
        super(UsMoController, self).__init__(
            'us_mo',
            SystemLevel.STATE,
            ingest_directory_path,
            storage_directory_path,
            max_delay_sec_between_files=max_delay_sec_between_files)

        self.row_pre_processors_by_file: Dict[str, List[Callable]] = {}
        self.row_post_processors_by_file: Dict[str, List[Callable]] = {
            'tak001_offender_identification': [
                copy_name_to_alias,
                # When first parsed, the info object just has a single
                # external id - the DOC id.
                gen_label_single_external_id_hook(US_MO_DOC),
                self.tak001_offender_identification_hydrate_alternate_ids,
            ],
        }

    def _get_file_tag_rank_list(self) -> List[str]:
        return [
            'tak001_offender_identification'
        ]

    def _get_row_pre_processors_for_file(self, file: str) -> List[Callable]:
        return self.row_pre_processors_by_file.get(file, [])

    def _get_row_post_processors_for_file(self, file: str) -> List[Callable]:
        return self.row_post_processors_by_file.get(file, [])

    # TODO(1882): If yaml format supported raw values and multiple children of
    #  the same type, then this would be no-longer necessary.
    @staticmethod
    def tak001_offender_identification_hydrate_alternate_ids(
            _row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StatePerson):
                external_ids_to_create = [
                    StatePersonExternalId(
                        state_person_external_id_id=_row['EK$SID'],
                        id_type=US_MO_SID),
                    StatePersonExternalId(
                        state_person_external_id_id=_row['EK$FBI'],
                        id_type=US_MO_FBI),
                    StatePersonExternalId(
                        state_person_external_id_id=_row['EK$OLN'],
                        id_type=US_MO_OLN)
                    ]

                for id_to_create in external_ids_to_create:
                    create_if_not_exists(
                        id_to_create,
                        extracted_object,
                        'state_person_external_ids')
