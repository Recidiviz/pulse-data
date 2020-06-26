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
import json
from typing import List, Dict, Optional, Callable

from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.common.constants.enum_overrides import EnumOverrides, EnumMapper, EnumIgnorePredicate
from recidiviz.common.constants.person_characteristics import Race, Gender, Ethnicity
from recidiviz.common.constants.state.external_id_types import US_PA_SID, US_PA_CONTROL, US_PA_PBPP
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import StateAssessmentType, StateAssessmentClass
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.common.str_field_utils import parse_days_from_duration_pieces
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller import CsvGcsfsDirectIngestController
from recidiviz.ingest.direct.direct_ingest_controller_utils import update_overrides_from_maps, create_if_not_exists
from recidiviz.ingest.direct.state_shared_row_posthooks import copy_name_to_alias, gen_label_single_external_id_hook, \
    gen_rationalize_race_and_ethnicity, gen_set_agent_type
from recidiviz.ingest.models.ingest_info import IngestObject, StatePerson, StatePersonExternalId, StateAssessment, \
    StateIncarcerationSentence, StateCharge, StateSentenceGroup
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
            'person_external_ids': [
                self._hydrate_person_external_ids
            ],
            'doc_person_info': [
                gen_label_single_external_id_hook(US_PA_CONTROL),
                self.gen_hydrate_alternate_external_ids({
                    'SID_Num': US_PA_SID,
                    'PBPP_Num': US_PA_PBPP,
                }),
                copy_name_to_alias,
                gen_rationalize_race_and_ethnicity(self.ENUM_OVERRIDES),
                self._compose_current_address,
                self._hydrate_sentence_group_ids
            ],
            'dbo_tblInmTestScore': [
                gen_label_single_external_id_hook(US_PA_CONTROL),
                self._generate_doc_assessment_external_id,
                self._enrich_doc_assessments,
            ],
            'dbo_Senrec': [
                self._set_incarceration_sentence_id,
                self._enrich_incarceration_sentence,
                self._strip_id_whitespace,
                gen_set_agent_type(StateAgentType.JUDGE),
            ],
            'dbo_Offender': [
                gen_label_single_external_id_hook(US_PA_PBPP),
                self.gen_hydrate_alternate_external_ids({
                    'OffSID': US_PA_SID,
                }),
                gen_rationalize_race_and_ethnicity(self.ENUM_OVERRIDES),
            ],
            'dbo_LSIR': [
                gen_label_single_external_id_hook(US_PA_PBPP),
                self._generate_pbpp_assessment_external_id,
                self._enrich_pbpp_assessments,
            ]
        }

        self.file_post_processors_by_file: Dict[str, List[Callable]] = {
            'person_external_ids': [],
            'doc_person_info': [],
            'dbo_tblInmTestScore': [],
            'dbo_Senrec': [],
            'dbo_Offender': [],
            'dbo_LSIR': [],
        }

    FILE_TAGS = [
        # Data source: Mixed
        'person_external_ids',

        # Data source: DOC
        'doc_person_info',
        'dbo_tblInmTestScore',
        'dbo_Senrec',

        # Data source: PBPP
        'dbo_Offender',
        'dbo_LSIR',
    ]

    ENUM_OVERRIDES: Dict[EntityEnum, List[str]] = {
        Race.ASIAN: ['ASIAN', 'A'],
        Race.BLACK: ['BLACK', 'B'],
        Race.AMERICAN_INDIAN_ALASKAN_NATIVE: ['AMERICAN INDIAN', 'I'],
        Race.OTHER: ['OTHER', 'N'],
        Race.WHITE: ['WHITE', 'W'],

        Ethnicity.HISPANIC: ['HISPANIC', 'H'],

        Gender.FEMALE: ['FEMALE', 'F'],
        Gender.MALE: ['MALE', 'M'],

        StateAssessmentType.CSSM: ['CSS-M'],
        StateAssessmentType.LSIR: ['LSI-R'],
        StateAssessmentType.PA_RST: ['RST'],
        StateAssessmentType.STATIC_99: ['ST99'],

        # TODO(3020): Confirm the COMPLETED codes below. Some may be intermediate and not appropriately mapped as final.
        StateSentenceStatus.COMPLETED: [
            'B',   # Bailed
            'CS',  # Change other Sentence
            'DA',  # Deceased - Assault
            'DN',  # Deceased - Natural
            'DS',  # Deceased - Suicide
            'DX',  # Deceased - Accident
            'DZ',  # Deceased - Non DOC Location
            'EX',  # Executed
            'FR',  # Federal Release
            'NC',  # Non-Return CSC
            'NF',  # Non-Return Furlough
            'NR',  # [Unlisted]
            'NW',  # Non-Return Work Release
            'P',   # Paroled
            'SC',  # Sentence Complete
            'SP',  # Serve Previous
            'TC',  # Transfer to County
            'TS',  # Transfer to Other State
        ],
        StateSentenceStatus.COMMUTED: [
            'RD',  # Release Detentioner
            'RE',  # Received in Error
        ],
        StateSentenceStatus.PARDONED: [
            'PD',  # Pardoned
        ],
        StateSentenceStatus.SERVING: [
            'AS',  # Actively Serving
            'CT',  # In Court
            'DC',  # Diag/Class
            'EC',  # Escape CSC
            'EI',  # Escape Institution
            'F',   # Furloughed
            'IC',  # In Custody Elsewhere
            'MH',  # Mental Health
            'SH',  # State Hospital
            'W',   # Waiting
            'WT',  # WRIT/ATA
        ],
        StateSentenceStatus.VACATED: [
            'VC',  # Vacated Conviction
            'VS',  # Vacated Sentence
        ],
        StateSentenceStatus.EXTERNAL_UNKNOWN: [
            'O',   # ??? (this is PA's own label; it means unknown within their own system)
        ],

        StateIncarcerationType.COUNTY_JAIL: [
            'C',  # County
        ],
        StateIncarcerationType.FEDERAL_PRISON: [
            'F',  # Federal
        ],
        StateIncarcerationType.OUT_OF_STATE: [
            'O',  # Transfer out of Pennsylvania
        ],
        StateIncarcerationType.STATE_PRISON: [
            'S',  # State
            'I',  # Transfer into Pennsylvania
            'T',  # County Transfer, i.e. transfer from county to state, usually for mental health services ("5B Case")
            'P',  # SIP Program
            'E',  # SIP Evaluation
        ],
    }
    ENUM_IGNORES: Dict[EntityEnumMeta, List[str]] = {}
    ENUM_MAPPERS: Dict[EntityEnumMeta, EnumMapper] = {}
    ENUM_IGNORE_PREDICATES: Dict[EntityEnumMeta, EnumIgnorePredicate] = {}

    @classmethod
    def get_file_tag_rank_list(cls) -> List[str]:
        return cls.FILE_TAGS

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
    def _hydrate_person_external_ids(
            _file_tag: str, row: Dict[str, str], extracted_objects: List[IngestObject], _cache: IngestObjectCache):
        for obj in extracted_objects:
            if isinstance(obj, StatePerson):
                control_numbers = row['control_numbers'].split(',') if row['control_numbers'] else []
                state_ids = row['state_ids'].split(',') if row['state_ids'] else []
                parole_numbers = row['parole_numbers'].split(',') if row['parole_numbers'] else []

                external_ids_to_create = []
                for state_id in state_ids:
                    external_ids_to_create.append(StatePersonExternalId(state_person_external_id_id=state_id,
                                                                        id_type=US_PA_SID))
                for control_number in control_numbers:
                    external_ids_to_create.append(StatePersonExternalId(state_person_external_id_id=control_number,
                                                                        id_type=US_PA_CONTROL))
                for parole_number in parole_numbers:
                    external_ids_to_create.append(StatePersonExternalId(state_person_external_id_id=parole_number,
                                                                        id_type=US_PA_PBPP))
                for id_to_create in external_ids_to_create:
                    create_if_not_exists(id_to_create, obj, 'state_person_external_ids')

    @staticmethod
    def _hydrate_sentence_group_ids(
            _file_tag: str, row: Dict[str, str], extracted_objects: List[IngestObject], _cache: IngestObjectCache):
        for obj in extracted_objects:
            if isinstance(obj, StatePerson):
                inmate_numbers = row['inmate_numbers'].split(',') if row['inmate_numbers'] else []

                sentence_groups_to_create = []
                for inmate_number in inmate_numbers:
                    sentence_groups_to_create.append(StateSentenceGroup(state_sentence_group_id=inmate_number))

                for sg_to_create in sentence_groups_to_create:
                    create_if_not_exists(sg_to_create, obj, 'state_sentence_groups')

    @staticmethod
    def _compose_current_address(
            _file_tag: str, row: Dict[str, str], extracted_objects: List[IngestObject], _cache: IngestObjectCache):
        """Composes all of the address-related fields into a single address."""
        line_1 = row['legal_address_1']
        line_2 = row['legal_address_2']
        city = row['legal_city']
        state = row['legal_state']
        zip_code = row['legal_zip_code']
        state_and_zip = f"{state} {zip_code}" if zip_code else state

        address = ', '.join(filter(None, (line_1, line_2, city, state_and_zip)))

        for obj in extracted_objects:
            if isinstance(obj, StatePerson):
                obj.current_address = address

    ASSESSMENT_CLASSES: Dict[str, StateAssessmentClass] = {
        'CSS-M': StateAssessmentClass.SOCIAL,
        'HIQ': StateAssessmentClass.SOCIAL,
        'LSI-R': StateAssessmentClass.RISK,
        'RST': StateAssessmentClass.RISK,
        'ST99': StateAssessmentClass.SEX_OFFENSE,
        'TCU': StateAssessmentClass.SUBSTANCE_ABUSE,
    }

    @staticmethod
    def _generate_doc_assessment_external_id(_file_tag: str,
                                             row: Dict[str, str],
                                             extracted_objects: List[IngestObject],
                                             _cache: IngestObjectCache):
        """Adds the assessment external_id to the extracted state assessment."""
        control_number = row['Control_Number']
        test_id = row['Test_Id']
        version_number = row['AsmtVer_Num']
        external_id = '-'.join([control_number, test_id, version_number])

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateAssessment):
                extracted_object.state_assessment_id = external_id

    def _enrich_doc_assessments(self,
                                _file_tag: str,
                                row: Dict[str, str],
                                extracted_objects: List[IngestObject],
                                _cache: IngestObjectCache):
        """Enriches the assessment object with additional metadata."""

        def _rst_metadata() -> Optional[Dict]:
            version_flag = row.get('RSTRvsd_Flg', None)
            if version_flag:
                return {'latest_version': version_flag in ['1', '-1']}
            return None

        for obj in extracted_objects:
            if isinstance(obj, StateAssessment):
                assessment_type = obj.assessment_type.strip() if obj.assessment_type else ''
                assessment_class = self.ASSESSMENT_CLASSES.get(assessment_type, None)
                if assessment_class:
                    obj.assessment_class = assessment_class.value

                if assessment_type == 'RST':
                    rst_metadata = _rst_metadata()
                    if rst_metadata:
                        obj.assessment_metadata = json.dumps(rst_metadata)

    @staticmethod
    def _generate_pbpp_assessment_external_id(_file_tag: str,
                                              row: Dict[str, str],
                                              extracted_objects: List[IngestObject],
                                              _cache: IngestObjectCache):
        """Adds the assessment external_id to the extracted state assessment."""
        parole_number = row['ParoleNumber']
        parole_count_id = row['ParoleCountID']
        lsir_instance = row['LsirID']
        external_id = '-'.join([parole_number, parole_count_id, lsir_instance])

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateAssessment):
                extracted_object.state_assessment_id = external_id

    @staticmethod
    def _enrich_pbpp_assessments(_file_tag: str,
                                 _row: Dict[str, str],
                                 extracted_objects: List[IngestObject],
                                 _cache: IngestObjectCache):
        """Enriches the assessment object with additional metadata."""
        for obj in extracted_objects:
            if isinstance(obj, StateAssessment):
                obj.assessment_type = StateAssessmentType.LSIR.value
                obj.assessment_class = StateAssessmentClass.RISK.value

    @staticmethod
    def _set_incarceration_sentence_id(_file_tag: str,
                                       row: Dict[str, str],
                                       extracted_objects: List[IngestObject],
                                       _cache: IngestObjectCache):
        sentence_group_id = row['curr_inmate_num']
        sentence_number = row['type_number']
        sentence_id = f"{sentence_group_id}-{sentence_number}"

        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationSentence):
                obj.state_incarceration_sentence_id = sentence_id

    @staticmethod
    def _enrich_incarceration_sentence(_file_tag: str,
                                       row: Dict[str, str],
                                       extracted_objects: List[IngestObject],
                                       _cache: IngestObjectCache):
        """Enriches incarceration sentences by setting sentence length and boolean fields."""
        max_years = row.get('max_cort_sent_yrs', '0')
        max_months = row.get('max_cort_sent_mths', '0')
        max_days = row.get('max_cort_sent_days', '0')
        min_years = row.get('min_cort_sent_yrs', '0')
        min_months = row.get('min_cort_sent_mths', '0')
        min_days = row.get('min_cort_sent_days', '0')

        sentence_class = row.get('class_of_sent', '')
        is_life = sentence_class in ('CL', 'LF')
        is_capital_punishment = sentence_class in ('EX', 'EP')

        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationSentence):
                start_date = obj.start_date
                max_time = parse_days_from_duration_pieces(
                    years_str=max_years, months_str=max_months, days_str=max_days, start_dt_str=start_date)
                min_time = parse_days_from_duration_pieces(
                    years_str=min_years, months_str=min_months, days_str=min_days, start_dt_str=start_date)

                if max_time:
                    obj.max_length = str(max_time)
                if min_time:
                    obj.min_length = str(min_time)

                obj.is_life = str(is_life)
                obj.is_capital_punishment = str(is_capital_punishment)

    # TODO(3020): When PA is switched to use SQL pre-processing, this will no longer be necessary
    @staticmethod
    def _strip_id_whitespace(_file_tag: str,
                             _row: Dict[str, str],
                             extracted_objects: List[IngestObject],
                             _cache: IngestObjectCache):
        """Strips id fields provided as strings with inconsistent whitespace padding to avoid id matching issues."""
        for obj in extracted_objects:
            if isinstance(obj, StateCharge):
                if obj.state_charge_id:
                    obj.state_charge_id = obj.state_charge_id.strip()
