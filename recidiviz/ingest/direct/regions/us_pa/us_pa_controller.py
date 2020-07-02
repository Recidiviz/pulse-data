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
from recidiviz.common.constants.state.state_incarceration_incident import StateIncarcerationIncidentOutcomeType, \
    StateIncarcerationIncidentType
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, StateSpecializedPurposeForIncarceration
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.common.str_field_utils import parse_days_from_duration_pieces
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller import CsvGcsfsDirectIngestController
from recidiviz.ingest.direct.direct_ingest_controller_utils import update_overrides_from_maps, create_if_not_exists
from recidiviz.ingest.direct.regions.us_pa.us_pa_enum_helpers import incarceration_period_release_reason_mapper, \
    concatenate_incarceration_period_end_codes, incarceration_period_purpose_mapper, \
    concatenate_incarceration_period_purpose_codes
from recidiviz.ingest.direct.state_shared_row_posthooks import copy_name_to_alias, gen_label_single_external_id_hook, \
    gen_rationalize_race_and_ethnicity, gen_set_agent_type, gen_convert_person_ids_to_external_id_objects
from recidiviz.ingest.extractor.csv_data_extractor import IngestFieldCoordinates
from recidiviz.ingest.models.ingest_info import IngestObject, StatePerson, StatePersonExternalId, StateAssessment, \
    StateIncarcerationSentence, StateCharge, StateSentenceGroup, StateIncarcerationPeriod, StateIncarcerationIncident, \
    StateIncarcerationIncidentOutcome
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache
from recidiviz.utils import environment


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
            'incarceration_period': [
                gen_label_single_external_id_hook(US_PA_CONTROL),
                self._concatenate_release_reason_codes,
                self._concatenate_incarceration_purpose_codes,
            ],
            'dbo_Miscon': [
                gen_label_single_external_id_hook(US_PA_CONTROL),
                self._specify_incident_location,
                self._specify_incident_type,
                self._specify_incident_details,
                self._specify_incident_outcome,
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
            'incarceration_period': [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type),
            ],
            'dbo_Miscon': [
                gen_convert_person_ids_to_external_id_objects(self._get_id_type),
            ],
            'dbo_Offender': [],
            'dbo_LSIR': [],
        }

        self.primary_key_override_hook_by_file: Dict[str, Callable] = {
            'incarceration_period': _generate_incarceration_period_primary_key,
        }

        self.ancestor_chain_overrides_callback_by_file: Dict[str, Callable] = {
            'incarceration_period': _state_incarceration_period_ancestor_chain_overrides,
        }

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
            'DC',  # Diag/Class (Diagnostics / Classification)
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

        StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN: [
            'AOTH',  # Other - Use Sparingly
        ],
        StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION: [
            'AA',    # Administrative
            'AB',    # Bail
            'AC',    # Court Commitment
            'ADET',  # Detentioner
            'AFED',  # Federal Commitment
        ],
        StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION: [
            'AOPV',  # Out Of State Probation/Parole Violator
            'APV',   # Parole Violator
        ],
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE: [
            'AE',  # Escape
        ],
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION: [
            'APD',  # Parole Detainee
        ],
        StateIncarcerationPeriodAdmissionReason.TRANSFER: [
            'ACT',  # County Transfer
            'AIT',  # In Transit
            'ASH',  # State Hospital
            'ATT',  # [Unlisted Transfer]
            'AW',   # WRIT/ATA (Writ of Habeas Corpus Ad Prosequendum)
            'PLC',  # Permanent Location Change
            'RTT',  # Return Temporary Transfer
            'STT',  # Send Temporary Transfer
            'TFM',  # From Medical Facility
            'TRN',  # To Other Institution Or CCC
            'TTM',  # To Medical Facility
            'XPT',  # Transfer Point
            # In this context, SC is being used as a transfer from one type of
            # incarceration to another, either between facilities or within the same facility
            'SC',  # Status Change
        ],

        StateIncarcerationIncidentOutcomeType.CELL_CONFINEMENT: [
            'C',  # Cell Confinement
        ],
        StateIncarcerationIncidentOutcomeType.RESTRICTED_CONFINEMENT: [
            'Y',  # Restricted Confinement
        ],
    }

    ENUM_MAPPERS: Dict[EntityEnumMeta, EnumMapper] = {
        StateIncarcerationPeriodReleaseReason: incarceration_period_release_reason_mapper,
        StateSpecializedPurposeForIncarceration: incarceration_period_purpose_mapper,
    }

    ENUM_IGNORES: Dict[EntityEnumMeta, List[str]] = {}
    ENUM_IGNORE_PREDICATES: Dict[EntityEnumMeta, EnumIgnorePredicate] = {}

    @classmethod
    def get_file_tag_rank_list(cls) -> List[str]:
        launched_file_tags = [
            # Data source: Mixed
            'person_external_ids',

            # Data source: DOC
            'doc_person_info',
            'dbo_tblInmTestScore',
        ]

        # TODO(3024): Move these tags to the list above as each one is ready to run in stage
        unlaunched_file_tags = [
            # Data source: DOC
            'dbo_Senrec',
            'incarceration_period',
            'dbo_Miscon',

            # Data source: PBPP
            'dbo_Offender',
            'dbo_LSIR',  # TODO(3024): Ready to launch, just blocked on preceding tags
        ]

        file_tags = launched_file_tags
        if not environment.in_gae():
            file_tags += unlaunched_file_tags

        return file_tags

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

    def _get_primary_key_override_for_file(self, file: str) -> Optional[Callable]:
        return self.primary_key_override_hook_by_file.get(file, None)

    def _get_ancestor_chain_overrides_callback_for_file(self, file: str) -> Optional[Callable]:
        return self.ancestor_chain_overrides_callback_by_file.get(file, None)

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
    def _get_id_type(file_tag: str) -> Optional[str]:
        if file_tag in ['incarceration_period', 'dbo_Miscon']:
            return US_PA_CONTROL
        return None

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
        inmate_number = row['Inmate_number']
        test_id = row['Test_Id']
        version_number = row['AsmtVer_Num']
        external_id = '-'.join([control_number, inmate_number, test_id, version_number])

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

    # TODO(3020): When PA is switched to use SQL pre-processing, this will no longer be necessary.
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

    @staticmethod
    def _concatenate_release_reason_codes(_file_tag: str,
                                          row: Dict[str, str],
                                          extracted_objects: List[IngestObject],
                                          _cache: IngestObjectCache):
        """Concatenates the incarceration period release reason-related codes to be parsed in the enum mapper."""
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                obj.release_reason = concatenate_incarceration_period_end_codes(row)

    @staticmethod
    def _concatenate_incarceration_purpose_codes(_file_tag: str,
                                                 row: Dict[str, str],
                                                 extracted_objects: List[IngestObject],
                                                 _cache: IngestObjectCache):
        """Concatenates the incarceration period specialized purpose-related codes to be parsed in the enum mapper."""
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                obj.specialized_purpose_for_incarceration = concatenate_incarceration_period_purpose_codes(row)

    @staticmethod
    def _specify_incident_location(_file_tag: str,
                                   row: Dict[str, str],
                                   extracted_objects: List[IngestObject],
                                   _cache: IngestObjectCache):
        """Specifies the exact location where the incarceration incident took place."""
        place_code = row.get('place_hvl_code', None)
        place_extended = row.get('place_extended', None)
        location = '-'.join(filter(None, [place_code, place_extended]))

        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationIncident):
                if location:
                    obj.location_within_facility = location

    @staticmethod
    def _specify_incident_type(_file_tag: str,
                               row: Dict[str, str],
                               extracted_objects: List[IngestObject],
                               _cache: IngestObjectCache):
        """Specifies the type of incarceration incident."""
        drug_related = row.get('drug_related', None)
        is_contraband = drug_related == 'Y'

        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationIncident):
                if is_contraband:
                    obj.incident_type = StateIncarcerationIncidentType.CONTRABAND.value
                else:
                    obj.incident_type = StateIncarcerationIncidentType.REPORT.value

    @staticmethod
    def _specify_incident_details(_file_tag: str,
                                  row: Dict[str, str],
                                  extracted_objects: List[IngestObject],
                                  _cache: IngestObjectCache):
        """Specifies the incarceration incident details. This is a grouping of flags indicating whether certain
        "classes" of charges are involved in the incident."""
        category_1 = row.get('ctgory_of_chrgs_1', None)
        category_2 = row.get('ctgory_of_chrgs_2', None)
        category_3 = row.get('ctgory_of_chrgs_3', None)
        category_4 = row.get('ctgory_of_chrgs_4', None)
        category_5 = row.get('ctgory_of_chrgs_5', None)

        details_mapping = {
            'category_1': category_1,
            'category_2': category_2,
            'category_3': category_3,
            'category_4': category_4,
            'category_5': category_5,
        }

        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationIncident):
                obj.incident_details = json.dumps(details_mapping)

    @staticmethod
    def _specify_incident_outcome(_file_tag: str,
                                  row: Dict[str, str],
                                  extracted_objects: List[IngestObject],
                                  _cache: IngestObjectCache):
        """Specifies the type of outcome of the incarceration incident."""
        misconduct_number = row.get('misconduct_number', None)
        confinement_code = row.get('confinement', None)
        confinement_date = row.get('confinement_date', None)
        is_restricted = confinement_code == 'Y'
        is_cell = confinement_code == 'C'

        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationIncidentOutcome):
                if misconduct_number:
                    obj.state_incarceration_incident_outcome_id = misconduct_number

                if is_restricted or is_cell:
                    obj.outcome_type = confinement_code
                    obj.date_effective = confinement_date


def _generate_incarceration_period_primary_key(_file_tag: str, row: Dict[str, str]) -> IngestFieldCoordinates:
    person_id = row['control_number']
    sentence_group_id = row['inmate_number']
    sequence_number = row['sequence_number']
    incarceration_period_id = f"{person_id}-{sentence_group_id}-{sequence_number}"

    return IngestFieldCoordinates('state_incarceration_period',
                                  'state_incarceration_period_id',
                                  incarceration_period_id)


def _state_incarceration_period_ancestor_chain_overrides(_file_tag: str, row: Dict[str, str]) -> Dict[str, str]:
    """This creates an incarceration sentence id for specifying the ancestor of a supervision period.

    Incarceration periods only have explicit links to sentence groups. However, we know that the vast majority of
    sentence groups in PA have a single sentence with a type number of 01, and the rest have 2 sentences with type
    numbers of 01 and 02. The fields for sentences 01 and 02 are highly similar and usually differ only as it
    relates to charge information. Thus, tying each incarceration period to sentence 01 in a given group appears
    to be safe.
    """
    sentence_group_id = row['inmate_number']
    assumed_type_number = '01'
    incarceration_sentence_id = f"{sentence_group_id}-{assumed_type_number}"

    return {'state_incarceration_sentence': incarceration_sentence_id}
