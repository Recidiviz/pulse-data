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
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import StateAssessmentType, StateAssessmentLevel
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, StateSpecializedPurposeForIncarceration
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodAdmissionReason, \
    StateSupervisionPeriodTerminationReason
from recidiviz.common.constants.state.state_supervision_violation import StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response import \
    StateSupervisionViolationResponseDecision, StateSupervisionViolationResponseType
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.common.str_field_utils import parse_days_from_duration_pieces, sorted_list_from_str
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller import CsvGcsfsDirectIngestController
from recidiviz.ingest.direct.direct_ingest_controller_utils import update_overrides_from_maps, create_if_not_exists
from recidiviz.ingest.direct.regions.us_id.us_id_constants import INTERSTATE_FACILITY_CODE, FUGITIVE_FACILITY_CODE, \
    JAIL_FACILITY_CODES, VIOLATION_REPORT_NO_RECOMMENDATION_VALUES, ALL_NEW_CRIME_TYPES, VIOLENT_CRIME_TYPES, \
    SEX_CRIME_TYPES, MAX_DATE_STR
from recidiviz.ingest.direct.regions.us_id.us_id_enum_helpers import incarceration_admission_reason_mapper, \
    incarceration_release_reason_mapper, supervision_admission_reason_mapper, supervision_termination_reason_mapper
from recidiviz.ingest.direct.state_shared_row_posthooks import copy_name_to_alias, gen_label_single_external_id_hook, \
    gen_rationalize_race_and_ethnicity, gen_convert_person_ids_to_external_id_objects
from recidiviz.ingest.models.ingest_info import IngestObject, StateAssessment, StateIncarcerationSentence, \
    StateCharge, StateAgent, StateCourtCase, StateSentenceGroup, StateSupervisionSentence, StateIncarcerationPeriod, \
    StateSupervisionPeriod, StateSupervisionViolation, StateSupervisionViolationResponse, \
    StateSupervisionViolationResponseDecisionEntry, StateSupervisionViolationTypeEntry, StatePerson
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache

from recidiviz.utils.params import str_to_bool


# TODO(2999): Consider adding completion date to sentences even if the sentence is completed and we have a
#  `projected_completion_date` that has passed.
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
            'offender_ofndr_dob': [
                copy_name_to_alias,
                # When first parsed, the info object just has a single external id - the DOC id.
                gen_label_single_external_id_hook(US_ID_DOC),
                gen_rationalize_race_and_ethnicity(self.ENUM_OVERRIDES),
            ],
            'ofndr_tst_ofndr_tst_cert': [
                self._add_lsir_to_assessments,
                gen_label_single_external_id_hook(US_ID_DOC),
            ],
            'mittimus_judge_sentence_offense_sentprob_incarceration_sentences': [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._add_statute_to_charge,
                self._add_judge_to_court_cases,
                self._set_extra_sentence_fields,
                self._set_generated_ids,
            ],
            'mittimus_judge_sentence_offense_sentprob_supervision_sentences': [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._add_statute_to_charge,
                self._add_judge_to_court_cases,
                self._set_extra_sentence_fields,
                self._set_generated_ids,
            ],
            'movement_facility_offstat_incarceration_periods': [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._set_generated_ids,
                self._clear_max_dates,
                self._add_rider_treatment,
                self._add_default_admission_reason,
                self._add_incarceration_type,
            ],
            'movement_facility_supervision_periods': [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._set_generated_ids,
                self._clear_max_dates,
                self._add_default_admission_reason,
                self._update_interstate_and_absconsion_periods,
            ],
            'ofndr_tst_tst_qstn_rspns_violation_reports': [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._set_generated_ids,
                self._set_violation_violent_sex_offense,
                self._hydrate_violation_types,
                self._hydrate_violation_report_fields,
            ],
            'ofndr_tst_tst_qstn_rspns_violation_reports_old': [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._set_generated_ids,
                self._set_violation_violent_sex_offense,
                self._hydrate_violation_types,
                self._hydrate_violation_report_fields,
            ],
            'ofndr_agnt_applc_usr_body_loc_cd_current_pos': [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._add_supervising_officer,
            ]
        }
        self.file_post_processors_by_file: Dict[str, List[Callable]] = {
            'offender_ofndr_dob': [],
            'ofndr_tst_ofndr_tst_cert': [],
            'mittimus_judge_sentence_offense_sentprob_incarceration_sentences': [],
            'mittimus_judge_sentence_offense_sentprob_supervision_sentences': [],
            'movement_facility_offstat_incarceration_periods': [],
            'movement_facility_supervision_periods': [],
            'ofndr_tst_tst_qstn_rspns_violation_reports': [],
            'ofndr_tst_tst_qstn_rspns_violation_reports_old': [],
            'ofndr_agnt_applc_usr_body_loc_cd_current_pos': [
                # TODO(1883): Would not need this file postprocessor if our data extractor would add the main entity to
                #  our 'extracted_objects' cache when a single id field is used in the following situation:
                #   - the only key in "keys"
                #   - reused as a child key
                gen_convert_person_ids_to_external_id_objects(self._get_id_type),
            ],
        }

    FILE_TAGS = [
        'offender_ofndr_dob',
        'ofndr_agnt_applc_usr_body_loc_cd_current_pos',
        'ofndr_tst_ofndr_tst_cert',
        'mittimus_judge_sentence_offense_sentprob_incarceration_sentences',
        'mittimus_judge_sentence_offense_sentprob_supervision_sentences',
        'movement_facility_offstat_incarceration_periods',
        'movement_facility_supervision_periods',
        'ofndr_tst_tst_qstn_rspns_violation_reports',
        'ofndr_tst_tst_qstn_rspns_violation_reports_old',
    ]

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

        StateAssessmentLevel.LOW: ['Minimum'],
        StateAssessmentLevel.LOW_MEDIUM: ['Low-Medium'],
        StateAssessmentLevel.MEDIUM_HIGH: ['High-Medium'],
        StateAssessmentLevel.HIGH: ['Maximum'],

        StateSentenceStatus.COMMUTED: [
            'M',  # Commuted
        ],
        # TODO(2999): Consider breaking out these sentence status enums in our schema (
        #  vacated, sealed, early discharge, expired, etc)
        StateSentenceStatus.COMPLETED: [
            'C',  # Completed
            'D',  # Discharged
            'E',  # Expired
            'F',  # Parole Early Discharge
            'G',  # Dismissed
            'H',  # Post conviction relief.
            'L',  # Sealed
            'Q',  # Vacated conviction
            'S',  # Satisfied
            'V',  # Vacated Sentence
            'X',  # Rule 35 - Reduction of illegal or overly harsh sentence.
            'Z',  # Reduced to misdemeanor TODO(2999): When is this used?
        ],
        StateSentenceStatus.REVOKED: [
            'K',  # Revoked
        ],
        StateSentenceStatus.SERVING: [
            'I',  # Imposed
            'J',  # RJ To Court - Used for probation after treatment
            'N',  # Interstate Parole
            'O',  # Correctional Compact - TODO(2999): Get more info from ID.
            'P',  # Bond Appeal - unused, but present in ID status table.
            'R',  # Court Retains Jurisdiction - used when a person on a rider. TODO(2999): Whats the difference
            # between this and 'W'?
            'T',  # Interstate probation - unused, but present in ID status table.
            'U',  # Unsupervised - probation
            'W',  # Witheld judgement - used when a person is on a rider.
            'Y',  # Drug Court - TODO(2999): Consider adding this as a court type.
        ],
        StateSentenceStatus.SUSPENDED: [
            'B',  # Suspended sentence - probation
        ],

        StateSupervisionViolationType.ABSCONDED: [
            'Absconding',   # From violation report 210
            'Absconder',    # From violation report 204
        ],
        StateSupervisionViolationType.FELONY: [
            'New Felony',   # From violation report 210/204
        ],
        StateSupervisionViolationType.MISDEMEANOR: [
            'New Misdemeanor',  # From violation report 210/204
        ],
        StateSupervisionViolationType.TECHNICAL: [
            'Technical (enter details below)',  # From violation report 210
            'Technical',                        # From violation report 204
        ],

        # TODO(2999): Go through values with ID to ensure we have the correct mappings
        StateSupervisionViolationResponseDecision.CONTINUANCE: [
            'Reinstatement',    # Parole/probation recommendation from violation report 210
            # TODO(2999): Is there a better enum for this (recommendation that max release date is used instead of min)?
            'Recommended Full Term Release Date',  # Parole recommendation from violation report 204
        ],
        StateSupervisionViolationResponseDecision.SPECIALIZED_COURT: [
            'Diversion - Problem Solving Court',    # Parole recommendation from violation report 210
            'Treatment Court',                      # Probation recommendation from violation report 210
            'Referral to Problem Solving Court',    # Probation recommendation from violation report 204
        ],
        StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION: [
            # TODO(2999): Are these 'Diversion' recs shock incarceration or treatment in prison?
            'Diversion - Jail',     # Parole recommendation from violation report 210
            'Diversion - CRC',      # Parole recommendation from violation report 210
            'Diversion - Prison',   # Parole recommendation from violation report 210
            'Local Jail Time',      # Probation recommendation from violation report 210/204
        ],
        StateSupervisionViolationResponseDecision.TREATMENT_IN_PRISON: [
            'Rider',                            # Probation recommendation from violation report 210
            'Rider Recommendation'              # Probation recommendation from violation report 204
            # TODO(2999): is this shock incarceration or treatment in prison?
            'PVC - Parole Violator Program'     # Parole recommendation from violation report 204
        ],
        StateSupervisionViolationResponseDecision.REVOCATION: [
            'Revocation',               # Parole recommendation from violation report 210
            'Imposition of Sentence',   # Probation recommendation from violation report 210/204
        ],

    }
    ENUM_IGNORES: Dict[EntityEnumMeta, List[str]] = {}
    ENUM_MAPPERS: Dict[EntityEnumMeta, EnumMapper] = {
        StateIncarcerationPeriodAdmissionReason: incarceration_admission_reason_mapper,
        StateIncarcerationPeriodReleaseReason: incarceration_release_reason_mapper,
        StateSupervisionPeriodAdmissionReason: supervision_admission_reason_mapper,
        StateSupervisionPeriodTerminationReason: supervision_termination_reason_mapper,
    }

    ENUM_IGNORE_PREDICATES: Dict[EntityEnumMeta, EnumIgnorePredicate] = {}

    def _get_file_tag_rank_list(self) -> List[str]:
        return self.FILE_TAGS

    def generate_enum_overrides(self) -> EnumOverrides:
        """Provides Idaho-specific overrides for enum mappings."""
        base_overrides = super(UsIdController, self).get_enum_overrides()
        return update_overrides_from_maps(
            base_overrides, self.ENUM_OVERRIDES, self.ENUM_IGNORES, self.ENUM_MAPPERS, self.ENUM_IGNORE_PREDICATES)

    def get_enum_overrides(self) -> EnumOverrides:
        return self.enum_overrides

    def _get_file_post_processors_for_file(self, file_tag: str) -> List[Callable]:
        return self.file_post_processors_by_file.get(file_tag, [])

    def _get_row_post_processors_for_file(self, file_tag: str) -> List[Callable]:
        return self.row_post_processors_by_file.get(file_tag, [])

    @staticmethod
    def _get_id_type(file_tag: str) -> Optional[str]:
        if file_tag in [
                'offender_ofndr_dob',
                'ofndr_agnt_applc_usr_body_loc_cd_current_pos',
                'ofndr_tst_ofndr_tst_cert',
                'mittimus_judge_sentence_offense_sentprob_incarceration_sentences',
                'mittimus_judge_sentence_offense_sentprob_supervision_sentences',
                'movement_facility_offstat_incarceration_periods',
                'movement_facility_supervision_periods',
                'ofndr_tst_tst_qstn_rspns_violation_reports',
                'ofndr_tst_tst_qstn_rspns_violation_reports_old',
        ]:
            return US_ID_DOC

        return None

    @staticmethod
    def _add_lsir_to_assessments(
            _file_tag: str,
            _row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        for obj in extracted_objects:
            if isinstance(obj, StateAssessment):
                obj.assessment_type = StateAssessmentType.LSIR.value

    @staticmethod
    def _add_statute_to_charge(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        statute_title = row.get('off_stat_title', '')
        statute_section = row.get('off_stat_sect', '')

        if not statute_title or not statute_section:
            return
        statute = f'{statute_title}-{statute_section}'

        for obj in extracted_objects:
            if isinstance(obj, StateCharge):
                obj.statute = statute

    @staticmethod
    def _add_judge_to_court_cases(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        judge_id = row.get('judge_cd', '')
        judge_name = row.get('judge_name', '')

        if not judge_id or not judge_name:
            return

        judge_to_create = StateAgent(
            state_agent_id=judge_id,
            full_name=judge_name,
            agent_type=StateAgentType.JUDGE.value)

        for obj in extracted_objects:
            if isinstance(obj, StateCourtCase):
                create_if_not_exists(judge_to_create, obj, 'judge')

    @staticmethod
    def _set_extra_sentence_fields(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        is_life = bool(row.get('lifer'))
        max_years = row.get('sent_max_yr')
        max_months = row.get('sent_max_mo')
        max_days = row.get('sent_max_da')
        min_years = row.get('sent_min_yr')
        min_months = row.get('sent_min_mo')
        min_days = row.get('sent_min_da')

        for obj in extracted_objects:
            if isinstance(obj, (StateIncarcerationSentence, StateSupervisionSentence)):
                start_date = obj.start_date
                max_time = parse_days_from_duration_pieces(
                    years_str=max_years, months_str=max_months, days_str=max_days, start_dt_str=start_date)
                min_time = parse_days_from_duration_pieces(
                    years_str=min_years, months_str=min_months, days_str=min_days, start_dt_str=start_date)

                if max_time:
                    obj.max_length = str(max_time)
                if min_time:
                    obj.min_length = str(min_time)
            if isinstance(obj, StateIncarcerationSentence):
                obj.is_life = str(is_life)
            if isinstance(obj, StateSupervisionSentence):
                obj.supervision_type = StateSupervisionType.PROBATION.value

    @staticmethod
    def _set_generated_ids(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """Sets all recidiviz-created ids on ingested objects. These are combinations of existing fields so that each
        external id is unique among all entities in US_ID.
        """
        person_id = row.get('docno', '')
        sentence_group_id = row.get('incrno', '')
        sentence_id = row.get('sent_no', '')
        court_case_id = row.get('caseno', '')
        period_id = row.get('period_id', '')
        violation_id = row.get('ofndr_tst_id', '')

        for obj in extracted_objects:
            if isinstance(obj, StateSentenceGroup):
                obj.state_sentence_group_id = f'{person_id}-{sentence_group_id}'

            if isinstance(obj, StateIncarcerationSentence):
                obj.state_incarceration_sentence_id = f'{person_id}-{sentence_id}'
            if isinstance(obj, StateSupervisionSentence):
                obj.state_supervision_sentence_id = f'{person_id}-{sentence_id}'

            if isinstance(obj, StateIncarcerationPeriod):
                obj.state_incarceration_period_id = f'{person_id}-{period_id}'
            if isinstance(obj, StateSupervisionPeriod):
                obj.state_supervision_period_id = f'{person_id}-{period_id}'

            # Only one charge per sentence so recycle sentence id for the charge.
            if isinstance(obj, StateCharge):
                obj.state_charge_id = f'{person_id}-{sentence_id}'

            if isinstance(obj, StateCourtCase):
                obj.state_court_case_id = f'{person_id}-{court_case_id}'

            if isinstance(obj, StateSupervisionViolation):
                obj.state_supervision_violation_id = f'{violation_id}'
            # One response per violation, so recycle violation id for the response.
            if isinstance(obj, StateSupervisionViolationResponse):
                obj.state_supervision_violation_response_id = f'{violation_id}'

    @staticmethod
    def _add_rider_treatment(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """Adds a specialized_purpose_for_incarceration to all incarceration periods that represent someone's time on a
        rider (treatment with possibility of probation afterwards).
        """
        rider = str_to_bool(row.get('rider', ''))
        if not rider:
            return

        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                obj.specialized_purpose_for_incarceration = \
                    StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON.value

    @staticmethod
    def _add_default_admission_reason(
            _file_tag: str,
            _row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """Adds a default admission reason to supervision/incarceration periods."""
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                if obj.admission_reason is None:
                    obj.admission_reason = StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value
            if isinstance(obj, StateSupervisionPeriod):
                if obj.admission_reason is None:
                    obj.admission_reason = StateSupervisionPeriodAdmissionReason.COURT_SENTENCE.value

    @staticmethod
    def _add_incarceration_type(
            _file_tag: str,
            _row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """Sets incarceration type on incarceration periods based on facility."""
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                # TODO(2999): Validate with ID that these are the only county jail facilities.
                if obj.facility in JAIL_FACILITY_CODES:
                    obj.incarceration_type = StateIncarcerationType.COUNTY_JAIL.value
                else:
                    obj.incarceration_type = StateIncarcerationType.STATE_PRISON.value

    @staticmethod
    def _clear_max_dates(
            _file_tag: str,
            _row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """Clears recidiviz-generated (from queries) maximum date fields which really signify that a period is
        currently unended.
        """
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                if obj.release_date == MAX_DATE_STR:
                    obj.release_date = None
            if isinstance(obj, StateSupervisionPeriod):
                if obj.termination_date == MAX_DATE_STR:
                    obj.termination_date = None

    @staticmethod
    def _update_interstate_and_absconsion_periods(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """Updates interstate and absconsion periods manually, as in US_ID both interstate and absconsion are treated
        as facilities.
        """
        prev_fac_cd = row.get('prev_fac_cd', '')
        next_fac_cd = row.get('next_fac_cd', '')
        cur_fac_cd = row.get('fac_cd', '')
        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):

                # Handle transfers to and from interstate
                if prev_fac_cd == INTERSTATE_FACILITY_CODE:
                    obj.admission_reason = INTERSTATE_FACILITY_CODE
                if next_fac_cd == INTERSTATE_FACILITY_CODE:
                    obj.termination_reason = INTERSTATE_FACILITY_CODE

                # If we're currently in an interstate period, set admission/release reason accordingly and clear
                # supervision site, as we don't have info on where exactly supervision is taking place.
                if cur_fac_cd == INTERSTATE_FACILITY_CODE:
                    obj.supervision_site = None
                    obj.admission_reason = INTERSTATE_FACILITY_CODE
                    if obj.termination_date:
                        obj.termination_reason = INTERSTATE_FACILITY_CODE

                # Handle absconsion periods.
                if cur_fac_cd == FUGITIVE_FACILITY_CODE:
                    obj.supervision_site = None
                    obj.admission_reason = StateSupervisionPeriodAdmissionReason.ABSCONSION.value
                    if obj.termination_date:
                        obj.termination_reason = StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION.value

    @staticmethod
    def _hydrate_violation_report_fields(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """Adds fields/children to the SupervisionViolationResponses as necessary. This assumes all
        SupervisionViolationResponses are of violation reports.
        """
        recommendations = set(
            sorted_list_from_str(row.get('parolee_placement_recommendation', ''))
            + sorted_list_from_str(row.get('probationer_placement_recommendation', '')))

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolationResponse):
                obj.response_type = StateSupervisionViolationResponseType.VIOLATION_REPORT.value

                for recommendation in recommendations:
                    if recommendation in VIOLATION_REPORT_NO_RECOMMENDATION_VALUES:
                        continue
                    recommendation_to_create = StateSupervisionViolationResponseDecisionEntry(decision=recommendation)
                    create_if_not_exists(
                        recommendation_to_create, obj, 'state_supervision_violation_response_decisions')

    @staticmethod
    def _hydrate_violation_types(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """Adds ViolationTypeEntries onto the already generated SupervisionViolations."""
        violation_types = sorted_list_from_str(row.get('violation_types', ''))

        if not violation_types:
            return

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolation):
                for violation_type in violation_types:
                    violation_type_to_create = StateSupervisionViolationTypeEntry(violation_type=violation_type)
                    create_if_not_exists(violation_type_to_create, obj, 'state_supervision_violation_types')

    @staticmethod
    def _set_violation_violent_sex_offense(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """Sets the fields `is_violent` and `is_sex_offense` onto StateSupervisionViolations based on fields passed in
        through the |row|.
        """
        new_crime_types = sorted_list_from_str(row.get('new_crime_types', ''))

        if not all(ct in ALL_NEW_CRIME_TYPES for ct in new_crime_types):
            raise ValueError(f'Unexpected new crime type: {new_crime_types}')

        violent = any([ct in VIOLENT_CRIME_TYPES for ct in new_crime_types])
        sex_offense = any([ct in SEX_CRIME_TYPES for ct in new_crime_types])

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolation):
                obj.is_violent = str(violent)
                obj.is_sex_offense = str(sex_offense)

    @staticmethod
    def _add_supervising_officer(
            _file_tag: str, row: Dict[str, str], extracted_objects: List[IngestObject], _cache: IngestObjectCache):
        agent_id = row.get('agnt_id', '')
        agent_name = row.get('name', '')
        if not agent_id or not agent_name:
            return

        for obj in extracted_objects:
            if isinstance(obj, StatePerson):
                agent_to_create = StateAgent(
                    state_agent_id=agent_id,
                    full_name=agent_name,
                    agent_type=StateAgentType.SUPERVISION_OFFICER.value)
                create_if_not_exists(agent_to_create, obj, 'supervising_officer')
