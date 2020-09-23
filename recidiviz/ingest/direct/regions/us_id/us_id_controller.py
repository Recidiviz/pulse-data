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
from recidiviz.common.constants.state.shared_enums import StateActingBodyType
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import StateAssessmentType, StateAssessmentLevel
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_early_discharge import StateEarlyDischargeDecision
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, StateSpecializedPurposeForIncarceration
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_contact import StateSupervisionContactLocation, \
    StateSupervisionContactType, StateSupervisionContactReason, StateSupervisionContactStatus
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodAdmissionReason, \
    StateSupervisionPeriodTerminationReason, StateSupervisionPeriodSupervisionType, StateSupervisionLevel
from recidiviz.common.constants.state.state_supervision_violation import StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response import \
    StateSupervisionViolationResponseDecision, StateSupervisionViolationResponseType
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.common.str_field_utils import safe_parse_days_from_duration_pieces, sorted_list_from_str
from recidiviz.ingest.direct.controllers.csv_gcsfs_direct_ingest_controller import CsvGcsfsDirectIngestController
from recidiviz.ingest.direct.direct_ingest_controller_utils import update_overrides_from_maps, create_if_not_exists
from recidiviz.ingest.direct.regions.us_id.us_id_constants import INTERSTATE_FACILITY_CODE, FUGITIVE_FACILITY_CODE, \
    VIOLATION_REPORT_NO_RECOMMENDATION_VALUES, ALL_NEW_CRIME_TYPES, VIOLENT_CRIME_TYPES, \
    SEX_CRIME_TYPES, MAX_DATE_STR, PREVIOUS_FACILITY_CODE, NEXT_FACILITY_CODE, CURRENT_FACILITY_CODE, \
    PAROLE_COMMISSION_CODE, LIMITED_SUPERVISION_LIVING_UNIT, BENCH_WARRANT_LIVING_UNIT, COURT_PROBATION_LIVING_UNIT, \
    LIMITED_SUPERVISION_UNIT_NAME, DISTRICT_0, UNKNOWN, IDOC_CUSTODIAL_AUTHORITY, CURRENT_FACILITY_NAME, \
    CURRENT_LOCATION_NAME, CURRENT_LIVING_UNIT_CODE, CURRENT_LIVING_UNIT_NAME, CURRENT_LOCATION_CODE, \
    CONTACT_TYPES_TO_BECOME_LOCATIONS, CONTACT_RESULT_ARREST, UNKNOWN_EMPLOYEE_SDESC, FEDERAL_CUSTODY_LOCATION_NAME, \
    DEPORTED_LOCATION_NAME, NEXT_LOCATION_NAME, PREVIOUS_LOCATION_NAME, FEDERAL_CUSTODIAL_AUTHORITY, \
    OTHER_COUNTRY_CUSTODIAL_AUTHORITY
from recidiviz.ingest.direct.regions.us_id.us_id_enum_helpers import incarceration_admission_reason_mapper, \
    incarceration_release_reason_mapper, supervision_admission_reason_mapper, supervision_termination_reason_mapper, \
    is_jail_facility, purpose_for_incarceration_mapper, supervision_period_supervision_type_mapper
from recidiviz.ingest.direct.state_shared_row_posthooks import copy_name_to_alias, gen_label_single_external_id_hook, \
    gen_rationalize_race_and_ethnicity, create_supervision_site
from recidiviz.ingest.models.ingest_info import IngestObject, StateAssessment, StateIncarcerationSentence, \
    StateCharge, StateAgent, StateCourtCase, StateSentenceGroup, StateSupervisionSentence, StateIncarcerationPeriod, \
    StateSupervisionPeriod, StateSupervisionViolation, StateSupervisionViolationResponse, \
    StateSupervisionViolationResponseDecisionEntry, StateSupervisionViolationTypeEntry, \
    StateEarlyDischarge, StateSupervisionContact, StateSupervisionCaseTypeEntry
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache

from recidiviz.utils.params import str_to_bool


class UsIdController(CsvGcsfsDirectIngestController):
    """Direct ingest controller implementation for US_ID."""

    def __init__(self,
                 ingest_directory_path: Optional[str] = None,
                 storage_directory_path: Optional[str] = None,
                 max_delay_sec_between_files: Optional[int] = None):
        super().__init__(
            'us_id',
            SystemLevel.STATE,
            ingest_directory_path,
            storage_directory_path,
            max_delay_sec_between_files=max_delay_sec_between_files)
        self.enum_overrides = self.generate_enum_overrides()
        self.row_post_processors_by_file: Dict[str, List[Callable]] = {
            'offender_ofndr_dob_address': [
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
            'early_discharge_incarceration_sentence': [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._set_generated_ids,
            ],
            'early_discharge_supervision_sentence': [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._set_generated_ids,
            ],
            'movement_facility_location_offstat_incarceration_periods': [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._override_facilities,
                self._set_generated_ids,
                self._clear_max_dates,
                self._add_incarceration_type,
                self._add_default_admission_reason,
            ],
            'movement_facility_location_offstat_supervision_periods': [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._set_generated_ids,
                self._clear_max_dates,
                self._supervision_period_admission_and_termination_overrides,
                self._add_default_admission_reason,
                self._set_supervision_site,
                self._set_case_type_from_supervision_level,
                self._add_supervising_officer,
                self._set_custodial_authority,
                self._override_supervision_type,
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
            'sprvsn_cntc': [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._add_supervision_contact_fields,
            ],
        }
        self.file_post_processors_by_file: Dict[str, List[Callable]] = {
            'offender_ofndr_dob_address': [],
            'ofndr_tst_ofndr_tst_cert': [],
            'mittimus_judge_sentence_offense_sentprob_incarceration_sentences': [],
            'mittimus_judge_sentence_offense_sentprob_supervision_sentences': [],
            'early_discharge_incarceration_sentence': [],
            'early_discharge_supervision_sentence': [],
            'movement_facility_location_offstat_incarceration_periods': [],
            'movement_facility_location_offstat_supervision_periods': [],
            'ofndr_tst_tst_qstn_rspns_violation_reports': [],
            'ofndr_tst_tst_qstn_rspns_violation_reports_old': [],
            'sprvsn_cntc': [],
        }

    FILE_TAGS = [
        'offender_ofndr_dob_address',
        'ofndr_tst_ofndr_tst_cert',
        'mittimus_judge_sentence_offense_sentprob_incarceration_sentences',
        'mittimus_judge_sentence_offense_sentprob_supervision_sentences',
        'early_discharge_incarceration_sentence',
        'early_discharge_supervision_sentence',
        'movement_facility_location_offstat_incarceration_periods',
        'movement_facility_location_offstat_supervision_periods',
        'ofndr_tst_tst_qstn_rspns_violation_reports',
        'ofndr_tst_tst_qstn_rspns_violation_reports_old',
        'sprvsn_cntc',
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
        # TODO(#3517): Consider breaking out these sentence status enums in our schema (
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
            'Z',  # Reduced to misdemeanor - person should not be in prison and no longer tracked by IDOC.
        ],
        StateSentenceStatus.REVOKED: [
            'K',  # Revoked
        ],
        StateSentenceStatus.SERVING: [
            'I',  # Imposed
            'J',  # RJ To Court - Used for probation after treatment
            'N',  # Interstate Parole
            'O',  # Correctional Compact - TODO(#3506): Get more info from ID.
            'P',  # Bond Appeal - unused, but present in ID status table.
            'R',  # Court Retains Jurisdiction - used when a person on a rider. TODO(#3506): Whats the difference
            # between this and 'W'?
            'T',  # Interstate probation - unused, but present in ID status table.
            'U',  # Unsupervised - probation
            'W',  # Witheld judgement - used when a person is on a rider.
            'Y',  # Drug Court
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

        # TODO(#3510): Go through values with ID to ensure we have the correct mappings
        StateSupervisionViolationResponseDecision.CONTINUANCE: [
            'Reinstatement',    # Parole/probation recommendation from violation report 210
            # TODO(#3510): Is there a better enum for this (recommendation that max release date is used instead of
            # min)?
            'Recommended Full Term Release Date',  # Parole recommendation from violation report 204
        ],
        StateSupervisionViolationResponseDecision.SPECIALIZED_COURT: [
            'Diversion - Problem Solving Court',    # Parole recommendation from violation report 210
            'Treatment Court',                      # Probation recommendation from violation report 210
            'Referral to Problem Solving Court',    # Probation recommendation from violation report 204
        ],
        StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION: [
            # TODO(#3510): Are these 'Diversion' recs shock incarceration or treatment in prison?
            'Diversion - Jail',     # Parole recommendation from violation report 210
            'Diversion - CRC',      # Parole recommendation from violation report 210
            'Diversion - Prison',   # Parole recommendation from violation report 210
            'Local Jail Time',      # Probation recommendation from violation report 210/204
        ],
        StateSupervisionViolationResponseDecision.TREATMENT_IN_PRISON: [
            'Rider',                            # Probation recommendation from violation report 210
            'Rider Recommendation',             # Probation recommendation from violation report 204
            # TODO(#3510): is this shock incarceration or treatment in prison?
            'PVC - Parole Violator Program',    # Parole recommendation from violation report 204
        ],
        StateSupervisionViolationResponseDecision.REVOCATION: [
            'Revocation',               # Parole recommendation from violation report 210
            'Imposition of Sentence',   # Probation recommendation from violation report 210/204
        ],

        StateEarlyDischargeDecision.SENTENCE_TERMINATION_GRANTED: [
            'Grant Request to Terminate',
        ],
        StateEarlyDischargeDecision.REQUEST_DENIED: [
            'Deny',
            'Deny - Committed Crime(s) While on Probation',
            'Deny - New Charges',
            'Deny - No Violations, but Unsatisfactory',
            'Deny - Other',
            'Deny - Pending Charge(s)',
            'Deny - Programming Needed',
            'Deny - Serious Nature of the Offense',
            'Deny - Unpaid Restitution',
            'Deny - Unpaid Restitution, Court Costs or Fees',
            'Deny - Unsatisfactory Performance to Date',
        ],
        StateEarlyDischargeDecision.UNSUPERVISED_PROBATION_GRANTED: [
            'Grant Unsupervised Probation',
        ],

        StateActingBodyType.COURT: [
            'PROBATION',
        ],
        StateActingBodyType.PAROLE_BOARD: [
            'PAROLE',
        ],
        StateActingBodyType.SUPERVISION_OFFICER: [
            'REQUEST FOR DISCHARGE: PROBATION',
            'SPECIAL PROGRESS REPORT FOR PAROLE COMMUTATION',
        ],
        StateActingBodyType.SENTENCED_PERSON: [
            'SPECIAL PROGRESS REPORT MOTION FOR PROBATION DISCHARGE BY DEFENDANT',
            'SPECIAL PROGRESS REPORT OFFENDER INITIATED PAROLE DISCHARGE REQUEST',
        ],

        StateSupervisionContactLocation.SUPERVISION_OFFICE: [
            'OFFICE',
            'ALTERNATE WORK SITE',
            'INTERSTATE OFFICE',
        ],
        StateSupervisionContactLocation.RESIDENCE: [
            'RESIDENCE',
            'OTHER RESIDENCE',
        ],
        StateSupervisionContactLocation.COURT: [
            'COURT',
            'DRUG COURT',
        ],
        StateSupervisionContactLocation.TREATMENT_PROVIDER: [
            'TREATMENT PROVIDER',
        ],
        StateSupervisionContactLocation.JAIL: [
            'JAIL',
        ],
        StateSupervisionContactLocation.FIELD: [
            'FIELD',
        ],
        StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT: [
            'EMPLOYER',
        ],
        StateSupervisionContactLocation.INTERNAL_UNKNOWN: [
            'LAW ENFORCEMENT AGENCY',   # TODO(#3511): Consider adding as enum
            'COMPACT STATE',            # TODO(#3511): Consider adding as enum
            'PAROLE COMMISSION',        # TODO(#3511): Consider adding as enum
            'WBOR',                     # Data entry error - WBOR is an online form filled out that isn't a location.

            # No longer used
            'OTHER',
            'COMMUNITY SERVICE SITE',
            'FAMILY',
            'ASSOCIATE',
            'CRIME SCENE',
            'CONVERSION',
        ],

        StateSupervisionContactType.FACE_TO_FACE: [
            'FACE TO FACE',
        ],
        StateSupervisionContactType.TELEPHONE: [
            'TELEPHONE',
        ],
        StateSupervisionContactType.WRITTEN_MESSAGE: [
            'FAX',
            'EMAIL',
            'MAIL',
        ],

        StateSupervisionContactReason.INITIAL_CONTACT: [
            '72 HOUR INITIAL'
        ],
        StateSupervisionContactReason.EMERGENCY_CONTACT: [
            'CRITICAL'
        ],
        StateSupervisionContactReason.GENERAL_CONTACT: [
            'GENERAL'
        ],
        StateSupervisionContactReason.INTERNAL_UNKNOWN: [
            # No longer valid codes
            'CONVERSION',
            'GENERAL REINFORCEMENT',
            'GENERAL DISAPPROVAL',
        ],

        StateSupervisionContactStatus.COMPLETED: [
            'SUCCESSFUL',
            'PROGRESS REVIEW',
            'FACE TO FACE',
            'ARREST',  # TODO(#3506): Is there another place to ingest arrest entities or should we take it from
                       #  sprvsn_cntc?
        ],
        StateSupervisionContactStatus.ATTEMPTED: [
            'ATTEMPTED',
            'INQUIRY',
            'FAILED TO REPORT',
        ],

        StateSupervisionLevel.UNSUPERVISED: [
            'UNSUPV COURT PROB',
        ],
        StateSupervisionLevel.LIMITED: [
            'LIMITED SUPERVISION',
        ],
        StateSupervisionLevel.EXTERNAL_UNKNOWN: [
            'UNCLASSIFIED',
        ],
        StateSupervisionLevel.IN_CUSTODY: [
            'FEDERAL CUSTODY',
            'ICE DETAINER',
        ],
        StateSupervisionLevel.INTERNAL_UNKNOWN: [
            'DOSAGE',                   # TODO(#3692): Figure out what this means
            'DOSAGE ELIGIBLE',          # TODO(#3692): Figure out what this means
            'ADMINISTRATIVE',           # Used for a non-standardized set of situations.
            'GOLD SEAL PENDING',        # Pending supervision termination (after parole board has approved termination)
            # No longer under IDOC authority
            'DEPORTED',
            # Historical values, now unused
            'CLOSE COMM SUPERVISION',
            'COMMUNITY',
            'ELECTRONIC MONITOR',
            'INTENSE',
            'SPECIAL NEEDS',
            'SEX OFFENSE',
            'TRANSITION',
        ],
        # TODO(#3692): Validate that we have the correct mappings for new values of LOW, MODERATE, HIGH
        StateSupervisionLevel.MINIMUM: [
            'LOW',
            'LEVEL 1',              # Historical value for minimum
            'MINIMUM',              # Historical value for minimum
            'SO LOW',               # Sex offense case load, minimum
            'SO LEVEL 1',           # Historical sex offense case load, minimum
            'SO TO GENERAL LOW',    # Previously on sex offense case load, now low general caseload
        ],
        StateSupervisionLevel.MEDIUM: [
            'MODERATE',
            'LEVEL 2',                  # Historical value for medium
            'MEDIUM',                   # Historical value for medium
            'MODERATE',                 # Historical value for medium
            'MODERATE- OLD',            # Historical value for medium

            'SO MODERATE',              # Sex offense case load, medium
            'SO LEVEL 2',               # Historical sex offense case load, medium
            'SO TO GENERAL MOD',        # Previously on sex offense case load, now medium general caseload
            'SO TO GENERAL LEVEL2',     # Historical value - previously on sex offense case load, now medium general
                                        # caseload
            'DUI OVERRIDE LEVEL 2',     # Historical DUI, medium
            'O R MODERATE',             # Historical value
        ],
        StateSupervisionLevel.HIGH: [
            'HIGH',
            'SO HIGH',                  # Sex offense case load, high
            'SO LEVEL 3',               # Historical sex offense case load, high
            'SO TO GENERAL HIGH',       # Previously on sex offense case load, now high general caseload.
            'SO TO GENERAL LEVEL3',     # Historical value - previously sex offense caseload, now high general caseload
            'LEVEL 3',                  # Historical value - High
            'O R HIGH',                 # Historical value
        ],
        StateSupervisionLevel.MAXIMUM: [
            'LEVEL 4',                  # Historical value for maximum
            'MAXIMUM',                  # Historical value for maximum
            'SO TO GENERAL LEVEL4',     # Historical value - Previously sex offense caseload, now on maximum general
                                        # caseload
        ],
        # TODO(#3692): Ask ID if there are new values to show whether someone is in a diversion court?
        StateSupervisionLevel.DIVERSION: [
            # All are historical values below
            'DRUG COURT',
            'DRUG COURT DIV',
            'FAMILY COURT',
            'MENTAL HEALTH COURT',
            'MENTAL HLTH CRT DIV',
            'SUBSTANCE ABUSE',
            'VETERANS COURT DIV',
            'VETERANS COURT',
        ],
        StateSupervisionLevel.INTERSTATE_COMPACT: [
            'INTERSTATE',
        ],

        StateSupervisionCaseType.SEX_OFFENDER: [
            'SO LOW',
            'SO MODERATE',
            'SO HIGH',
            'SO LEVEL 1',           # Historical values as of Sept 2020
            'SO LEVEL 2',
            'SO LEVEL 3',
            'SEX OFFENSE',          # Historical value
        ],
        StateSupervisionCaseType.DRUG_COURT: [
            'DRUG COURT',
            'DRUG COURT DIV',       # Historical value
        ],
        StateSupervisionCaseType.MENTAL_HEALTH_COURT: [
            'MENTAL HEALTH COURT',
            'MENTAL HLTH CRT DIV',  # Historical value
        ],
        StateSupervisionCaseType.VETERANS_COURT: [
            'VETERANS COURT',
            'VETERANS COURT DIV',   # Historical value
        ],
        StateSupervisionCaseType.FAMILY_COURT: [
            'FAMILY COURT',
        ],
    }
    ENUM_IGNORES: Dict[EntityEnumMeta, List[str]] = {}
    ENUM_MAPPERS: Dict[EntityEnumMeta, EnumMapper] = {
        StateIncarcerationPeriodAdmissionReason: incarceration_admission_reason_mapper,
        StateIncarcerationPeriodReleaseReason: incarceration_release_reason_mapper,
        StateSupervisionPeriodAdmissionReason: supervision_admission_reason_mapper,
        StateSupervisionPeriodTerminationReason: supervision_termination_reason_mapper,
        StateSpecializedPurposeForIncarceration: purpose_for_incarceration_mapper,
        StateSupervisionPeriodSupervisionType: supervision_period_supervision_type_mapper,
    }

    ENUM_IGNORE_PREDICATES: Dict[EntityEnumMeta, EnumIgnorePredicate] = {}

    @classmethod
    def get_file_tag_rank_list(cls) -> List[str]:
        return cls.FILE_TAGS

    def generate_enum_overrides(self) -> EnumOverrides:
        """Provides Idaho-specific overrides for enum mappings."""
        base_overrides = super().get_enum_overrides()
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
                'offender_ofndr_dob_address',
                'ofndr_tst_ofndr_tst_cert',
                'mittimus_judge_sentence_offense_sentprob_incarceration_sentences',
                'mittimus_judge_sentence_offense_sentprob_supervision_sentences',
                'movement_facility_location_offstat_incarceration_periods',
                'movement_facility_location_offstat_supervision_periods',
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
                max_time = safe_parse_days_from_duration_pieces(
                    years_str=max_years, months_str=max_months, days_str=max_days, start_dt_str=start_date)
                min_time = safe_parse_days_from_duration_pieces(
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
    def _override_facilities(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """Overrides the recorded facility for the person if IDOC data indicates that the person is at another
        location.
        """
        location_cd = row.get(CURRENT_LOCATION_CODE, '')
        location_name = row.get(CURRENT_LOCATION_NAME, '')
        if not (location_cd and location_name) or location_cd == '001':   # Present at facility
            return

        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                obj.facility = location_name

    @staticmethod
    def _set_custodial_authority(
            _file_tag: str, row: Dict[str, str], extracted_objects: List[IngestObject], _cache: IngestObjectCache):
        """Sets custodial authority on the StateSupervisionPeriod entities."""
        custodial_authority = IDOC_CUSTODIAL_AUTHORITY

        # Determine if there should be overrides for custodial authority that are not IDOC.
        facility_cd = row.get(CURRENT_FACILITY_CODE, '')
        location_name = row.get(CURRENT_LOCATION_NAME, '')
        if facility_cd in (INTERSTATE_FACILITY_CODE, PAROLE_COMMISSION_CODE):
            custodial_authority = location_name
        elif location_name == DEPORTED_LOCATION_NAME:
            custodial_authority = OTHER_COUNTRY_CUSTODIAL_AUTHORITY
        elif location_name == FEDERAL_CUSTODY_LOCATION_NAME:
            custodial_authority = FEDERAL_CUSTODIAL_AUTHORITY

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                obj.custodial_authority = custodial_authority

    @staticmethod
    def _override_supervision_type(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """If necessary, overrides supervision level for supervision periods based on supervision level data that
        appears in the non-standard wrkld_cat location.
        """

        living_unit_cd = row.get(CURRENT_LIVING_UNIT_CODE, '')
        supervision_type_override = None
        if living_unit_cd in (BENCH_WARRANT_LIVING_UNIT, COURT_PROBATION_LIVING_UNIT):
            supervision_type_override = living_unit_cd

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                if supervision_type_override:
                    obj.supervision_period_supervision_type = supervision_type_override

    # TODO(#2912): Add custodial authority to incarceration periods
    @staticmethod
    def _set_supervision_site(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """Sets supervision_site based on granular location info."""
        facility_cd = row.get(CURRENT_FACILITY_CODE, '')
        facility_name = row.get(CURRENT_FACILITY_NAME, '')
        location_name = row.get(CURRENT_LOCATION_NAME, '')
        living_unit_cd = row.get(CURRENT_LIVING_UNIT_CODE, '')
        living_unit_name = row.get(CURRENT_LIVING_UNIT_NAME, '')

        # default values
        supervision_site: Optional[str] = create_supervision_site(supervising_district_id=facility_name,
                                                                  supervision_specific_location=living_unit_name)

        # Interstate and parole commission facilities mean some non-IDOC entity is supervising the person.
        if facility_cd in (INTERSTATE_FACILITY_CODE, PAROLE_COMMISSION_CODE):
            supervision_site = create_supervision_site(supervising_district_id=facility_name,
                                                       supervision_specific_location=location_name)
        # Absconders have no granular facility info.
        elif facility_cd == FUGITIVE_FACILITY_CODE:
            supervision_site = None
        # People on limited supervision are marked as a part of 'DISTRICT 4' in the data because it started out as a
        # District 4 only program. Now, however, they count anyone on limited supervision as a part of DISTRICT 0
        elif living_unit_cd == LIMITED_SUPERVISION_LIVING_UNIT:
            supervision_site = create_supervision_site(
                supervising_district_id=DISTRICT_0, supervision_specific_location=LIMITED_SUPERVISION_UNIT_NAME)
        # TODO(#3309): Consider adding warrant spans to schema.
        # Folks with an existing bench warrant are not actively supervised and have no office info.
        elif living_unit_cd == BENCH_WARRANT_LIVING_UNIT:
            supervision_site = create_supervision_site(supervising_district_id=facility_name,
                                                       supervision_specific_location=UNKNOWN)
        # Folks with court probation do not have office info.
        elif living_unit_cd == COURT_PROBATION_LIVING_UNIT:
            supervision_site = create_supervision_site(supervising_district_id=facility_name,
                                                       supervision_specific_location=UNKNOWN)

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                obj.supervision_site = supervision_site

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
        if not person_id:
            person_id = row.get('ofndr_num', '')

        sentence_group_id = row.get('incrno', '')
        sentence_id = row.get('sent_no', '')
        court_case_id = row.get('caseno', '')
        period_id = row.get('period_id', '')
        violation_id = row.get('ofndr_tst_id', '')
        early_discharge_id = row.get('early_discharge_id', '')
        early_discharge_sent_id = row.get('early_discharge_sent_id', '')

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

            # While early_discharge_sent_id is unique for every sentence-level early discharge request,
            # early_discharge_id can be repeated across sentences if discharge was requested for the
            # sentences at the same time. Decisions are made on a sentence level, so we need to include
            # early_discharge_sent_id in the key. We prepend early_discharge_id in case it is useful in calculate to
            # know which sentences had early discharge requested together.
            if isinstance(obj, StateEarlyDischarge):
                obj.state_early_discharge_id = f'{early_discharge_id}-{early_discharge_sent_id}'

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
                if obj.facility is not None and is_jail_facility(obj.facility):
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

    def _set_case_type_from_supervision_level(
            self,
            _file_tag: str,
            _row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """Sets supervision period case type from the supervision level if necessary."""
        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                supervision_level = obj.supervision_level
                if supervision_level and \
                        self.get_enum_overrides().parse(supervision_level, StateSupervisionCaseType) is not None:
                    case_type_to_create = StateSupervisionCaseTypeEntry(case_type=supervision_level)
                    create_if_not_exists(case_type_to_create, obj, 'state_supervision_case_type_entries')

    @staticmethod
    def _supervision_period_admission_and_termination_overrides(
            _file_tag: str,
            row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache):
        """Update in and out edges of supervision periods if the normal mappings are insufficient."""
        prev_fac_cd = row.get(PREVIOUS_FACILITY_CODE, '')
        next_fac_cd = row.get(NEXT_FACILITY_CODE, '')
        cur_fac_cd = row.get(CURRENT_FACILITY_CODE, '')

        prev_loc_name = row.get(PREVIOUS_LOCATION_NAME, '')
        next_loc_name = row.get(NEXT_LOCATION_NAME, '')
        cur_loc_name = row.get(CURRENT_LOCATION_NAME, '')

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                # Determine in and out edges from previous/next facilities
                # Handle transfers to and from interstate
                if prev_fac_cd == INTERSTATE_FACILITY_CODE:
                    obj.admission_reason = INTERSTATE_FACILITY_CODE
                if prev_loc_name == DEPORTED_LOCATION_NAME:
                    obj.admission_reason = DEPORTED_LOCATION_NAME

                if next_fac_cd == INTERSTATE_FACILITY_CODE:
                    obj.termination_reason = INTERSTATE_FACILITY_CODE
                if next_loc_name == DEPORTED_LOCATION_NAME:
                    obj.termination_reason = DEPORTED_LOCATION_NAME

                # Override in an out edges based on current facilities

                # If we're currently in an interstate or deported period, set admission/release reason accordingly.
                if cur_fac_cd == INTERSTATE_FACILITY_CODE:
                    obj.admission_reason = INTERSTATE_FACILITY_CODE
                    if obj.termination_date:
                        obj.termination_reason = INTERSTATE_FACILITY_CODE

                # If the person has been deported set admission/release reason accordingly.
                # TODO(#3800): Consider adding specific enum for Deported
                if cur_loc_name == DEPORTED_LOCATION_NAME:
                    obj.admission_reason = DEPORTED_LOCATION_NAME
                    if obj.termination_date:
                        obj.termination_reason = DEPORTED_LOCATION_NAME

                # Handle absconsion periods.
                if cur_fac_cd == FUGITIVE_FACILITY_CODE:
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
        parole_recommendation = row.get('parolee_placement_recommendation', '')
        probation_recommendation = row.get('probationer_placement_recommendation', '')
        recommendations = list(filter(None, [parole_recommendation, probation_recommendation]))

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
        agent_id = row.get('empl_sdesc', '')
        agent_name = row.get('empl_ldesc', '')
        if not agent_id or not agent_name or agent_id == UNKNOWN_EMPLOYEE_SDESC:
            return

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                agent_to_create = StateAgent(
                    state_agent_id=agent_id,
                    full_name=agent_name,
                    agent_type=StateAgentType.SUPERVISION_OFFICER.value)
                create_if_not_exists(agent_to_create, obj, 'supervising_officer')

    @staticmethod
    def _add_supervision_contact_fields(
            _file_tag: str, row: Dict[str, str], extracted_objects: List[IngestObject], _cache: IngestObjectCache):
        """Adds all extra fields needed on SupervisionContact entities that cannot be automatically mapped via YAMLs."""
        agent_id = row.get('usr_id', '')
        agent_name = row.get('name', '')

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionContact):
                if agent_id and agent_name:
                    agent_to_create = StateAgent(
                        state_agent_id=agent_id,
                        full_name=agent_name,
                        agent_type=StateAgentType.SUPERVISION_OFFICER.value)
                    create_if_not_exists(agent_to_create, obj, 'contacted_agent')

                obj.resulted_in_arrest = str(obj.status == CONTACT_RESULT_ARREST)

                if obj.location in CONTACT_TYPES_TO_BECOME_LOCATIONS:
                    obj.contact_type = obj.location
                    obj.location = None


def _get_bool_from_row(arg: str, row: Dict[str, str]):
    val = row.get(arg)
    if not val:
        return False
    return str_to_bool(val)
