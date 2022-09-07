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
from enum import Enum
from typing import Dict, List, Optional, Type

from recidiviz.common.constants.enum_overrides import (
    EnumIgnorePredicate,
    EnumMapperFn,
    EnumOverrides,
)
from recidiviz.common.constants.state.external_id_types import US_ID_DOC
from recidiviz.common.constants.state.standard_enum_overrides import (
    legacy_mappings_standard_enum_overrides,
)
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_early_discharge import (
    StateEarlyDischargeDecision,
    StateEarlyDischargeDecisionStatus,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_shared_enums import (
    StateActingBodyType,
    StateCustodialAuthority,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.str_field_utils import (
    parse_date,
    safe_parse_days_from_duration_pieces,
    sorted_list_from_str,
)
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.legacy_ingest_mappings.direct_ingest_controller_utils import (
    create_if_not_exists,
    update_overrides_from_maps,
)
from recidiviz.ingest.direct.legacy_ingest_mappings.legacy_ingest_view_processor import (
    IngestAncestorChainOverridesCallable,
    IngestFilePostprocessorCallable,
    IngestPrimaryKeyOverrideCallable,
    IngestRowPosthookCallable,
    IngestRowPrehookCallable,
    LegacyIngestViewProcessorDelegate,
)
from recidiviz.ingest.direct.legacy_ingest_mappings.state_shared_row_posthooks import (
    IngestGatingContext,
    copy_name_to_alias,
    gen_label_single_external_id_hook,
    gen_rationalize_race_and_ethnicity,
)
from recidiviz.ingest.direct.regions.us_id.us_id_constants import (
    ALL_NEW_CRIME_TYPES,
    BENCH_WARRANT_LIVING_UNIT,
    COURT_PROBATION_LIVING_UNIT,
    CURRENT_FACILITY_CODE,
    CURRENT_FACILITY_NAME,
    CURRENT_LIVING_UNIT_CODE,
    CURRENT_LIVING_UNIT_NAME,
    CURRENT_LOCATION_CODE,
    CURRENT_LOCATION_NAME,
    DEPORTED_LOCATION_NAME,
    DISTRICT_0,
    FEDERAL_CUSTODY_LOCATION_CODE,
    FEDERAL_CUSTODY_LOCATION_NAMES,
    FUGITIVE_FACILITY_CODE,
    HISTORY_FACILITY_TYPE,
    INTERSTATE_FACILITY_CODE,
    LIMITED_SUPERVISION_LIVING_UNIT,
    LIMITED_SUPERVISION_UNIT_NAME,
    MAX_DATE_STR,
    NEXT_FACILITY_CODE,
    NEXT_FACILITY_TYPE,
    NEXT_LOCATION_NAME,
    PAROLE_COMMISSION_CODE,
    PREVIOUS_FACILITY_CODE,
    PREVIOUS_LOCATION_NAME,
    SEX_CRIME_TYPES,
    UNKNOWN,
    UNKNOWN_EMPLOYEE_SDESC,
    VIOLATION_REPORT_CONSTANTS_INCLUDING_COMMA,
    VIOLATION_REPORT_NO_RECOMMENDATION_VALUES,
    VIOLENT_CRIME_TYPES,
)
from recidiviz.ingest.direct.regions.us_id.us_id_legacy_enum_helpers import (
    custodial_authority_mapper,
    incarceration_admission_reason_mapper,
    incarceration_release_reason_mapper,
    is_jail_facility,
    purpose_for_incarceration_mapper,
    supervision_admission_reason_mapper,
    supervision_period_supervision_type_mapper,
    supervision_termination_reason_mapper,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.models.ingest_info import (
    IngestObject,
    StateAgent,
    StateAssessment,
    StateCharge,
    StateCourtCase,
    StateEarlyDischarge,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache
from recidiviz.utils.params import str_to_bool


# TODO(#8900): Delete LegacyIngestViewProcessorDelegate superclass when we have fully
#  migrated this state to new ingest mappings version.
class UsIdController(BaseDirectIngestController, LegacyIngestViewProcessorDelegate):
    """Direct ingest controller implementation for US_ID."""

    @classmethod
    def region_code(cls) -> str:
        return StateCode.US_ID.value.lower()

    def __init__(self, ingest_instance: DirectIngestInstance):
        super().__init__(ingest_instance)
        self.enum_overrides = self.generate_enum_overrides()
        early_discharge_deleted_rows_processors = [
            gen_label_single_external_id_hook(US_ID_DOC),
            self._set_generated_ids,
            self._set_invalid_early_discharge_status,
        ]
        self.row_post_processors_by_file: Dict[str, List[IngestRowPosthookCallable]] = {
            "offender_ofndr_dob_address": [
                copy_name_to_alias,
                # When first parsed, the info object just has a single external id - the DOC id.
                gen_label_single_external_id_hook(US_ID_DOC),
                gen_rationalize_race_and_ethnicity(self.ENUM_OVERRIDES),
            ],
            "ofndr_tst_ofndr_tst_cert": [
                self._add_lsir_to_assessments,
                gen_label_single_external_id_hook(US_ID_DOC),
            ],
            "mittimus_judge_sentence_offense_sentprob_incarceration_sentences": [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._add_statute_to_charge,
                self._set_is_violent,
                self._set_is_sex_offense,
                self._attempt_to_set_offense_date,
                self._set_extra_sentence_fields,
                self._set_generated_ids,
            ],
            "early_discharge_incarceration_sentence": [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._set_generated_ids,
                self._set_early_discharge_status,
            ],
            "early_discharge_supervision_sentence": [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._set_generated_ids,
                self._set_early_discharge_status,
            ],
            "movement_facility_location_offstat_incarceration_periods": [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._override_facilities,
                self._set_generated_ids,
                self._clear_max_dates,
                self._add_incarceration_type,
                self._add_default_admission_reason,
                self._add_details_when_transferred_to_history,
            ],
            "movement_facility_location_offstat_supervision_periods": [
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
                self._add_details_when_transferred_to_history,
            ],
            "ofndr_tst_tst_qstn_rspns_violation_reports": [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._set_generated_ids,
                self._set_violation_violent_sex_offense,
                self._hydrate_violation_types,
                self._hydrate_violation_report_fields,
            ],
            "ofndr_tst_tst_qstn_rspns_violation_reports_old": [
                gen_label_single_external_id_hook(US_ID_DOC),
                self._set_generated_ids,
                self._set_violation_violent_sex_offense,
                self._hydrate_violation_types,
                self._hydrate_violation_report_fields,
            ],
            "early_discharge_incarceration_sentence_deleted_rows": early_discharge_deleted_rows_processors,
            "early_discharge_supervision_sentence_deleted_rows": early_discharge_deleted_rows_processors,
        }
        self.file_post_processors_by_file: Dict[
            str, List[IngestFilePostprocessorCallable]
        ] = {
            "offender_ofndr_dob_address": [],
            "ofndr_tst_ofndr_tst_cert": [],
            "mittimus_judge_sentence_offense_sentprob_incarceration_sentences": [],
            "mittimus_judge_sentence_offense_sentprob_supervision_sentences": [],
            "early_discharge_incarceration_sentence": [],
            "early_discharge_supervision_sentence": [],
            "movement_facility_location_offstat_incarceration_periods": [],
            "movement_facility_location_offstat_supervision_periods": [],
            "ofndr_tst_tst_qstn_rspns_violation_reports": [],
            "ofndr_tst_tst_qstn_rspns_violation_reports_old": [],
        }

    ENUM_OVERRIDES: Dict[Enum, List[str]] = {
        StateRace.ASIAN: ["A"],
        StateRace.BLACK: ["B"],
        StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE: ["I"],
        StateRace.OTHER: ["O"],
        StateRace.NATIVE_HAWAIIAN_PACIFIC_ISLANDER: ["P"],
        StateRace.EXTERNAL_UNKNOWN: ["U"],
        StateRace.WHITE: ["W"],
        StateEthnicity.HISPANIC: ["H"],
        StateGender.MALE: ["M"],
        StateGender.FEMALE: ["F"],
        StateGender.TRANS_MALE: ["X"],
        StateGender.TRANS_FEMALE: ["Y"],
        StateGender.EXTERNAL_UNKNOWN: ["U"],
        StateAssessmentLevel.LOW: ["Minimum"],
        StateAssessmentLevel.LOW_MEDIUM: ["Low-Medium"],
        StateAssessmentLevel.MEDIUM_HIGH: ["High-Medium"],
        StateAssessmentLevel.HIGH: ["Maximum"],
        StateSentenceStatus.COMMUTED: [
            "M",  # Commuted
        ],
        StateSentenceStatus.VACATED: [
            "V",  # Vacated Sentence
            "Q",  # Vacated conviction
        ],
        # TODO(#3517): Consider breaking out these sentence status enums in our schema (
        #  sealed, early_discharge, expired, etc)
        StateSentenceStatus.COMPLETED: [
            "C",  # Completed
            "D",  # Discharged
            "E",  # Expired
            "F",  # Parole Early Discharge
            "G",  # Dismissed
            "H",  # Post conviction relief.
            "L",  # Sealed
            "S",  # Satisfied
            "X",  # Rule 35 - Reduction of illegal or overly harsh sentence.
            "Z",  # Reduced to misdemeanor - person should not be in prison and no longer tracked by IDOC.
        ],
        StateSentenceStatus.REVOKED: [
            "K",  # Revoked
        ],
        StateSentenceStatus.SERVING: [
            "I",  # Imposed
            "J",  # RJ To Court - Used for probation after treatment
            "N",  # Interstate Parole
            "O",  # Correctional Compact - TODO(#3506): Get more info from ID.
            "P",  # Bond Appeal - unused, but present in ID status table.
            "R",  # Court Retains Jurisdiction - used when a person on a rider. TODO(#3506): Whats the difference
            # between this and 'W'?
            "T",  # Interstate probation - unused, but present in ID status table.
            "U",  # Unsupervised - probation
            "W",  # Witheld judgement - used when a person is on a rider.
            "Y",  # Drug Court
        ],
        StateSentenceStatus.SUSPENDED: [
            "B",  # Suspended sentence - probation
        ],
        StateSupervisionViolationType.ABSCONDED: [
            "Absconding",  # From violation report 210
            "Absconder",  # From violation report 204
        ],
        StateSupervisionViolationType.FELONY: [
            "New Felony",  # From violation report 210/204
        ],
        StateSupervisionViolationType.MISDEMEANOR: [
            "New Misdemeanor",  # From violation report 210/204
        ],
        StateSupervisionViolationType.TECHNICAL: [
            "Technical (enter details below)",  # From violation report 210
            "Technical",  # From violation report 204
        ],
        StateSupervisionViolationResponseDecision.CONTINUANCE: [
            "Reinstatement",  # Parole/probation recommendation from violation report 210
            "Recommended Full Term Release Date",  # Parole recommendation from violation report 204
        ],
        StateSupervisionViolationResponseDecision.SPECIALIZED_COURT: [
            "Diversion - Problem Solving Court",  # Parole recommendation from violation report 210
            "Treatment Court",  # Probation recommendation from violation report 210
            "Referral to Problem Solving Court",  # Probation recommendation from violation report 204
        ],
        StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION: [
            "Diversion - Jail",  # Parole recommendation from violation report 210
            "Diversion - CRC",  # Parole recommendation from violation report 210
            "Diversion - Prison",  # Parole recommendation from violation report 210
            "Local Jail Time",  # Probation recommendation from violation report 210/204
        ],
        StateSupervisionViolationResponseDecision.TREATMENT_IN_PRISON: [
            "Rider",  # Probation recommendation from violation report 210
            "Rider Recommendation",  # Probation recommendation from violation report 204
            "PVC - Parole Violator Program",  # Parole recommendation from violation report 204
        ],
        StateSupervisionViolationResponseDecision.REVOCATION: [
            "Revocation",  # Parole recommendation from violation report 210
            "Imposition of Sentence",  # Probation recommendation from violation report 210/204
        ],
        StateEarlyDischargeDecision.SENTENCE_TERMINATION_GRANTED: [
            "Grant Request to Terminate",
        ],
        StateEarlyDischargeDecision.REQUEST_DENIED: [
            "Deny",
            "Deny - Committed Crime(s) While on Probation",
            "Deny - New Charges",
            "Deny - No Violations, but Unsatisfactory",
            "Deny - Other",
            "Deny - Pending Charge(s)",
            "Deny - Programming Needed",
            "Deny - Serious Nature of the Offense",
            "Deny - Unpaid Restitution",
            "Deny - Unpaid Restitution, Court Costs or Fees",
            "Deny - Unsatisfactory Performance to Date",
        ],
        StateEarlyDischargeDecision.UNSUPERVISED_PROBATION_GRANTED: [
            "Grant Unsupervised Probation",
        ],
        StateActingBodyType.COURT: [
            "PROBATION",
        ],
        StateActingBodyType.PAROLE_BOARD: [
            "PAROLE",
        ],
        StateActingBodyType.SUPERVISION_OFFICER: [
            "REQUEST FOR DISCHARGE: PROBATION",
            "SPECIAL PROGRESS REPORT FOR PAROLE COMMUTATION",
        ],
        StateActingBodyType.SENTENCED_PERSON: [
            "SPECIAL PROGRESS REPORT MOTION FOR PROBATION DISCHARGE BY DEFENDANT",
            "SPECIAL PROGRESS REPORT OFFENDER INITIATED PAROLE DISCHARGE REQUEST",
        ],
        StateSupervisionLevel.UNSUPERVISED: [
            "UNSUPV COURT PROB",
        ],
        StateSupervisionLevel.LIMITED: [
            "LIMITED SUPERVISION",
        ],
        StateSupervisionLevel.UNASSIGNED: [
            "UNCLASSIFIED",
        ],
        StateSupervisionLevel.IN_CUSTODY: [
            "FEDERAL CUSTODY",
            "ICE DETAINER",
        ],
        StateSupervisionLevel.INTERNAL_UNKNOWN: [
            "DOSAGE",  # TODO(#3749): Remap to separate enum
            "DOSAGE ELIGIBLE",
            "D7 DUI COURT",
            "EXPANDED CRC",
            "BENCH WARRANT",  # SupervisionType, not SupervisionLevel
            "ADMINISTRATIVE",  # Used for a non-standardized set of situations.
            "GOLD SEAL PENDING",  # Pending supervision termination (after parole board has approved termination)
            # No longer under IDOC authority
            "DEPORTED",
            # Historical values, now unused
            "CLOSE COMM SUPERVISION",
            "COMMUNITY",
            "ELECTRONIC MONITOR",
            "INTENSE",
            "SPECIAL NEEDS",
            "SEX OFFENSE",
            "TRANSITION",
        ],
        StateSupervisionLevel.MINIMUM: [
            "LOW",
            "LEVEL 1",  # Historical value for minimum
            "MINIMUM",  # Historical value for minimum
            "SO LOW",  # Sex offense case load, minimum
            "SO LEVEL 1",  # Historical sex offense case load, minimum
            "SO TO GENERAL LOW",  # Previously on sex offense case load, now low general caseload
        ],
        StateSupervisionLevel.MEDIUM: [
            "MODERATE",
            "LEVEL 2",  # Historical value for medium
            "MEDIUM",  # Historical value for medium
            "MODERATE",  # Historical value for medium
            "MODERATE- OLD",  # Historical value for medium
            "SO MODERATE",  # Sex offense case load, medium
            "SO LEVEL 2",  # Historical sex offense case load, medium
            "SO TO GENERAL MOD",  # Previously on sex offense case load, now medium general caseload
            "SO TO GENERAL LEVEL2",  # Historical value - previously on sex offense case load, now medium general
            # caseload
            "DUI OVERRIDE LEVEL 2",  # Historical DUI, medium
            "O R MODERATE",  # Historical value
        ],
        StateSupervisionLevel.HIGH: [
            "HIGH",
            "SO HIGH",  # Sex offense case load, high
            "SO LEVEL 3",  # Historical sex offense case load, high
            "SO TO GENERAL HIGH",  # Previously on sex offense case load, now high general caseload.
            "SO TO GENERAL LEVEL3",  # Historical value - previously sex offense caseload, now high general caseload
            "LEVEL 3",  # Historical value - High
            "O R HIGH",  # Historical value
        ],
        StateSupervisionLevel.MAXIMUM: [
            "LEVEL 4",  # Historical value for maximum
            "MAXIMUM",  # Historical value for maximum
            "SO TO GENERAL LEVEL4",  # Historical value - Previously sex offense caseload, now on maximum general
            # caseload
        ],
        StateSupervisionLevel.DIVERSION: [
            # All are historical values below
            "DRUG COURT",
            "DRUG COURT DIV",
            "FAMILY COURT",
            "MENTAL HEALTH COURT",
            "MENTAL HLTH CRT DIV",
            "SUBSTANCE ABUSE",
            "VETERANS COURT DIV",
            "VETERANS COURT",
        ],
        StateSupervisionLevel.INTERSTATE_COMPACT: [
            "INTERSTATE",
        ],
        StateSupervisionCaseType.SEX_OFFENSE: [
            "SO LOW",
            "SO MODERATE",
            "SO HIGH",
            "SO LEVEL 1",  # Historical values as of Sept 2020
            "SO LEVEL 2",
            "SO LEVEL 3",
            "SEX OFFENSE",  # Historical value
        ],
        StateSupervisionCaseType.DRUG_COURT: [
            "DRUG COURT",
            "DRUG COURT DIV",  # Historical value
        ],
        StateSupervisionCaseType.MENTAL_HEALTH_COURT: [
            "MENTAL HEALTH COURT",
            "MENTAL HLTH CRT DIV",  # Historical value
        ],
        StateSupervisionCaseType.VETERANS_COURT: [
            "VETERANS COURT",
            "VETERANS COURT DIV",  # Historical value
        ],
        StateSupervisionCaseType.FAMILY_COURT: [
            "FAMILY COURT",
        ],
    }
    ENUM_IGNORES: Dict[Type[Enum], List[str]] = {}
    ENUM_MAPPER_FUNCTIONS: Dict[Type[Enum], EnumMapperFn] = {
        StateIncarcerationPeriodAdmissionReason: incarceration_admission_reason_mapper,
        StateIncarcerationPeriodReleaseReason: incarceration_release_reason_mapper,
        StateSupervisionPeriodAdmissionReason: supervision_admission_reason_mapper,
        StateSupervisionPeriodTerminationReason: supervision_termination_reason_mapper,
        StateSpecializedPurposeForIncarceration: purpose_for_incarceration_mapper,
        StateSupervisionPeriodSupervisionType: supervision_period_supervision_type_mapper,
        StateCustodialAuthority: custodial_authority_mapper,
    }

    ENUM_IGNORE_PREDICATES: Dict[Type[Enum], EnumIgnorePredicate] = {}

    def get_ingest_view_rank_list(self) -> List[str]:
        """Returns a list of string ingest view names in the order they should be
        processed for data we received on a particular date.
        """
        return [
            "offender_ofndr_dob_address",
            "ofndr_tst_ofndr_tst_cert",
            "mittimus_judge_sentence_offense_sentprob_incarceration_sentences",
            "mittimus_judge_sentence_offense_sentprob_supervision_sentences",
            "early_discharge_incarceration_sentence",
            "early_discharge_supervision_sentence",
            "movement_facility_location_offstat_incarceration_periods",
            "movement_facility_location_offstat_supervision_periods",
            "ofndr_tst_tst_qstn_rspns_violation_reports",
            "ofndr_tst_tst_qstn_rspns_violation_reports_old",
            "early_discharge_incarceration_sentence_deleted_rows",
            "early_discharge_supervision_sentence_deleted_rows",
            "sprvsn_cntc_v3",
            "agnt_case_updt",
        ]

    @classmethod
    def generate_enum_overrides(cls) -> EnumOverrides:
        """Provides Idaho-specific overrides for enum mappings."""
        base_overrides = legacy_mappings_standard_enum_overrides()
        return update_overrides_from_maps(
            base_overrides,
            cls.ENUM_OVERRIDES,
            cls.ENUM_IGNORES,
            cls.ENUM_MAPPER_FUNCTIONS,
            cls.ENUM_IGNORE_PREDICATES,
        )

    def get_enum_overrides(self) -> EnumOverrides:
        return self.enum_overrides

    # TODO(#8900): Delete LegacyIngestViewProcessorDelegate methods when we have fully
    #  migrated this state to new ingest mappings version.
    def get_file_post_processors_for_file(
        self, file_tag: str
    ) -> List[IngestFilePostprocessorCallable]:
        return self.file_post_processors_by_file.get(file_tag, [])

    def get_row_post_processors_for_file(
        self, file_tag: str
    ) -> List[IngestRowPosthookCallable]:
        return self.row_post_processors_by_file.get(file_tag, [])

    def get_row_pre_processors_for_file(
        self, _file_tag: str
    ) -> List[IngestRowPrehookCallable]:
        return []

    def get_ancestor_chain_overrides_callback_for_file(
        self, _file_tag: str
    ) -> Optional[IngestAncestorChainOverridesCallable]:
        return None

    def get_primary_key_override_for_file(
        self, _file_tag: str
    ) -> Optional[IngestPrimaryKeyOverrideCallable]:
        return None

    def get_files_to_set_with_empty_values(self) -> List[str]:
        return []

    @staticmethod
    def _get_id_type(file_tag: str) -> Optional[str]:
        if file_tag in [
            "offender_ofndr_dob_address",
            "ofndr_tst_ofndr_tst_cert",
            "mittimus_judge_sentence_offense_sentprob_incarceration_sentences",
            "mittimus_judge_sentence_offense_sentprob_supervision_sentences",
            "movement_facility_location_offstat_incarceration_periods",
            "movement_facility_location_offstat_supervision_periods",
            "ofndr_tst_tst_qstn_rspns_violation_reports",
            "ofndr_tst_tst_qstn_rspns_violation_reports_old",
        ]:
            return US_ID_DOC

        return None

    @staticmethod
    def _add_lsir_to_assessments(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        for obj in extracted_objects:
            if isinstance(obj, StateAssessment):
                obj.assessment_type = StateAssessmentType.LSIR.value

    @staticmethod
    def _add_statute_to_charge(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        statute_title = row.get("off_stat_title", "")
        statute_section = row.get("off_stat_sect", "")

        if not statute_title or not statute_section:
            return
        statute = f"{statute_title}-{statute_section}"

        for obj in extracted_objects:
            if isinstance(obj, StateCharge):
                obj.statute = statute

    @staticmethod
    def _set_is_violent(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        violent_code = row.get("off_viol")
        is_violent = violent_code and violent_code.upper() == "V"

        for obj in extracted_objects:
            if isinstance(obj, StateCharge):
                obj.is_violent = str(is_violent)

    @staticmethod
    def _set_is_sex_offense(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        sex_offense_code = row.get("off_sxo_flg")
        is_sex_offense = sex_offense_code and sex_offense_code.upper() == "X"

        for obj in extracted_objects:
            if isinstance(obj, StateCharge):
                obj.is_sex_offense = str(is_sex_offense)

    @staticmethod
    def _attempt_to_set_offense_date(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets the charge date, if possible.

        The sentence.off_dtd field from ID has various formats, most of which are
        parseable by our parse_date logic. Some of these are not parseable, however,
        and we are content to simply not set this field in those cases.

        It's also possible that a small number of attempts will parse the date incorrectly,
        e.g. "083094" does not parse to August 30, 1994 as expected, but rather to
        September 4, 830.

        As an precaution against this, we drop any dates that are parsed without error
        but produce years that are not in the 20th or 21st centuries, which we assume to
        be incorrect and unintended. This is imperfect, but we accept this small number
        of errors in order to have a significant coverage of dates."""
        offense_date_unparsed = row.get("off_dtd")
        if not offense_date_unparsed:
            return

        try:
            offense_date = parse_date(offense_date_unparsed)
        except ValueError:
            return

        for obj in extracted_objects:
            if isinstance(obj, StateCharge):
                if offense_date and 1900 <= offense_date.year <= 2100:
                    obj.offense_date = str(offense_date)

    @staticmethod
    def _set_extra_sentence_fields(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets additional fields on sentence objects ingested from the given row of data."""
        is_life = bool(row.get("lifer"))
        max_years = row.get("sent_max_yr")
        max_months = row.get("sent_max_mo")
        max_days = row.get("sent_max_da")
        min_years = row.get("sent_min_yr")
        min_months = row.get("sent_min_mo")
        min_days = row.get("sent_min_da")

        for obj in extracted_objects:
            if isinstance(obj, (StateIncarcerationSentence, StateSupervisionSentence)):
                start_date = obj.start_date
                max_time = safe_parse_days_from_duration_pieces(
                    years_str=max_years,
                    months_str=max_months,
                    days_str=max_days,
                    start_dt_str=start_date,
                )
                min_time = safe_parse_days_from_duration_pieces(
                    years_str=min_years,
                    months_str=min_months,
                    days_str=min_days,
                    start_dt_str=start_date,
                )

                if max_time:
                    obj.max_length = str(max_time)
                if min_time:
                    obj.min_length = str(min_time)
            if isinstance(obj, StateIncarcerationSentence):
                obj.is_life = str(is_life)
            if isinstance(obj, StateSupervisionSentence):
                obj.supervision_type = (
                    StateSupervisionSentenceSupervisionType.PROBATION.value
                )

    @staticmethod
    def _override_facilities(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Overrides the recorded facility for the person if IDOC data indicates that the person is at another
        location.
        """
        location_cd = row.get(CURRENT_LOCATION_CODE, "")
        location_name = row.get(CURRENT_LOCATION_NAME, "")
        if (
            not (location_cd and location_name) or location_cd == "001"
        ):  # Present at facility
            return

        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                obj.facility = location_name

    @staticmethod
    def _set_custodial_authority(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets custodial authority on the StateSupervisionPeriod entities."""
        facility_cd = row.get(CURRENT_FACILITY_CODE, "")
        location_name = row.get(CURRENT_LOCATION_NAME, "")
        living_unit_cd = row.get(CURRENT_LIVING_UNIT_CODE, "")

        # Default values
        custodial_authority_code = facility_cd

        if location_name in FEDERAL_CUSTODY_LOCATION_NAMES:
            custodial_authority_code = FEDERAL_CUSTODY_LOCATION_CODE
        # People on limited supervision are marked as a part of 'DISTRICT 4' in the data because it started out as a
        # District 4 only program. Now, however, they count anyone on limited supervision as a part of DISTRICT 0
        elif location_name == DEPORTED_LOCATION_NAME:
            custodial_authority_code = DEPORTED_LOCATION_NAME
        elif living_unit_cd == LIMITED_SUPERVISION_LIVING_UNIT:
            custodial_authority_code = "D0"

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod) and custodial_authority_code:
                obj.custodial_authority = custodial_authority_code

    @staticmethod
    def _override_supervision_type(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """If necessary, overrides supervision level for supervision periods based on supervision level data that
        appears in the non-standard wrkld_cat location.
        """

        living_unit_cd = row.get(CURRENT_LIVING_UNIT_CODE, "")
        supervision_type_override = None
        if living_unit_cd in (BENCH_WARRANT_LIVING_UNIT, COURT_PROBATION_LIVING_UNIT):
            supervision_type_override = living_unit_cd

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                if supervision_type_override:
                    obj.supervision_type = supervision_type_override

    # TODO(#2912): Add custodial authority to incarceration periods
    @staticmethod
    def _set_supervision_site(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets supervision_site based on granular location info."""
        facility_cd = row.get(CURRENT_FACILITY_CODE, "")
        facility_name = row.get(CURRENT_FACILITY_NAME, "")
        location_name = row.get(CURRENT_LOCATION_NAME, "")
        living_unit_cd = row.get(CURRENT_LIVING_UNIT_CODE, "")
        living_unit_name = row.get(CURRENT_LIVING_UNIT_NAME, "")

        # default values
        supervision_site: Optional[str] = create_supervision_site(
            supervising_district_id=facility_name,
            supervision_specific_location=living_unit_name,
        )

        # Interstate and parole commission facilities mean some non-IDOC entity is supervising the person.
        if facility_cd in (INTERSTATE_FACILITY_CODE, PAROLE_COMMISSION_CODE):
            supervision_site = create_supervision_site(
                supervising_district_id=facility_name,
                supervision_specific_location=location_name,
            )
        # Absconders have no granular facility info.
        elif facility_cd == FUGITIVE_FACILITY_CODE:
            supervision_site = None
        # People on limited supervision are marked as a part of 'DISTRICT 4' in the data because it started out as a
        # District 4 only program. Now, however, they count anyone on limited supervision as a part of DISTRICT 0
        elif living_unit_cd == LIMITED_SUPERVISION_LIVING_UNIT:
            supervision_site = create_supervision_site(
                supervising_district_id=DISTRICT_0,
                supervision_specific_location=LIMITED_SUPERVISION_UNIT_NAME,
            )
        # TODO(#3309): Consider adding warrant spans to schema.
        # Folks with an existing bench warrant are not actively supervised and have no office info.
        elif living_unit_cd == BENCH_WARRANT_LIVING_UNIT:
            supervision_site = create_supervision_site(
                supervising_district_id=facility_name,
                supervision_specific_location=UNKNOWN,
            )
        # Folks with court probation do not have office info.
        elif living_unit_cd == COURT_PROBATION_LIVING_UNIT:
            supervision_site = create_supervision_site(
                supervising_district_id=facility_name,
                supervision_specific_location=UNKNOWN,
            )

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                obj.supervision_site = supervision_site

    @staticmethod
    def _set_invalid_early_discharge_status(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        for obj in extracted_objects:
            if isinstance(obj, StateEarlyDischarge):
                obj.decision_status = StateEarlyDischargeDecisionStatus.INVALID.value

    @staticmethod
    def _set_early_discharge_status(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        for obj in extracted_objects:
            if isinstance(obj, StateEarlyDischarge):
                if obj.decision is not None:
                    obj.decision_status = (
                        StateEarlyDischargeDecisionStatus.DECIDED.value
                    )
                else:
                    obj.decision_status = (
                        StateEarlyDischargeDecisionStatus.PENDING.value
                    )

    @staticmethod
    def _set_generated_ids(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets all recidiviz-created ids on ingested objects. These are combinations of existing fields so that each
        external id is unique among all entities in US_ID.
        """
        person_id = row.get("docno", "")
        if not person_id:
            person_id = row.get("ofndr_num", "")

        mittimus_id = row.get("mitt_srl", "")
        sentence_id = row.get("sent_no", "")
        court_case_id = row.get("caseno", "")
        period_id = row.get("period_id", "")
        violation_id = row.get("ofndr_tst_id", "")
        early_discharge_id = row.get("early_discharge_id", "")
        early_discharge_sent_id = row.get("early_discharge_sent_id", "")

        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationSentence):
                obj.state_incarceration_sentence_id = (
                    f"{person_id}-{mittimus_id}-{sentence_id}"
                )
            if isinstance(obj, StateSupervisionSentence):
                obj.state_supervision_sentence_id = (
                    f"{person_id}-{mittimus_id}-{sentence_id}"
                )

            if isinstance(obj, StateIncarcerationPeriod):
                obj.state_incarceration_period_id = f"{person_id}-{period_id}"
            if isinstance(obj, StateSupervisionPeriod):
                obj.state_supervision_period_id = f"{person_id}-{period_id}"

            # Only one charge per sentence so recycle sentence id for the charge.
            if isinstance(obj, StateCharge):
                obj.state_charge_id = f"{person_id}-{mittimus_id}-{sentence_id}"

            if isinstance(obj, StateCourtCase):
                obj.state_court_case_id = f"{person_id}-{court_case_id}"

            if isinstance(obj, StateSupervisionViolation):
                obj.state_supervision_violation_id = f"{violation_id}"
            # One response per violation, so recycle violation id for the response.
            if isinstance(obj, StateSupervisionViolationResponse):
                obj.state_supervision_violation_response_id = f"{violation_id}"

            # While early_discharge_sent_id is unique for every sentence-level early discharge request,
            # early_discharge_id can be repeated across sentences if discharge was requested for the
            # sentences at the same time. Decisions are made on a sentence level, so we need to include
            # early_discharge_sent_id in the key. We prepend early_discharge_id in case it is useful in calculate to
            # know which sentences had early discharge requested together.
            if isinstance(obj, StateEarlyDischarge):
                obj.state_early_discharge_id = (
                    f"{early_discharge_id}-{early_discharge_sent_id}"
                )

    @staticmethod
    def _add_default_admission_reason(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Adds a default admission reason to supervision/incarceration periods."""
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                if obj.admission_reason is None:
                    obj.admission_reason = (
                        StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value
                    )
            if isinstance(obj, StateSupervisionPeriod):
                if obj.admission_reason is None:
                    obj.admission_reason = (
                        StateSupervisionPeriodAdmissionReason.COURT_SENTENCE.value
                    )

    @staticmethod
    def _add_details_when_transferred_to_history(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """When someone is transferred to history (removed from IDOC custody), we can get some detailed information as
        to why this person was released. This method adds those details to the out edges of Incarceration/Supervision
        periods when they exist.
        """
        next_fac_typ = row.get(NEXT_FACILITY_TYPE, "")
        next_loc_name = row.get(NEXT_LOCATION_NAME, "")

        if next_fac_typ != HISTORY_FACILITY_TYPE:
            return

        for obj in extracted_objects:
            period_end_reason = f"{next_fac_typ}-{next_loc_name}"
            if isinstance(obj, StateIncarcerationPeriod):
                obj.release_reason = period_end_reason
            if isinstance(obj, StateSupervisionPeriod):
                obj.termination_reason = period_end_reason

    @staticmethod
    def _add_incarceration_type(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets incarceration type on incarceration periods based on facility."""
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                if obj.facility is not None and is_jail_facility(obj.facility):
                    obj.incarceration_type = StateIncarcerationType.COUNTY_JAIL.value
                else:
                    obj.incarceration_type = StateIncarcerationType.STATE_PRISON.value

    @staticmethod
    def _clear_max_dates(
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
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
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets supervision period case type from the supervision level if necessary."""
        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                supervision_level = obj.supervision_level
                if (
                    supervision_level
                    and self.get_enum_overrides().parse(
                        supervision_level, StateSupervisionCaseType
                    )
                    is not None
                ):
                    case_type_to_create = StateSupervisionCaseTypeEntry(
                        case_type=supervision_level
                    )
                    create_if_not_exists(
                        case_type_to_create, obj, "state_supervision_case_type_entries"
                    )

    @staticmethod
    def _supervision_period_admission_and_termination_overrides(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Update in and out edges of supervision periods if the normal mappings are insufficient."""
        prev_fac_cd = row.get(PREVIOUS_FACILITY_CODE, "")
        next_fac_cd = row.get(NEXT_FACILITY_CODE, "")
        cur_fac_cd = row.get(CURRENT_FACILITY_CODE, "")

        prev_loc_name = row.get(PREVIOUS_LOCATION_NAME, "")
        next_loc_name = row.get(NEXT_LOCATION_NAME, "")
        cur_loc_name = row.get(CURRENT_LOCATION_NAME, "")

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
                    obj.admission_reason = (
                        StateSupervisionPeriodAdmissionReason.ABSCONSION.value
                    )
                    if obj.termination_date:
                        obj.termination_reason = (
                            StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION.value
                        )

    @staticmethod
    def _hydrate_violation_report_fields(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Adds fields/children to the SupervisionViolationResponses as necessary. This assumes all
        SupervisionViolationResponses are of violation reports.
        """
        parole_recommendations = _split_violation_response_row(
            row.get("parolee_placement_recommendation")
        )
        probation_recommendations = _split_violation_response_row(
            row.get("probationer_placement_recommendation")
        )
        recommendations = [
            rec for rec in parole_recommendations + probation_recommendations if rec
        ]

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolationResponse):
                obj.response_type = (
                    StateSupervisionViolationResponseType.VIOLATION_REPORT.value
                )

                for recommendation in recommendations:
                    if recommendation in VIOLATION_REPORT_NO_RECOMMENDATION_VALUES:
                        continue
                    recommendation_to_create = (
                        StateSupervisionViolationResponseDecisionEntry(
                            decision=recommendation
                        )
                    )
                    create_if_not_exists(
                        recommendation_to_create,
                        obj,
                        "state_supervision_violation_response_decisions",
                    )

    @staticmethod
    def _hydrate_violation_types(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Adds ViolationTypeEntries onto the already generated SupervisionViolations."""
        violation_types = sorted_list_from_str(row.get("violation_types", ""))

        if not violation_types:
            return

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolation):
                for violation_type in violation_types:
                    violation_type_to_create = StateSupervisionViolationTypeEntry(
                        violation_type=violation_type
                    )
                    create_if_not_exists(
                        violation_type_to_create,
                        obj,
                        "state_supervision_violation_types",
                    )

    @staticmethod
    def _set_violation_violent_sex_offense(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Sets the fields `is_violent` and `is_sex_offense` onto StateSupervisionViolations based on fields passed in
        through the |row|.
        """
        new_crime_types = sorted_list_from_str(row.get("new_crime_types", ""))

        if not all(ct in ALL_NEW_CRIME_TYPES for ct in new_crime_types):
            raise ValueError(f"Unexpected new crime type: {new_crime_types}")

        violent = any(ct in VIOLENT_CRIME_TYPES for ct in new_crime_types)
        sex_offense = any(ct in SEX_CRIME_TYPES for ct in new_crime_types)

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolation):
                obj.is_violent = str(violent)
                obj.is_sex_offense = str(sex_offense)

    @staticmethod
    def _add_supervising_officer(
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        agent_id = row.get("empl_sdesc", "")
        agent_name = row.get("empl_ldesc", "")
        if not agent_id or not agent_name or agent_id == UNKNOWN_EMPLOYEE_SDESC:
            return

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                agent_to_create = StateAgent(
                    state_agent_id=agent_id,
                    full_name=agent_name,
                    agent_type=StateAgentType.SUPERVISION_OFFICER.value,
                )
                create_if_not_exists(agent_to_create, obj, "supervising_officer")


def _get_bool_from_row(arg: str, row: Dict[str, str]) -> bool:
    val = row.get(arg)
    if not val:
        return False
    return str_to_bool(val)


def _split_violation_response_row(row: Optional[str]) -> List[str]:
    if not row:
        return []
    split_row = []
    for comma_containing_constant in VIOLATION_REPORT_CONSTANTS_INCLUDING_COMMA:
        if comma_containing_constant in row:
            row = row.replace(comma_containing_constant, "")
            split_row.append(comma_containing_constant)
    split_row.extend(val for val in row.split(",") if val)
    return split_row


def create_supervision_site(
    supervising_district_id: str, supervision_specific_location: str
) -> str:
    """Returns a string which combines district and specific supervision office/location information."""
    return supervising_district_id + "|" + supervision_specific_location
