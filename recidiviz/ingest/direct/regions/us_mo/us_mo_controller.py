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
"""Direct ingest controller implementation for US_MO."""
import datetime
import logging
import re
from enum import Enum
from typing import Callable, Dict, List, Optional, Type

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common import ncic
from recidiviz.common.constants.enum_overrides import (
    EnumIgnorePredicate,
    EnumMapperFn,
    EnumOverrides,
)
from recidiviz.common.constants.standard_enum_overrides import (
    get_standard_enum_overrides,
)
from recidiviz.common.constants.state.external_id_types import US_MO_DOC
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_charge import StateChargeClassificationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import munge_date_string
from recidiviz.common.str_field_utils import (
    parse_days,
    parse_days_from_duration_pieces,
    parse_yyyymmdd_date,
    sorted_list_from_str,
)
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.controllers.legacy_ingest_view_processor import (
    IngestAncestorChainOverridesCallable,
    IngestPrimaryKeyOverrideCallable,
    IngestRowPosthookCallable,
    LegacyIngestViewProcessorDelegate,
)
from recidiviz.ingest.direct.direct_ingest_controller_utils import (
    create_if_not_exists,
    update_overrides_from_maps,
)
from recidiviz.ingest.direct.regions.us_mo.us_mo_constants import (
    CHARGE_COUNTY_CODE,
    CITATION_ID_PREFIX,
    CITATION_KEY_SEQ,
    CYCLE_ID,
    DOC_ID,
    FIELD_KEY_SEQ,
    INCARCERATION_SENTENCE_LENGTH_DAYS,
    INCARCERATION_SENTENCE_LENGTH_MONTHS,
    INCARCERATION_SENTENCE_LENGTH_YEARS,
    INCARCERATION_SENTENCE_MIN_RELEASE_TYPE,
    INCARCERATION_SENTENCE_PAROLE_INELIGIBLE_YEARS,
    INCARCERATION_SENTENCE_START_DATE,
    MOST_RECENT_SENTENCE_STATUS_CODE,
    MOST_RECENT_SENTENCE_STATUS_DATE,
    ORAS_ASSESSMENT_ID,
    ORAS_ASSESSMENTS_DOC_ID,
    PERIOD_CLOSE_CODE,
    PERIOD_CLOSE_CODE_SUBTYPE,
    SENTENCE_COMPLETED_FLAG,
    SENTENCE_COUNTY_CODE,
    SENTENCE_KEY_SEQ,
    SENTENCE_OFFENSE_DATE,
    SUPERVISION_SENTENCE_LENGTH_DAYS,
    SUPERVISION_SENTENCE_LENGTH_MONTHS,
    SUPERVISION_SENTENCE_LENGTH_YEARS,
    SUPERVISION_SENTENCE_TYPE,
    SUPERVISION_VIOLATION_RECOMMENDATIONS,
    SUPERVISION_VIOLATION_TYPES,
    SUPERVISION_VIOLATION_VIOLATED_CONDITIONS,
    TAK076_PREFIX,
    TAK291_PREFIX,
    VIOLATION_KEY_SEQ,
    VIOLATION_REPORT_ID_PREFIX,
)
from recidiviz.ingest.direct.regions.us_mo.us_mo_enum_helpers import (
    MID_INCARCERATION_TREATMENT_COMMITMENT_STATUSES,
    MID_INCARCERATION_TREATMENT_FAILURE_STATUSES,
    PAROLE_REVOKED_WHILE_INCARCERATED_STATUS_CODES,
    incarceration_period_admission_reason_mapper,
    supervising_officer_mapper,
    supervision_period_admission_reason_mapper,
    supervision_period_termination_reason_mapper,
)
from recidiviz.ingest.direct.state_shared_row_posthooks import (
    IngestGatingContext,
    copy_name_to_alias,
    gen_convert_person_ids_to_external_id_objects,
    gen_label_single_external_id_hook,
    gen_map_ymd_counts_to_max_length_field_posthook,
    gen_normalize_county_codes_posthook,
    gen_set_field_as_concatenated_values_hook,
    gen_set_is_life_sentence_hook,
)
from recidiviz.ingest.extractor.csv_data_extractor import IngestFieldCoordinates
from recidiviz.ingest.models.ingest_info import (
    IngestObject,
    StateAgent,
    StateCharge,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateSentenceGroup,
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolatedConditionEntry,
    StateSupervisionViolation,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.ingest.models.ingest_object_cache import IngestObjectCache


# TODO(#8899): Delete LegacyIngestViewProcessorDelegate superclass when we have fully
#  migrated this state to new ingest mappings version.
class UsMoController(BaseDirectIngestController, LegacyIngestViewProcessorDelegate):
    """Direct ingest controller implementation for US_MO."""

    PERIOD_SEQUENCE_PRIMARY_COL_PREFIX = "F1"

    PRIMARY_COL_PREFIXES_BY_FILE_TAG = {
        "tak001_offender_identification": "EK",
        "tak040_offender_cycles": "DQ",
        "tak022_tak023_tak025_tak026_offender_sentence_institution": "BS",
        "tak022_tak024_tak025_tak026_offender_sentence_supervision": "BS",
        "tak158_tak023_tak026_incarceration_period_from_incarceration_sentence": "BT",
        "tak158_tak024_tak026_incarceration_period_from_supervision_sentence": "BU",
        "tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods": "",
        "tak028_tak042_tak076_tak024_violation_reports": "BY",
        "tak291_tak292_tak024_citations": "JT",
    }

    REVOKED_PROBATION_SENTENCE_STATUS_CODES = {
        "45O2000",  # Prob Rev - Technical
        "45O2005",  # Prob Rev - New Felony Conv
        "45O2015",  # Prob Rev - Felony Law Viol
        "45O2010",  # Prob Rev - New Misd Conv
        "45O2020",  # Prob Rev - Misd Law Viol
    }

    SUSPENDED_SENTENCE_STATUS_CODES = {
        "35I3500",  # Bond Supv-Pb Suspended-Revisit
        "65O2015",  # Court Probation Suspension
        "65O3015",  # Court Parole Suspension
        "95O3500",  # Bond Supv-Pb Susp-Completion
        "95O3505",  # Bond Supv-Pb Susp-Bond Forfeit
        "95O3600",  # Bond Supv-Pb Susp-Trm-Tech
        "95O7145",  # DATA ERROR-Suspended
    }

    COMMUTED_SENTENCE_STATUS_CODES = {
        "90O1020",  # Institutional Commutation Comp
        "95O1025",  # Field Commutation
        "99O1020",  # Institutional Commutation
        "99O1025",  # Field Commutation
    }

    # TODO(#2604): Figure out if we should do anything special with these
    SENTENCE_MAGICAL_DATES = ["0", "20000000", "66666666", "88888888", "99999999"]
    PERIOD_MAGICAL_DATES = ["0", "99999999"]

    # TODO(#2898): Complete transition to TAK026 for IncarcerationPeriod statuses
    ENUM_MAPPER_FUNCTIONS: Dict[Type[Enum], EnumMapperFn] = {
        StateAgentType: supervising_officer_mapper,
        StateSupervisionPeriodAdmissionReason: supervision_period_admission_reason_mapper,
        StateSupervisionPeriodTerminationReason: supervision_period_termination_reason_mapper,
        StateIncarcerationPeriodAdmissionReason: incarceration_period_admission_reason_mapper,
    }

    ENUM_IGNORE_PREDICATES: Dict[Type[Enum], EnumIgnorePredicate] = {}

    ENUM_OVERRIDES: Dict[Enum, List[str]] = {
        StateSupervisionLevel.INTERNAL_UNKNOWN: [
            "CPP",  # Residential Community Placement (treatment)
            "EMP",  # Electronic Monitoring
            "ESC",  # Absconder
            "IAP",  # Initial Assessment Phase
            "NSV",  # Not Under Supervision (case closed by court/board but still open at district level)
            # The following codes are no longer used according to MO, but may appear in historical records
            "ENH",  # Enhanced
            "MIN",  # Minimum
            "MSC",  # Automated Minimum
            "NOI",  # No Contract Monitoring Level I
            "NII",  # No Contract Monitoring Level II
            "PSI",  # Pre Sentence Investigation
            "REG",  # Regular Supervision
            "RTF",  # Residential Treatment facility
            "VCT",  # Residential Violator Center
        ],
        StateSupervisionLevel.MINIMUM: [
            "OOI",  # Low Risk (low risk on ORAS)
        ],
        StateSupervisionLevel.MEDIUM: [
            "OII",  # Moderate Risk (moderate/low or moderate on ORAS)
        ],
        StateSupervisionLevel.HIGH: [
            "III",  # High Risk (high risk on ORAS)
        ],
        StateSupervisionLevel.MAXIMUM: [
            "ISP",  # Intensive Supervision (specialized programming + very high risk on ORAS)
        ],
        StateSupervisionLevel.INCARCERATED: [
            "ITC",  # Institutional Treatment Center
            "JAL",  # Incarceration/Jail
            "PRS",  # Incarceration/Prison
            "SHK",  # 120 Day Incarceration
        ],
        StateChargeClassificationType.INFRACTION: ["L"],  # Local/ordinance
        StateSupervisionType.INTERNAL_UNKNOWN: ["UNKNOWN"],
        StateSupervisionType.PAROLE: [
            "PAROLE",
        ],
        StateSupervisionType.PROBATION: ["PROBATION"],
        StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE: [
            # TODO(#2898) - Use TAK026 statuses to populate release reason
            "BP-FF",  # Board Parole
            "BP-FM",
            "BP-FT",
            "FB-TR",  # Field Administrative
            "CR-FF",  # Conditional Release
            "CR-FT",
            "IT-BD",  # Institutional Release to Supervision
            "IT-BH",
            "IT-BP",
            "IT-CD",
            "IT-CR",
            "IT-EM",
            "IT-IC",
            "IT-IN",
            "IT-RF",
            "IC-IC",  # Institutional Release to Probation
            "RT-BH",  # Board Return
            # Inmate Release to EMP (electronic monitoring program). Only 1
            # occurrence of any of these in the last 10 years.
            "EM-FB",
            "EM-FF",
            "EM-FM",
            "EM-FT",
        ],
        StateIncarcerationPeriodReleaseReason.COURT_ORDER: [
            # TODO(#2898) - Use TAK026 statuses to populate release reason
            "IB-EI",  # Institutional Administrative
            "IB-ER",
            "IB-IB",
            "IB-RB",
            "IB-RR",
            "IB-TR",
            "IB-XX",
            "OR-OR",  # Off Records; Suspension
        ],
        StateIncarcerationPeriodReleaseReason.DEATH: [
            # TODO(#2898) - Use TAK026 statuses to populate release reason
            "DE-DE",  # Death
            "DE-XX",
        ],
        StateIncarcerationPeriodReleaseReason.ESCAPE: [
            # TODO(#2898) - Use TAK026 statuses to populate release reason
            "IE-IE",  # Institutional Escape
            "IE-XX",
            "IW-IW",  # Institutional Walkaway
            "IW-XX",
        ],
        StateIncarcerationPeriodReleaseReason.EXECUTION: [
            # TODO(#2898) - Use TAK026 statuses to populate release reason
            "DE-EX",  # Execution
        ],
        StateIncarcerationPeriodReleaseReason.EXTERNAL_UNKNOWN: [
            # TODO(#2898) - Use TAK026 statuses to populate release reason
            "XX-XX",  # Unknown (Not Associated)
            "??-??",  # Code Unknown
            "??-XX",
        ],
        StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN: [
            "CN-FB",  # Committed New Charge- No Vio: seems erroneous
            "CN-NV",
            "RV-FF",  # Revoked: seems erroneous
            "RV-FM",
            "RV-FT",
        ],
        StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY: [
            # These statuses indicate an end to a period of temporary hold since
            # it has now been determined that the person has had their parole
            # revoked or has been given some other sanction. With the exception of a few
            # rare cases, these statuses are always closing a board hold period when
            # seen as an end code.
            *PAROLE_REVOKED_WHILE_INCARCERATED_STATUS_CODES,
            *MID_INCARCERATION_TREATMENT_COMMITMENT_STATUSES,
        ],
        StateIncarcerationPeriodReleaseReason.STATUS_CHANGE: [
            # These statuses indicate a failure of treatment causing mandate to serve
            # rest of sentence
            *MID_INCARCERATION_TREATMENT_FAILURE_STATUSES,
        ],
        StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED: [
            # TODO(#2898) - Use TAK026 statuses to populate release reason
            "DC-DC",  # Discharge
            "DC-DO",  # Inst. Converted-Inactive
            "DC-XX",  # Discharge - Unknown
            "ID-DC",  # Institutional Discharge - Discharge
            "ID-DO",  # Institutional Discharge - Other
            "ID-DR",  # Institutional Discharge - Director's Release
            "ID-ID",  # Institutional Discharge - Institutional Discharge
            "ID-PD",  # Institutional Discharge - Pardoned
            "ID-RR",  # Institutional Discharge - Reversed and Remanded
            "ID-XX",  # Institutional Discharge - Unknown
        ],
        StateSpecializedPurposeForIncarceration.GENERAL: [
            "S",  # Serving Sentence
        ],
        StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION: [
            "O",  # 120-Day Shock
            "R",  # Regimented Disc Program
        ],
        StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON: [
            "A",  # Assessment
            "I",  # Inst Treatment Center
            "L",  # Long Term Drug Treatment
        ],
        StateSupervisionCaseType.DOMESTIC_VIOLENCE: [
            "DVS",  # Domestic Violence Supervision
            "DOM",  # Domestic Violence
        ],
        StateSupervisionCaseType.SERIOUS_MENTAL_ILLNESS: [
            "SMI",  # Seriously Mentally Ill Caseload
        ],
        StateSupervisionCaseType.SEX_OFFENSE: [
            "DSO",  # Designated Sex Offenders
            "ISO",  # Interstate Sex Offenders
        ],
        StateSupervisionViolationResponseDecidingBodyType.COURT: [
            "CT",  # New Court Commitment
            "IS",  # Interstate Case
        ],
        StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD: [
            "BH",  # Board Holdover
            "BP",  # Board Parole
        ],
        StateSupervisionViolationType.ABSCONDED: ["A"],
        StateSupervisionViolationType.ESCAPED: ["E"],
        StateSupervisionViolationType.FELONY: ["F"],
        StateSupervisionViolationType.MISDEMEANOR: ["M"],
        StateSupervisionViolationType.MUNICIPAL: ["O"],
        StateSupervisionViolationType.TECHNICAL: ["T"],
        StateSupervisionViolationResponseDecision.REVOCATION: [
            "I",  # Inmate Return
            "R",  # Revocation
        ],
        StateSupervisionViolationResponseDecision.CONTINUANCE: ["C"],
        StateSupervisionViolationResponseDecision.DELAYED_ACTION: ["D"],
        StateSupervisionViolationResponseDecision.EXTENSION: ["E"],
        StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION: [
            "CO",  # Court Ordered Detention Sanction
        ],
        StateSupervisionViolationResponseDecision.SUSPENSION: ["S"],
        StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED: [
            "RN"  # SIS revoke to SES
        ],
        StateSupervisionViolationResponseDecision.SERVICE_TERMINATION: ["T"],
        StateSupervisionViolationResponseDecision.WARRANT_ISSUED: [
            "A",  # Capias,
        ],
        StateAssessmentType.INTERNAL_UNKNOWN: [
            "Diversion Instrument"  # One record with this entry in DB.
        ],
        StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING: [
            "Community Supervision Screening Tool - 9 Items"
        ],
        StateAssessmentType.ORAS_COMMUNITY_SUPERVISION: ["Community Supervision Tool"],
        StateAssessmentType.ORAS_PRISON_INTAKE: ["Prison Intake Tool"],
        StateAssessmentType.ORAS_REENTRY: ["Reentry Tool", "Reentry Instrument"],
        StateAssessmentType.ORAS_SUPPLEMENTAL_REENTRY: ["Supplemental Reentry Tool"],
        StateAssessmentLevel.LOW: ["Low"],
        StateAssessmentLevel.LOW_MEDIUM: ["Low/Moderate"],
        StateAssessmentLevel.MEDIUM: ["Moderate"],
        StateAssessmentLevel.HIGH: ["High"],
        StateAssessmentLevel.VERY_HIGH: ["Very High"],
    }

    ENUM_IGNORES: Dict[Type[Enum], List[str]] = {
        StateSupervisionType: ["INT"],  # Unknown meaning, rare
        StateSpecializedPurposeForIncarceration: [
            "X",  # Unknown
        ],
        StateSupervisionViolationResponseDecision: [
            "NOREC",  # No Recommendation
        ],
    }

    @classmethod
    def region_code(cls) -> str:
        return StateCode.US_MO.value.lower()

    def __init__(self, ingest_bucket_path: GcsfsBucketPath):
        super().__init__(ingest_bucket_path)

        self.enum_overrides = self.generate_enum_overrides()
        self.row_pre_processors_by_file: Dict[str, List[Callable]] = {}

        incarceration_period_row_posthooks: List[IngestRowPosthookCallable] = [
            self._replace_invalid_release_date,
            self._gen_clear_magical_date_value(
                "release_date", self.PERIOD_MAGICAL_DATES, StateIncarcerationPeriod
            ),
            self._set_incarceration_period_status,
            gen_set_field_as_concatenated_values_hook(
                StateIncarcerationPeriod,
                "release_reason",
                [PERIOD_CLOSE_CODE, PERIOD_CLOSE_CODE_SUBTYPE],
            ),
        ]

        tak001_offender_identification_row_processors: List[Callable] = [
            copy_name_to_alias,
            # When first parsed, the info object just has a single
            # external id - the DOC id.
            gen_label_single_external_id_hook(US_MO_DOC),
            self.normalize_sentence_group_ids,
        ]
        tak040_offender_cycles_row_processors: List[Callable] = [
            gen_label_single_external_id_hook(US_MO_DOC),
            self.normalize_sentence_group_ids,
        ]

        tak291_tak292_tak024_citations_row_processors: List[Callable] = [
            self._gen_violation_response_type_posthook(
                StateSupervisionViolationResponseType.CITATION
            ),
            self._set_deciding_body_as_supervising_officer,
            self._set_violated_conditions_on_violation,
            self._set_violation_response_id_from_violation,
            self._set_finally_formed_date_on_response,
        ]

        tak028_tak042_tak076_tak024_violation_reports_row_processors: List[Callable] = [
            self._gen_violation_response_type_posthook(
                StateSupervisionViolationResponseType.VIOLATION_REPORT
            ),
            self._set_deciding_body_as_supervising_officer,
            self._set_violated_conditions_on_violation,
            self._set_violation_type_on_violation,
            self._set_recommendations_on_violation_response,
            self._set_violation_response_id_from_violation,
            self._set_finally_formed_date_on_response,
            self._set_decision_agent,
        ]
        tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods_row_processors: List[
            IngestRowPosthookCallable
        ] = [
            self._gen_clear_magical_date_value(
                "termination_date", self.PERIOD_MAGICAL_DATES, StateSupervisionPeriod
            ),
            self._set_supervising_officer_on_period,
            self._parse_case_types,
            self._set_empty_admisssion_releases_as_transfers,
        ]

        self.row_post_processors_by_file: Dict[str, List[IngestRowPosthookCallable]] = {
            # SQL Preprocessing View
            "tak001_offender_identification": tak001_offender_identification_row_processors,
            "tak040_offender_cycles": tak040_offender_cycles_row_processors,
            "tak022_tak023_tak025_tak026_offender_sentence_institution": self.get_tak022_tak023_tak025_tak026_offender_sentence_institution_row_processors(),
            "tak022_tak024_tak025_tak026_offender_sentence_supervision": self.get_tak022_tak023_tak025_tak026_offender_sentence_supervision_row_processors(),
            "tak158_tak023_tak026_incarceration_period_from_incarceration_sentence": incarceration_period_row_posthooks,
            "tak158_tak024_tak026_incarceration_period_from_supervision_sentence": incarceration_period_row_posthooks,
            "tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods": tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods_row_processors,
            "tak028_tak042_tak076_tak024_violation_reports": tak028_tak042_tak076_tak024_violation_reports_row_processors,
            "tak291_tak292_tak024_citations": tak291_tak292_tak024_citations_row_processors,
        }

        self.primary_key_override_by_file: Dict[
            str, IngestPrimaryKeyOverrideCallable
        ] = {
            # SQL Preprocessing View
            "oras_assessments_weekly": self._generate_assessment_id_coords,
            "tak022_tak023_tak025_tak026_offender_sentence_institution": self._generate_incarceration_sentence_id_coords,
            "tak022_tak024_tak025_tak026_offender_sentence_supervision": self._generate_supervision_sentence_id_coords,
            "tak158_tak023_tak026_incarceration_period_from_incarceration_sentence": self._generate_incarceration_period_id_coords,
            "tak158_tak024_tak026_incarceration_period_from_supervision_sentence": self._generate_incarceration_period_id_coords,
            "tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods": self._generate_supervision_period_id_coords,
            "tak028_tak042_tak076_tak024_violation_reports": self._generate_supervision_violation_id_coords_for_reports,
            "tak291_tak292_tak024_citations": self._generate_supervision_violation_id_coords_for_citations,
        }

        self.ancestor_chain_override_by_file: Dict[
            str, IngestAncestorChainOverridesCallable
        ] = {
            # SQL Preprocessing View
            "tak022_tak023_tak025_tak026_offender_sentence_institution": self._sentence_group_ancestor_chain_override,
            "tak022_tak024_tak025_tak026_offender_sentence_supervision": self._sentence_group_ancestor_chain_override,
            "tak158_tak023_tak026_incarceration_period_from_incarceration_sentence": self._incarceration_sentence_ancestor_chain_override,
            "tak158_tak024_tak026_incarceration_period_from_supervision_sentence": self._supervision_sentence_ancestor_chain_override,
            "tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods": self._sentence_group_ancestor_chain_override,
            "tak028_tak042_tak076_tak024_violation_reports": self._supervision_violation_report_ancestor_chain_override,
            "tak291_tak292_tak024_citations": self._supervision_violation_citation_ancestor_chain_override,
        }

    def get_file_tag_rank_list(self) -> List[str]:
        file_tags = [
            # SQL Preprocessing View
            "tak001_offender_identification",
            "oras_assessments_weekly",
            "tak040_offender_cycles",
            "tak022_tak023_tak025_tak026_offender_sentence_institution",
            "tak022_tak024_tak025_tak026_offender_sentence_supervision",
            "tak158_tak023_tak026_incarceration_period_from_incarceration_sentence",
            "tak158_tak024_tak026_incarceration_period_from_supervision_sentence",
            "tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods",
            "tak028_tak042_tak076_tak024_violation_reports",
            "tak291_tak292_tak024_citations",
        ]
        return file_tags

    # TODO(#8899): Delete LegacyIngestViewProcessorDelegate methods when we have fully
    #  migrated this state to new ingest mappings version.
    def get_row_pre_processors_for_file(self, file_tag: str) -> List[Callable]:
        return self.row_pre_processors_by_file.get(file_tag, [])

    def get_row_post_processors_for_file(
        self, file_tag: str
    ) -> List[IngestRowPosthookCallable]:
        return self.row_post_processors_by_file.get(file_tag, [])

    def get_file_post_processors_for_file(self, _file_tag: str) -> List[Callable]:
        post_processors: List[Callable] = [
            gen_convert_person_ids_to_external_id_objects(self._get_id_type),
        ]
        return post_processors

    def get_primary_key_override_for_file(
        self, file_tag: str
    ) -> Optional[IngestPrimaryKeyOverrideCallable]:
        return self.primary_key_override_by_file.get(file_tag, None)

    def get_ancestor_chain_overrides_callback_for_file(
        self, file_tag: str
    ) -> Optional[Callable]:
        return self.ancestor_chain_override_by_file.get(file_tag, None)

    def get_files_to_set_with_empty_values(self) -> List[str]:
        return []

    @classmethod
    def generate_enum_overrides(cls) -> EnumOverrides:
        """Provides Missouri-specific overrides for enum mappings."""
        base_overrides = get_standard_enum_overrides()
        return update_overrides_from_maps(
            base_overrides,
            cls.ENUM_OVERRIDES,
            cls.ENUM_IGNORES,
            cls.ENUM_MAPPER_FUNCTIONS,
            cls.ENUM_IGNORE_PREDICATES,
        )

    def get_enum_overrides(self) -> EnumOverrides:
        return self.enum_overrides

    @staticmethod
    def _get_id_type(file_tag: str) -> Optional[str]:
        if file_tag in [
            # SQL Preprocessing View
            "oras_assessments_weekly",
            "tak022_tak023_tak025_tak026_offender_sentence_institution",
            "tak022_tak024_tak025_tak026_offender_sentence_supervision",
            "tak158_tak023_tak026_incarceration_period_from_incarceration_sentence",
            "tak158_tak024_tak026_incarceration_period_from_supervision_sentence",
            "tak034_tak026_tak039_apfx90_apfx91_supervision_enhancements_supervision_periods",
            "tak028_tak042_tak076_tak024_violation_reports",
            "tak291_tak292_tak024_citations",
        ]:
            return US_MO_DOC

        return None

    @classmethod
    def tak022_tak023_set_parole_eligibility_date(
        cls,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        sentence_start_date = parse_yyyymmdd_date(
            row[INCARCERATION_SENTENCE_START_DATE]
        )
        if not sentence_start_date:
            return

        parole_ineligible_days = parse_days_from_duration_pieces(
            years_str=row[INCARCERATION_SENTENCE_PAROLE_INELIGIBLE_YEARS]
        )

        date = sentence_start_date + datetime.timedelta(days=parole_ineligible_days)

        date_iso = date.isoformat()
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationSentence):
                obj.parole_eligibility_date = date_iso

    @classmethod
    def set_charge_id_from_sentence_id(
        cls,
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationSentence):
                sentence_id = obj.state_incarceration_sentence_id
                charges = obj.state_charges
            elif isinstance(obj, StateSupervisionSentence):
                sentence_id = obj.state_supervision_sentence_id
                charges = obj.state_charges
            else:
                continue

            if len(charges) > 1:
                raise ValueError(
                    f"Expected a maximum of one charge per sentence, but found sentence {sentence_id} "
                    f"with {str(len(charges))} charges"
                )

            if charges:
                charges[0].state_charge_id = sentence_id

    @classmethod
    def _set_charge_is_violent_from_ncic(
        cls,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        ncic_code = row.get("BS_NCI", None)
        if not ncic_code:
            return

        is_violent = ncic.get_is_violent(ncic_code)
        if is_violent is None:
            return

        for extracted_object in extracted_objects:
            if isinstance(extracted_object, StateCharge):
                extracted_object.is_violent = str(is_violent)

    @classmethod
    def _gen_violation_response_type_posthook(
        cls, response_type: StateSupervisionViolationResponseType
    ) -> IngestRowPosthookCallable:
        def _set_response_type(
            _gating_context: IngestGatingContext,
            _row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache,
        ) -> None:
            for obj in extracted_objects:
                if isinstance(obj, StateSupervisionViolation):
                    for response in obj.state_supervision_violation_responses:
                        response.response_type = response_type.value

        return _set_response_type

    @classmethod
    def _set_deciding_body_as_supervising_officer(
        cls,
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolation):
                for response in obj.state_supervision_violation_responses:
                    response.deciding_body_type = (
                        StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER.value
                    )

    # TODO(#2701): Remove posthook in place of general child-id setting solution.
    @classmethod
    def _set_violation_response_id_from_violation(
        cls,
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolation):
                violation_id = obj.state_supervision_violation_id
                for response in obj.state_supervision_violation_responses:
                    response.state_supervision_violation_response_id = violation_id

    @classmethod
    def _set_finally_formed_date_on_response(
        cls,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Finally formed documents are the ones that are no longer in a draft state.

        Updates the SupervisionViolationResponses in |extracted_objects| based on whether or not a finally formed
        date is present in the given |row|.
        """
        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolation):
                julian_date_str = row.get("FINAL_FORMED_CREATE_DATE", None)
                finally_formed_date = cls.mo_julian_date_to_iso(julian_date_str)

                for response in obj.state_supervision_violation_responses:
                    is_draft = True
                    if finally_formed_date:
                        response.response_date = finally_formed_date
                        is_draft = False
                    response.is_draft = str(is_draft)

    @classmethod
    def _set_violated_conditions_on_violation(
        cls,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Manually adds StateSupervisionViolatedConditionEntries to StateSupervisionViolations."""
        conditions_txt = row.get(SUPERVISION_VIOLATION_VIOLATED_CONDITIONS, "")
        if conditions_txt == "":
            return
        conditions = conditions_txt.split(",")

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolation):
                for condition in conditions:
                    # We assign the violated conditions to the is_violent field as a
                    # hack in the ingest mappings to make sure that the
                    # StateSupervisionViolation entity is processed by this post-hook,
                    # but the is_violent field should always be unset
                    obj.is_violent = None
                    vc = StateSupervisionViolatedConditionEntry(condition=condition)
                    create_if_not_exists(
                        vc, obj, "state_supervision_violated_conditions"
                    )

    @classmethod
    def _set_violation_type_on_violation(
        cls,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Manually adds StateSupervisionViolationTypeEntries to StateSupervisionViolations."""
        violation_types_txt = row.get(SUPERVISION_VIOLATION_TYPES, "")
        if violation_types_txt == "":
            return
        violation_types = list(violation_types_txt)

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolation):
                for violation_type in violation_types:
                    vt = StateSupervisionViolationTypeEntry(
                        violation_type=violation_type
                    )
                    create_if_not_exists(vt, obj, "state_supervision_violation_types")

    def _set_recommendations_on_violation_response(
        self,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Manually adds StateSupervisionViolationResponses to StateSupervisionViolations."""
        recommendation_txt = row.get(SUPERVISION_VIOLATION_RECOMMENDATIONS, "")
        # Return if there is no recommendation, or if the text explicitly refers
        # to either "No Recommendation".
        if recommendation_txt in ("", "NOREC"):
            return

        if recommendation_txt in ("CO", "RN"):
            # CO and RN are the only recommendations we process that are more
            # than one letter, and they do not occur with any other
            # recommendations.
            recommendations = [recommendation_txt]
        else:
            # If not one of the above recommendations, any number of single
            # character recommendations can be provided.
            recommendations = list(recommendation_txt)

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolation):
                for response in obj.state_supervision_violation_responses:
                    for recommendation in recommendations:
                        rec = StateSupervisionViolationResponseDecisionEntry(
                            decision=recommendation,
                        )
                        create_if_not_exists(
                            rec,
                            response,
                            "state_supervision_violation_response_decisions",
                        )

    @classmethod
    def _set_decision_agent(
        cls,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Adds the responding agent to any SupervisionViolationRespones."""
        agent_to_create = cls._get_agent_from_row(row)
        if not agent_to_create:
            return

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionViolation):
                for response in obj.state_supervision_violation_responses:
                    create_if_not_exists(agent_to_create, response, "decision_agents")

    @classmethod
    def _set_supervising_officer_on_period(
        cls,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:

        agent_to_create = cls._get_agent_from_row(row)
        if not agent_to_create:
            return

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                create_if_not_exists(agent_to_create, obj, "supervising_officer")

    @classmethod
    def _parse_case_types(
        cls,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:

        case_types = cls._sorted_list_from_col(row, "CASE_TYPE_LIST")

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                for case_type in case_types:
                    case_type_to_create = StateSupervisionCaseTypeEntry(
                        case_type=case_type
                    )
                    create_if_not_exists(
                        case_type_to_create, obj, "state_supervision_case_type_entries"
                    )

    @classmethod
    def _set_empty_admisssion_releases_as_transfers(
        cls,
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:

        for obj in extracted_objects:
            if isinstance(obj, StateSupervisionPeriod):
                if not obj.admission_reason and obj.start_date:
                    obj.admission_reason = (
                        StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE.value
                    )
                if not obj.termination_reason and obj.termination_date:
                    obj.termination_reason = (
                        StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE.value
                    )

    @classmethod
    def _get_agent_from_row(cls, row: Dict[str, str]) -> Optional[StateAgent]:
        agent_id = row.get("BDGNO", "")
        agent_type = row.get("CLSTTL", "")
        given_names = row.get("FNAME", "")
        surname = row.get("LNAME", "")
        middle_names = row.get("MINTL", "")

        if not agent_id:
            return None

        return StateAgent(
            state_agent_id=agent_id,
            agent_type=agent_type,
            given_names=given_names,
            middle_names=middle_names,
            surname=surname,
        )

    def _set_completion_date_if_necessary(
        self,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:

        completion_date = row[MOST_RECENT_SENTENCE_STATUS_DATE]
        for obj in extracted_objects:
            if isinstance(obj, (StateIncarcerationSentence, StateSupervisionSentence)):
                if obj.status in (
                    StateSentenceStatus.COMMUTED.value,
                    StateSentenceStatus.COMPLETED.value,
                    StateSentenceStatus.REVOKED.value,
                ):
                    obj.__setattr__("completion_date", completion_date)

    def _set_sentence_status(
        self,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:

        status_enum_str = self._sentence_status_enum_str_from_row(row)
        for obj in extracted_objects:
            if isinstance(obj, (StateIncarcerationSentence, StateSupervisionSentence)):
                obj.__setattr__("status", status_enum_str)

    def _sentence_status_enum_str_from_row(self, row: Dict[str, str]) -> str:
        """Derives an accurate sentence status from the data in the given row."""
        raw_status_str = row[MOST_RECENT_SENTENCE_STATUS_CODE]
        sentence_completed_flag = row[SENTENCE_COMPLETED_FLAG]
        supervision_sentence_type = row.get(SUPERVISION_SENTENCE_TYPE, None)

        is_probation_sentence = (
            supervision_sentence_type
            and self.get_enum_overrides().parse(
                supervision_sentence_type, StateSupervisionType
            )
            == StateSupervisionType.PROBATION
        )

        if (
            is_probation_sentence
            and raw_status_str in self.REVOKED_PROBATION_SENTENCE_STATUS_CODES
        ):
            return StateSentenceStatus.REVOKED.value

        if sentence_completed_flag == "Y":
            return StateSentenceStatus.COMPLETED.value

        # TODO(#2806): This might be a bad way to determine if a sentence is
        #  suspended since there could be, in theory, statuses that come between
        #  the suspension status and the actual status that means the probation
        #  has been reinstated (like a a random warrant status)
        if raw_status_str in self.SUSPENDED_SENTENCE_STATUS_CODES:
            return StateSentenceStatus.SUSPENDED.value
        if raw_status_str in self.COMMUTED_SENTENCE_STATUS_CODES:
            return StateSentenceStatus.COMMUTED.value

        if sentence_completed_flag == "N":
            return StateSentenceStatus.SERVING.value

        return StateSentenceStatus.EXTERNAL_UNKNOWN.value

    @classmethod
    def _clear_zero_date_string(
        cls,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        offense_date = row.get(SENTENCE_OFFENSE_DATE, None)

        if offense_date and offense_date == "0":
            for obj in extracted_objects:
                if isinstance(obj, StateCharge):
                    obj.offense_date = None

    @classmethod
    def _replace_invalid_release_date(
        cls,
        _gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        """Replaces a 99999999 release date with the most recent status update date."""
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                if obj.release_date == "99999999":
                    obj.release_date = row.get(MOST_RECENT_SENTENCE_STATUS_DATE, None)

    @classmethod
    def _gen_clear_magical_date_value(
        cls,
        field_name: str,
        magical_dates: List[str],
        sentence_type: Type[IngestObject],
    ) -> IngestRowPosthookCallable:
        def _clear_magical_date_values(
            _gating_context: IngestGatingContext,
            _row: Dict[str, str],
            extracted_objects: List[IngestObject],
            _cache: IngestObjectCache,
        ) -> None:
            for obj in extracted_objects:
                if isinstance(obj, sentence_type):
                    if obj.__getattribute__(field_name) in magical_dates:
                        obj.__setattr__(field_name, None)

        return _clear_magical_date_values

    def _revocation_admission_reason(
        self, ip_admission_reason: Optional[str]
    ) -> Optional[StateIncarcerationPeriodAdmissionReason]:
        if not ip_admission_reason:
            return None

        ip_admission_reason_enum = self.get_enum_overrides().parse(
            ip_admission_reason, StateIncarcerationPeriodAdmissionReason
        )

        if ip_admission_reason_enum is None:
            return None

        if not isinstance(
            ip_admission_reason_enum, StateIncarcerationPeriodAdmissionReason
        ):
            raise ValueError(
                f"Unexpected enum type returned: [{ip_admission_reason_enum}]"
            )

        if ip_admission_reason_enum in {
            StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION,
        }:
            return ip_admission_reason_enum
        return None

    @classmethod
    def _deciding_body_type(
        cls, revocation_admission_reason: StateIncarcerationPeriodAdmissionReason
    ) -> StateSupervisionViolationResponseDecidingBodyType:
        if (
            revocation_admission_reason
            == StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION
        ):
            return StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD
        if (
            revocation_admission_reason
            == StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION
        ):
            return StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD
        if (
            revocation_admission_reason
            == StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION
        ):
            return StateSupervisionViolationResponseDecidingBodyType.COURT
        raise ValueError(
            f"Unexpected revocation_admission_reason [{revocation_admission_reason}]."
        )

    @classmethod
    def _set_incarceration_period_status(
        cls,
        _gating_context: IngestGatingContext,
        _row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        for obj in extracted_objects:
            if isinstance(obj, StateIncarcerationPeriod):
                if obj.release_date:
                    obj.status = StateIncarcerationPeriodStatus.NOT_IN_CUSTODY.value
                else:
                    obj.status = StateIncarcerationPeriodStatus.IN_CUSTODY.value

    @classmethod
    def _sentence_group_ancestor_chain_override(
        cls, gating_context: IngestGatingContext, row: Dict[str, str]
    ) -> Dict[str, str]:
        coords = cls._generate_sentence_group_id_coords(gating_context, row)
        return {coords.class_name: coords.field_value}

    @classmethod
    def _incarceration_sentence_ancestor_chain_override(
        cls, gating_context: IngestGatingContext, row: Dict[str, str]
    ) -> Dict[str, str]:
        group_coords = cls._generate_sentence_group_id_coords(gating_context, row)
        sentence_coords = cls._generate_incarceration_sentence_id_coords(
            gating_context, row
        )

        return {
            group_coords.class_name: group_coords.field_value,
            sentence_coords.class_name: sentence_coords.field_value,
        }

    @classmethod
    def _supervision_sentence_ancestor_chain_override(
        cls, gating_context: IngestGatingContext, row: Dict[str, str]
    ) -> Dict[str, str]:
        group_coords = cls._generate_sentence_group_id_coords(gating_context, row)
        sentence_coords = cls._generate_supervision_sentence_id_coords(
            gating_context, row
        )

        return {
            group_coords.class_name: group_coords.field_value,
            sentence_coords.class_name: sentence_coords.field_value,
        }

    @classmethod
    def _supervision_violation_report_ancestor_chain_override(
        cls, gating_context: IngestGatingContext, row: Dict[str, str]
    ) -> Dict[str, str]:
        group_coords = cls._generate_sentence_group_id_coords(gating_context, row)
        if row.get(f"{TAK076_PREFIX}_{FIELD_KEY_SEQ}", "0") == "0":
            sentence_coords = cls._generate_incarceration_sentence_id_coords(
                gating_context, row, TAK076_PREFIX
            )
        else:
            sentence_coords = cls._generate_supervision_sentence_id_coords(
                gating_context, row, TAK076_PREFIX
            )
        return {
            group_coords.class_name: group_coords.field_value,
            sentence_coords.class_name: sentence_coords.field_value,
        }

    @classmethod
    def _supervision_violation_citation_ancestor_chain_override(
        cls, gating_context: IngestGatingContext, row: Dict[str, str]
    ) -> Dict[str, str]:
        group_coords = cls._generate_sentence_group_id_coords(gating_context, row)
        if row.get(f"{TAK291_PREFIX}_{FIELD_KEY_SEQ}", "0") == "0":
            sentence_coords = cls._generate_incarceration_sentence_id_coords(
                gating_context, row, TAK291_PREFIX
            )
        else:
            sentence_coords = cls._generate_supervision_sentence_id_coords(
                gating_context, row, TAK291_PREFIX
            )
        return {
            group_coords.class_name: group_coords.field_value,
            sentence_coords.class_name: sentence_coords.field_value,
        }

    @classmethod
    def normalize_sentence_group_ids(
        cls,
        gating_context: IngestGatingContext,
        row: Dict[str, str],
        extracted_objects: List[IngestObject],
        _cache: IngestObjectCache,
    ) -> None:
        col_prefix = cls.primary_col_prefix_for_file_tag(gating_context.file_tag)
        for obj in extracted_objects:
            if isinstance(obj, StateSentenceGroup):
                obj.state_sentence_group_id = cls._generate_sentence_group_id(
                    col_prefix, row
                )

    @classmethod
    def _generate_assessment_id_coords(
        cls, _gating_context: IngestGatingContext, row: Dict[str, str]
    ) -> IngestFieldCoordinates:

        doc_id = row.get(ORAS_ASSESSMENTS_DOC_ID, "")
        person_assessment_id = row.get(ORAS_ASSESSMENT_ID, "")
        return IngestFieldCoordinates(
            "state_assessment",
            "state_assessment_id",
            f"{doc_id}-{person_assessment_id}",
        )

    @classmethod
    def _generate_sentence_group_id_coords(
        cls, gating_context: IngestGatingContext, row: Dict[str, str]
    ) -> IngestFieldCoordinates:
        col_prefix = cls.primary_col_prefix_for_file_tag(gating_context.file_tag)
        return IngestFieldCoordinates(
            "state_sentence_group",
            "state_sentence_group_id",
            cls._generate_sentence_group_id(col_prefix, row),
        )

    @classmethod
    def _generate_supervision_sentence_id_coords(
        cls,
        gating_context: IngestGatingContext,
        row: Dict[str, str],
        col_prefix: str = None,
    ) -> IngestFieldCoordinates:
        if not col_prefix:
            col_prefix = cls.primary_col_prefix_for_file_tag(gating_context.file_tag)
        return IngestFieldCoordinates(
            "state_supervision_sentence",
            "state_supervision_sentence_id",
            cls._generate_sentence_id(col_prefix, row),
        )

    @classmethod
    def _generate_incarceration_sentence_id_coords(
        cls,
        gating_context: IngestGatingContext,
        row: Dict[str, str],
        col_prefix: str = None,
    ) -> IngestFieldCoordinates:
        if not col_prefix:
            col_prefix = cls.primary_col_prefix_for_file_tag(gating_context.file_tag)
        return IngestFieldCoordinates(
            "state_incarceration_sentence",
            "state_incarceration_sentence_id",
            cls._generate_sentence_id(col_prefix, row),
        )

    @classmethod
    def _generate_supervision_period_id_coords(
        cls, gating_context: IngestGatingContext, row: Dict[str, str]
    ) -> IngestFieldCoordinates:
        col_prefix = cls.primary_col_prefix_for_file_tag(gating_context.file_tag)

        sentence_group_id = cls._generate_sentence_group_id(col_prefix, row)

        field_assignment_seq = row["FIELD_ASSIGNMENT_SEQ_NUM"]
        start_status_seq_num = row["START_STATUS_SEQ_NUM"]
        supervision_period_id = (
            f"{sentence_group_id}-{field_assignment_seq}-{start_status_seq_num}"
        )

        return IngestFieldCoordinates(
            "state_supervision_period",
            "state_supervision_period_id",
            supervision_period_id,
        )

    @classmethod
    def _generate_incarceration_period_id_coords(
        cls, gating_context: IngestGatingContext, row: Dict[str, str]
    ) -> IngestFieldCoordinates:
        col_prefix = cls.primary_col_prefix_for_file_tag(gating_context.file_tag)

        sentence_group_id = cls._generate_sentence_group_id(col_prefix, row)

        # TODO(#2728): The SQN is potentially not a stable ID if status
        #  information gets backdated and the SQN numbers generated in the
        #  roll-up shift.
        subcycle_seq_num = row[f"{cls.PERIOD_SEQUENCE_PRIMARY_COL_PREFIX}_SQN"]
        start_status_seq_num = row["START_STATUS_SEQ_NUM"]
        incarceration_period_id = (
            f"{sentence_group_id}-{subcycle_seq_num}-{start_status_seq_num}"
        )

        return IngestFieldCoordinates(
            "state_incarceration_period",
            "state_incarceration_period_id",
            incarceration_period_id,
        )

    @classmethod
    def _generate_supervision_violation_id_coords_for_reports(
        cls, gating_context: IngestGatingContext, row: Dict[str, str]
    ) -> IngestFieldCoordinates:
        col_prefix = cls.primary_col_prefix_for_file_tag(gating_context.file_tag)
        return IngestFieldCoordinates(
            "state_supervision_violation",
            "state_supervision_violation_id",
            cls._generate_supervision_violation_id_with_report_prefix(col_prefix, row),
        )

    @classmethod
    def _generate_supervision_violation_id_coords_for_citations(
        cls, gating_context: IngestGatingContext, row: Dict[str, str]
    ) -> IngestFieldCoordinates:
        col_prefix = cls.primary_col_prefix_for_file_tag(gating_context.file_tag)
        return IngestFieldCoordinates(
            "state_supervision_violation",
            "state_supervision_violation_id",
            cls._generate_supervision_violation_id_with_citation_prefix(
                col_prefix, row
            ),
        )

    @classmethod
    def _generate_sentence_group_id(cls, col_prefix: str, row: Dict[str, str]) -> str:

        if col_prefix:
            doc_id = row[f"{col_prefix}_{DOC_ID}"]
            cyc_id = row[f"{col_prefix}_{CYCLE_ID}"]
        else:
            doc_id = row[DOC_ID]
            cyc_id = row[CYCLE_ID]
        return f"{doc_id}-{cyc_id}"

    @classmethod
    def _generate_sentence_id(cls, col_prefix: str, row: Dict[str, str]) -> str:
        sentence_group_id = cls._generate_sentence_group_id(col_prefix, row)
        sen_seq_num = row[f"{col_prefix}_{SENTENCE_KEY_SEQ}"]
        return f"{sentence_group_id}-{sen_seq_num}"

    @classmethod
    def _generate_supervision_violation_id_with_report_prefix(
        cls, col_prefix: str, row: Dict[str, str]
    ) -> str:
        return cls._generate_supervision_violation_id_with_prefix(
            col_prefix,
            row,
            TAK076_PREFIX,
            VIOLATION_REPORT_ID_PREFIX,
            VIOLATION_KEY_SEQ,
        )

    @classmethod
    def _generate_supervision_violation_id_with_citation_prefix(
        cls, col_prefix: str, row: Dict[str, str]
    ) -> str:
        return cls._generate_supervision_violation_id_with_prefix(
            col_prefix, row, TAK291_PREFIX, CITATION_ID_PREFIX, CITATION_KEY_SEQ
        )

    @classmethod
    def _generate_supervision_violation_id_with_prefix(
        cls,
        col_prefix: str,
        row: Dict[str, str],
        xref_prefix: str,
        violation_id_prefix: str,
        violation_key_seq: str,
    ) -> str:
        violation_seq_num = row[f"{col_prefix}_{violation_key_seq}"]
        group_id = cls._generate_sentence_group_id(xref_prefix, row)

        # TODO(#1883): Remove use of SEO (sentence_seq_id) and FSO (Field Seq No)
        # once extractor supports multiple paths to entities with the same id.
        # Until then, we need to keep all ids for SupervisionViolations unique
        # through the data extractor and proto conversion.
        #
        # Currently, the SEO is removed from the violation ids as a
        # pre-processing hook in entity matching. From there, matching
        # violations can be properly merged together.
        sentence_seq_id = row[f"{xref_prefix}_{SENTENCE_KEY_SEQ}"]
        field_seq_no = row[f"{xref_prefix}_{FIELD_KEY_SEQ}"]

        return f"{group_id}-{violation_id_prefix}{violation_seq_num}-{sentence_seq_id}-{field_seq_no}"

    @classmethod
    def _generate_supervision_violation_id(
        cls, col_prefix: str, row: Dict[str, str], violation_id_prefix: str
    ) -> str:
        group_id = cls._generate_sentence_group_id(TAK076_PREFIX, row)
        sentence_seq_id = row[f"{TAK076_PREFIX}_{SENTENCE_KEY_SEQ}"]
        violation_seq_num = row[f"{col_prefix}_{VIOLATION_KEY_SEQ}"]
        return f"{group_id}-{violation_id_prefix}{violation_seq_num}-{sentence_seq_id}"

    @classmethod
    def primary_col_prefix_for_file_tag(cls, file_tag: str) -> str:
        return cls.PRIMARY_COL_PREFIXES_BY_FILE_TAG[file_tag]

    @classmethod
    def _test_length_string(cls, time_string: str) -> bool:
        """Tests the length string to see if it will cause an overflow beyond the Python MAXYEAR."""
        try:
            parse_days(time_string)
            return True
        except ValueError:
            return False

    @classmethod
    def _parse_days_with_long_range(cls, time_string: str) -> str:
        """Parses a time string that we assume to have a range long enough that it cannot be parsed by our standard
        Python date parsing utilities.

        Some length strings in Missouri sentence data, particularly for life sentences, will have very long ranges,
        like '9999Y 99M 99D'. These cannot be natively interpreted by the Python date parser, which has a MAXYEAR
        setting that cannot be altered. So we check for these kinds of strings and parse them using basic, approximate
        arithmetic to generate a usable value.

        If there is a structural issue that this function cannot handle, it returns the given time string unaltered.
        """
        try:
            date_string = munge_date_string(time_string)
            components = date_string.split(" ")
            total_days = 0.0

            for component in components:
                if "year" in component:
                    year_count_str = component.split("year")[0]
                    try:
                        year_count = int(year_count_str)
                        total_days += year_count * 365.25
                    except ValueError:
                        pass
                elif "month" in component:
                    month_count_str = component.split("month")[0]
                    try:
                        month_count = int(month_count_str)
                        total_days += month_count * 30.5
                    except ValueError:
                        pass
                elif "day" in component:
                    day_count_str = component.split("day")[0]
                    try:
                        day_count = int(day_count_str)
                        total_days += day_count
                    except ValueError:
                        pass

            return str(int(total_days))
        except ValueError:
            return time_string

    JULIAN_DATE_STR_REGEX = re.compile(r"(\d?\d\d)(\d\d\d)")

    @classmethod
    def mo_julian_date_to_iso(cls, julian_date_str: Optional[str]) -> Optional[str]:
        """
        Parse julian-formatted date strings used by MO in a number of DB fields that encode a date using the number of
        years since 1900 concatenated with the number of days since Jan 1 of that year (1-indexed). Returns the date in
        ISO date format.

        E.g.:
             85001 -> 1985-01-01
            118365 -> 2018-12-31
        """
        if not julian_date_str or int(julian_date_str) == 0:
            return None

        match = re.match(cls.JULIAN_DATE_STR_REGEX, julian_date_str)
        if match is None:
            logging.warning("Could not parse MO date [%s]", julian_date_str)
            return None

        years_since_1900 = int(match.group(1))
        days_since_jan_1 = int(match.group(2)) - 1

        date = datetime.date(
            year=(years_since_1900 + 1900), month=1, day=1
        ) + datetime.timedelta(days=days_since_jan_1)
        return date.isoformat()

    @classmethod
    def _sorted_list_from_col(cls, row: Dict[str, str], col_name: str) -> List[str]:
        value = row.get(col_name, "")
        return sorted_list_from_str(value)

    def get_tak022_tak023_tak025_tak026_offender_sentence_institution_row_processors(
        self,
    ) -> List[IngestRowPosthookCallable]:
        return [
            gen_normalize_county_codes_posthook(
                self.region.region_code, CHARGE_COUNTY_CODE, StateCharge
            ),
            gen_normalize_county_codes_posthook(
                self.region.region_code,
                SENTENCE_COUNTY_CODE,
                StateIncarcerationSentence,
            ),
            gen_map_ymd_counts_to_max_length_field_posthook(
                INCARCERATION_SENTENCE_LENGTH_YEARS,
                INCARCERATION_SENTENCE_LENGTH_MONTHS,
                INCARCERATION_SENTENCE_LENGTH_DAYS,
                StateIncarcerationSentence,
                test_for_fallback=self._test_length_string,
                fallback_parser=self._parse_days_with_long_range,
            ),
            gen_set_is_life_sentence_hook(
                INCARCERATION_SENTENCE_MIN_RELEASE_TYPE,
                "LIF",
                StateIncarcerationSentence,
            ),
            self._gen_clear_magical_date_value(
                "projected_max_release_date",
                self.SENTENCE_MAGICAL_DATES,
                StateIncarcerationSentence,
            ),
            self._gen_clear_magical_date_value(
                "projected_min_release_date",
                self.SENTENCE_MAGICAL_DATES,
                StateIncarcerationSentence,
            ),
            self._set_sentence_status,
            self._set_completion_date_if_necessary,
            self._clear_zero_date_string,
            self.tak022_tak023_set_parole_eligibility_date,
            self.set_charge_id_from_sentence_id,
            self._set_charge_is_violent_from_ncic,
        ]

    def get_tak022_tak023_tak025_tak026_offender_sentence_supervision_row_processors(
        self,
    ) -> List[IngestRowPosthookCallable]:
        return [
            gen_normalize_county_codes_posthook(
                self.region.region_code, CHARGE_COUNTY_CODE, StateCharge
            ),
            gen_normalize_county_codes_posthook(
                self.region.region_code, SENTENCE_COUNTY_CODE, StateSupervisionSentence
            ),
            gen_map_ymd_counts_to_max_length_field_posthook(
                SUPERVISION_SENTENCE_LENGTH_YEARS,
                SUPERVISION_SENTENCE_LENGTH_MONTHS,
                SUPERVISION_SENTENCE_LENGTH_DAYS,
                StateSupervisionSentence,
                test_for_fallback=self._test_length_string,
                fallback_parser=self._parse_days_with_long_range,
            ),
            self._gen_clear_magical_date_value(
                "start_date", self.SENTENCE_MAGICAL_DATES, StateSupervisionSentence
            ),
            self._gen_clear_magical_date_value(
                "projected_completion_date",
                self.SENTENCE_MAGICAL_DATES,
                StateSupervisionSentence,
            ),
            self._set_sentence_status,
            self._set_completion_date_if_necessary,
            self._clear_zero_date_string,
            self.set_charge_id_from_sentence_id,
            self._set_charge_is_violent_from_ncic,
        ]
