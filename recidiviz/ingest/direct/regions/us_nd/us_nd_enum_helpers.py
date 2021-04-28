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
"""US_ND specific enum helper methods."""
from typing import Dict, List

from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta
from recidiviz.common.constants.enum_overrides import (
    EnumOverrides,
    EnumMapper,
    EnumIgnorePredicate,
)
from recidiviz.common.constants.person_characteristics import Gender, Race
from recidiviz.common.constants.standard_enum_overrides import (
    get_standard_enum_overrides,
)
from recidiviz.common.constants.state.state_assessment import StateAssessmentLevel
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_charge import StateChargeClassificationType
from recidiviz.common.constants.state.state_court_case import StateCourtCaseStatus
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
    StateIncarcerationIncidentType,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
)
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactType,
    StateSupervisionContactReason,
    StateSupervisionContactLocation,
    StateSupervisionContactStatus,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseRevocationType,
)
from recidiviz.ingest.direct.direct_ingest_controller_utils import (
    update_overrides_from_maps,
)


def incarceration_period_status_mapper(label: str) -> StateIncarcerationPeriodStatus:
    """Parses the custody status from a string containing the external movement edge direction and active flag."""

    # TODO(#2865): Update enum normalization so that we separate by a dash instead of spaces
    direction_code, active_flag = label.split(" ")

    if direction_code == "OUT":
        return StateIncarcerationPeriodStatus.NOT_IN_CUSTODY

    if direction_code == "IN":
        if active_flag == "Y":
            return StateIncarcerationPeriodStatus.IN_CUSTODY
        if active_flag == "N":
            # If the active flag is 'N' we know that the person has left this period of custody, even if the table
            # happens to be missing an OUT edge.
            return StateIncarcerationPeriodStatus.NOT_IN_CUSTODY

    raise ValueError(f"Unexpected incarceration period raw text value [{label}]")


def generate_enum_overrides() -> EnumOverrides:
    """Provides North Dakota-specific overrides for enum mappings.

    The keys herein are raw strings directly from the source data, and the values are the enums that they are
    mapped to within our schema. The values are a list because a particular string may be used in multiple
    distinct columns in the source data.
    """
    overrides: Dict[EntityEnum, List[str]] = {
        Gender.FEMALE: ["2"],
        Gender.MALE: ["1"],
        Race.WHITE: ["1"],
        Race.BLACK: ["2"],
        Race.AMERICAN_INDIAN_ALASKAN_NATIVE: ["3", "NAT"],
        Race.ASIAN: ["4"],
        Race.NATIVE_HAWAIIAN_PACIFIC_ISLANDER: ["6", "HAW"],
        Race.OTHER: ["MUL"],
        StatePersonAliasType.AFFILIATION_NAME: ["GNG"],
        StatePersonAliasType.ALIAS: ["A", "O"],
        StatePersonAliasType.GIVEN_NAME: ["G", "CN"],
        StatePersonAliasType.MAIDEN_NAME: ["M"],
        StatePersonAliasType.NICKNAME: ["N"],
        StateSentenceStatus.COMPLETED: ["C"],
        StateSentenceStatus.SERVING: ["O"],
        StateChargeClassificationType.FELONY: ["IF"],
        StateChargeClassificationType.MISDEMEANOR: ["IM"],
        StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR: ["ADM ERROR"],
        StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN: ["OTHER", "PREA"],
        StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION: [
            "ADMN",
            "RAB",
            "DEF",
        ],
        StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION: ["PARL", "PV"],
        StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION: [
            "NPRB",
            "NPROB",
            "PRB",
            "RPRB",
        ],
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE: ["REC", "RECA"],
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE: [
            "READMN"
        ],
        StateIncarcerationPeriodAdmissionReason.TRANSFER: [
            "CONF",
            "CRT",
            "DETOX",
            "FED",
            "HOSP",
            "HOSPS",
            "HOSPU",
            "INT",
            "JOB",
            "MED",
            "PROG",
            "RB",
            "SUPL",
        ],
        StateIncarcerationPeriodAdmissionReason.TRANSFERRED_FROM_OUT_OF_STATE: ["OOS"],
        StateIncarcerationPeriodReleaseReason.ESCAPE: ["ESC", "ESCP", "ABSC"],
        StateIncarcerationPeriodReleaseReason.RELEASED_IN_ERROR: ["ERR"],
        StateIncarcerationPeriodReleaseReason.EXTERNAL_UNKNOWN: ["OTHER"],
        StateIncarcerationPeriodReleaseReason.COMMUTED: ["CMM"],
        StateIncarcerationPeriodReleaseReason.COMPASSIONATE: ["COM"],
        StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE: [
            "PARL",
            "PRB",
            "PV",
            "RPAR",
            "RPRB",
            "SUPL",
        ],
        StateIncarcerationPeriodReleaseReason.COURT_ORDER: ["CO"],
        StateIncarcerationPeriodReleaseReason.DEATH: ["DECE"],
        StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED: ["XSNT"],
        StateIncarcerationPeriodReleaseReason.TRANSFER: [
            "CONF",
            "CRT",
            "DETOX",
            "HOSP",
            "HOSPS",
            "HOSPU",
            "INT",
            "JOB",
            "MED",
            "PROG",
            "RB",
            "SUPL",
        ],
        StateIncarcerationPeriodReleaseReason.TRANSFER_OUT_OF_STATE: ["TRN"],
        StateSupervisionType.HALFWAY_HOUSE: ["COMMUNITY PLACEMENT PGRM"],
        StateSupervisionType.PAROLE: ["SSOP"],
        # TODO(#2891): Ensure that this gets mapped down to a supervision_period_supervision_type of INVESTIGATION
        # on the supervision period that this gets copied down to in the hook for Docstars Offender Cases
        StateSupervisionType.PRE_CONFINEMENT: ["PRE-TRIAL"],
        StateSupervisionViolationResponseRevocationType.REINCARCERATION: [
            "COUNTY JAIL SENTENCE",
            "COUNTY JAIL SENTENCE FOLLOWED BY PROBATION",
            "DOCR INMATE SENTENCE",
            "DOCR INMATE SENTENCE FOLLOWED BY PROBATION",
            "RESENTENCED TO FIVE YEARS MORE",
        ],
        StateIncarcerationIncidentType.DISORDERLY_CONDUCT: [
            "DAMAGE",
            "DISCON",
            "ESCAPE_ATT",
            "INS",
            "SEXCONTACT",
            "UNAUTH",
            "NON",
        ],
        StateIncarcerationIncidentType.CONTRABAND: [
            "CONT",
            "GANG",
            "GANGREL",
            "PROP",
            "TOB",
        ],
        StateIncarcerationIncidentType.MINOR_OFFENSE: ["SWIFT"],
        StateIncarcerationIncidentType.POSITIVE: ["POSREPORT"],
        StateIncarcerationIncidentType.REPORT: ["STAFFREP"],
        StateIncarcerationIncidentType.PRESENT_WITHOUT_INFO: ["CONV"],
        StateIncarcerationIncidentType.VIOLENCE: [
            "IIASSAULT",
            "IIASSAULTINJ",
            "IIFIGHT",
            "FGHT",
            "IISUBNOINJ",
            "ISASSAULT",
            "ISASSAULTINJ",
            "ISSUBNOINJ",
            "SEXUAL",
            "THREAT",
        ],
        StateIncarcerationIncidentOutcomeType.PRIVILEGE_LOSS: [
            "LCP",
            "LOR",
            "LCO",
            "LVPVV",
            "LOP",
            "LVP",
            "LPJES",
            "FREM",
            "RTQ",
            "UREST",
            "LPH",
            "LSE",
            "CCF",
            "SREM",
        ],
        StateIncarcerationIncidentOutcomeType.FINANCIAL_PENALTY: [
            "RES",
            "PAY",
            "FIN",
            "PRO",
            "LJB",
        ],
        StateIncarcerationIncidentOutcomeType.SOLITARY: ["SEG", "DD", "RAS"],
        StateIncarcerationIncidentOutcomeType.TREATMENT: ["RTX"],
        StateIncarcerationIncidentOutcomeType.DISMISSED: ["DSM"],
        StateIncarcerationIncidentOutcomeType.EXTERNAL_PROSECUTION: ["RSA"],
        StateIncarcerationIncidentOutcomeType.MISCELLANEOUS: [
            "COMB",
            "DELETED",
            "RED",
            "TRA",
        ],
        StateIncarcerationIncidentOutcomeType.DISCIPLINARY_LABOR: ["EXD"],
        StateIncarcerationIncidentOutcomeType.GOOD_TIME_LOSS: ["LG", "STP"],
        StateIncarcerationIncidentOutcomeType.WARNING: ["WAR", "NS"],
        # 0 means no calculated level
        StateSupervisionLevel.PRESENT_WITHOUT_INFO: ["0"],
        StateSupervisionLevel.MINIMUM: ["1"],
        StateSupervisionLevel.MEDIUM: ["2"],
        # 6 is Drug Court which is max with specific rules
        StateSupervisionLevel.MAXIMUM: ["3", "6"],
        StateSupervisionLevel.DIVERSION: ["7"],
        StateSupervisionLevel.INTERSTATE_COMPACT: ["9"],
        # 4 and 8 are no longer used now, 5 means not classified yet
        StateSupervisionLevel.EXTERNAL_UNKNOWN: ["4", "5", "8"],
        StateSupervisionPeriodTerminationReason.ABSCONSION: [
            "13"  # Terminated - Absconded (Active Petition To Revoke)
        ],
        StateSupervisionPeriodTerminationReason.DEATH: ["11"],  # "Terminated - Death
        StateSupervisionPeriodTerminationReason.DISCHARGE: [
            "1",  # Terminated - Dismissal (Deferred Imp.)
            "2",  # Terminated - Early Dismissal (Deferred Imp.)
            "5",  # Terminated - Termination-Positive (Susp. Sent)"
            "8",  # Terminated - Released from Community Placement
            "12",  # Terminated - Returned to Original State-Voluntary
            "15",  # Terminated - Released from Custody
            "16",  # Terminated - CCC
            "17",  # Terminated - Returned to Original State-Violation
        ],
        StateSupervisionPeriodTerminationReason.EXPIRATION: [
            "4",  # Terminated - Expiration (Susp. Sentence)
            "7",  # Terminated - Expiration (Parole)
            "19",  # Terminated - Expiration (IC Parole)
            "20",  # Terminated - Expiration (IC Probation)
        ],
        StateSupervisionPeriodTerminationReason.EXTERNAL_UNKNOWN: [
            "14"  # Terminated - Other
        ],
        # TODO(#2891): Ensure that all of these codes are migrated to to new admission and release reasons
        # when we migrate these periods to a supervision_period_supervision_type of INVESTIGATION
        StateSupervisionPeriodTerminationReason.INVESTIGATION: [
            "21",  # Guilty
            "22",  # Guilty of Lesser Charge
            "23",  # Not Guilty
            "24",  # Dismissed
            "25",  # Mistrial
            "26",  # Deferred Prosecution
            "27",  # Post-Conviction Supervision
            "28",  # Closed with Active FTA
            "29",  # Early Termination
            "30",  # No Conditions Imposed
        ],
        StateSupervisionPeriodTerminationReason.REVOCATION: [
            "9",  # Terminated - Revocation
            "10",  # Terminated - Revocation with Continuation
            "18",  # Terminated - Returned to Custody from CPP
        ],
        StateSupervisionPeriodTerminationReason.SUSPENSION: [
            "3",  # Terminated - Termination (Deferred Imp.)
            "6",  # Terminated - Termination-Negative (Susp. Sent)
        ],
        StateProgramAssignmentParticipationStatus.PENDING: [
            "Submitted",
            "Pending Coordinator",
        ],
        StateProgramAssignmentParticipationStatus.REFUSED: ["Refused"],
        StateSupervisionCaseType.GENERAL: ["0"],
        StateSupervisionCaseType.SEX_OFFENSE: ["-1"],
        StateAssessmentLevel.EXTERNAL_UNKNOWN: ["NOT APPLICABLE", "UNDETERMINED"],
        StateSupervisionContactReason.GENERAL_CONTACT: ["SUPERVISION"],
        StateSupervisionContactType.FACE_TO_FACE: [
            "HV",  # Visit at Supervisee's Home
            "OO",  # Visit at Supervisee's Work or Public Area
            "OV",  # Visit at Supervision Agent's Office
        ],
        StateSupervisionContactLocation.SUPERVISION_OFFICE: [
            "OV"  # Visit at Supervision Agent's Office
        ],
        StateSupervisionContactLocation.RESIDENCE: ["HV"],  # Visit at Supervisee's Home
        StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT: [
            "OO"  # Visit at Supervisee's Work or Public Area
        ],
        StateSupervisionContactStatus.COMPLETED: [
            # We infer that any contact that constitutes a face to face visit
            # should have the contact status as completed
            "HV",  # Visit at Supervisee's Home
            "OO",  # Visit at Supervisee's Work or Public Area
            "OV",  # Visit at Supervision Agent's Office
        ],
        StateSupervisionContactStatus.INTERNAL_UNKNOWN: [
            "SG",
            "FR",
        ],
    }

    ignores: Dict[EntityEnumMeta, List[str]] = {
        # TODO(#2305): What are the appropriate court case statuses?
        StateCourtCaseStatus: ["A", "STEP"],
        StateIncarcerationPeriodAdmissionReason: ["COM", "CONT", "CONV", "NTAD"],
        StateIncarcerationPeriodReleaseReason: [
            "ADMN",
            "CONT",
            "CONV",
            "REC",
            "4139",
        ],
    }

    override_mappers: Dict[EntityEnumMeta, EnumMapper] = {
        StateIncarcerationPeriodStatus: incarceration_period_status_mapper,
    }

    ignore_predicates: Dict[EntityEnumMeta, EnumIgnorePredicate] = {}

    base_overrides = get_standard_enum_overrides()
    return update_overrides_from_maps(
        base_overrides, overrides, ignores, override_mappers, ignore_predicates
    )
