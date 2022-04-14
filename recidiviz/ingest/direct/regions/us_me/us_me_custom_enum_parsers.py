# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Custom enum parsers functions for US_ME. Can be referenced in an ingest view manifest
like this:
my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_me_custom_enum_parsers.<function name>
"""
from typing import Optional

from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)

# Movement and Transfer type and reasons
FURLOUGH_MOVEMENT_TYPES = ["FURLOUGH", "FURLOUGH HOSPITAL"]

OTHER_JURISDICTION_TRANSFER_TYPES = [
    "NON-DOC IN",
    "NON-DOC COUNTY JAIL IN",
    "DOC COUNTY JAIL RETURN FROM",
    "DOC INTERSTATE COMPACT RETURN FROM/IN",
    "DOC INTERSTATE ACTIVE DETAINER RETURN",
]

SUPERVISION_VIOLATION_TRANSFER_REASONS = [
    "VIOLATION OF PROBATION",
    "VIOLATION OF PROBATION - PROB. TO TERMINATE(J)",
    "VIOLATION OF PAROLE",
    "VIOLATION OF SCCP",
]
DISCHARGE_MOVEMENT_TYPE = "DISCHARGE"
RELEASE_MOVEMENT_TYPE = "RELEASE"
TRANSFER_MOVEMENT_TYPE = "TRANSFER"
SOCIETY_IN_TRANSFER_TYPE = "SOCIETY IN"
SENTENCE_DISPOSITION = "SENTENCE/DISPOSITION"
OTHER_JURISDICTION_TRANSFER_REASON = "OTHER JURISDICTION"
SCCP_PLACEMENT_TRANSFER_REASON = "SCCP PLACEMENT"
ESCAPE = "ESCAPE"
TEMPORARY_CUSTODY_TRANSFER_REASONS = ["SAFE KEEPERS", "FEDERAL HOLD", "FEDERAL BOP"]

SUPERVISION_COURT_SENTENCE_TRANSFER_REASONS = [
    "RELEASE TO PROBATION/JCCO",
    SENTENCE_DISPOSITION,
    "SUPERVISION ACCEPTED",
]

# Statuses
COUNTY_JAIL_STATUS = "COUNTY JAIL"
INCARCERATED_STATUSES = [
    "INCARCERATED",
    COUNTY_JAIL_STATUS,
    "INTERSTATE COMPACT IN",
    "PARTIAL REVOCATION - INCARCERATED",
]
OTHER_JURISDICTION_STATUSES = [
    "INTERSTATE COMPACT OUT",
    "INTERSTATE ACTIVE DETAINER",
]
COMMUNITY_CONFINEMENT_STATUS = "SCCP"
PAROLE_STATUS = "PAROLE"
PROBATION_STATUS = "PROBATION"
SUPERVISION_PRECEDING_INCARCERATION_STATUSES = [
    "PENDING VIOLATION",
    "PENDING VIOLATION - INCARCERATED",
    "WARRANT ABSCONDED",
    "PARTIAL REVOCATION - COUNTY JAIL",
]
WARRANT_ABSCONDED_STATUS = "WARRANT ABSCONDED"
ABSCONSION_STATUSES = [WARRANT_ABSCONDED_STATUS, ESCAPE]
SUPERVISION_STATUSES = [
    COMMUNITY_CONFINEMENT_STATUS,
    PAROLE_STATUS,
    PROBATION_STATUS,
]
ALL_SUPERVISION_STATUSES = (
    SUPERVISION_STATUSES
    + SUPERVISION_PRECEDING_INCARCERATION_STATUSES
    + [
        "REFERRAL",
        "ACTIVE",
    ]
)
CONDITIONAL_RELEASE_STATUSES = [COMMUNITY_CONFINEMENT_STATUS, PAROLE_STATUS]
INACTIVE_STATUS = "INACTIVE"

# Location types are from the CIS_908_CCS_LOCATION.Cis_9080_Ccs_Location_Type_Cd column
DOC_FACILITY_LOCATION_TYPES = [
    "2",  # Adult DOC Facilities
    "7",  # Pre-Release Centers
]
SUPERVISION_LOCATION_TYPES = ["4", "1"]  # Adult Supervision Offices
COUNTY_JAIL_LOCATION_TYPES = ["9"]  # County Jails
SOCIETY_OUT_LOCATION_TYPES = ["8", "13"]  # Maine's counties and all US states
OTHER_JURISDICTION_LOCATION_TYPES = ["8", "19"]  # All US States and Federal
DECEASED_LOCATION_TYPE = "14"

MAINE_STATE = "MAINE"


def _in_temporary_custody(
    current_status: str,
    location_type: str,
    transfer_reason: Optional[str],
) -> bool:
    return (
        (
            current_status == COUNTY_JAIL_STATUS
            and location_type in DOC_FACILITY_LOCATION_TYPES
        )
        or transfer_reason in TEMPORARY_CUSTODY_TRANSFER_REASONS
        or location_type in COUNTY_JAIL_LOCATION_TYPES
    )


def _is_new_admission(transfer_reason: str, movement_type: str) -> bool:
    return SENTENCE_DISPOSITION in [movement_type, transfer_reason]


def _previously_not_on_supervision(
    previous_jurisdiction_location_type: str, previous_status: str
) -> bool:
    return (
        previous_jurisdiction_location_type == "NONE"
        or previous_jurisdiction_location_type
        in DOC_FACILITY_LOCATION_TYPES
        + COUNTY_JAIL_LOCATION_TYPES
        + SOCIETY_OUT_LOCATION_TYPES
    ) and previous_status not in ALL_SUPERVISION_STATUSES


def parse_incarceration_type(raw_text: str) -> StateIncarcerationType:
    normalized_raw_text = raw_text.upper()
    if normalized_raw_text in COUNTY_JAIL_LOCATION_TYPES:
        return StateIncarcerationType.COUNTY_JAIL
    if normalized_raw_text in DOC_FACILITY_LOCATION_TYPES:
        return StateIncarcerationType.STATE_PRISON
    return StateIncarcerationType.EXTERNAL_UNKNOWN


def parse_specialized_purpose_for_incarceration(
    raw_text: str,
) -> StateSpecializedPurposeForIncarceration:
    (
        current_status,
        transfer_reason,
        location_type,
    ) = raw_text.upper().split("@@")
    if _in_temporary_custody(current_status, location_type, transfer_reason):
        return StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY
    return StateSpecializedPurposeForIncarceration.GENERAL


def parse_admission_reason(raw_text: str) -> StateIncarcerationPeriodAdmissionReason:
    """Parse admission reason from raw text"""
    (
        previous_status,
        previous_location_type,
        current_status,
        movement_type,
        transfer_type,
        transfer_reason,
        location_type,
    ) = raw_text.upper().split("@@")

    if (
        previous_status
        in SUPERVISION_STATUSES + SUPERVISION_PRECEDING_INCARCERATION_STATUSES
    ) or previous_location_type in SUPERVISION_LOCATION_TYPES:
        # This includes when someone violates the terms of their community confinement (SCCP) and is sent back to
        # prison. Maine does not use the term "revocation" to describe these instances, but they fit our internal
        # definition of revocations.
        return StateIncarcerationPeriodAdmissionReason.REVOCATION

    if _is_new_admission(transfer_reason, movement_type):
        return StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION

    if movement_type in FURLOUGH_MOVEMENT_TYPES:
        return StateIncarcerationPeriodAdmissionReason.RETURN_FROM_TEMPORARY_RELEASE

    # Check for escapes before temporary custody since many escape movements flow through
    # County Jail beforehand.
    if ESCAPE in [previous_status, movement_type]:
        return StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE

    if _in_temporary_custody(current_status, location_type, transfer_reason):
        return StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY

    if (
        transfer_type in OTHER_JURISDICTION_TRANSFER_TYPES
        or transfer_reason == OTHER_JURISDICTION_TRANSFER_REASON
        or previous_status in OTHER_JURISDICTION_STATUSES
    ):
        return StateIncarcerationPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION

    if (
        previous_status == "NONE"
    ) and transfer_reason in SUPERVISION_VIOLATION_TRANSFER_REASONS:
        # These rare cases happen if the previous supervision period occurred in a supervision office, or if the
        # supervision period occurred before the year 2000.
        return StateIncarcerationPeriodAdmissionReason.REVOCATION

    return StateIncarcerationPeriodAdmissionReason.TRANSFER


def parse_release_reason(
    raw_text: str,
) -> Optional[StateIncarcerationPeriodReleaseReason]:
    """Parse release reason from raw text"""
    (
        current_status,
        next_status,
        next_movement_type,
        transfer_reason,
        location_type,
        next_location_type,
    ) = raw_text.upper().split("@@")
    if next_movement_type == DISCHARGE_MOVEMENT_TYPE:
        return StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED

    if next_location_type == DECEASED_LOCATION_TYPE:
        return StateIncarcerationPeriodReleaseReason.DEATH

    if next_movement_type in FURLOUGH_MOVEMENT_TYPES:
        return StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE

    if ESCAPE in [next_status, next_movement_type]:
        return StateIncarcerationPeriodReleaseReason.ESCAPE

    if (
        next_status in SUPERVISION_STATUSES
        or next_movement_type == RELEASE_MOVEMENT_TYPE
        or next_location_type in SUPERVISION_LOCATION_TYPES
    ):
        return StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION

    if _in_temporary_custody(current_status, location_type, transfer_reason):
        return StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY

    if (
        next_status in INCARCERATED_STATUSES + OTHER_JURISDICTION_STATUSES
        and next_location_type not in DOC_FACILITY_LOCATION_TYPES
    ):
        return StateIncarcerationPeriodReleaseReason.TRANSFER_TO_OTHER_JURISDICTION

    if next_movement_type == TRANSFER_MOVEMENT_TYPE or (
        next_status in INCARCERATED_STATUSES
        and next_location_type in DOC_FACILITY_LOCATION_TYPES
    ):
        return StateIncarcerationPeriodReleaseReason.TRANSFER

    return StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN


def parse_supervision_violation_response_decision(
    raw_text: str,
) -> Optional[StateSupervisionViolationResponseDecision]:
    """Parses the supervision violation response decision from a combination of the violation finding and the
    disposition raw texts.

    If the disposition contains a known non-null value, it can be directly mapped to a particular decision. If it
    doesn't, however, then we need to examine the violation finding.
    """
    (
        violation_finding_description,
        disposition_description,
    ) = raw_text.upper().split("@@")

    if "REVOCATION" in disposition_description:
        return StateSupervisionViolationResponseDecision.REVOCATION

    if disposition_description == "VIOLATION FOUND - CONDITIONS AMENDED":
        return StateSupervisionViolationResponseDecision.NEW_CONDITIONS

    if disposition_description == "VIOLATION FOUND - NO SANCTION":
        return StateSupervisionViolationResponseDecision.NO_SANCTION

    if violation_finding_description == "WARNING BY OFFICER":
        return StateSupervisionViolationResponseDecision.WARNING

    if violation_finding_description in (
        "DISMISSED BY COURT",
        "NOT APPROVED BY PROSECUTING ATTORNEY",
        "VIOLATION NOT FOUND",
        "WITHDRAWN BY OFFICER",
    ):
        # In each case, there was no official/formal/final violation response provided
        return StateSupervisionViolationResponseDecision.VIOLATION_UNFOUNDED

    if violation_finding_description == "GRADUATED SANCTION BY OFFICER":
        return StateSupervisionViolationResponseDecision.OTHER

    if violation_finding_description in (
        "RETURN TO FACILITY BY OFFICER",
        "ABSCONDED - FACILITY NOTIFIED",
    ):
        # Each case leads to a return to a DOC facility, hence the mapping to REVOCATION. Both codes are fairly rare
        return StateSupervisionViolationResponseDecision.REVOCATION

    if violation_finding_description == "VIOLATION FOUND":
        # At this point, it's acknowledged that a violation was found but no disposition is available
        return StateSupervisionViolationResponseDecision.INTERNAL_UNKNOWN

    return StateSupervisionViolationResponseDecision.INTERNAL_UNKNOWN


def parse_supervision_violation_response_type(
    raw_text: str,
) -> Optional[StateSupervisionViolationResponseType]:
    """Parses the supervision violation response decision from a combination of the violation finding and the
    disposition raw texts.

    If the disposition contains a known non-null value, it can be directly mapped to a particular type. If it
    doesn't, however, then we need to examine the violation finding.
    """
    (
        violation_finding_description,
        disposition_description,
    ) = raw_text.upper().split("@@")

    if "REVOCATION" in disposition_description:
        return StateSupervisionViolationResponseType.PERMANENT_DECISION

    if disposition_description == "VIOLATION FOUND - CONDITIONS AMENDED":
        return StateSupervisionViolationResponseType.PERMANENT_DECISION

    if disposition_description == "VIOLATION FOUND - NO SANCTION":
        return StateSupervisionViolationResponseType.VIOLATION_REPORT

    if violation_finding_description == "WARNING BY OFFICER":
        return StateSupervisionViolationResponseType.CITATION

    if violation_finding_description in (
        "DISMISSED BY COURT",
        "NOT APPROVED BY PROSECUTING ATTORNEY",
        "VIOLATION NOT FOUND",
        "WITHDRAWN BY OFFICER",
    ):
        # This should never happen because we have set these values as ignorable in the YAML manifest for US_ME,
        # but we leave this here to throw a loud error in the case that some derivation on this ends up in the raw data
        return None

    if violation_finding_description == "GRADUATED SANCTION BY OFFICER":
        return StateSupervisionViolationResponseType.PERMANENT_DECISION

    if violation_finding_description in (
        "RETURN TO FACILITY BY OFFICER",
        "ABSCONDED - FACILITY NOTIFIED",
    ):
        return StateSupervisionViolationResponseType.PERMANENT_DECISION

    if violation_finding_description == "VIOLATION FOUND":
        # At this point, it's acknowledged that a violation was found but no disposition is available
        return StateSupervisionViolationResponseType.VIOLATION_REPORT

    return None


def parse_deciding_body_type(
    raw_text: str,
) -> Optional[StateSupervisionViolationResponseDecidingBodyType]:
    """Parses the supervision violation response deciding body type from a combination of the violation finding and the
    disposition raw texts.
    """
    (
        violation_finding_description,
        disposition_description,
    ) = raw_text.upper().split("@@")

    # By default, the court has final say. So unless this is a direct officer action, it's the court
    if violation_finding_description in (
        "ABSCONDED - FACILITY NOTIFIED",
        "DISMISSED BY COURT",
        "NOT APPROVED BY PROSECUTING ATTORNEY",
        "VIOLATION NOT FOUND",
        "VIOLATION FOUND",
    ):
        return StateSupervisionViolationResponseDecidingBodyType.COURT

    # These indicate a final finding/action taken directly by the officer without court involvement
    if violation_finding_description in (
        "GRADUATED SANCTION BY OFFICER",
        "RETURN TO FACILITY BY OFFICER",
        "WARNING BY OFFICER",
        "WITHDRAWN BY OFFICER",
    ):
        return StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER

    # Capture cases where the violation finding is NONE but there was a disposition
    if disposition_description != "NONE":
        return StateSupervisionViolationResponseDecidingBodyType.COURT

    # This is unreachable since we are filtering out NONE@@NONE in the enum_mapping conditional, and we should
    # never be returning None from this function.
    return None


def parse_supervision_sentence_status(
    raw_text: str,
) -> Optional[StateSentenceStatus]:
    """Parses the supervision sentence status from a combination of the community override reason code
    and the court order status code. If there is a community override reason code that represents a sentence
    status, we use that value. Otherwise, we use the court order status code.
    """
    (
        community_override_reason,
        court_order_status_description,
    ) = raw_text.upper().split("@@")

    if "REVOCATION" in community_override_reason:
        return StateSentenceStatus.REVOKED

    if community_override_reason in ["EARLY TERMINATION"]:
        return StateSentenceStatus.COMPLETED
    if community_override_reason in ["VACATED SENTENCE"]:
        return StateSentenceStatus.VACATED
    if community_override_reason in ["COMMUTATION/PARDON"]:
        return StateSentenceStatus.COMMUTED

    if court_order_status_description in ["COMPLETE"]:
        return StateSentenceStatus.COMPLETED
    if court_order_status_description in ["PENDING", "PREDISPOSITION"]:
        return StateSentenceStatus.PENDING
    if court_order_status_description in ["COURT SANCTION"]:
        return StateSentenceStatus.SANCTIONED
    if court_order_status_description in ["COMMITTED", "PROBATION", "TOLLED"]:
        return StateSentenceStatus.SERVING

    return StateSentenceStatus.PRESENT_WITHOUT_INFO


def parse_supervision_type(
    raw_text: str,
) -> StateSupervisionPeriodSupervisionType:
    """Parses the supervision type from the current status and jurisdiction location type."""
    (
        current_jurisdiction_location_type,
        current_status,
        _officer_status,
    ) = raw_text.upper().split("@@")
    # TODO(#11875): Add Absconsion Type or Status depending on schema change request
    if current_status == WARRANT_ABSCONDED_STATUS:
        return StateSupervisionPeriodSupervisionType.BENCH_WARRANT

    if current_status == PAROLE_STATUS:
        return StateSupervisionPeriodSupervisionType.PAROLE
    if (
        current_status == COMMUNITY_CONFINEMENT_STATUS
        or current_jurisdiction_location_type in DOC_FACILITY_LOCATION_TYPES
    ):
        return StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT

    return StateSupervisionPeriodSupervisionType.PROBATION


def parse_supervision_admission_reason(
    raw_text: str,
) -> Optional[StateSupervisionPeriodAdmissionReason]:
    """Parses the supervision admission reason."""
    (
        previous_status,
        current_status,
        previous_jurisdiction_location,
        previous_jurisdiction_location_type,
        current_jurisdiction_location_type,
        transfer_type,
        transfer_reason,
    ) = raw_text.upper().split("@@")
    # This could be a change in supervision site or assigned officer
    if current_status in ABSCONSION_STATUSES and previous_status in ABSCONSION_STATUSES:
        return StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE

    if current_status in ABSCONSION_STATUSES:
        return StateSupervisionPeriodAdmissionReason.ABSCONSION

    # Must check RETURN_FROM_ABSCONSION after ABSCONSION
    if previous_status in ABSCONSION_STATUSES:
        return StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION

    if (
        current_jurisdiction_location_type in DOC_FACILITY_LOCATION_TYPES
        or current_status in CONDITIONAL_RELEASE_STATUSES
    ) and _previously_not_on_supervision(
        previous_jurisdiction_location_type, previous_status
    ):
        # Released early from incarceration to a community confinement program
        return StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE

    if (
        previous_jurisdiction_location_type in OTHER_JURISDICTION_LOCATION_TYPES
        and previous_jurisdiction_location != MAINE_STATE
    ):
        return StateSupervisionPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION

    if (
        transfer_type == SOCIETY_IN_TRANSFER_TYPE
        or transfer_reason in SUPERVISION_COURT_SENTENCE_TRANSFER_REASONS
        or _previously_not_on_supervision(
            previous_jurisdiction_location_type, previous_status
        )
    ):
        # These could include scenarios where a person on supervision has returned to incarceration
        # as a violation response, and then must complete the previous supervision sentence. There are
        # two types of revocations, Partial Revocation - Probation to Terminate and
        # Partial Revocation - Probation to Continue. For "Probation to Terminate", the sentence ends
        # with the incarceration period. For "Probation to Continue", the probation period continues
        # after the incarceration period ends. These cases will be handled in the entity normalization step
        # and will reference the associated sentences.
        return StateSupervisionPeriodAdmissionReason.COURT_SENTENCE

    # Assume everything else is a transfer between supervision locations, officers, or status changes
    return StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE


def parse_supervision_termination_reason(
    raw_text: str,
) -> Optional[StateSupervisionPeriodTerminationReason]:
    """Parses the supervision termination reason."""
    (
        next_status,
        next_jurisdiction_location,
        next_jurisdiction_location_type,
        current_status,
    ) = raw_text.upper().split("@@")
    if next_jurisdiction_location_type == DECEASED_LOCATION_TYPE:
        return StateSupervisionPeriodTerminationReason.DEATH

    if (
        current_status in ABSCONSION_STATUSES
        and next_status != "NONE"
        and next_status not in ABSCONSION_STATUSES
        and next_jurisdiction_location_type
        not in DOC_FACILITY_LOCATION_TYPES + COUNTY_JAIL_LOCATION_TYPES
    ):
        return StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION

    if current_status not in ABSCONSION_STATUSES and next_status in ABSCONSION_STATUSES:
        return StateSupervisionPeriodTerminationReason.ABSCONSION

    if (
        next_jurisdiction_location_type in OTHER_JURISDICTION_LOCATION_TYPES
        and next_jurisdiction_location != MAINE_STATE
    ):
        return StateSupervisionPeriodTerminationReason.TRANSFER_TO_OTHER_JURISDICTION

    if next_status == INACTIVE_STATUS or (
        next_status == "NONE" and current_status not in ABSCONSION_STATUSES
    ):
        return StateSupervisionPeriodTerminationReason.EXPIRATION

    if next_jurisdiction_location_type in COUNTY_JAIL_LOCATION_TYPES:
        # These could be in a County Jail pending charges or investigation for a supervision violation.
        return StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE

    if (
        next_jurisdiction_location_type in DOC_FACILITY_LOCATION_TYPES
        and next_status in INCARCERATED_STATUSES
    ):
        # These could be directly to a DOC Facility if in a Community Confinement program, or a revocation
        # which will be updated to REVOCATION reason in the entity normalization step.
        return StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION

    return StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE
