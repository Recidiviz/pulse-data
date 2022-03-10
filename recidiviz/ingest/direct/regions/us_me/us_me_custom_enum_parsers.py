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
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)

FURLOUGH_MOVEMENT_TYPES = ["Furlough", "Furlough Hospital"]

OTHER_JURISDICTION_TRANSFER_TYPES = [
    "Non-DOC In",
    "Non-DOC County Jail In",
    "DOC County Jail Return From",
    "DOC Interstate Compact Return From/In",
    "DOC Interstate Active Detainer Return",
]

SUPERVISION_VIOLATION_TRANSFER_REASONS = [
    "Violation of Probation",
    "Violation of Probation - Prob. to Terminate(J)",
    "Violation of Parole",
    "Violation of SCCP",
]

TEMPORARY_CUSTODY_TRANSFER_REASONS = ["Safe Keepers", "Federal Hold", "Federal BOP"]

INCARCERATED_STATUSES = [
    "Incarcerated",
    "County Jail",
    "Interstate Compact In",
    "Partial Revocation - incarcerated",
]

OTHER_JURISDICTION_STATUSES = [
    "Interstate Compact Out",
    "Interstate Active Detainer",
]

COMMUNITY_CONFINEMENT_STATUS = "SCCP"

SUPERVISION_PRECEDING_INCARCERATION_STATUSES = [
    "Pending Violation",
    "Pending Violation - Incarcerated",
    "Warrant Absconded",
    "Partial Revocation - County Jail",
]

SUPERVISION_STATUSES = [
    COMMUNITY_CONFINEMENT_STATUS,
    "Probation",
    "Parole",
]

# Location types are from the CIS_908_CCS_LOCATION.Cis_9080_Ccs_Location_Type_Cd column
DOC_FACILITY_LOCATION_TYPES = [
    "2",  # Adult DOC Facilities
    "7",  # Pre-Release Centers
]
SUPERVISION_LOCATION_TYPES = ["4"]  # Adult Supervision Offices
COUNTY_JAIL_LOCATION_TYPES = ["9"]  # County Jails


def _in_temporary_custody(
    current_status: str,
    location_type: str,
    transfer_reason: Optional[str],
) -> bool:
    return (
        (
            current_status == "County Jail"
            and location_type in DOC_FACILITY_LOCATION_TYPES
        )
        or transfer_reason in TEMPORARY_CUSTODY_TRANSFER_REASONS
        or location_type in COUNTY_JAIL_LOCATION_TYPES
    )


def _is_new_admission(transfer_reason: str, movement_type: str) -> bool:
    return (
        movement_type == "Sentence/Disposition"
        or transfer_reason == "Sentence/Disposition"
    )


def parse_incarceration_type(raw_text: str) -> StateIncarcerationType:
    if raw_text in COUNTY_JAIL_LOCATION_TYPES:
        return StateIncarcerationType.COUNTY_JAIL
    if raw_text in DOC_FACILITY_LOCATION_TYPES:
        return StateIncarcerationType.STATE_PRISON
    return StateIncarcerationType.EXTERNAL_UNKNOWN


def parse_specialized_purpose_for_incarceration(
    raw_text: str,
) -> StateSpecializedPurposeForIncarceration:
    (
        current_status,
        transfer_reason,
        location_type,
    ) = raw_text.split("@@")
    if _in_temporary_custody(current_status, location_type, transfer_reason):
        return StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY
    return StateSpecializedPurposeForIncarceration.GENERAL


def parse_admission_reason(raw_text: str) -> StateIncarcerationPeriodAdmissionReason:
    """Parse admission reason from raw text"""
    (
        previous_status,
        current_status,
        movement_type,
        transfer_type,
        transfer_reason,
        location_type,
    ) = raw_text.split("@@")

    if (
        previous_status
        in SUPERVISION_STATUSES + SUPERVISION_PRECEDING_INCARCERATION_STATUSES
    ):
        # This includes when someone violates the terms of their community confinement (SCCP) and is sent back to
        # prison. Maine does not use the term "revocation" to describe these instances, but they fit our internal
        # definition of revocations.
        return StateIncarcerationPeriodAdmissionReason.REVOCATION

    if _is_new_admission(transfer_reason, movement_type):
        return StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION

    if movement_type in FURLOUGH_MOVEMENT_TYPES:
        return StateIncarcerationPeriodAdmissionReason.RETURN_FROM_TEMPORARY_RELEASE

    if _in_temporary_custody(current_status, location_type, transfer_reason):
        return StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY

    if (
        transfer_type in OTHER_JURISDICTION_TRANSFER_TYPES
        or transfer_reason == "Other Jurisdiction"
        or previous_status in OTHER_JURISDICTION_STATUSES
    ):
        return StateIncarcerationPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION

    if (
        previous_status == "NONE"
    ) and transfer_reason in SUPERVISION_VIOLATION_TRANSFER_REASONS:
        # These rare cases happen if the previous supervision period occurred in a supervision office, or if the
        # supervision period occurred before the year 2000.
        return StateIncarcerationPeriodAdmissionReason.REVOCATION

    if previous_status == "Escape" or movement_type == "Escape":
        return StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE

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
    ) = raw_text.split("@@")
    if next_movement_type == "Discharge":
        return StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED

    if next_location_type == "14":
        return StateIncarcerationPeriodReleaseReason.DEATH

    if next_movement_type in FURLOUGH_MOVEMENT_TYPES:
        return StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE

    if next_status == "Escape" or next_movement_type == "Escape":
        return StateIncarcerationPeriodReleaseReason.ESCAPE

    if (
        next_status in SUPERVISION_STATUSES
        or next_movement_type == "Release"
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

    if next_movement_type == "Transfer" or (
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
    ) = raw_text.split("@@")

    normalized_violation_finding = violation_finding_description.upper()
    normalized_disposition = disposition_description.upper()

    if "REVOCATION" in normalized_disposition:
        return StateSupervisionViolationResponseDecision.REVOCATION

    if normalized_disposition == "VIOLATION FOUND - CONDITIONS AMENDED":
        return StateSupervisionViolationResponseDecision.NEW_CONDITIONS

    if normalized_disposition == "VIOLATION FOUND - NO SANCTION":
        return StateSupervisionViolationResponseDecision.NO_SANCTION

    if normalized_violation_finding == "WARNING BY OFFICER":
        return StateSupervisionViolationResponseDecision.WARNING

    if normalized_violation_finding in (
        "DISMISSED BY COURT",
        "NOT APPROVED BY PROSECUTING ATTORNEY",
        "VIOLATION NOT FOUND",
        "WITHDRAWN BY OFFICER",
    ):
        # In each case, there was no official/formal/final violation response provided
        return StateSupervisionViolationResponseDecision.VIOLATION_UNFOUNDED

    if normalized_violation_finding == "GRADUATED SANCTION BY OFFICER":
        return StateSupervisionViolationResponseDecision.OTHER

    if normalized_violation_finding in (
        "RETURN TO FACILITY BY OFFICER",
        "ABSCONDED - FACILITY NOTIFIED",
    ):
        # Each case leads to a return to a DOC facility, hence the mapping to REVOCATION. Both codes are fairly rare
        return StateSupervisionViolationResponseDecision.REVOCATION

    if normalized_violation_finding == "VIOLATION FOUND":
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
    ) = raw_text.split("@@")

    normalized_violation_finding = violation_finding_description.upper()
    normalized_disposition = disposition_description.upper()

    if "REVOCATION" in normalized_disposition:
        return StateSupervisionViolationResponseType.PERMANENT_DECISION

    if normalized_disposition == "VIOLATION FOUND - CONDITIONS AMENDED":
        return StateSupervisionViolationResponseType.PERMANENT_DECISION

    if normalized_disposition == "VIOLATION FOUND - NO SANCTION":
        return StateSupervisionViolationResponseType.VIOLATION_REPORT

    if normalized_violation_finding == "WARNING BY OFFICER":
        return StateSupervisionViolationResponseType.CITATION

    if normalized_violation_finding in (
        "DISMISSED BY COURT",
        "NOT APPROVED BY PROSECUTING ATTORNEY",
        "VIOLATION NOT FOUND",
        "WITHDRAWN BY OFFICER",
    ):
        # This should never happen because we have set these values as ignorable in the YAML manifest for US_ME,
        # but we leave this here to throw a loud error in the case that some derivation on this ends up in the raw data
        return None

    if normalized_violation_finding == "GRADUATED SANCTION BY OFFICER":
        return StateSupervisionViolationResponseType.PERMANENT_DECISION

    if normalized_violation_finding in (
        "RETURN TO FACILITY BY OFFICER",
        "ABSCONDED - FACILITY NOTIFIED",
    ):
        return StateSupervisionViolationResponseType.PERMANENT_DECISION

    if normalized_violation_finding == "VIOLATION FOUND":
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
    ) = raw_text.split("@@")

    normalized_violation_finding = violation_finding_description.upper()
    normalized_disposition = disposition_description.upper()

    # By default, the court has final say. So unless this is a direct officer action, it's the court
    if normalized_violation_finding in (
        "ABSCONDED - FACILITY NOTIFIED",
        "DISMISSED BY COURT",
        "NOT APPROVED BY PROSECUTING ATTORNEY",
        "VIOLATION NOT FOUND",
        "VIOLATION FOUND",
    ):
        return StateSupervisionViolationResponseDecidingBodyType.COURT

    # These indicate a final finding/action taken directly by the officer without court involvement
    if normalized_violation_finding in (
        "GRADUATED SANCTION BY OFFICER",
        "RETURN TO FACILITY BY OFFICER",
        "WARNING BY OFFICER",
        "WITHDRAWN BY OFFICER",
    ):
        return StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER

    # Capture cases where the violation finding is NONE but there was a disposition
    if normalized_disposition != "NONE":
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
    ) = raw_text.split("@@")

    normalized_community_override_reason = community_override_reason.upper()
    normalized_court_order_status = court_order_status_description.upper()

    if "REVOCATION" in normalized_community_override_reason:
        return StateSentenceStatus.REVOKED

    if normalized_community_override_reason in ["EARLY TERMINATION"]:
        return StateSentenceStatus.COMPLETED
    if normalized_community_override_reason in ["VACATED SENTENCE"]:
        return StateSentenceStatus.VACATED
    if normalized_community_override_reason in ["COMMUTATION/PARDON"]:
        return StateSentenceStatus.COMMUTED

    if normalized_court_order_status in ["COMPLETE"]:
        return StateSentenceStatus.COMPLETED
    if normalized_court_order_status in ["PENDING", "PREDISPOSITION"]:
        return StateSentenceStatus.PENDING
    if normalized_court_order_status in ["COURT SANCTION"]:
        return StateSentenceStatus.SANCTIONED
    if normalized_court_order_status in ["COMMITTED", "PROBATION", "TOLLED"]:
        return StateSentenceStatus.SERVING

    return StateSentenceStatus.PRESENT_WITHOUT_INFO
