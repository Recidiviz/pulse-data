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
