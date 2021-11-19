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

OTHER_JURISDICTION_TRANSFER_TYPES = [
    "Non-DOC In",
    "Non-DOC County Jail In",
    "DOC County Jail Return From",
    "DOC Interstate Compact Return From/In",
    "DOC Interstate Active Detainer Return",
]

TEMPORARY_CUSTODY_TRANSFER_REASONS = ["Safe Keepers", "Federal Hold", "Federal BOP"]

INCARCERATED_STATUSES = [
    "Incarcerated",
    "County Jail",
    "Interstate Compact In",
    "Partial Revocation - incarcerated",
]

SUPERVISION_STATUSES = [
    "SCCP",
    "Probation",
    "Parole",
    "Warrant Absconded",
    "Pending Violation - Incarcerated",
    "Partial Revocation - probation to continue",
    "Partial Revocation - probation to terminate",
]

# Location types are from the CIS_908_CCS_LOCATION.Cis_9080_Ccs_Location_Type_Cd column
DOC_FACILITY_LOCATION_TYPES = [
    "2",  # Adult DOC Facilities
    "3",  # Juvenile DOC Facilities
    "7",  # Pre-Release Centers
]
COUNTY_JAIL_LOCATION_TYPES = ["9", "13"]  # County Jails and Maine Counties
SUPERVISION_LOCATION_TYPES = ["4"]


def in_temporary_custody(
    current_status: str,
    location_type: str,
    transfer_reason: Optional[str],
) -> bool:
    return (
        current_status == "County Jail" and location_type in DOC_FACILITY_LOCATION_TYPES
    ) or transfer_reason in TEMPORARY_CUSTODY_TRANSFER_REASONS


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
    if in_temporary_custody(current_status, location_type, transfer_reason):
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

    if transfer_reason == "Sentence/Disposition":
        if previous_status in SUPERVISION_STATUSES:
            return StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION

        return StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION

    if movement_type in ("Furlough", "Furlough Hospital"):
        return StateIncarcerationPeriodAdmissionReason.RETURN_FROM_TEMPORARY_RELEASE

    if in_temporary_custody(current_status, location_type, transfer_reason):
        return StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY

    if (
        transfer_type in OTHER_JURISDICTION_TRANSFER_TYPES
        or transfer_reason == "Other Jurisdiction"
    ):
        return StateIncarcerationPeriodAdmissionReason.TRANSFER_FROM_OTHER_JURISDICTION

    if (
        transfer_reason
        in [
            "Violation of Probation",
            "Violation of Probation - Prob. to Terminate(J)",
        ]
        or current_status == "Partial Revocation - incarcerated"
    ):
        return StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION

    if transfer_reason == "Violation of Parole":
        return StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION

    if previous_status in SUPERVISION_STATUSES:
        return StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION

    if previous_status == "Escape" or movement_type == "Escape":
        return StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE

    return StateIncarcerationPeriodAdmissionReason.TRANSFER


def parse_release_reason(raw_text: str) -> StateIncarcerationPeriodReleaseReason:
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

    if next_status == "Inactive" and next_location_type == "14":
        return StateIncarcerationPeriodReleaseReason.DEATH

    if next_status in SUPERVISION_STATUSES:
        return StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION

    if next_status == "Escape" or next_movement_type == "Escape":
        return StateIncarcerationPeriodReleaseReason.ESCAPE

    if in_temporary_custody(current_status, location_type, transfer_reason):
        return StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY

    if (
        next_status in INCARCERATED_STATUSES
        and next_location_type not in DOC_FACILITY_LOCATION_TYPES
    ):
        return StateIncarcerationPeriodReleaseReason.TRANSFER_TO_OTHER_JURISDICTION

    if next_movement_type in ("Furlough", "Furlough Hospital"):
        return StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE

    if next_movement_type == "Transfer":
        return StateIncarcerationPeriodReleaseReason.TRANSFER

    return StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN
