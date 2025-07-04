# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Custom enum parsers functions for US_TX. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_tx_custom_enum_parsers.<function name>
"""
import re

from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)


def parse_custodial_auth(
    raw_text: str,
) -> StateCustodialAuthority:
    """Determines which supervision custodial authroity to map to based on a few key words"""
    # TODO(#35061) Figure out how to determine if someone is OOS
    if "TEXAS COUNTY JAIL" in raw_text:
        return StateCustodialAuthority.COUNTY
    if "DPO" in raw_text:
        return StateCustodialAuthority.SUPERVISION_AUTHORITY
    if "STATE JAIL" in raw_text:
        return StateCustodialAuthority.STATE_PRISON
    if "IMMIGRATION & CUSTOMS ENFORCEMENT -" == raw_text:
        return StateCustodialAuthority.FEDERAL
    # TDCJ uses country character codes which are 2 characters
    if len(raw_text) == 2:
        return StateCustodialAuthority.FEDERAL
    return StateCustodialAuthority.INTERNAL_UNKNOWN


IN_CUSTODY_REGEX = re.compile(
    "|".join(
        [
            "IN CUSTODY",
            "REVOKED",
            "PRE-REVOCATION",
        ]
    )
)
NOT_IN_CUSTODY_REGEX = re.compile(
    "|".join(
        [
            "NOT REVOKED",
            "NOT IN CUSTODY",
        ]
    )
)

WARRANT_STATUSES = {"PENDING WARRANT CLOSURE", "PRE-REVOCATION - NOT IN CUSTODY"}


def parse_supervision_level(
    raw_text: str,
) -> StateSupervisionLevel:
    """
    Determines the supervision level in this order:
        1. Checking if the status indicates that the person is in custody
        2. Checking the special conditions for a known special case
        3. Checking for a provided case type
        4. Checking for a provided assessment level
    """
    raw_text = raw_text.upper()
    (
        case_type,
        status,
        assessment_level,
    ) = raw_text.split("@@")

    if status in WARRANT_STATUSES:
        return StateSupervisionLevel.WARRANT

    if IN_CUSTODY_REGEX.search(status) and not NOT_IN_CUSTODY_REGEX.search(status):
        return StateSupervisionLevel.IN_CUSTODY

    if case_type == "ANNUAL":
        return StateSupervisionLevel.LIMITED

    if case_type == "NON-REPORTING":
        return StateSupervisionLevel.UNSUPERVISED

    if case_type in {
        "SUBSTANCE ABUSE - PHASE 1",
        "SUBSTANCE ABUSE - PHASE 2",
        "SUBSTANCE ABUSE - PHASE 1B",
        "ELECTRONIC MONITORING",
    }:
        return StateSupervisionLevel.MAXIMUM

    if assessment_level == "L":
        return StateSupervisionLevel.MINIMUM
    if assessment_level == "LM":
        return StateSupervisionLevel.MEDIUM
    if assessment_level == "M":
        return StateSupervisionLevel.HIGH
    if assessment_level in ("MH", "H"):
        return StateSupervisionLevel.MAXIMUM

    # This can happen when special_conditions, case_type, status, and
    # assessment_level are all unknown for a given period.
    # For example, TX creates periods for the XREF_UPDATE_DATE critical dt
    # before establishing case_type from CTH_CREATION_DATE, status from
    # OSTS_UPDATE_DATE, etc.
    return StateSupervisionLevel.INTERNAL_UNKNOWN
