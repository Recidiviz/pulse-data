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

from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority


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
