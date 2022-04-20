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
"""Custom enum parsers functions for US_ND. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_nd_custom_enum_parsers.<function name>
"""
import datetime
from typing import Dict, List, Type

from recidiviz.common.constants.entity_enum import EntityEnum, EnumParsingError
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.str_field_utils import parse_datetime

OTHER_STATE_FACILITY = "OOS"

POST_JULY_2017_CUSTODIAL_AUTHORITY_ENUM_MAP: Dict[
    StateCustodialAuthority, List[str]
] = {
    StateCustodialAuthority.COURT: [
        "CJ",
        "DEFP",
        # There are only a few of these, and they seem to represent judicial
        # districts in ND
        "NW",
        "SC",
        "SW",
    ],
    StateCustodialAuthority.EXTERNAL_UNKNOWN: [
        # Could be a county jail or another state's facility
        "NTAD",
    ],
    StateCustodialAuthority.STATE_PRISON: [
        "BTC",
        "CONT",
        "CPP",
        "DWCRC",
        "FTPFAR",
        "FTPMND",
        "GFC",
        "HACTC",
        "HRCC",
        "INACT",
        "JRCC",
        "LRRP",
        "MRCC",
        "MTPFAR",
        "MTPMDN",
        "MTPMND",
        "NCCRC",
        "NDSP",
        "OUT",
        "PREA",
        "PROB",
        # TODO(#10432): We don't know what facility "TABLET" is - ask ND how to
        #   properly map this.
        "TABLET",
        "TRC",
        "TRCC",
        "TRN",
        "YCC",
    ],
}

POST_JULY_2017_PFI_ENUM_MAP: Dict[
    StateSpecializedPurposeForIncarceration, List[str]
] = {
    StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY: [
        "CJ",
        "DEFP",
        "NTAD",
        # There are only a few of these, and they seem to represent judicial
        # districts in ND
        "NW",
        "SC",
        "SW",
    ],
    StateSpecializedPurposeForIncarceration.GENERAL: [
        "BTC",
        "CONT",
        "CPP",
        "DWCRC",
        "FTPFAR",
        "FTPMND",
        "GFC",
        "HACTC",
        "HRCC",
        "INACT",
        "JRCC",
        "LRRP",
        "MRCC",
        "MTPFAR",
        "MTPMDN",
        "MTPMND",
        "NCCRC",
        "NDSP",
        "OUT",
        "PREA",
        "PROB",
        # TODO(#10432): We don't know what facility "TABLET" is - ask ND how to
        #   properly map this.
        "TABLET",
        "TRC",
        "TRCC",
        "TRN",
        "YCC",
    ],
}

POST_JULY_2017_CUSTODIAL_AUTHORITY_RAW_TEXT_TO_ENUM_MAP: Dict[
    str, StateCustodialAuthority
] = {
    raw_text_value: custodial_authority
    for custodial_authority, raw_text_values in POST_JULY_2017_CUSTODIAL_AUTHORITY_ENUM_MAP.items()
    for raw_text_value in raw_text_values
}

POST_JULY_2017_PFI_RAW_TEXT_TO_ENUM_MAP: Dict[
    str, StateSpecializedPurposeForIncarceration
] = {
    raw_text_value: custodial_authority
    for custodial_authority, raw_text_values in POST_JULY_2017_PFI_ENUM_MAP.items()
    for raw_text_value in raw_text_values
}


def _datetime_str_is_before_2017_custodial_authority_cutoff(
    datetime_str: str, enum_type_being_parsed: Type[EntityEnum]
) -> bool:
    comparison_date = parse_datetime(datetime_str)

    if not comparison_date:
        raise EnumParsingError(
            enum_type_being_parsed,
            "Unable to parse custodial authority without a valid date on the IP. "
            f"Found: {datetime_str}.",
        )

    return comparison_date < datetime.datetime(year=2017, month=7, day=1)


def custodial_authority_from_facility_and_dates(
    raw_text: str,
) -> StateCustodialAuthority:
    facility, datetime_str_for_comparison = raw_text.split("-")

    if facility == OTHER_STATE_FACILITY:
        return StateCustodialAuthority.OTHER_STATE

    # Everything except OOS (checked above) was overseen by DOCR before July 1, 2017.
    if _datetime_str_is_before_2017_custodial_authority_cutoff(
        datetime_str_for_comparison, StateCustodialAuthority
    ):
        return StateCustodialAuthority.STATE_PRISON

    if facility not in POST_JULY_2017_CUSTODIAL_AUTHORITY_RAW_TEXT_TO_ENUM_MAP:
        raise EnumParsingError(
            StateCustodialAuthority,
            "Found facility without a mapping to a custodial authority: "
            f"{facility}.",
        )

    return POST_JULY_2017_CUSTODIAL_AUTHORITY_RAW_TEXT_TO_ENUM_MAP[facility]


def pfi_from_facility_and_dates(
    raw_text: str,
) -> StateSpecializedPurposeForIncarceration:
    facility, datetime_str_for_comparison = raw_text.split("-")

    if facility == OTHER_STATE_FACILITY:
        return StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN

    # There were no periods of temporary custody before July 1, 2017.
    if _datetime_str_is_before_2017_custodial_authority_cutoff(
        datetime_str_for_comparison, StateSpecializedPurposeForIncarceration
    ):
        return StateSpecializedPurposeForIncarceration.GENERAL

    if facility not in POST_JULY_2017_PFI_RAW_TEXT_TO_ENUM_MAP:
        raise EnumParsingError(
            StateSpecializedPurposeForIncarceration,
            "Found facility without a mapping to a pfi: " f"{facility}.",
        )

    return POST_JULY_2017_PFI_RAW_TEXT_TO_ENUM_MAP[facility]
