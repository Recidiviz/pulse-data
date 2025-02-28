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
"""Custom enum parsers functions for US_NE. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_ne_custom_enum_parsers.<function name>
"""
from typing import Optional

from recidiviz.common.constants.state.state_sentence import StateSentencingAuthority
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority


def parse_sentencing_authority(
    raw_text: str,
) -> Optional[StateSentencingAuthority]:
    """
    Determine sentencing authority from county
    """
    COUNTY = raw_text

    if COUNTY == "US MARSHAL/ATTORNEY":
        return StateSentencingAuthority.FEDERAL

    if COUNTY == "OUT OF STATE":
        return StateSentencingAuthority.OTHER_STATE

    return StateSentencingAuthority.COUNTY


def parse_custodial_authority(
    raw_text: str,
) -> Optional[StateCustodialAuthority]:
    """
    Determine custodial authority from in/out state indicator codes
    See https://drive.google.com/drive/folders/1C1GDoQFttK_gJtOR0dcfJ2mRd9o2PVuI
    for Nebraska usage of inOutStateIndicator1Code (code1),
    inOutStateIndicator2Code (location code), and inOutStateIndicator3Code (code2)
    """
    code1, location, code2 = raw_text.split("@@")

    if ((code1 == "2" and code2 == "1") or code1 == "4") and location != "NE":
        return StateCustodialAuthority.OTHER_STATE

    return StateCustodialAuthority.SUPERVISION_AUTHORITY
