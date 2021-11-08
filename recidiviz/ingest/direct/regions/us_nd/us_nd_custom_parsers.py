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

my_flat_field:
    $custom:
        $function: us_nd_custom_parsers.<function name>
        $args:
            arg_1: <expression>
            arg_2: <expression>
"""

import re
from re import Pattern

from recidiviz.common import ncic
from recidiviz.common.str_field_utils import parse_days_from_duration_pieces
from recidiviz.ingest.direct.regions.us_nd.us_nd_county_code_reference import (
    COUNTY_CODES,
    normalized_county_code,
)


def normalize_county_code(county_code: str) -> str:
    """Takes in an ND raw county code (either a number or two-letter value) and returns
    a Recidiviz-normalized county code in US_XX_YYYYY format.
    """
    if not county_code:
        return ""

    normalized_code = normalized_county_code(county_code, COUNTY_CODES)
    if normalized_code is None:
        raise ValueError(f"Found null normalized county code for code: [{county_code}]")
    return normalized_code


_DOCSTARS_NEGATIVE_PATTERN: Pattern = re.compile(r"^\((?P<value>-?\d+)\)$")


def parse_supervision_sentence_max_length(
    years: str, months: str, effective_date: str
) -> str:
    """Returns a string number of days for the max length of a given sentence, in XXXXd
    format.
    """
    # It appears a recent change to Docstars files started passing negative values inside of parentheses instead
    # of after a '-' sign
    match = re.match(_DOCSTARS_NEGATIVE_PATTERN, months)
    if match is not None:
        value = match.group("value")
        months = "-" + value

    if not years and not months:
        return ""

    total_days = parse_days_from_duration_pieces(
        years_str=years,
        months_str=months,
        days_str=None,
        start_dt_str=effective_date,
    )
    day_string = f"{total_days}d"
    return day_string


def are_new_offenses_violent(
    new_offense_1_ncic: str,
    new_offense_2_ncic: str,
    new_offense_3_ncic: str,
) -> bool:
    """Returns whether any of the NCIC codes are for violent offenses."""
    violent_flags = [
        ncic.get_is_violent(code)
        for code in [new_offense_1_ncic, new_offense_2_ncic, new_offense_3_ncic]
        if code
    ]
    return any(violent_flags)
