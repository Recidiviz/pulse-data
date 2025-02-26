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
"""Custom parser functions for US_ND. Can be referenced in an ingest view manifest
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
from typing import Optional

from recidiviz.common import ncic
from recidiviz.common.str_field_utils import (
    parse_days_from_duration_pieces,
    safe_parse_days_from_duration_str,
)
from recidiviz.ingest.direct.legacy_ingest_mappings.state_shared_row_posthooks import (
    get_normalized_ymd_str,
)
from recidiviz.ingest.direct.regions.us_nd.us_nd_county_code_reference import (
    COUNTY_CODES,
    normalized_county_code,
)


def decimal_str_as_int_str(dec_str: str) -> str:
    """Converts a comma-separated string representation of an integer into a string
    representing a simple integer with no commas.

    E.g. _decimal_str_as_int_str('1,234.00') -> '1234'
    """
    if not dec_str:
        return dec_str

    return str(int(float(dec_str.replace(",", ""))))


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


def max_length_days_from_ymd(years: str, months: str, days: str) -> str:
    normalized_ymd_str = get_normalized_ymd_str(
        years_numerical_str=years,
        months_numerical_str=months,
        days_numerical_str=days,
    )

    return str(safe_parse_days_from_duration_str(normalized_ymd_str))


def classification_type_raw_text_from_raw_text(raw_charge_text: str) -> Optional[str]:
    classification_str = raw_charge_text.upper()
    if classification_str.startswith("F") or classification_str.startswith("M"):
        return classification_str[0]
    if classification_str in ("IM", "IF"):
        return classification_str[1]
    return None


def classification_subtype_from_raw_text(raw_charge_text: str) -> Optional[str]:
    classification_str = raw_charge_text.upper()
    if (
        classification_str.startswith("F") or classification_str.startswith("M")
    ) and len(classification_str) > 1:
        return classification_str[1:]
    return None


def extract_description_from_ncic_code(ncic_code: str) -> Optional[str]:
    return ncic.get_description(ncic_code)


def extract_is_violent_from_ncic_code(ncic_code: str) -> Optional[bool]:
    return ncic.get_is_violent(ncic_code)
