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
import logging
import re
from re import Pattern
from typing import Optional

from recidiviz.common import ncic
from recidiviz.common.str_field_utils import (
    parse_days_from_duration_pieces,
    safe_parse_days_from_duration_pieces,
)
from recidiviz.ingest.direct.regions.us_nd.us_nd_county_code_reference import (
    COUNTY_CODES,
    normalized_county_code,
)


def decimal_str_as_int_str(dec_str: str) -> str:
    """Converts a comma-separated string representation of an integer into a string
    representing a simple integer with no commas.

    As of 7/25/23 there is one case of a ROOT_OFFENDER_ID being reused. This is handled
    by appending -2 to the value in a raw data migration, in which case the value is
    already a string representation of an integer and is returned as is.

    E.g. _decimal_str_as_int_str('1,234.00') -> '1234'
         _decimal_str_as_int_str('1234-2') -> '1234-2'
    """
    if not dec_str or "-" in dec_str:
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
    """Returns a string number of days for the max length of a given sentence, in XXXX
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
    return str(total_days)


def are_new_offenses_violent(
    new_offense_1_ncic: str,
    new_offense_2_ncic: str,
    new_offense_3_ncic: str,
) -> bool:
    """Returns whether any of the NCIC codes are for violent offenses."""
    violent_flags = [
        extract_is_violent_from_ncic_code(code)
        for code in [new_offense_1_ncic, new_offense_2_ncic, new_offense_3_ncic]
        if code
    ]
    return any(violent_flags)


def max_length_days_from_ymd(years: str, months: str, days: str) -> Optional[str]:
    result = safe_parse_days_from_duration_pieces(
        years_str=years, months_str=months, days_str=days
    )
    if result is None:
        return None
    return str(result)


def classification_type_raw_text_from_raw_text(raw_charge_text: str) -> Optional[str]:
    classification_str = raw_charge_text.upper()
    if classification_str.startswith("F") or classification_str.startswith("M"):
        return classification_str[0]
    if classification_str in ("IM", "IF"):
        return classification_str[1]
    return ""


def classification_subtype_from_raw_text(raw_charge_text: str) -> Optional[str]:
    if raw_charge_text:
        classification_str = raw_charge_text.upper()
        if (
            classification_str.startswith("F") or classification_str.startswith("M")
        ) and len(classification_str) > 1:
            return classification_str[1:]
        return None
    return None


def extract_description_from_ncic_code(ncic_code: str) -> Optional[str]:
    return ncic.get_description(ncic_code)


def extract_is_violent_from_ncic_code(ncic_code: str) -> Optional[bool]:
    return ncic.get_is_violent(ncic_code)


def extract_is_drug_from_ncic_code(ncic_code: str) -> Optional[bool]:
    return ncic.get_is_drug(ncic_code)


def normalize_ncic_code(ncic_code: str) -> Optional[str]:
    code = ncic.get(ncic_code)
    if code:
        return code.ncic_code
    return None


_JUDICIAL_DISTRICT_CODE_MAPPINGS = {
    # ND Jurisdictions
    "SE": "SOUTHEAST",
    "EAST": "EAST_CENTRAL",
    "SC": "SOUTH_CENTRAL",
    "NW": "NORTHWEST",
    "NE": "NORTHEAST",
    "SW": "SOUTHWEST",
    "NEC": "NORTHEAST_CENTRAL",
    "NC": "NORTH_CENTRAL",
    # Non-ND Jurisidctions
    "OOS": "OUT_OF_STATE",
    "OS": "OUT_OF_STATE",
    "FD": "FEDERAL",
}


def normalize_judicial_district_code(judicial_district_code_text: str) -> Optional[str]:
    if not judicial_district_code_text:
        return None

    if judicial_district_code_text not in _JUDICIAL_DISTRICT_CODE_MAPPINGS:
        logging.warning(
            "Found new judicial district code not in reference cache: [%s]",
            judicial_district_code_text,
        )
        return judicial_district_code_text

    return _JUDICIAL_DISTRICT_CODE_MAPPINGS[judicial_district_code_text]


def get_score_sum(
    *,
    d1: str,
    d2: str,
    d3: str,
    d4: str,
    d5: str,
    d6: str,
    d7: str,
    d8: str,
    d9: str,
    d10: str,
) -> str:
    """Given a set of scores |d1-d10|, returns the sum of all scores as a string."""
    score_sum = 0
    scores = [
        d1,
        d2,
        d3,
        d4,
        d5,
        d6,
        d7,
        d8,
        d9,
        d10,
    ]
    for score in scores:
        score = str(score)
        if score.strip():
            score_sum += int(score.strip())
    return str(score_sum)


def get_loc_within_facility(facility: str, facility_with_loc: str) -> Optional[str]:
    if facility and facility_with_loc:
        facility_prefix = f"{facility}-"
        if facility_with_loc.startswith(facility_prefix):
            return facility_with_loc[len(facility_prefix) :]
    return None


def get_punishment_days(months: str, days: str, effective_date: str) -> Optional[str]:
    if months or days:
        return str(
            parse_days_from_duration_pieces(
                years_str=None,
                months_str=months,
                days_str=days,
                start_dt_str=effective_date,
            )
        )
    return None
