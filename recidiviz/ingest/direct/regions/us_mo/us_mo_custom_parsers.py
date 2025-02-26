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
"""Custom parser functions for US_MO. Can be referenced in an ingest view manifest
like this:

my_flat_field:
    $custom:
        $function: us_mo_custom_parsers.<function name>
        $args:
            arg_1: <expression>
            arg_2: <expression>
"""
import datetime
import logging
import re
from typing import Optional

from recidiviz.common import ncic
from recidiviz.common.str_field_utils import (
    parse_days_from_duration_pieces,
    parse_yyyymmdd_date,
    safe_parse_date_from_date_pieces,
    safe_parse_days_from_duration_pieces,
)
from recidiviz.ingest.direct.regions.us_mo.us_mo_county_code_reference import (
    COUNTY_CODES,
)
from recidiviz.ingest.direct.regions.us_nd.us_nd_county_code_reference import (
    normalized_county_code,
)


def normalize_county_code(county_code: str) -> str:
    """Takes in a MO raw county code and returns
    a Recidiviz-normalized county code in US_XX_YYYYY format.
    """
    if not county_code:
        return ""

    normalized_code = normalized_county_code(county_code, COUNTY_CODES)
    if normalized_code is None:
        raise ValueError(f"Found null normalized county code for code: [{county_code}]")
    return normalized_code


def max_length_days_from_ymd(years: str, months: str, days: str) -> Optional[str]:
    result = safe_parse_days_from_duration_pieces(
        years_str=years, months_str=months, days_str=days
    )
    if result is None:
        return None
    return str(result)


def set_parole_eligibility_date(start_date: str, parole_ineligible_years: str) -> str:
    sentence_start_date = parse_yyyymmdd_date(start_date)

    if not sentence_start_date:
        return ""

    parole_ineligible_days = parse_days_from_duration_pieces(
        years_str=parole_ineligible_years
    )
    date = sentence_start_date + datetime.timedelta(days=parole_ineligible_days)

    date_iso = date.isoformat()

    return str(date_iso)


def set_charge_is_violent_from_ncic(ncic_code: str) -> bool:
    is_violent = ncic.get_is_violent(ncic_code)
    return bool(is_violent)


def set_response_date(final_formed_create_date: str, response_date: str) -> str:
    """Finally formed documents are the ones that are no longer in a draft state.
    Updates the SupervisionViolationResponses based on whether or not a finally formed
    date is present in the raw data.
    """

    finally_formed_date = mo_julian_date_to_yyyymmdd(final_formed_create_date)

    if not finally_formed_date:
        date = response_date
    else:
        date = finally_formed_date
    return date


JULIAN_DATE_STR_REGEX = re.compile(r"(\d?\d\d)(\d\d\d)")


def mo_julian_date_to_yyyymmdd(julian_date_str: Optional[str]) -> Optional[str]:
    """
    Parse julian-formatted date strings used by MO in a number of DB fields that encode a date using the number of
    years since 1900 concatenated with the number of days since Jan 1 of that year (1-indexed). Returns the date in
    YYYYMMDD date format.

    E.g.:
        85001 -> 19850101
        118365 -> 20181231
    """
    if not julian_date_str or int(julian_date_str) == 0:
        return None

    match = re.match(JULIAN_DATE_STR_REGEX, julian_date_str)
    if match is None:
        logging.warning("Could not parse MO date [%s]", julian_date_str)
        return None

    years_since_1900 = int(match.group(1))
    days_since_jan_1 = int(match.group(2)) - 1

    date = datetime.date(
        year=(years_since_1900 + 1900), month=1, day=1
    ) + datetime.timedelta(days=days_since_jan_1)
    return date.isoformat().replace("-", "")


def null_if_magic_date(date: str) -> Optional[str]:
    """
    if the date is a special date, then return none
    if the date is not a special date, then return the date
    """

    # special date codes
    date_codes = [
        "0",
        "19000000",
        "20000000",
        "66666666",
        "77777777",
        "88888888",
        "99999999",
    ]

    if date in date_codes:
        return None
    return date


def null_if_invalid_date(date: str) -> Optional[str]:
    """
    Some (229/535028 as of 2023-03-13) of the dates in the TAK044 CG_MD (parole
    eligibility date) column are invalid: sets these un-parsable dates to None to
    prevent ingest errors. A potential alternative would be to use the closest valid date,
    instead of removing the invalid date entirely. This probably isn't necessary unless
    many more invalid dates start making it into the data transfers.
    """
    if len(date) == 8:
        year_substr, month_substr, day_substr = date[:4], date[4:6], date[6:]
        if safe_parse_date_from_date_pieces(year_substr, month_substr, day_substr):
            return date
    return None
