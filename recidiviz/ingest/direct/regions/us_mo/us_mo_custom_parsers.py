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
"""Custom enum parsers functions for US_MO. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_mo_custom_enum_parsers.<function name>
"""
import datetime

from recidiviz.common import ncic
from recidiviz.common.str_field_utils import (
    parse_days_from_duration_pieces,
    parse_yyyymmdd_date,
    safe_parse_days_from_duration_str,
)
from recidiviz.ingest.direct.legacy_ingest_mappings.state_shared_row_posthooks import (
    get_normalized_ymd_str,
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


def max_length_days_from_ymd(years: str, months: str, days: str) -> str:
    normalized_ymd_str = get_normalized_ymd_str(
        years_numerical_str=years,
        months_numerical_str=months,
        days_numerical_str=days,
    )
    return str(safe_parse_days_from_duration_str(normalized_ymd_str))


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
