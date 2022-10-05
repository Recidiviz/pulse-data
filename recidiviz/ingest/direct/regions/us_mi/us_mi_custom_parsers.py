# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Custom enum parsers functions for US_MI. Can be referenced in an ingest view manifest
like this:

my_flat_field:
    $custom:
        $function: us_mi_custom_parsers.<function name>
        $args:
            arg_1: <expression>
            arg_2: <expression>
"""
from typing import Optional

from dateutil.relativedelta import relativedelta

from recidiviz.common.str_field_utils import (
    parse_datetime,
    parse_duration_pieces,
    safe_parse_days_from_duration_pieces,
)


def max_and_min_lengths_days(
    years_str: str,
    months_str: str,
    days_str: str,
) -> Optional[str]:
    """Returns the duration in days from days, months, and years"""
    result = safe_parse_days_from_duration_pieces(years_str, months_str, days_str)
    if result:
        if result == 0:
            return None
        return str(result)
    return None


def calc_parole_elibility_date(
    years_str: str,
    months_str: str,
    days_str: str,
    start_date_str: str,
) -> Optional[str]:
    """Returns the duration in days from a start date with given number of years, months
    and days."""

    try:
        parse_duration_pieces(years_str, months_str, days_str)
    except ValueError:
        return None
    else:
        years, months, days = parse_duration_pieces(years_str, months_str, days_str)
        start_dt = parse_datetime(start_date_str)
        if start_dt:
            end_dt = start_dt + relativedelta(years=years, months=months, days=days)
            return str(end_dt)
        return None


def parole_possible(min_life: str, mcl: str, attempt: str) -> bool:
    """Returns a boolean for whether parole is possible for this offense"""

    # This logic comes from code that MI uses to determine whether a person would be eligible for parole
    # A person may only be ineligible for parole if they have a life sentence, and it then depends on the type of offense

    # mcl = offense code
    # attempt = reference code for the type of offense (including whether it was an attempt/conspiracy)
    # min_flag = flag for whether the minimum sentence is a life sentence

    if (
        min_life == "1"
        and mcl[:7]
        == "750.316"  # prefix for first degree/felony murder/homicide offense codes
        and attempt != "1933"  # Attempt
        and attempt != "1932"  # Conspiracy
    ):
        # if this person has a life sentence for a first degree or felony murder and it was not just attempt/conspiracy,
        # then they are not eligible for parole
        return False

    if min_life == "1" and (
        mcl[:10]
        in (
            "333.177647",  # CS-Sale Adulterated/Misbranded Drugs w/Intent-Causing Death
            "750.200I2E",  # I think this is explosives related but I can't find this offense code in the current table anymore
            # so the offense code might have changed (which is something MI says happens)
            "750.211A2F",  # Same situation as '750.200I2E'
            "750.520B2C",  # CSC-1st Degree (Person u/13, Defendant 17 Or Older)-2nd Off.
        )
        or mcl[:8]
        in (
            "750.165A",  # Drugs-Adulterate/Misbrand/Substitute w/Intent -Causing Death
            "750.543F",  # Terrorism
        )
        or mcl[:7]
        == "750.187"  # This matches both "Drugs - Adulterate to Affect Quality w/Intent -Causing Death"
        # and "Aid Escape From State Mental Inst" but I think this likely only applies to the
        # drug offense and the offense code being the same for both is a data error on MI's part
        or mcl[:9]
        in (
            "750.2042E",  # Explosive-Send With Intent to Injure/Destroy Causing Death
            "750.2072E",  # Explosives - Placing Near Property Causing Death
            "750.2091E",  # Explosives-Placing Offensive Substance Causing Death
            "750.2102E",  # Explosives-Possess Bombs W/Intent-Causing Death
            "750.4362E",  # Poisoning - Food/Drink/Medicine/Water Supply - Causing Death
        )
    ):
        # if the person has a life sentence for any of the above offenses, then they are not eligible for parole
        return False

    return True
