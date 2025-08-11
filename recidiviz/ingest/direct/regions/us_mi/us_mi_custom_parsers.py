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
import datetime
from re import findall, search
from typing import Optional

from dateutil.relativedelta import relativedelta

from recidiviz.common.str_field_utils import (
    parse_datetime,
    parse_duration_pieces,
    safe_parse_days_from_duration_pieces,
)

nonbondable_offense_code = [
    "007",  # Assault and Battery (Prisoner victim)
    "008",  # Assault and Battery (Staff victim)
    "009",  # Assault and Battery (Other Victim)
    "003",  # Assault Resulting in Serious Physical Injury (Prisoner victim)
    "004",  # Assault Resulting in Serious Physical Injury (Staff victim)
    "005",  # Assault Resulting in Serious Physical Injury (Other Victim)
    "001",  # Escape from Level 1
    "050",  # Escape from secure facility
    "017",  # Failure to Disperse
    "014",  # Fighting
    "010",  # Homicide
    "022",  # Incite to Riot or Strike, Rioting or Striking
    "030",  # Possession of Dangerous Contraband
    "029",  # Possession of a Weapon
    "013",  # Sexual Assault (Prisoner victom, sexual acts)
    "051",  # Sexual Assault (Prisoner victim, abusive sexual contact)
    "052",  # Sexual Assault (Staff victim)
    "053",  # Sexual Assault (Other victim)
    "045",  # Smuggling
    "012",  # Threatening Behavior
]


def max_and_min_lengths_days(
    years_str: str,
    months_str: str,
    days_str: str,
) -> Optional[str]:
    """Returns the duration in days from days, months, and years"""
    result = safe_parse_days_from_duration_pieces(
        years_str=years_str, months_str=months_str, days_str=days_str
    )
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
        parse_duration_pieces(
            years_str=years_str, months_str=months_str, days_str=days_str
        )
    except ValueError:
        return None

    years, months, _, days, _ = parse_duration_pieces(
        years_str=years_str, months_str=months_str, days_str=days_str
    )
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


def parse_charge_subclass(description: str) -> Optional[str]:
    """If found in the description, returns a standardized string describing the degree of the charge"""

    # match the word before the substring "DEG" in the description
    degree_str = findall(r"\w+(?=\s+DEG)", description.upper())

    # if we found no matches or more than one match, disregard and return None
    if len(degree_str) == 0 or len(degree_str) > 1:
        return None

    # else standardize and return
    deg_str = degree_str[0]

    if deg_str == "FIRST":
        return "1ST DEGREE"

    if deg_str in ("SECOND", "COND-2ND"):
        return "2ND DEGREE"

    if deg_str == "THIRD":
        return "3RD DEGREE"

    if deg_str == "FOURTH":
        return "4TH DEGREE"

    # all other cases are already 1st, 2nd, 3rd, or 4th
    return f"{deg_str} DEGREE"


def parse_offense_type_ids(offense_type_ids: str, result_field: str) -> bool:
    """Returns whether the offense_type_ids list contains an offense_type that would indicate the offense was a violent offense"""

    id_types = {
        "is_violent": [
            "9715",  # Assaultive Felony
            "7582",  # Assaultive Misdemeanor
            "14503",  # Other Assaultive
            "13553",  # PA 487 Violent Felonies
        ],
        "is_sex_offense": [
            "2178",  # Sex Offender Registration
            "14502",  # Sexual Offense
        ],
    }

    offense_type_list = offense_type_ids.split(",")

    for id_type in id_types[result_field]:
        if id_type in offense_type_list:
            return True

    return False


def is_coms_level(supervision_level_value: str) -> bool:
    """Returns True if the supervsion level is coming from COMS (based on the fact it has multiple parts separated by underscores)"""
    return len(supervision_level_value.split("_")) > 1


def supervision_type_info_from_COMS(supervision_level: str, modifier: str) -> bool:
    """Returns whether the COMS supervision level or modifier data contains information about supervision type"""

    if search(
        r"ARRESTED OUT OF STATE|PAROLE #2|PROBATION WARRANT",
        supervision_level.upper(),
    ):
        return True

    if search(
        r"ABSCONDED|ESCAPED|ARRESTED OUT OF STATE|WARRANT STATUS|#2 WARRANT",
        modifier.upper(),
    ):
        return True

    return False


def custodial_authority_info_from_COMS(supervision_level: str, modifier: str) -> bool:
    """Returns whether the COMS supervision level or modifier data contains information about supervision type"""

    if search(
        r"SUPERVISED OUT OF STATE|ARRESTED OUT OF STATE", supervision_level.upper()
    ):
        return True

    if search(
        r"SUPERVISED OUT OF STATE|PAROLED TO CUSTODY (FED/OUTSTATE)|ARRESTED OUT OF STATE",
        modifier.upper(),
    ):
        return True

    return False


def nonbondable_offenses(offense_codes: str) -> str:
    """Returns whether the list of offense codes includes a nonbondable offense"""

    offense_codes_list = offense_codes.split("@@")

    return ",".join(
        sorted((set(offense_codes_list)).intersection(set(nonbondable_offense_code)))
    )


def bondable_offenses(offense_codes: str) -> str:
    """Returns whether the list of offense codes includes a bondable offense"""

    offense_codes_list = offense_codes.split("@@")

    return ",".join(sorted(set(offense_codes_list) - set(nonbondable_offense_code)))


def parse_class_i(offense_codes: str) -> str:
    """Returns whether the list of offense codes includes a class I offense"""

    offense_codes_list = offense_codes.split("@@")

    for offense_code in offense_codes_list:
        if offense_code.startswith("0") and offense_code != "049":
            return "True"

    return "False"


def parse_class_ii(offense_codes: str) -> str:
    """Returns whether the list of offense codes includes a class II offense"""

    offense_codes_list = offense_codes.split("@@")

    for offense_code in offense_codes_list:
        if offense_code.startswith("4"):
            return "True"

    return "False"


def parse_class_iii(offense_codes: str) -> str:
    """Returns whether the list of offense codes includes a class III offense"""

    offense_codes_list = offense_codes.split("@@")

    for offense_code in offense_codes_list:
        if offense_code == "049":
            return "True"

    return "False"


def period_before_COMS_migration(period_start_date: str) -> bool:
    """Returns whether the period start date was before the COMS migration on 8/14/2023"""
    parsedDate = parse_datetime(period_start_date)
    if parsedDate is not None and parsedDate < datetime.datetime(2023, 8, 14):
        return True
    return False


def convert_sc_level_to_int(sc_level: str) -> str:
    """Converts security classification level from roman numerals to an integer."""

    if sc_level == "I":
        return "1"

    if sc_level == "II":
        return "2"

    if sc_level == "IV":
        return "4"

    if sc_level == "V":
        return "5"

    raise ValueError(
        f"This parser is being called on an invalid security classification level: {sc_level}"
    )
