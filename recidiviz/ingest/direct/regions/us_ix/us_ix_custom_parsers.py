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

"""Custom parser functions for US_IX. Can be referenced in an ingest view manifest
like this:

my_flat_field:
    $custom:
        $function: us_me_custom_parsers.<function name>
        $args:
            arg_1: <expression>
            arg_2: <expression>
"""

import re
from typing import Optional

from recidiviz.common.str_field_utils import (
    parse_datetime,
    safe_parse_days_from_duration_pieces,
)

# Sex Offense Statute Codes
US_IX_SEX_OFFENSE_STATUTE_CODES = [
    "I18-1505B",
    "I18-1505B(1)",
    "I18-1506",
    "I18-1506(1)(A)",
    "I18-1506(1)(B)",
    "I18-1506(1)(C)",
    "I18-1506(AT)",
    "I18-1507",
    "I18-1508 {AT}",
    "I18-1508 {F}",
    "I18-1508",
    "I18-1508A(1)(A)",
    "I18-1508A(1)(C)",
    "I18-4502",
    "I18-6101 (AB)",
    "I18-6101 {A}",
    "I18-6101 {AT}",
    "I18-6101-1 {F}",
    "I18-6101",
    "I18-6101(1)",
    "I18-6101(2)",
    "I18-6101(3)",
    "I18-6101(4)",
    "I18-6101(5)",
    "I18-6108",
    "I18-6602",
    "I18-6605",
    "I18-6606 (AT)",
    "I18-6606",
    "I18-6609(3)",
    "I18-8304",
    "I18-8307",
    "I18-8308",
    "I18-924",
    "I18-925",
    "I19-2520C",
]


def parse_duration_from_two_dates(
    start_date_str: str, end_date_str: str
) -> Optional[str]:
    try:
        start_dt = parse_datetime(start_date_str)
        end_dt = parse_datetime(end_date_str)

        if start_dt and end_dt and end_dt.year != 9999:
            return str((end_dt - start_dt).days)
    except ValueError:
        return None
    return None


def parse_supervision_violation_is_violent(new_crime_types: str) -> bool:
    for new_crime_type in new_crime_types.split(","):
        if (
            "VIOLENT" in new_crime_type.upper()
            and "NON-VIOLENT" not in new_crime_type.upper()
        ):
            return True
    return False


def parse_supervision_violation_is_sex_offense(new_crime_types: str) -> bool:
    for new_crime_type in new_crime_types.split(","):
        if "SEX" in new_crime_type.upper():
            return True
    return False


def parse_is_life_from_date(proj_completion_date: str) -> bool:
    return proj_completion_date[:4] == "9999"


def parse_is_combined_caseload(first_name: str, last_name: str) -> bool:
    """
    takes in first name and last name and returns true if it's a name used to denote a combined caseload
    """
    if first_name.lower() in [
        "court",
        "bench",
        "re",
        "cmbnd",
        "combined",
        "caseload",
        "drug",
        "limited",
        "dosage",
        "level1",
        "level",
        "prob",
        "commission",
    ]:
        return True
    if last_name.lower() in [
        "d1",
        "done",
        "dthree",
        "dtwo",
        "dtwomoscow",
        "d2",
        "d3",
        "d4",
        "dseven",
        "d7",
        "dfour",
        "colimsup",
        "dfive",
        "dfiveburley",
        "dfivejerome",
        "d5",
        "dseven",
        "misd",
        "dsix",
        "parole",
        "misdemeanor",
    ]:
        return True

    return False


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


def is_district(location_name: str, district_num: str) -> bool:
    district_of_interest = re.match(
        r"DISTRICT " + district_num + r"\b", location_name.upper()
    )

    return district_of_interest is not None


def is_ws(supervising_officer_external_id: str) -> bool:
    officer_id_includes_bw = re.match(
        r"D[0-9]+BENCH", supervising_officer_external_id.upper()
    )
    return officer_id_includes_bw is not None


def judicial_district_from_county(county_code: str) -> Optional[str]:
    """Maps county code to judicial district"""
    if county_code in [
        "12940",  # Benewah
        "12944",  # Bonner
        "12946",  # Boundary
        "12963",  # Kootenai
        "12976",  # Shoshone
    ]:
        return "FIRST JUDICIAL DISTRICT"

    if county_code in [
        "12953",  # Clearwater
        "12960",  # Idaho
        "12964",  # Latah
        "12966",  # Lewis
        "12970",  # Nez Perce
    ]:
        return "SECOND JUDICIAL DISTRICT"

    if county_code in [
        "12937",  # Adams
        "12949",  # Canyon
        "12958",  # Gem
        "12973",  # Owyhee
        "12974",  # Payette
        "12980",  # Washington
    ]:
        return "THIRD JUDICIAL DISTRICT"

    if county_code in [
        "12936",  # Ada
        "12943",  # Boise
        "12955",  # Elmore
        "12979",  # Valley
    ]:
        return "FOURTH JUDICIAL DISTRICT"

    if county_code in [
        "12942",  # Blaine
        "12948",  # Camas
        "12951",  # Cassia
        "12959",  # Gooding
        "12962",  # Jerome
        "12967",  # Lincoln
        "12969",  # Minidoka
        "12978",  # Twin Falls
    ]:
        return "FIFTH JUDICIAL DISTRICT"

    if county_code in [
        "12938",  # Bannock
        "12939",  # Bear Lake
        "12950",  # Caribou
        "12956",  # Franklin
        "12971",  # Oneida
        "12975",  # Power
    ]:
        return "SIXTH JUDICIAL DISTRICT"

    if county_code in [
        "12941",  # Bingham
        "12945",  # Bonneville
        "12947",  # Butte
        "12952",  # Clark
        "12954",  # Custer
        "12957",  # Fremont
        "12961",  # Jefferson
        "12965",  # Lehmi
        "12968",  # Madison
        "12977",  # Teton
    ]:
        return "SEVENTH JUDICIAL DISTRICT"

    return None


def parse_charge_is_sex_offense(statute: str) -> bool:
    if statute.upper() in US_IX_SEX_OFFENSE_STATUTE_CODES:
        return True
    return False


def is_county_jail(supervision_site: str) -> bool:
    return "COUNTY JAIL" in supervision_site.upper()


def is_valid_year(segment_start_date: str) -> bool:
    return segment_start_date != "" and segment_start_date[0:4] != "9999"
