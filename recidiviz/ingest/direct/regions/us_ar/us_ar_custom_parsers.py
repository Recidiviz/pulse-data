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
"""Custom parser functions for US_AR. Can be referenced in an ingest view manifest
like this:

my_flat_field:
    $custom:
        $function: us_ar_custom_parsers.<function name>
        $args:
            arg_1: <expression>
            arg_2: <expression>
"""

from datetime import datetime
from typing import Optional

from recidiviz.common.date import calendar_unit_date_diff
from recidiviz.common.str_field_utils import (
    parse_datetime,
    safe_parse_days_from_duration_pieces,
)
from recidiviz.ingest.direct.regions.custom_enum_parser_utils import (
    invert_str_to_str_mappings,
)

DESCRIPTION_TO_STATUTE_DICT = {
    # TODO(#33691): Update this dictionary to include a "SEX_OFFENSE" category when we
    # receive the necessary information.
    "CAPITAL_OFFENSE": ["0510101", "0551201"],  # Capital Murder  # Treason
    "MURDER_1": [
        "0510102",
    ],
    "RAPE": ["0514103"],
    "KIDNAPPING": ["0511102"],
    "AGG_ROBBERY": ["0512103"],
    "ESCAPE": [
        "0554110",  # Escape-1st Degree
        "0554111",  # Escape-2nd Degree
        "0554112",  # Escape-3rd Degree
    ],
    "ATTEMPT_FLAG": ["410701", "0503201"],
    "SOLICITATION_FLAG": ["410705", "0503301"],
    "CONSPIRACY_FLAG": ["410707", "0503401"],
}
STATUTE_TO_DESCRIPTION_DICT = invert_str_to_str_mappings(DESCRIPTION_TO_STATUTE_DICT)


def is_attempted(offense_types: str) -> Optional[bool]:
    if offense_types is not None:
        return "ATTEMPT_FLAG" in offense_types
    return None


def parse_offense_types(
    statute1: str,
    statute2: str,
    statute3: str,
    statute4: str,
) -> Optional[str]:
    """Uses the statutes associated with a charge to produce a string containing each
    of the known offense types, separated by '@@'. As we start accounting for more offense
    types, they will be added along with their corresponding statute codes to DESCRIPTION_TO_STATUTE_DICT.

    Due to the way AR uses that STATUTE columns, there are 3 flags can show up in the offense
    type string that do NOT constitute an actual offense, but rather add information about
    the other offenses given by the statute columns. These are ATTEMPT_FLAG, SOLICITATION_FLAG,
    and CONSPIRACY_FLAG. Therefore, an offense type string output by this function may look
    like 'ESCAPE@@ATTEMPT_FLAG', indicating an attempted escape. If any of these 3 flags
    are present in an offense type string, the charge should be treated as an inchoate offense.
    """
    # TODO(#33239): The statute-to-description mappings have been pulled from the code
    # value reference sheet. Once this sheet has been uploaded as a reference table, we can
    # join against it in the ingest view itself, allowing us to add offense type columns
    # with data for every statute, rather than just the ones that have been hardcoded. For
    # now, it makes the most sense to manually identify the relevant offense types.
    statutes = [
        STATUTE_TO_DESCRIPTION_DICT[statute_code]
        for statute_code in [statute1, statute2, statute3, statute4]
        if statute_code is not None and statute_code in set(STATUTE_TO_DESCRIPTION_DICT)
    ]
    if len(statutes) == 0:
        return None
    return "@@".join(statutes)


def is_known_staff_id(staff_id: str) -> bool:
    """Some staff IDs returned in ingest views don't have a matching ID in the source
    table used for state_staff, resulting in invalid staff IDs making it through ingest.
    To prevent errors, these IDs are tagged with 'UNKNOWN' (e.g. 12345-UNKNOWN) and then
    nulled out in the mappings, such that the ID itself is still visible in the ingest view
    results but doesn't try to create a new state staff entity. Currently only used for the
    program_assignment view (staff_role_location_period has the same issue, but those entities
    simply aren't ingested if there's no match for the ID).
    """
    # TODO(#28833): Revisit once we know why there are gaps in PERSONPROFILE, to make sure
    # that we're ingesting state_staff from the correct sources.
    return "UNKNOWN" not in staff_id


def is_suspended_sentence(sentence_types: str) -> bool:
    """Identifies suspended sentences; returns True if every sentence type is
    'Suspended Sentence','Pre-Adjudicated', or null."""
    suspended_type_list = [
        "SI",  # Suspended Sentence
        "PA",  # Pre-Adjudicated
    ]
    if all(s in suspended_type_list for s in sentence_types.split("-")):
        return True
    return False


def parse_employment_category(
    disabled: str,
    unemployed: str,
    occupation: str,
) -> Optional[str]:
    """Determines what special employment category an individual belongs to, if any.
    Options are 'STUDENT', 'UNEMPLOYED', 'DISABLED', or None (returned if the individual
    is employed normally).
    """
    if occupation == "STU":
        return "STUDENT"
    if disabled == "Y":
        return "SSI_DISABLED"
    if unemployed == "Y":
        return "UNEMPLOYED"
    return None


def parse_address_pieces(
    stnum: str,
    stname: str,
    sttype: str,
    suite: str,
    apt: str,
    po: str,
    city: str,
    st: str,
    zipcode: str,
) -> Optional[str]:
    """Concatenates address components into a single string."""

    if not any([stnum, stname, suite, apt, po, city, st, zipcode]):
        return None
    constructed_address = ""

    if stnum and stname:
        constructed_address += stnum + " " + stname
        if sttype:
            constructed_address += " " + sttype
    if suite:
        constructed_address += ", SUITE " + suite
    if apt:
        constructed_address += ", APT. " + apt
    if po:
        constructed_address += ", PO BOX " + po
    if city and st:
        constructed_address += ", " + city + " " + st
    if zipcode:
        constructed_address += " " + zipcode

    return constructed_address.upper()


def max_length_days_from_ymd(years: str, months: str, days: str) -> Optional[str]:
    result = safe_parse_days_from_duration_pieces(
        years_str=years, months_str=months, days_str=days
    )
    if result is None:
        return None
    return str(result)


def null_far_future_date(date: str) -> Optional[str]:
    """
    Some placeholder / erroneous dates appear very far in the future, and result in parsing
    errors. With this function, we null out dates that are 100+ years in the future, since
    these are either incorrect or simply indicating a life sentence. Note that this isn't used
    on '9999-' magic dates, which are handled within the view logic itself.
    """
    future_date = parse_datetime(date)
    if future_date is not None:
        distance_to_date = future_date - datetime.now()
        if distance_to_date.days / 365 < 100:
            return date
    return None


def date_diff_in_days(start: str, end: str) -> Optional[str]:
    if (
        start > end
        or null_far_future_date(start) is None
        or null_far_future_date(end) is None
    ):
        return None
    result = calendar_unit_date_diff(start_date=start, end_date=end, time_unit="days")
    if result is None:
        return None
    return str(result)
