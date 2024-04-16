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

from typing import Optional

from recidiviz.common.date import calendar_unit_date_diff
from recidiviz.common.str_field_utils import safe_parse_days_from_duration_pieces


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


def date_diff_in_days(start: str, end: str) -> Optional[str]:
    result = calendar_unit_date_diff(start_date=start, end_date=end, time_unit="days")
    if result is None:
        return None
    return str(result)
