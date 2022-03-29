# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
# ============================================================================
"""
Common utils for processing str fields (parsing into primitive types,
normalizing, etc).
"""

import datetime
import json
import locale
import re
import string
from distutils.util import strtobool  # pylint: disable=no-name-in-module
from typing import List, Optional

import dateparser
from dateutil.relativedelta import relativedelta

from recidiviz.common.date import munge_date_string


def parse_dollars(dollar_string: str) -> int:
    """Parses a string and returns an int dollar amount"""
    clean_string = dollar_string.strip(" ").strip("$")
    if not clean_string:
        return 0
    try:
        return int(locale.atof(clean_string))
    except Exception as e:
        raise ValueError(f"Cannot parse dollar value: {dollar_string}") from e


def parse_bool(bool_string: str) -> bool:
    """Parses a string and returns a bool."""
    try:
        return bool(strtobool(bool_string))
    except Exception as e:
        raise ValueError(f"Cannot parse bool value: {bool_string}") from e


def parse_int(int_string: str) -> int:
    """Parses a string and returns an int. If the string has a decimal, the floor of the value is returned."""
    try:
        return int(float(normalize(int_string)))
    except Exception as e:
        raise ValueError(f"Cannot parse int value: {int_string}") from e


# TODO(#2365): All usages of this function should pass in a datetime, otherwise
#  the value returned for strings like '9M 10D' depend on the date the code is
#  being executed, leading to flaky tests.
def parse_days(time_string: str, from_dt: Optional[datetime.datetime] = None) -> int:
    """
    Converts the given string into an int number of days, using |from_dt|
    as a base for any relative dates.
    """
    if time_string == "" or time_string.isspace():
        return 0
    try:
        return int(time_string)
    except ValueError:
        # Parses the string '1 YEAR 2 DAYS' to mean 1 year and 2 days prior.
        current_dt = from_dt or datetime.datetime.now()
        past_dt = parse_datetime(time_string, from_dt=current_dt)
        if past_dt:
            return (current_dt - past_dt).days

    raise ValueError(f"Cannot parse time duration: {time_string}")


def safe_parse_days_from_duration_str(
    duration_str: str, start_dt_str: Optional[str] = None
) -> Optional[int]:
    """Same as safe_parse_days_from_duration_pieces below, but processes a concatenated duration string directly
    (e.g. '2Y 4D' or '1M 14D').
    """
    pieces = duration_str.upper().split(" ")
    years_str = None
    months_str = None
    days_str = None
    for piece in pieces:
        if piece.endswith("D"):
            days_str = piece.rstrip("D")
        elif piece.endswith("M"):
            months_str = piece.rstrip("M")
        elif piece.endswith("Y"):
            years_str = piece.rstrip("Y")
        else:
            # Not a valid string - can't parse
            return None

    return safe_parse_days_from_duration_pieces(
        years_str=years_str,
        months_str=months_str,
        days_str=days_str,
        start_dt_str=start_dt_str,
    )


def safe_parse_days_from_duration_pieces(
    years_str: Optional[str] = None,
    months_str: Optional[str] = None,
    days_str: Optional[str] = None,
    start_dt_str: Optional[str] = None,
) -> Optional[int]:
    """Same as parse_days_from_duration_pieces below, but returns None if a number of days cannot be parsed."""
    try:
        return parse_days_from_duration_pieces(
            years_str=years_str,
            months_str=months_str,
            days_str=days_str,
            start_dt_str=start_dt_str,
        )
    except ValueError:
        return None


def parse_days_from_duration_pieces(
    years_str: Optional[str] = None,
    months_str: Optional[str] = None,
    days_str: Optional[str] = None,
    start_dt_str: Optional[str] = None,
) -> int:
    """Returns a number of days specified by the given duration strings. If a
    start date is specified, will calculate the number of days after that date,
    otherwise, assumes that a month is 30 days long and a year is 365.25 days
    long. All calculations are rounded to the nearest full day.

    Args:
        years_str: A string representation of an int number of years
        months_str: A string representation of an int number of months
        days_str: A string representation of an int number of days
        start_dt_str: A string representation of a date to start counting from.

    Returns:
        A number of days specified by the duration strings.

    Raises:
        ValueError: If one of the provided strings cannot be properly parsed.
    """

    if not any((years_str, months_str, days_str)):
        raise ValueError("One of (years_str, months_str, days_str) must be nonnull")

    years = parse_int(years_str) if years_str else 0
    months = parse_int(months_str) if months_str else 0
    days = parse_int(days_str) if days_str else 0

    if start_dt_str and not is_str_field_none(start_dt_str):
        start_dt = parse_datetime(start_dt_str)
        if start_dt:
            end_dt = start_dt + relativedelta(years=years, months=months, days=days)
            if end_dt:
                return (end_dt - start_dt).days

    return int(years * 365.25) + (months * 30) + days


def safe_parse_date_from_date_pieces(
    year: Optional[str], month: Optional[str], day: Optional[str]
) -> Optional[datetime.date]:
    """Same as parse_date_from_date_pieces below, but returns None if a given string field cannot be parsed."""
    try:
        return parse_date_from_date_pieces(year, month, day)
    except ValueError:
        return None


def parse_date_from_date_pieces(
    year: Optional[str], month: Optional[str], day: Optional[str]
) -> Optional[datetime.date]:
    """Parses a date from the provided year, month, and day strings."""
    if year and month and day:
        return datetime.date(year=int(year), month=int(month), day=int(day))
    return None


def _is_str_field_zeros(str_field: str) -> bool:
    """Checks if a string field is a procession of any number of zeros, i.e. '0' or '000000', or all zeroes separated by
    punctuation/spacing."""
    try:
        field_as_int = int(str_field)
        return field_as_int == 0
    except ValueError:
        pass

    # Look for strings like '0 0 0' or '0000-00-00'
    parts = normalize(str_field, remove_punctuation=True).split()

    if len(parts) > 1:
        if all(_is_str_field_zeros(p) for p in parts):
            return True

    # This was a check for date strings that are just processions of 0s. If the string or all its non-punctuation parts
    # are not parseable ints, that means is not.
    return False


def parse_datetime(
    date_string: str, from_dt: Optional[datetime.datetime] = None
) -> Optional[datetime.datetime]:
    """
    Parses a string into a datetime.datetime object, using |from_dt| as a base
    for any relative dates.
    """
    if (
        date_string == ""
        or date_string.isspace()
        or _is_str_field_zeros(date_string)
        or is_str_field_none(date_string)
    ):
        return None

    if is_iso_datetime(date_string):
        return datetime.datetime.fromisoformat(date_string)

    if is_yyyymmdd_date(date_string):
        as_date = parse_yyyymmdd_date(date_string)
        if not as_date:
            raise ValueError(
                f"Parsed date for string [{date_string}] is unexpectedly None."
            )
        return datetime.datetime(
            year=as_date.year, month=as_date.month, day=as_date.day
        )
    if (parsed_datetime := parse_mmddyyyy_datetime(date_string)) is not None:
        # Due to a change in dateparser, values like `03122008` are no longer
        # correctly parsed. We add this in to preserve backwards-compatibility.
        return parsed_datetime

    settings: "dateparser._Settings" = {"PREFER_DAY_OF_MONTH": "first"}
    if from_dt:
        settings["RELATIVE_BASE"] = from_dt

    date_string = munge_date_string(date_string)

    # Only special-case strings that start with a - (to avoid parsing regular
    # timestamps like '2016-05-14') and that include non punctuation (to avoid
    # ingested values like '--')
    if date_string.startswith("-") and _has_non_punctuation(date_string):
        parsed = parse_datetime_with_negative_component(date_string, settings)
    else:
        parsed = dateparser.parse(date_string, languages=["en"], settings=settings)
    if parsed:
        return parsed

    raise ValueError(f"cannot parse date: {date_string}")


def _has_non_punctuation(date_string: str) -> bool:
    return any(ch not in string.punctuation for ch in date_string)


def parse_datetime_with_negative_component(
    date_string: str, settings: "dateparser._Settings"
) -> Optional[datetime.datetime]:
    """Handles relative date strings that have negative values in them, like
    '2 year -5month'.

    Instead of parsing the entire relative string all at once as in
    parse_datetime, we parse each individual component one by one, converting
    negative components to look ahead in time instead of backwards.

    '5month' is parsed by dateparser as '5 months ago'. dateparser does not
    automatically convert '-5month' into '-5 months ago' or 'in 5 months', so we
    manually convert '-5month' into 'in 5month' to achieve the desired effect.
    """
    parsed_date = None
    latest_relative = settings.get("RELATIVE_BASE", datetime.datetime.now())

    components = date_string.split(" ")
    for component in components:
        settings["RELATIVE_BASE"] = latest_relative

        updated_component = component
        if updated_component.startswith("-"):
            updated_component = updated_component.replace("-", "in ")
        parsed_date = dateparser.parse(
            updated_component, languages=["en"], settings=settings
        )

        if parsed_date is not None:
            latest_relative = parsed_date

    return parsed_date


def is_iso_datetime(datetime_str: str) -> bool:
    try:
        datetime.datetime.fromisoformat(datetime_str)
    except ValueError:
        return False

    return True


def is_yyyymmdd_date(date_string: str) -> bool:
    try:
        datetime.datetime.strptime(date_string, "%Y%m%d")
    except ValueError:
        return False

    return True


def parse_yyyymmdd_date(date_str: str) -> Optional[datetime.date]:
    if not is_yyyymmdd_date(date_str):
        return None

    return datetime.datetime.strptime(date_str, "%Y%m%d").date()


def parse_mmddyyyy_datetime(date_str: str) -> Optional[datetime.datetime]:
    try:
        return datetime.datetime.strptime(date_str, "%m%d%Y")
    except ValueError:
        return None


def parse_date(
    date_string: str, from_dt: Optional[datetime.datetime] = None
) -> Optional[datetime.date]:
    """
    Parses a string into a datetime.date object, using |from_dt| as a base for
    any relative dates.
    """
    parsed = parse_datetime(date_string, from_dt=from_dt)
    return parsed.date() if parsed else None


def normalize(s: str, remove_punctuation: bool = False) -> str:
    """Normalizes whitespace within the provided string by converting all groups
    of whitespaces into ' ', and uppercases the string."""
    if remove_punctuation:
        translation = str.maketrans(dict.fromkeys(string.punctuation, " "))
        label_without_punctuation = s.translate(translation)
        if not label_without_punctuation.isspace():
            s = label_without_punctuation

    if s is None or s == "" or s.isspace():
        raise ValueError("Cannot normalize None or empty/whitespace string")
    return " ".join(s.split()).upper()


def normalize_truncated(message: str) -> str:
    """Truncates |message| to length used for string fields in schema"""
    return normalize(message)[:255]


def is_str_field_none(s: str) -> bool:
    """Returns True if the string value should be parsed as None."""
    return normalize(s, remove_punctuation=True) in {
        "N A",
        "NONE",
        "NONE SET",
        "NOT SPECIFIED",
    }


_FIRST_CAP_REGEX = re.compile("(.)([A-Z][a-z]+)")
_ALL_CAP_REGEX = re.compile("([a-z0-9])([A-Z])")


def to_snake_case(capital_case_name: str) -> str:
    """Converts a capital case string (i.e. 'SupervisionViolationResponse'
    to a snake case string (i.e. 'supervision_violation_response'). See
    https://stackoverflow.com/questions/1175208/
    elegant-python-function-to-convert-camelcase-to-snake-case.
    """
    s1 = _FIRST_CAP_REGEX.sub(r"\1_\2", capital_case_name)
    return _ALL_CAP_REGEX.sub(r"\1_\2", s1).lower()


def snake_to_camel(s: str) -> str:
    """Converts a snake case string (e.g. "given_names") to a camel case string
    (e.g. "givenNames")."""
    parts = iter(s.split("_"))
    return next(parts) + "".join(i.title() for i in parts)


# https://www.oreilly.com/library/view/regular-expressions-cookbook/9780596802837/ch06s09.html
_ROMAN_NUMERAL_REGEX = re.compile(
    r"\b(?=[MDCLXVI])M*(C[MD]|D?C*)(X[CL]|L?X*)(I[XV]|V?I*)\b", re.IGNORECASE
)


def roman_numeral_uppercase(s: str) -> str:
    """Converts any roman numerals found in a string to uppercase."""
    return _ROMAN_NUMERAL_REGEX.sub(lambda m: m[0].upper(), s)


def person_name_case(name: str) -> str:
    """Converts a string to title case with special handling for exceptions
    specific to people's names (e.g. a III suffix)"""
    return roman_numeral_uppercase(name.title().strip())


def sorted_list_from_str(value: str, delimiter: str = ",") -> List[str]:
    """Converts a string with delimiter-separated values into a sorted list containing those values as separate
    entries.
    """
    if not value:
        return []

    unsorted = [
        result_str.strip()
        for result_str in value.split(delimiter)
        if result_str.strip()
    ]
    return sorted(unsorted)


def normalize_flat_json(json_str: str) -> str:
    """Parses a JSON string and returns a JSON string where the values are normalized, but their keys remain intact.

    NOTE: This only supports un-nested JSON where all values are optional strings. Throws if values have a
    non-string type.
    """

    normalized_values_dict = {}
    loaded_json = json.loads(json_str)
    if not isinstance(loaded_json, dict):
        raise ValueError(
            f"JSON must have top-level type dict, found [{type(loaded_json)}]."
        )
    for k, v in loaded_json.items():
        normalized_value = None
        if v:
            if not isinstance(v, str):
                raise ValueError(
                    f"Unexpected value type [{type(v)}] for field [{k}]. Expected value type str."
                )
            normalized_value = normalize(v)
        normalized_values_dict[k] = normalized_value or ""

    return json.dumps(normalized_values_dict, sort_keys=True)
