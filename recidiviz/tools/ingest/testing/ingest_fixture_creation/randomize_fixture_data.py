# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""This module has functionality to randomize fixture data."""

import re
from datetime import datetime, timedelta
from enum import Enum

import pandas as pd
from faker import Faker
from faker.providers import BaseProvider

from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)


# TODO(#40727) Use RawTableColumnFieldType name values, when they exist
class NameType(Enum):
    """Enum for the different types of names."""

    FIRST_NAME = "first_name"
    LAST_NAME = "last_name"
    FULL_NAME = "name"


KNOWN_BIRTHDATE_SUBSTRINGS = {
    "birth",
    "brth",
    "dob",
}

KNOWN_NAME_SUBSTRINGS = {
    NameType.FIRST_NAME: {
        "frst_nm",
        "frstnm",
        "firstnm",
        "first_name",
        "firstname",
        "fname",
        "mname",
        "middle_name",
        "middle",
        "name",
    },
    NameType.LAST_NAME: {
        "last_nm",
        "lst_nm",
        "lstnm",
        "last_name",
        "lastname",
        "lname",
    },
    NameType.FULL_NAME: {
        "full_name",
        "fullnm",
    },
}


class RecidivizFixtureFaker(Faker):
    """
    A version of Faker to randomize fixture data.

    The public methods set a random seed according to the input value, so that the same
    input value will always produce the same random output. This allows randomization
    of PII that is consistent across runs, but different randomization for different data.
    """

    class StringWithSameCharPatternProvider(BaseProvider):
        """Custom provider to retain special characters and string format in randomized strings."""

        def random_char(self, char: str) -> str:
            if char.isdigit():
                return str(self.random_digit())
            if char.islower():
                return self.random_lowercase_letter()
            if char.isupper():
                return self.random_uppercase_letter()
            return char

        def patterned_string(self, input_to_randomize: str) -> str:
            return "".join(self.random_char(char) for char in input_to_randomize)

    def __init__(self) -> None:
        super().__init__(locale=["en-US"])
        self.add_provider(self.StringWithSameCharPatternProvider)

    def randomize_string(
        self, value: str, null_values_to_skip: list[str] | None
    ) -> str:
        if not value:
            return ""
        if null_values_to_skip and value in null_values_to_skip:
            return value
        self.seed_instance(value)
        return self.patterned_string(value)

    def randomize_name(
        self, value: str, name_type: NameType, null_values_to_skip: list[str] | None
    ) -> str:
        """Returns a random first name, last name, or full name based on the input value."""
        if not value:
            return ""
        if null_values_to_skip and value in null_values_to_skip:
            return value
        self.seed_instance(value)
        match name_type:
            case NameType.FIRST_NAME:
                return self.first_name()
            case NameType.LAST_NAME:
                return self.last_name()
            case NameType.FULL_NAME:
                return self.name()
        raise ValueError("Unknown name type: {name_type}. Please use a valid NameType.")

    def _shuffle_date(self, dt_value: datetime) -> datetime:
        self.seed_instance(dt_value.isoformat())
        return dt_value + timedelta(days=self.random_int(-365, 365))

    def randomize_birthdate(
        self, value: str, sql_parsers: list[str], null_values_to_skip: list[str] | None
    ) -> str:
        """Moves the given date by a random number of days and returns the new date as a string in the original format."""
        if not value:
            return ""
        if null_values_to_skip and value in null_values_to_skip:
            return value
        for statement in sql_parsers:
            # sql_parser statements start with the function name, e.g.: SAFE.DATE_PARSE('format string', text arg)
            # This regext grabs the format string because it is the first argument.
            # The text field may have other function calls, so there may be other matches later in the regex.
            # In that case, the parsing will likely fail and raise down below if no other parsers work.
            datetime_match = re.search(r"\(\s*'([^']*)'", statement)
            if not datetime_match:
                raise ValueError(
                    f"Could not find a date format string in the SQL parser '{statement}'."
                )
            dt_format = datetime_match.group(1)
            try:
                dt_value = self._shuffle_date(datetime.strptime(value, dt_format))
            except ValueError:
                continue
            return dt_value.strftime(dt_format)
        raise ValueError(
            f"Could not parse the date string '{value}' with any of the provided formats: {sql_parsers}"
        )


# TODO(#40727) Create name RawTableColumnFieldType values for names
def _randomize_string_column(
    df: pd.DataFrame,
    col: RawTableColumnInfo,
    faker: RecidivizFixtureFaker,
    null_values: list[str] | None,
) -> pd.Series:
    """Randomizes the values in a string column based on the column name."""
    # TODO(#40727) No longer do this when name types are well defined in configs
    for name_type in NameType:
        if any(sub in col.name.lower() for sub in KNOWN_NAME_SUBSTRINGS[name_type]):
            return df[col.name].apply(
                faker.randomize_name,
                name_type=name_type,
                null_values_to_skip=null_values,
            )
    return df[col.name].apply(faker.randomize_string, null_values_to_skip=null_values)


# TODO(#40717) Update when BIRTHDATE exists
def _randomize_datetime_column(
    df: pd.DataFrame,
    col: RawTableColumnInfo,
    faker: RecidivizFixtureFaker,
    null_values: list[str] | None,
) -> pd.Series:
    """Randomizes the values in a datetime column based on the column name."""
    if any(sub in col.name.lower() for sub in KNOWN_BIRTHDATE_SUBSTRINGS):
        if not col.datetime_sql_parsers:
            raise ValueError(
                "No SQL parsers provided to parse the datetime format. "
                f"Please add datetime_sql_parsers to {col.name}."
            )
        return df[col.name].apply(
            faker.randomize_birthdate,
            sql_parsers=col.datetime_sql_parsers,
            null_values_to_skip=null_values,
        )
    raise ValueError(
        "Birthdates are currently the only datetime values we allow as PII. "
        f"Column {col.name} was not identified as a birthdate."
        "Please update KNOWN_BIRTHDATE_SUBSTRINGS or remove is_pii from the column."
    )


def randomize_fixture_data(
    df: pd.DataFrame,
    config: DirectIngestRawFileConfig,
) -> pd.DataFrame:
    """Randomizes PII in the given fixture data.

    We currently only randomize birthdates, names, and string values marked as PII.
    We set a seed unique to the file tag to ensure that the same randomization is applied
    across runs, but different randomization is applied to different files.

    Args:
        df: The DataFrame containing the fixture data.
        config: The configuration for the raw file.
        seed: The integer hash of the file tag.

    Returns:
        A DataFrame with randomized values.
    """
    faker = RecidivizFixtureFaker()
    for col in config.current_pii_columns:
        if col.field_type == RawTableColumnFieldType.DATETIME:
            df[col.name] = _randomize_datetime_column(df, col, faker, col.null_values)
        elif col.field_type == RawTableColumnFieldType.STRING:
            df[col.name] = _randomize_string_column(df, col, faker, col.null_values)
        elif col.field_type in (
            # TODO(#39681) Encrypt IDs instead to match raw fixture configs
            RawTableColumnFieldType.PERSON_EXTERNAL_ID,
            RawTableColumnFieldType.STAFF_EXTERNAL_ID,
        ):
            df[col.name] = df[col.name].apply(faker.randomize_string)
        else:
            raise ValueError(
                f"Field type {col.field_type} is not supported for randomization."
            )
    return df
