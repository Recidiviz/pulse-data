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
"""Tests randomize_fixture_data.py."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.tools.ingest.testing.ingest_fixture_creation.randomize_fixture_data import (
    NameType,
    RecidivizFixtureFaker,
    randomize_fixture_data,
)

FAKER = RecidivizFixtureFaker()


def test_randomize_string() -> None:
    """Tests that the randomize_string function returns a string of the same length."""
    pure_text = [
        ("Doppler Nick", "Rqybish Bgct"),
        ("Recidiviz", "Cfrbqtvlb"),
        ("Test-123-XyZ", "Zifr-082-HtT"),
        ("example@@concat", "mygwgjp@@xaihhj"),
        ("null", "null"),
        ("dont", "dont"),
    ]
    # Run each one twice to ensure that the randomization is consistent
    for value, expected in pure_text + pure_text:
        randomized_value = FAKER.randomize_string(
            value, null_values_to_skip=["null", "dont"]
        )
        assert len(randomized_value) == len(value)
        assert randomized_value == expected

    # Almost any full numeric will likely be a real seen value, so
    # we don't show expected values here. We just test it is different
    # and in the expected format
    digits = ["12345", "19665", "123", "970"]
    for value in digits + digits:
        randomized_value = FAKER.randomize_string(value, null_values_to_skip=None)
        assert len(randomized_value) == len(value)
        assert randomized_value != value
        assert randomized_value.isdigit()


def test_randomize_name() -> None:
    """Tests that the randomize_name function returns a name of the same length."""
    pairs = [
        # Intentionally prefixing Test to not leak any real name randomization here.
        (("Test Doppler Nick", "Edward Stevens"), NameType.FULL_NAME),
        (("Test Tallant", "Drake"), NameType.LAST_NAME),
        (("Test Dominik", "Karen"), NameType.FIRST_NAME),
        (("Test Szoboszlai", "Hodges"), NameType.LAST_NAME),
        (("Test Mohamed Salah", "John Villanueva"), NameType.FULL_NAME),
    ]
    # Run each one twice to ensure that the randomization is consistent
    for (value, expected), name_type in pairs + pairs:
        randomized_value = FAKER.randomize_name(
            value, name_type, null_values_to_skip=["null"]
        )
        assert randomized_value == expected


def test_randomize_birthdate() -> None:
    """Tests that the randomize_birthdate function returns a date of the same format."""
    # Using dates from 1221 and 1495, which we won't see in our real data
    # Note that the dates are the same, even if the format is different!
    pairs = [
        (("1221-01-01", ["DATE_PARSE('%Y-%m-%d', {col_name})"]), "1220-12-19"),
        (("1221/01/01", ["DATE_PARSE('%Y/%m/%d', {col_name})"]), "1220/12/19"),
        (("1221.01.01", ["DATE_PARSE('%Y.%m.%d', {col_name})"]), "1220.12.19"),
        (
            ("1221-01-01 00:00:00", ["DATE_PARSE('%Y-%m-%d %H:%M:%S', {col_name})"]),
            "1220-12-19 00:00:00",
        ),
        (
            (
                "1221-01-01 00:00:00.000000",
                ["DATE_PARSE('%Y-%m-%d %H:%M:%S.%f', {col_name})"],
            ),
            "1220-12-19 00:00:00.000000",
        ),
        (("1495-04-30", ["DATE_PARSE('%Y-%m-%d', {col_name})"]), "1496-04-02"),
        (("1495/04/30", ["DATE_PARSE('%Y/%m/%d', {col_name})"]), "1496/04/02"),
        (("1495.04.30", ["DATE_PARSE('%Y.%m.%d', {col_name})"]), "1496.04.02"),
        (("1900.01.01", ["DATE_PARSE('%Y.%m.%d', {col_name})"]), "1900.01.01"),
    ]
    # Run each one twice to ensure that the randomization is consistent
    for (value, parsers), expected in pairs + pairs:
        randomized_value = FAKER.randomize_birthdate(
            value, parsers, null_values_to_skip=["1900.01.01"]
        )
        assert randomized_value == expected

    assert (
        FAKER.randomize_birthdate(
            "", ["DATE_PARSE('%Y-%m-%d', {col_name})"], null_values_to_skip=None
        )
        == ""
    )

    with pytest.raises(
        ValueError,
        match="Could not parse the date string '1495-04-30' with any of the provided formats",
    ):
        FAKER.randomize_birthdate(
            "1495-04-30",
            [
                "DATE_PARSE('%Y/%m/%d', {col_name})",
            ],
            null_values_to_skip=None,
        )


def test_randomize_fixture_data() -> None:
    """
    Tests that the randomize_fixture_data function returns a DataFrame with the same shape and columns.

    Input data
    ----------
            name   birthdate  important  numbers
    0   GP Janet  1478-01-01       holy        1
    1   GP Chidi  1480-02-02       moly        2
    2  GP Elanor  1482-03-03  forkballs        3
    3  GP Tahani  1486-05-05     batman        4
    4   GP Jason  1486-05-05        wow        5

    Turns into this
    ----------------
           name   birthdate  important  numbers
    0     Randy  1477-07-28       OFNW        1
    1  Lawrence  1480-01-19       GULS        2
    2   Shannon  1483-02-25  NGKKWBLJV        3
    3    Curtis  1486-05-20     WVVQVQ        4
    4   Jeffrey  1486-05-20        WEC        5
    """
    data = {
        # Intentionally prefixing GP to not leak any real name randomization here.
        "name": ["GP Janet", "GP Chidi", "GP Elanor", "GP Tahani", "GP Jason"],
        # Intentionally using really old dates to not leak any real date randomization here.
        "birthdate": [
            "1478-01-01",
            "1480-02-02",
            "1482-03-03",
            "1486-05-05",
            "1486-05-05",
        ],
        "important": ["holy", "moly", "forkballs", "batman", "wow"],
        "numbers": [1, 2, 3, 4, 5],
    }

    df = pd.DataFrame(data)

    config = MagicMock(DirectIngestRawFileConfig)
    config.file_tag = "test"
    config.current_pii_columns = [
        RawTableColumnInfo(
            state_code=StateCode.US_XX,
            file_tag="test",
            name="name",
            description="name",
            field_type=RawTableColumnFieldType.STRING,
            is_pii=True,
        ),
        RawTableColumnInfo(
            state_code=StateCode.US_XX,
            file_tag="test",
            name="birthdate",
            description="birthdate",
            field_type=RawTableColumnFieldType.DATETIME,
            datetime_sql_parsers=[
                "SAFE.PARSE_DATETIME('%Y-%m-%d', {col_name})",
            ],
            is_pii=True,
        ),
        RawTableColumnInfo(
            state_code=StateCode.US_XX,
            file_tag="test",
            description="important",
            name="important",
            field_type=RawTableColumnFieldType.STRING,
            is_pii=True,
        ),
    ]

    df = randomize_fixture_data(df, config)
    assert df.name.to_list() == ["Randy", "Lawrence", "Shannon", "Curtis", "Jeffrey"]
    assert df.birthdate.to_list() == [
        "1477-07-28",
        "1480-01-19",
        "1483-02-25",
        "1486-05-20",
        "1486-05-20",
    ]
    assert df.important.to_list() == ["yiti", "timh", "ieebwfgpo", "epknej", "nos"]
    assert df.numbers.to_list() == [1, 2, 3, 4, 5]
