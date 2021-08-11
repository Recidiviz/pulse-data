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
# =============================================================================
"""Tests for str_field_utils.py"""
import datetime
from json.decoder import JSONDecodeError
from unittest import TestCase

from recidiviz.common.str_field_utils import (
    normalize_flat_json,
    parse_bool,
    parse_date,
    parse_date_from_date_pieces,
    parse_datetime,
    parse_days,
    parse_days_from_duration_pieces,
    parse_dollars,
    parse_int,
    safe_parse_date_from_date_pieces,
    safe_parse_days_from_duration_str,
)


class TestStrFieldUtils(TestCase):
    """Test conversion util methods."""

    def test_parseInt(self) -> None:
        assert parse_int("123") == 123

    def test_parseInt_floatProvided(self) -> None:
        assert parse_int("123.6") == 123

    def test_parseInt_invalidStr(self) -> None:
        with self.assertRaises(ValueError):
            parse_int("hello")

    def test_parseTimeDurationDays(self) -> None:
        assert parse_days("10") == 10

    def test_parseTimeDurationFormat(self) -> None:
        source_date = datetime.datetime(2001, 1, 1)
        # 2000 was a leap year
        assert parse_days("1 YEAR 2 DAYS", from_dt=source_date) == 368

    def test_parseDaysFromDurationPieces(self) -> None:
        two_years_in_days = parse_days_from_duration_pieces(years_str="2")
        assert two_years_in_days == (365 * 2)

        four_years_in_days = parse_days_from_duration_pieces(years_str="4")
        assert four_years_in_days == (365 * 4 + 1)  # Include leap day

        leap_year_source_date = datetime.datetime(2000, 1, 1)

        two_months = parse_days_from_duration_pieces(
            months_str="2", start_dt_str=str(leap_year_source_date)
        )

        assert two_months == (31 + 29)

        leap_year_in_days = parse_days_from_duration_pieces(
            years_str="1", start_dt_str=str(leap_year_source_date)
        )
        assert leap_year_in_days == 366

        combo_duration = parse_days_from_duration_pieces(
            years_str="1", months_str="2", days_str="3"
        )

        assert combo_duration == (1 * 365 + 2 * 30 + 3)

        combo_duration_with_start_date = parse_days_from_duration_pieces(
            years_str="0",
            months_str="2",
            days_str="3",
            start_dt_str=str(leap_year_source_date),
        )

        assert combo_duration_with_start_date == (2 * 30 + 3)

    def test_parseDaysFromDurationPieces_negativePieces(self) -> None:
        duration = parse_days_from_duration_pieces(years_str="2", months_str="-5")

        assert duration == (2 * 365 + -5 * 30)

        duration = parse_days_from_duration_pieces(
            years_str="2", months_str="-5", days_str="-15"
        )

        assert duration == (2 * 365 + -5 * 30 - 15)

    def test_parseDaysFromDurationStr(self) -> None:
        self.assertEqual(365, safe_parse_days_from_duration_str("1Y"))
        self.assertEqual(30, safe_parse_days_from_duration_str("1M 0D"))
        self.assertEqual(100, safe_parse_days_from_duration_str("100D"))
        self.assertEqual(40, safe_parse_days_from_duration_str("1M 10D"))
        self.assertEqual(405, safe_parse_days_from_duration_str("1Y 1M 10D"))

    def test_parseDateFromDatePieces(self) -> None:
        parsed = parse_date_from_date_pieces("2005", "10", "12")
        assert parsed == datetime.date(year=2005, month=10, day=12)

    def test_parseDateFromDatePieces_noneValues(self) -> None:
        parsed = parse_date_from_date_pieces(None, None, None)
        assert parsed is None

    def test_parseDateFromDatePieces_invalidValues(self) -> None:
        with self.assertRaises(ValueError):
            parse_date_from_date_pieces("abc", "def", "LIFE")

    def test_safeParseDateFromDatePieces_invalidValues(self) -> None:
        parsed = safe_parse_date_from_date_pieces("abc", "def", "LIFE")
        assert parsed is None

    def test_parseBadTimeDuration(self) -> None:
        with self.assertRaises(ValueError):
            parse_days("ABC")

    def test_parseDollarAmount(self) -> None:
        assert parse_dollars("$100.00") == 100
        assert parse_dollars("$") == 0

    def test_parseBadDollarAmount(self) -> None:
        with self.assertRaises(ValueError):
            parse_dollars("ABC")

    def test_parseBool(self) -> None:
        assert parse_bool("True") is True

    def test_parseBadBoolField(self) -> None:
        with self.assertRaises(ValueError):
            parse_bool("ABC")

    def test_parseDateTime(self) -> None:
        assert parse_datetime("Jan 1, 2018 1:40") == datetime.datetime(
            year=2018, month=1, day=1, hour=1, minute=40
        )

    def test_parseDateTime_yearMonth(self) -> None:
        assert parse_datetime(
            "2018-04", from_dt=datetime.datetime(2019, 3, 15)
        ) == datetime.datetime(year=2018, month=4, day=1)

    def test_parseDateTime_yyyymmdd_format(self) -> None:
        assert parse_datetime("19990629", from_dt=None) == datetime.datetime(
            year=1999, month=6, day=29
        )

    def test_parseDateTime_relative(self) -> None:
        assert parse_datetime(
            "1y 1m 1d", from_dt=datetime.datetime(2000, 1, 1)
        ) == datetime.datetime(year=1998, month=11, day=30)

    def test_parseDateTime_zero(self) -> None:
        assert parse_datetime("0") is None

    def test_parseDateTime_zeroes(self) -> None:
        assert parse_datetime("00000000") is None

    def test_parseDateTime_zeroes_weird(self) -> None:
        assert parse_datetime("0 0 0") is None
        assert parse_datetime("0000-00-00") is None

    def test_parseDate(self) -> None:
        assert parse_date("Jan 1, 2018") == datetime.date(year=2018, month=1, day=1)

    def test_parseDate_slash_separators(self) -> None:
        assert parse_date("05/12/2013") == datetime.date(year=2013, month=5, day=12)

    def test_parseDate_slash_separators_no_date(self) -> None:
        assert parse_date("11/2010") == datetime.date(year=2010, month=11, day=1)

    def test_parseDate_slash_separators_two_digit_year(self) -> None:
        assert parse_date("8/21/10") == datetime.date(year=2010, month=8, day=21)

    def test_parseDate_dot_separators(self) -> None:
        assert parse_date("12.25.2007") == datetime.date(year=2007, month=12, day=25)

    def test_parseDate_dot_separators_part_string_part_number(self) -> None:
        assert parse_date("APR.2012") == datetime.date(year=2012, month=4, day=1)

    def test_parseDate_space_separators(self) -> None:
        assert parse_date("01 14 2018") == datetime.date(year=2018, month=1, day=14)

    def test_parseDate_space_separators_no_date(self) -> None:
        assert parse_date("01 2018") == datetime.date(year=2018, month=1, day=1)

    def test_parseDate_space_separators_part_string_part_number(self) -> None:
        assert parse_date("MAY 2003") == datetime.date(year=2003, month=5, day=1)

    def test_parseDate_no_separators(self) -> None:
        assert parse_date("03122008") == datetime.date(year=2008, month=3, day=12)

    def test_parseDate_no_separators_part_string_part_number(self) -> None:
        assert parse_date("June2016") == datetime.date(year=2016, month=6, day=1)

    def test_parseDate_zero(self) -> None:
        assert parse_date("0") is None

    def test_parseDate_zeroes(self) -> None:
        assert parse_date("00000000") is None

    def test_parseDate_zeroes_weird(self) -> None:
        assert parse_date("0 0 0") is None
        assert parse_date("0000-00-00") is None

    def test_parseNoDate(self) -> None:
        assert parse_date("None set") is None

    def test_parseBadDate(self) -> None:
        with self.assertRaises(ValueError):
            parse_datetime("ABC")

    def test_parseJSON(self) -> None:
        self.assertEqual("{}", normalize_flat_json("{}"))
        self.assertEqual('{"foo": "HELLO"}', normalize_flat_json('{"foo": "hello"}'))
        self.assertEqual(
            '{"bar": "123", "foo": "HELLO"}',
            normalize_flat_json('{"foo": "hello", "bar": "123"}'),
        )
        self.assertEqual(
            '{"bar": "123", "foo": "HELLO"}',
            normalize_flat_json('{"bar": "123", "foo": "hello"}'),
        )
        self.assertEqual(
            '{"foo": "A &&&"}', normalize_flat_json('{"foo": "a    &&& "}')
        )

    def test_parseJSON_NotFlatStringJSON(self) -> None:
        with self.assertRaises(ValueError):
            normalize_flat_json('{"foo": "hello", "bar": 123}')
        with self.assertRaises(ValueError):
            normalize_flat_json('{"foo": "hello", "bar": []]}')
        with self.assertRaises(ValueError):
            normalize_flat_json('[{"foo": "hello"}]')

    def test_parseJSON_Malformed(self) -> None:
        with self.assertRaises(TypeError):
            normalize_flat_json(None)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            normalize_flat_json({"foo": "bar"})  # type: ignore[arg-type]
        with self.assertRaises(JSONDecodeError):
            normalize_flat_json("")
        with self.assertRaises(JSONDecodeError):
            normalize_flat_json("{")
