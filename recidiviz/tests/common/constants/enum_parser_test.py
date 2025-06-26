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
"""Tests for strict_enum_parser.py."""
import unittest
from enum import Enum
from typing import Optional

from recidiviz.common.constants.enum_parser import EnumParser, EnumParsingError
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
)


class _MyEnum(Enum):
    ITEM1 = "xx"
    ITEM2 = "yy"


def ethnicity_mapper(label: str) -> Optional[StateEthnicity]:
    if "IS HISPANIC" in label:
        return StateEthnicity.HISPANIC
    if "NOT HISPANIC" in label:
        return StateEthnicity.NOT_HISPANIC
    return None


def mapper_that_throws(label: str) -> Optional[StateEthnicity]:
    raise ValueError(f"Crash! {label}")


def ignore_my_enum(label: str) -> bool:
    return "X" in label


class TestEnumParser(unittest.TestCase):
    """Tests for EnumParser."""

    def setUp(self) -> None:
        self.race_parser = (
            EnumParser(StateRace)
            .add_raw_text_mapping(StateRace.BLACK, "BLACK")
            .add_raw_text_mapping(StateRace.WHITE, "WHITE")
        )

        self.gender_parser = (
            EnumParser(StateGender)
            .add_raw_text_mapping(StateGender.MALE, "MA")
            .add_raw_text_mapping(StateGender.FEMALE, "FE")
            .add_raw_text_mapping(StateGender.FEMALE, "fem")
            .ignore_raw_text_value("X")
        )

        self.ethnicity_parser = EnumParser(StateEthnicity).add_mapper_fn(
            ethnicity_mapper
        )

        self.my_enum_parser = EnumParser(_MyEnum).add_raw_text_mapping(
            _MyEnum.ITEM1, "ITEMA"
        )

    def test_parse_bad_str(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = (
                EnumParser(StateRace)
                .add_raw_text_mapping(StateRace.BLACK, "BAR")
                .parse("FOO")
            )

    def test_mapper_that_throws(self) -> None:
        parser = EnumParser(StateEthnicity).add_mapper_fn(mapper_that_throws)

        with self.assertRaises(EnumParsingError):
            _ = parser.parse("X")

    def test_parse_explicit_mapping(self) -> None:
        self.assertEqual(
            StateGender.MALE,
            self.gender_parser.parse("MA"),
        )
        self.assertEqual(StateGender.FEMALE, self.gender_parser.parse("FE"))

        self.assertEqual(StateRace.BLACK, self.race_parser.parse("BLACK"))
        self.assertEqual(StateRace.WHITE, self.race_parser.parse("WHITE"))

        self.assertEqual(self.race_parser.parse(None), None)
        self.assertEqual(self.race_parser.parse(""), None)

    def test_parse_missing_mapping(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = self.gender_parser.parse("M")

        with self.assertRaises(EnumParsingError):
            _ = self.gender_parser.parse("F")

        with self.assertRaises(EnumParsingError):
            _ = self.race_parser.parse("B")

        with self.assertRaises(EnumParsingError):
            _ = self.race_parser.parse("W")

    def test_parse_mapping_does_not_match_case(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = self.gender_parser.parse("ma")

        with self.assertRaises(EnumParsingError):
            _ = self.race_parser.parse("White")

    def test_parse_default_mapping_unnormalized(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = self.gender_parser.parse("m")

        with self.assertRaises(EnumParsingError):
            _ = self.race_parser.parse("b")

    def test_parse_with_mapper(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = self.ethnicity_parser.parse("IS_HISPANIC")

        self.assertEqual(
            StateEthnicity.HISPANIC,
            self.ethnicity_parser.parse("IS HISPANIC"),
        )

        with self.assertRaises(EnumParsingError):
            self.ethnicity_parser.parse("NOT_HISPANIC")

        self.assertEqual(
            StateEthnicity.NOT_HISPANIC,
            self.ethnicity_parser.parse("NOT HISPANIC"),
        )

    def test_parse_with_mapper_returns_None(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = self.ethnicity_parser.parse("XXX")

    def test_parse_value_not_normalized_in_overrides(self) -> None:
        self.assertEqual(
            StateGender.FEMALE,
            self.gender_parser.parse("fem"),
        )

        with self.assertRaises(EnumParsingError):
            _ = self.gender_parser.parse("FEM")

    def test_parse_ignored(self) -> None:
        # X is a value we should ignore
        self.assertIsNone(self.gender_parser.parse("X"))
        with self.assertRaises(EnumParsingError):
            # Case does not match ignored value
            _ = self.gender_parser.parse("x")

    def test_double_add_fails(self) -> None:
        parser = EnumParser(enum_cls=_MyEnum)
        parser.add_raw_text_mapping(_MyEnum.ITEM1, "A")
        with self.assertRaisesRegex(
            ValueError, r"Raw text value \[A\] already mapped."
        ):
            parser.add_raw_text_mapping(_MyEnum.ITEM1, "A")

        with self.assertRaisesRegex(
            ValueError, r"Raw text value \[A\] already mapped."
        ):
            # Adding for a different enum value also fails
            parser.add_raw_text_mapping(_MyEnum.ITEM2, "A")

    def test_add_and_ignore_same_value(self) -> None:
        parser = EnumParser(enum_cls=_MyEnum)
        parser.add_raw_text_mapping(_MyEnum.ITEM1, "A")
        with self.assertRaisesRegex(
            ValueError, r"Raw text value \[A\] already mapped."
        ):
            # Adding for a different enum value also fails
            parser.ignore_raw_text_value("A")

    def test_ignore_and_add_same_value(self) -> None:
        parser = EnumParser(enum_cls=_MyEnum)
        parser.ignore_raw_text_value("A")
        with self.assertRaisesRegex(
            ValueError, r"Raw text value \[A\] already mapped."
        ):
            # Adding for a different enum value also fails
            parser.add_raw_text_mapping(_MyEnum.ITEM2, "A")

    def test_double_add_different_case(self) -> None:
        parser = EnumParser(enum_cls=_MyEnum)
        parser.add_raw_text_mapping(_MyEnum.ITEM1, "A")
        parser.add_raw_text_mapping(_MyEnum.ITEM1, "a")

        self.assertEqual(_MyEnum.ITEM1, parser.parse("A"))
        self.assertEqual(_MyEnum.ITEM1, parser.parse("a"))

    def test_add_mapper_fn_after_direct_mappings(self) -> None:
        parser = EnumParser(enum_cls=_MyEnum)
        parser.add_raw_text_mapping(_MyEnum.ITEM1, "A")
        with self.assertRaisesRegex(
            ValueError,
            r"Must define either a mapper function or raw text mappings for "
            r"\[_MyEnum\] but not both.",
        ):
            parser.add_mapper_fn(lambda x: _MyEnum.ITEM1)

    def test_add_direct_mappings_after_mapper_fn(self) -> None:
        parser = EnumParser(enum_cls=_MyEnum)
        parser.add_mapper_fn(lambda x: _MyEnum.ITEM1)
        with self.assertRaisesRegex(
            ValueError,
            r"Must define either a mapper function or raw text mappings for "
            r"\[_MyEnum\] but not both.",
        ):
            parser.add_raw_text_mapping(_MyEnum.ITEM1, "A")
