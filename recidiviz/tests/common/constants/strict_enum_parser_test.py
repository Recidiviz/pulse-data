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

from recidiviz.common.constants.enum_parser import EnumParsingError
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
)
from recidiviz.common.constants.strict_enum_parser import StrictEnumParser


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


class TestStrictEnumParser(unittest.TestCase):
    """Tests for StrictEnumParser."""

    def setUp(self) -> None:
        self.race_parser = (
            StrictEnumParser(StateRace)
            .add_raw_text_mapping(StateRace.BLACK, "BLACK")
            .add_raw_text_mapping(StateRace.WHITE, "WHITE")
        )

        self.gender_parser = (
            StrictEnumParser(StateGender)
            .add_raw_text_mapping(StateGender.MALE, "MA")
            .add_raw_text_mapping(StateGender.FEMALE, "FE")
            .add_raw_text_mapping(StateGender.FEMALE, "fem")
            .ignore_raw_text_value("X")
        )

        self.ethnicity_parser = StrictEnumParser(StateEthnicity).add_mapper_fn(
            ethnicity_mapper
        )

        self.my_enum_parser = StrictEnumParser(_MyEnum).add_raw_text_mapping(
            _MyEnum.ITEM1, "ITEMA"
        )

    def test_parse_bad_str(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = StrictEnumParser(StateRace).parse("FOO")

    def test_mapper_that_throws(self) -> None:
        parser = StrictEnumParser(StateEthnicity).add_mapper_fn(mapper_that_throws)

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

    def test_parse_default_mapping(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = self.gender_parser.parse("M")

        with self.assertRaises(EnumParsingError):
            _ = self.gender_parser.parse("F")

        with self.assertRaises(EnumParsingError):
            _ = self.race_parser.parse("B")

        with self.assertRaises(EnumParsingError):
            _ = self.race_parser.parse("W")

    def test_parse_explicit_mapping_unnormalized(self) -> None:
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
        self.assertIsNone(self.gender_parser.parse("X"))
        with self.assertRaises(EnumParsingError):
            _ = self.gender_parser.parse("x")
