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
from typing import Dict, Optional

from recidiviz.common.constants.entity_enum import (
    EntityEnum,
    EntityEnumMeta,
    EnumParsingError,
)
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
)
from recidiviz.common.constants.strict_enum_parser import StrictEnumParser


class _MyEntityEnum(EntityEnum, metaclass=EntityEnumMeta):
    ITEM1 = "xx"
    ITEM2 = "yy"

    @staticmethod
    def _get_default_map() -> Dict[str, "_MyEntityEnum"]:
        return {"ITEM1": _MyEntityEnum.ITEM1, "ITEM2": _MyEntityEnum.ITEM2}


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
        self.overrides = (
            EnumOverrides.Builder()
            .add("BLACK", StateRace.BLACK)
            .add("WHITE", StateRace.WHITE)
            .add("MA", StateGender.MALE)
            .add("FE", StateGender.FEMALE)
            .add("fem", StateGender.FEMALE, normalize_label=False)
            .ignore("X", StateGender)
            .add_mapper_fn(ethnicity_mapper, StateEthnicity)
            .add("ITEMA", _MyEntityEnum.ITEM1)
            .ignore_with_predicate(ignore_my_enum, _MyEntityEnum)
            .build()
        )

    def test_parse_bad_str(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = StrictEnumParser("FOO", StateRace, EnumOverrides.empty()).parse()

    def test_mapper_that_throws(self) -> None:
        overrides = (
            EnumOverrides.Builder()
            .add_mapper_fn(mapper_that_throws, StateEthnicity)
            .build()
        )

        with self.assertRaises(EnumParsingError):
            _ = StrictEnumParser("X", StateEthnicity, overrides).parse()

    def test_parse_explicit_mapping(self) -> None:
        self.assertEqual(
            StateGender.MALE,
            StrictEnumParser("MA", StateGender, self.overrides).parse(),
        )
        self.assertEqual(
            StateGender.FEMALE,
            StrictEnumParser("FE", StateGender, self.overrides).parse(),
        )

        self.assertEqual(
            StateRace.BLACK,
            StrictEnumParser("BLACK", StateRace, self.overrides).parse(),
        )
        self.assertEqual(
            StateRace.WHITE,
            StrictEnumParser("WHITE", StateRace, self.overrides).parse(),
        )

    def test_parse_default_mapping(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = StrictEnumParser("M", StateGender, self.overrides).parse()

        with self.assertRaises(EnumParsingError):
            _ = StrictEnumParser("F", StateGender, self.overrides).parse()

        with self.assertRaises(EnumParsingError):
            _ = StrictEnumParser("B", StateRace, self.overrides).parse()

        with self.assertRaises(EnumParsingError):
            _ = StrictEnumParser("W", StateRace, self.overrides).parse()

    def test_parse_explicit_mapping_unnormalized(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = StrictEnumParser("ma", StateGender, self.overrides).parse()

        with self.assertRaises(EnumParsingError):
            _ = StrictEnumParser("White", StateRace, self.overrides).parse()

    def test_parse_default_mapping_unnormalized(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = StrictEnumParser("m", StateGender, self.overrides).parse()

        with self.assertRaises(EnumParsingError):
            _ = StrictEnumParser("b", StateRace, self.overrides).parse()

    def test_parse_with_mapper(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = StrictEnumParser("IS_HISPANIC", StateEthnicity, self.overrides).parse()

        self.assertEqual(
            StateEthnicity.HISPANIC,
            StrictEnumParser("IS HISPANIC", StateEthnicity, self.overrides).parse(),
        )

        with self.assertRaises(EnumParsingError):
            StrictEnumParser("NOT_HISPANIC", StateEthnicity, self.overrides).parse()

        self.assertEqual(
            StateEthnicity.NOT_HISPANIC,
            StrictEnumParser("NOT HISPANIC", StateEthnicity, self.overrides).parse(),
        )

    def test_parse_with_mapper_returns_None(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = StrictEnumParser("XXX", StateEthnicity, self.overrides).parse()

    def test_parse_value_not_normalized_in_overrides(self) -> None:
        self.assertEqual(
            StateGender.FEMALE,
            StrictEnumParser("fem", StateGender, self.overrides).parse(),
        )

        with self.assertRaises(EnumParsingError):
            _ = StrictEnumParser("FEM", StateGender, self.overrides).parse()

    def test_parse_ignored(self) -> None:
        self.assertIsNone(StrictEnumParser("X", StateGender, self.overrides).parse())
        with self.assertRaises(EnumParsingError):
            _ = StrictEnumParser("x", StateGender, self.overrides).parse()

    def test_ignore_with_predicate(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = StrictEnumParser("x", _MyEntityEnum, self.overrides).parse()
        self.assertIsNone(StrictEnumParser("X", _MyEntityEnum, self.overrides).parse())

        self.assertEqual(
            _MyEntityEnum.ITEM1,
            StrictEnumParser("ITEMA", _MyEntityEnum, self.overrides).parse(),
        )

        with self.assertRaises(EnumParsingError):
            _ = StrictEnumParser("YYY", _MyEntityEnum, self.overrides).parse()
