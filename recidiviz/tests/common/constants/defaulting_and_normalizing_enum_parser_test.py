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
"""Tests for DefaultingAndNormalizingEnumParser."""

import unittest
from typing import Dict, Optional

from recidiviz.common.constants.county.person_characteristics import (
    Ethnicity,
    Gender,
    Race,
)
from recidiviz.common.constants.defaulting_and_normalizing_enum_parser import (
    DefaultingAndNormalizingEnumParser,
)
from recidiviz.common.constants.entity_enum import (
    EntityEnum,
    EntityEnumMeta,
    EnumParsingError,
)
from recidiviz.common.constants.enum_overrides import EnumOverrides


class _MyEntityEnum(EntityEnum, metaclass=EntityEnumMeta):
    ITEM1 = "xx"
    ITEM2 = "yy"

    @staticmethod
    def _get_default_map() -> Dict[str, "_MyEntityEnum"]:
        return {"ITEM1": _MyEntityEnum.ITEM1, "ITEM2": _MyEntityEnum.ITEM2}


def ethnicity_mapper(label: str) -> Optional[Ethnicity]:
    if "IS HISPANIC" in label:
        return Ethnicity.HISPANIC
    if "NOT HISPANIC" in label:
        return Ethnicity.NOT_HISPANIC
    return None


def mapper_that_throws(label: str) -> Optional[Ethnicity]:
    raise ValueError(f"Crash! {label}")


def ignore_my_enum(label: str) -> bool:
    return "X" in label


class TestDefaultingAndNormalizingEnumParser(unittest.TestCase):
    """Tests for DefaultingAndNormalizingEnumParser."""

    def setUp(self) -> None:
        self.overrides = (
            EnumOverrides.Builder()
            .add("BLACK", Race.BLACK)
            .add("WHITE", Race.WHITE)
            .add("MA", Gender.MALE)
            .add("FE", Gender.FEMALE)
            .add("fem", Gender.FEMALE, normalize_label=False)
            .ignore("X", Gender)
            .add_mapper_fn(ethnicity_mapper, Ethnicity)
            .ignore_with_predicate(ignore_my_enum, _MyEntityEnum)
            .build()
        )

    def test_parse_bad_str(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = DefaultingAndNormalizingEnumParser(
                "FOO", Race, EnumOverrides.empty()
            ).parse()

    def test_mapper_that_throws(self) -> None:
        overrides = (
            EnumOverrides.Builder().add_mapper_fn(mapper_that_throws, Ethnicity).build()
        )

        with self.assertRaises(EnumParsingError):
            _ = DefaultingAndNormalizingEnumParser("X", Ethnicity, overrides).parse()

    def test_parse_explicit_mapping(self) -> None:
        self.assertEqual(
            Gender.MALE,
            DefaultingAndNormalizingEnumParser("MA", Gender, self.overrides).parse(),
        )
        self.assertEqual(
            Gender.FEMALE,
            DefaultingAndNormalizingEnumParser("FE", Gender, self.overrides).parse(),
        )

        self.assertEqual(
            Race.BLACK,
            DefaultingAndNormalizingEnumParser("BLACK", Race, self.overrides).parse(),
        )
        self.assertEqual(
            Race.WHITE,
            DefaultingAndNormalizingEnumParser("WHITE", Race, self.overrides).parse(),
        )

    def test_parse_default_mapping(self) -> None:
        self.assertEqual(
            Gender.MALE,
            DefaultingAndNormalizingEnumParser("M", Gender, self.overrides).parse(),
        )
        self.assertEqual(
            Gender.FEMALE,
            DefaultingAndNormalizingEnumParser("F", Gender, self.overrides).parse(),
        )

        self.assertEqual(
            Race.BLACK,
            DefaultingAndNormalizingEnumParser("B", Race, self.overrides).parse(),
        )
        self.assertEqual(
            Race.WHITE,
            DefaultingAndNormalizingEnumParser("W", Race, self.overrides).parse(),
        )

    def test_parse_explicit_mapping_unnormalized(self) -> None:
        self.assertEqual(
            Gender.MALE,
            DefaultingAndNormalizingEnumParser("ma", Gender, self.overrides).parse(),
        )
        self.assertEqual(
            Race.WHITE,
            DefaultingAndNormalizingEnumParser("White", Race, self.overrides).parse(),
        )

    def test_parse_default_mapping_unnormalized(self) -> None:
        self.assertEqual(
            Gender.MALE,
            DefaultingAndNormalizingEnumParser("m", Gender, self.overrides).parse(),
        )

        self.assertEqual(
            Race.BLACK,
            DefaultingAndNormalizingEnumParser("b", Race, self.overrides).parse(),
        )

    def test_parse_with_mapper(self) -> None:
        self.assertEqual(
            Ethnicity.HISPANIC,
            DefaultingAndNormalizingEnumParser(
                "IS_HISPANIC", Ethnicity, self.overrides
            ).parse(),
        )
        self.assertEqual(
            Ethnicity.HISPANIC,
            DefaultingAndNormalizingEnumParser(
                "IS HISPANIC", Ethnicity, self.overrides
            ).parse(),
        )

        self.assertEqual(
            Ethnicity.NOT_HISPANIC,
            DefaultingAndNormalizingEnumParser(
                "NOT_HISPANIC", Ethnicity, self.overrides
            ).parse(),
        )
        self.assertEqual(
            Ethnicity.NOT_HISPANIC,
            DefaultingAndNormalizingEnumParser(
                "NOT HISPANIC", Ethnicity, self.overrides
            ).parse(),
        )

    def test_parse_with_mapper_returns_None(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = DefaultingAndNormalizingEnumParser(
                "XXX", Ethnicity, self.overrides
            ).parse()

    def test_parse_value_not_normalized_in_overrides(self) -> None:
        with self.assertRaises(EnumParsingError):
            _ = DefaultingAndNormalizingEnumParser(
                "fem", Gender, self.overrides
            ).parse()

        with self.assertRaises(EnumParsingError):
            _ = DefaultingAndNormalizingEnumParser(
                "FEM", Gender, self.overrides
            ).parse()

    def test_parse_ignored(self) -> None:
        self.assertIsNone(
            DefaultingAndNormalizingEnumParser("X", Gender, self.overrides).parse()
        )
        self.assertIsNone(
            DefaultingAndNormalizingEnumParser("x", Gender, self.overrides).parse()
        )

    def test_ignore_with_predicate(self) -> None:
        self.assertIsNone(
            DefaultingAndNormalizingEnumParser(
                "x", _MyEntityEnum, self.overrides
            ).parse()
        )
        self.assertIsNone(
            DefaultingAndNormalizingEnumParser(
                "X", _MyEntityEnum, self.overrides
            ).parse()
        )

        self.assertEqual(
            _MyEntityEnum.ITEM1,
            DefaultingAndNormalizingEnumParser(
                "ITEM1", _MyEntityEnum, self.overrides
            ).parse(),
        )

        with self.assertRaises(EnumParsingError):
            _ = DefaultingAndNormalizingEnumParser(
                "YYY", _MyEntityEnum, self.overrides
            ).parse()
