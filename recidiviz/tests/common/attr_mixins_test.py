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
"""Tests for BuildableAttr base class."""

from enum import Enum
from typing import List, Optional
from datetime import date
import unittest
import attr

from recidiviz.common.attr_mixins import (
    BuilderException,
    BuildableAttr,
    DefaultableAttr,
)


@attr.s
class FakeBuildableAttr(BuildableAttr):
    required_field = attr.ib()
    field_with_default = attr.ib(factory=list)


@attr.s
class FakeDefaultableAttr(DefaultableAttr):
    field = attr.ib()
    field_another = attr.ib()
    field_with_default = attr.ib(default=1)
    factory_field = attr.ib(factory=list)


class FakeEnum(Enum):
    A = "A"
    B = "B"


class InvalidFakeEnum(Enum):
    A = "A"
    C = "C"
    D = "D"


@attr.s
class FakeBuildableAttrDeluxe(BuildableAttr):
    required_field = attr.ib()
    another_required_field = attr.ib()
    enum_nonnull_field: FakeEnum = attr.ib()
    enum_field: Optional[FakeEnum] = attr.ib(default=None)
    date_field: Optional[date] = attr.ib(default=None)
    field_list: List[str] = attr.ib(factory=list)
    field_forward_ref: Optional["FakeBuildableAttr"] = attr.ib(default=None)


class BuildableAttrTests(unittest.TestCase):
    """Tests for BuildableAttr base class."""

    def testBuild_WithRequiredFields_BuildsAttr(self):
        # Arrange
        subject = FakeBuildableAttr.builder()
        subject.required_field = "TEST"

        # Act
        result = subject.build()

        # Assert
        expected_result = FakeBuildableAttr(
            required_field="TEST", field_with_default=[]
        )

        self.assertEqual(result, expected_result)

    def testBuild_MissingRequiredField_RaisesException(self):
        # Arrange
        subject = FakeBuildableAttr.builder()

        # Act + Assert
        with self.assertRaises(BuilderException):
            subject.build()

    def testBuild_ExtraField_RaisesException(self):
        # Arrange
        subject = FakeBuildableAttr.builder()
        subject.required_field = "TEST"
        subject.not_a_real_field = "TEST_2"

        # Act + Assert
        with self.assertRaises(BuilderException):
            subject.build()

    def testInstantiateAbstractBuildableAttr_RaisesException(self):
        with self.assertRaises(Exception):
            BuildableAttr()

    def testNewWithDefaults(self):
        # Arrange
        subject = FakeDefaultableAttr.new_with_defaults(field="field")

        # Assert
        expected_result = FakeDefaultableAttr(
            field="field", field_another=None, field_with_default=1, factory_field=[]
        )

        self.assertEqual(subject, expected_result)

    def testInstantiateDefaultableAttr_RaisesException(self):
        with self.assertRaises(Exception):
            DefaultableAttr()

    def testBuildFromDictionary(self):
        # Construct dictionary representation
        subject_dict = {
            "required_field": "value",
            "another_required_field": "another_value",
            "enum_nonnull_field": FakeEnum.A.value,
            "enum_field": FakeEnum.B.value,
        }

        # Build from dictionary
        subject = FakeBuildableAttrDeluxe.build_from_dictionary(subject_dict)

        # Assert
        expected_result = FakeBuildableAttrDeluxe(
            required_field="value",
            another_required_field="another_value",
            enum_nonnull_field=FakeEnum.A,
            enum_field=FakeEnum.B,
        )

        self.assertEqual(subject, expected_result)

    def testBuildFromDictionary_Enum(self):
        # Construct dictionary representation
        subject_dict = {
            "required_field": "value",
            "another_required_field": "another_value",
            "enum_nonnull_field": FakeEnum.A,
        }

        # Build from dictionary
        subject = FakeBuildableAttrDeluxe.build_from_dictionary(subject_dict)

        # Assert
        expected_result = FakeBuildableAttrDeluxe(
            required_field="value",
            another_required_field="another_value",
            enum_nonnull_field=FakeEnum.A,
        )

        self.assertEqual(subject, expected_result)

    def testBuildFromDictionary_MissingRequiredArgs(self):
        with self.assertRaises(Exception):
            # Construct dictionary representation
            subject_dict = {"required_field": "value"}

            # Build from dictionary
            _ = FakeBuildableAttrDeluxe.build_from_dictionary(subject_dict)

    def testBuildFromDictionary_MissingNonnullEnum(self):
        with self.assertRaises(Exception):
            # Construct dictionary representation
            subject_dict = {
                "required_field": "value",
                "another_required_field": "another_value",
            }

            # Build from dictionary
            _ = FakeBuildableAttrDeluxe.build_from_dictionary(subject_dict)

    def testBuildFromDictionary_EmptyDict(self):
        with self.assertRaises(ValueError):
            _ = FakeBuildableAttr.build_from_dictionary({})

    def testBuildFromDictionary_ExtraArguments(self):
        # Construct dictionary representation
        subject_dict = {
            "required_field": "value",
            "another_required_field": "another_value",
            "enum_nonnull_field": FakeEnum.A.value,
            "extra_invalid_field": "extra_value",
        }

        # Build from dictionary
        subject = FakeBuildableAttrDeluxe.build_from_dictionary(subject_dict)

        # Assert
        expected_result = FakeBuildableAttrDeluxe(
            required_field="value",
            another_required_field="another_value",
            enum_nonnull_field=FakeEnum.A,
        )

        self.assertEqual(subject, expected_result)

    def testBuildFromDictionary_WrongEnum(self):
        with self.assertRaises(ValueError):
            # Construct dictionary representation
            subject_dict = {
                "required_field": "value",
                "another_required_field": "another_value",
                "enum_field": InvalidFakeEnum.C,
            }

            # Build from dictionary
            _ = FakeBuildableAttrDeluxe.build_from_dictionary(subject_dict)

    def testBuildFromDictionary_WrongEnumSameValue(self):
        with self.assertRaises(ValueError):
            # Construct dictionary representation
            subject_dict = {
                "required_field": "value",
                "another_required_field": "another_value",
                "enum_field": InvalidFakeEnum.A,
            }

            # Build from dictionary
            _ = FakeBuildableAttrDeluxe.build_from_dictionary(subject_dict)

    def testBuildFromDictionary_ListInDict(self):
        # Construct dictionary representation
        subject_dict = {
            "required_field": "value",
            "another_required_field": "another_value",
            "enum_nonnull_field": FakeEnum.A,
            "field_list": ["a", "b", "c"],
        }

        subject = FakeBuildableAttrDeluxe.build_from_dictionary(subject_dict)

        # Assert
        expected_result = FakeBuildableAttrDeluxe(
            required_field="value",
            another_required_field="another_value",
            enum_nonnull_field=FakeEnum.A,
            field_list=["a", "b", "c"],
        )

        self.assertEqual(subject, expected_result)

    def testBuildFromDictionary_InvalidForwardRefInDict(self):
        with self.assertRaises(ValueError):

            # Construct dictionary representation
            subject_dict = {
                "required_field": "value",
                "another_required_field": "another_value",
                "field_forward_ref": FakeBuildableAttr("a", ["a", "b"]),
            }

            # Build from dictionary
            _ = FakeBuildableAttrDeluxe.build_from_dictionary(subject_dict)

    def testBuildFromDictionary_WithDate(self):
        # Construct dictionary representation
        subject_dict = {
            "required_field": "value",
            "another_required_field": "another_value",
            "enum_nonnull_field": FakeEnum.A.value,
            "date_field": "2001-01-08",
        }

        # Build from dictionary
        subject = FakeBuildableAttrDeluxe.build_from_dictionary(subject_dict)

        # Assert
        expected_result = FakeBuildableAttrDeluxe(
            required_field="value",
            another_required_field="another_value",
            enum_nonnull_field=FakeEnum.A,
            date_field=date.fromisoformat("2001-01-08"),
        )

        self.assertEqual(subject, expected_result)

    def testBuildFromDictionary_WithEmptyDate(self):
        # Construct dictionary representation
        subject_dict = {
            "required_field": "value",
            "another_required_field": "another_value",
            "enum_nonnull_field": FakeEnum.A.value,
            "date_field": None,
        }

        # Build from dictionary
        subject = FakeBuildableAttrDeluxe.build_from_dictionary(subject_dict)

        # Assert
        expected_result = FakeBuildableAttrDeluxe(
            required_field="value",
            another_required_field="another_value",
            enum_nonnull_field=FakeEnum.A,
            date_field=None,
        )

        self.assertEqual(subject, expected_result)

    def testBuildFromDictionary_WithInvalidDateFormat(self):
        with self.assertRaises(ValueError):

            # Construct dictionary representation
            subject_dict = {
                "required_field": "value",
                "another_required_field": "another_value",
                "enum_nonnull_field": FakeEnum.A.value,
                "date_field": "01-01-1999",
            }

            # Build from dictionary
            _ = FakeBuildableAttrDeluxe.build_from_dictionary(subject_dict)

    def testBuildFromDictionary_WithInvalidDateString(self):
        with self.assertRaises(ValueError):

            # Construct dictionary representation
            subject_dict = {
                "required_field": "value",
                "another_required_field": "another_value",
                "enum_nonnull_field": FakeEnum.A.value,
                "date_field": "YYYY-MM-DD",
            }

            # Build from dictionary
            _ = FakeBuildableAttrDeluxe.build_from_dictionary(subject_dict)
