# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

import unittest

import attr

from recidiviz.common.attr_mixins import BuilderException, BuildableAttr, \
    DefaultableAttr


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
            required_field="TEST",
            field_with_default=[]
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
            field='field', field_another=None, field_with_default=1,
            factory_field=[])

        self.assertEqual(subject, expected_result)

    def testInstantiateDefaultableAttr_RaisesException(self):
        with self.assertRaises(Exception):
            DefaultableAttr()
