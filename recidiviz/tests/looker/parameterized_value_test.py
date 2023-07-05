# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests functionality of ParameterizedValue functions"""

import unittest

from recidiviz.looker.parameterized_value import ParameterizedValue


class ParameterizedValueTest(unittest.TestCase):
    """Tests correctness of ParameterizedValue"""

    def test_parameterized_value_output_one_option(self) -> None:
        class_output = ParameterizedValue(
            parameter_name="name",
            parameter_options=["key1"],
            value_builder=lambda s: s + "_value",
            indentation_level=0,
        ).build_liquid_template()
        expected_output = """{% if name._parameter_value == 'key1' %} key1_value
{% endif %}"""
        self.assertEqual(class_output, expected_output)

    def test_parameterized_value_output_many_options(self) -> None:
        class_output = ParameterizedValue(
            parameter_name="name",
            parameter_options=["key1", "key2", "key3"],
            value_builder=lambda s: s + "_value",
            indentation_level=2,
        ).build_liquid_template()
        expected_output = """{% if name._parameter_value == 'key1' %} key1_value
    {% elsif name._parameter_value == 'key2' %} key2_value
    {% elsif name._parameter_value == 'key3' %} key3_value
    {% endif %}"""
        self.assertEqual(class_output, expected_output)

    def test_parameterized_value_no_options_throw(self) -> None:
        with self.assertRaises(
            ValueError,
            msg="Parameter options must not be empty.",
        ):
            _ = ParameterizedValue(
                parameter_name="name",
                parameter_options=[],
                value_builder=lambda _: "",
                indentation_level=0,
            )

    def test_parameterized_value_no_name_throw(self) -> None:
        with self.assertRaises(
            ValueError,
            msg="Parameter name must not be empty.",
        ):
            _ = ParameterizedValue(
                parameter_name="",
                parameter_options=["key1"],
                value_builder=lambda _: "",
                indentation_level=0,
            )

    def test_parameterized_value_negative_indentation_throw(self) -> None:
        with self.assertRaises(
            ValueError,
            msg="Indentation level must be non-negative.",
        ):
            _ = ParameterizedValue(
                parameter_name="name",
                parameter_options=["key1"],
                value_builder=lambda _: "",
                indentation_level=-137,
            )
