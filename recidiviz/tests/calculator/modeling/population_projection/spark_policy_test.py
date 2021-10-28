# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Test the SparkPolicy object"""

import unittest
from typing import Any

from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy


class TestSparkPolicy(unittest.TestCase):
    """Test the SparkPolicy class runs correctly"""

    @staticmethod
    def dummy_policy_method(arg: Any) -> None:
        """Placeholder method to use for initializing the SparkPolicy test objects"""

    def setUp(self) -> None:
        self.policy_list = [
            SparkPolicy(
                policy_fn=TestSparkPolicy.dummy_policy_method,
                spark_compartment="prison",
                sub_population={"test1": "value"},
                policy_ts=0,
                apply_retroactive=True,
            ),
            SparkPolicy(
                policy_fn=TestSparkPolicy.dummy_policy_method,
                spark_compartment="jail",
                sub_population={"test2": "value"},
                policy_ts=1,
                apply_retroactive=True,
            ),
        ]

    def test_get_sub_population_policies(self) -> None:
        expected_list = self.policy_list[:1]
        result_list = SparkPolicy.get_sub_population_policies(
            self.policy_list, sub_population={"test1": "value"}
        )
        self.assertEqual(expected_list, result_list)

    def test_get_compartment_policies(self) -> None:
        expected_list = self.policy_list[1:]
        result_list = SparkPolicy.get_compartment_policies(
            self.policy_list, spark_compartment="jail"
        )
        self.assertEqual(expected_list, result_list)

    def test_get_ts_policies(self) -> None:
        expected_list = self.policy_list[1:]
        result_list = SparkPolicy.get_ts_policies(self.policy_list, time_step=1)
        self.assertEqual(expected_list, result_list)
