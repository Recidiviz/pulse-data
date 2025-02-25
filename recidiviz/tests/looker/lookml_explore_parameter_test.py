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
"""Tests functionality of LookMLExploreParameter types"""

import unittest

from recidiviz.looker.lookml_explore_parameter import (
    JoinCardinality,
    JoinType,
    LookMLExploreParameter,
    LookMLJoinParameter,
)


class LookMLExploreParameterTest(unittest.TestCase):
    """
    Tests LookMLExploreParameter
    """

    def test_display_parameters(self) -> None:
        # Test display parameter types
        description = LookMLExploreParameter.description("test description").build()
        expected_description = 'description: "test description"'
        self.assertEqual(description, expected_description)

        group_label = LookMLExploreParameter.group_label("test group label").build()
        expected_group_label = 'group_label: "test group label"'
        self.assertEqual(group_label, expected_group_label)

    def test_join_parameters(self) -> None:
        # Test a join parameter with different types of inner parameters
        inner_params = [
            LookMLJoinParameter.from_parameter("test_table"),
            LookMLJoinParameter.sql_on(
                "${my_left_view.test1} = ${my_right_view.test2}"
            ),
            LookMLJoinParameter.relationship(JoinCardinality.MANY_TO_MANY),
            LookMLJoinParameter.type(JoinType.LEFT_OUTER),
        ]

        join_param = LookMLExploreParameter.join("test_join_name", inner_params).build()
        expected_join_param = """join: test_join_name {
    from: test_table
    sql_on: ${my_left_view.test1} = ${my_right_view.test2};;
    relationship: many_to_many
    type: left_outer
  }
"""
        self.assertEqual(join_param, expected_join_param)
