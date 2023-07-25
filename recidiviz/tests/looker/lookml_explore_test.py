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
"""Tests functionality of LookMLExplore functions"""
import unittest

from recidiviz.looker.lookml_explore import LookMLExplore


class LookMLExploreTest(unittest.TestCase):
    """Tests correctness of LookML view generation"""

    def test_empty_lookml_explore(self) -> None:
        # Empty explore
        explore = LookMLExplore(
            explore_name="test_explore",
            parameters=[],
        ).build()
        expected = """explore: test_explore {

}"""
        self.assertEqual(explore, expected)

    def test_lookml_explore_extension_required(self) -> None:
        # Empty explore with extension required
        explore = LookMLExplore(
            explore_name="test_explore",
            extension_required=True,
            parameters=[],
        ).build()
        expected = """explore: test_explore {
  extension: required

}"""
        self.assertEqual(explore, expected)

    def test_lookml_explore_view_name(self) -> None:
        # Empty explore with some view name
        explore = LookMLExplore(
            explore_name="test_explore",
            view_name="test_view_name",
            parameters=[],
        ).build()
        expected = """explore: test_explore {

  view_name: test_view_name

}"""
        self.assertEqual(explore, expected)

    def test_lookml_explore_extends(self) -> None:
        # With one extended explore
        explore = LookMLExplore(
            explore_name="test_explore",
            extended_explores=["test1"],
            parameters=[],
        ).build()
        expected = """explore: test_explore {
  extends: [
    test1
  ]

}"""
        self.assertEqual(explore, expected)

        # With multiple extended explores
        explore = LookMLExplore(
            explore_name="test_explore",
            extended_explores=["test1", "test2", "test3"],
            parameters=[],
        ).build()
        expected = """explore: test_explore {
  extends: [
    test1,
    test2,
    test3
  ]

}"""
        self.assertEqual(explore, expected)
