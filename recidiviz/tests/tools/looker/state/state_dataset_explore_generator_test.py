# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for state_dataset_explore_generator.py."""
import unittest

from recidiviz.tools.looker.state.state_dataset_explore_generator import (
    generate_state_person_lookml_explore,
    generate_state_staff_lookml_explore,
)


class TestStateDatasetExploreGenerator(unittest.TestCase):
    """Tests for state_dataset_explore_generator.py."""

    def test_generate_state_person(self) -> None:
        # Assert doesn't crash
        _ = generate_state_person_lookml_explore()

    def test_generate_state_staff(self) -> None:
        # Assert doesn't crash
        _ = generate_state_staff_lookml_explore()
