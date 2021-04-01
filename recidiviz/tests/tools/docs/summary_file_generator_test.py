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
"""Tests for summary_file_generator's methods"""
import os
import unittest

from mock import patch

from recidiviz.tools.docs.summary_file_generator import update_summary_file

FIXTURES_DIRECTORY = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "fixtures"
)
SUMMARY_FIXTURE = os.path.join(FIXTURES_DIRECTORY, "test_SUMMARY.md")


class TestUpdateSummaryFile(unittest.TestCase):
    """Tests for update_summary_file"""

    def tearDown(self) -> None:
        with open(SUMMARY_FIXTURE, "w") as fixture_file:
            fixture_file.write(
                "## Section 1\n\n- Test\n\n## Section 2\n\n- Test\n\n## End Section"
            )

    @patch("recidiviz.tools.docs.summary_file_generator.SUMMARY_PATH", SUMMARY_FIXTURE)
    def test_update_summary_file(self) -> None:
        with open(SUMMARY_FIXTURE, "r") as current_file:
            current_lines = current_file.readlines()
        update_summary_file(["## Section 1\n\n", "- Test\n"], "## Section 1")
        update_summary_file(["## Section 2\n\n", "- Test\n"], "## Section 2")
        with open(SUMMARY_FIXTURE, "r") as updated_file:
            self.assertEqual(current_lines, updated_file.readlines())
