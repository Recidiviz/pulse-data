# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for methods in git.py."""
import unittest
from unittest.mock import patch

from recidiviz.common.git import get_normalized_git_username


class TestGit(unittest.TestCase):
    def test_get_normalized_git_username(self) -> None:
        with patch("recidiviz.common.git._get_git_username") as mock_get_git_username:
            mock_get_git_username.return_value = "Emily Turner"
            self.assertEqual(get_normalized_git_username(), "emily-turner")

            mock_get_git_username.return_value = "Emily Turner Jr."
            self.assertEqual(get_normalized_git_username(), "emily-turner-jr-")

            mock_get_git_username.return_value = "Flannery O'Connor"
            self.assertEqual(get_normalized_git_username(), "flannery-o-connor")
