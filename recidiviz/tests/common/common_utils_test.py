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
"""Tests for methods in common_utils.py."""

import unittest

from recidiviz.common.common_utils import create_generated_id, is_generated_id


class CommonUtilsTest(unittest.TestCase):
    """Tests for common_utils.py."""

    def test_create_generated_id(self):
        generated_id = create_generated_id(object())
        self.assertTrue(generated_id.endswith("_GENERATE"))

    def test_is_generated_id(self):
        id_str = "id_str_GENERATE"
        self.assertTrue(is_generated_id(id_str))

    def test_is_not_generated_id(self):
        id_str = "id_str"
        self.assertFalse(is_generated_id(id_str))
