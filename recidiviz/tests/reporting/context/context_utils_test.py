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

"""Tests for context_utils.py."""

from copy import copy
from unittest import TestCase

from recidiviz.reporting.context.context_utils import singular_or_plural


_PREPARED_DATA: dict = {
    'singular_value': '1',
    'plural_value': '2',
    'zero_value': '0'
}

SINGULAR_TEXT = 'Monstera Deliciosa'
PLURAL_TEXT = 'Monsteras Deliciosa'


class ContextUtilsTest(TestCase):
    """Tests for context_utils.py."""

    def test_singular_or_plural_singular(self):
        expected = SINGULAR_TEXT
        prepared_data = copy(_PREPARED_DATA)

        singular_or_plural(prepared_data, 'singular_value', 'final_text', SINGULAR_TEXT, PLURAL_TEXT)
        actual = prepared_data['final_text']
        self.assertEqual(expected, actual)

    def test_singular_or_plural_plural(self):
        expected = PLURAL_TEXT
        prepared_data = copy(_PREPARED_DATA)

        singular_or_plural(prepared_data, 'plural_value', 'final_text', SINGULAR_TEXT, PLURAL_TEXT)
        actual = prepared_data['final_text']
        self.assertEqual(expected, actual)

    def test_singular_or_plural_zero(self):
        expected = PLURAL_TEXT
        prepared_data = copy(_PREPARED_DATA)

        singular_or_plural(prepared_data, 'zero_value', 'final_text', SINGULAR_TEXT, PLURAL_TEXT)
        actual = prepared_data['final_text']
        self.assertEqual(expected, actual)
