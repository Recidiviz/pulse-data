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
import textwrap
from copy import copy
from datetime import datetime
from unittest import TestCase

from recidiviz.reporting.context.context_utils import singular_or_plural, format_date, align_columns

_PREPARED_DATA: dict = {
    'singular_value': '1',
    'plural_value': '2',
    'zero_value': '0'
}

SINGULAR_TEXT = 'Monstera Deliciosa'
PLURAL_TEXT = 'Monsteras Deliciosa'


class ContextUtilsTest(TestCase):
    """Tests for context_utils.py."""

    def test_singular_or_plural_singular(self) -> None:
        expected = SINGULAR_TEXT
        prepared_data = copy(_PREPARED_DATA)

        singular_or_plural(prepared_data, 'singular_value', 'final_text', SINGULAR_TEXT, PLURAL_TEXT)
        actual = prepared_data['final_text']
        self.assertEqual(expected, actual)

    def test_singular_or_plural_plural(self) -> None:
        expected = PLURAL_TEXT
        prepared_data = copy(_PREPARED_DATA)

        singular_or_plural(prepared_data, 'plural_value', 'final_text', SINGULAR_TEXT, PLURAL_TEXT)
        actual = prepared_data['final_text']
        self.assertEqual(expected, actual)

    def test_singular_or_plural_zero(self) -> None:
        expected = PLURAL_TEXT
        prepared_data = copy(_PREPARED_DATA)

        singular_or_plural(prepared_data, 'zero_value', 'final_text', SINGULAR_TEXT, PLURAL_TEXT)
        actual = prepared_data['final_text']
        self.assertEqual(expected, actual)

    def test_format_date(self) -> None:
        date = datetime.strptime('20201205112344', '%Y%m%d%H%M%S')
        actual = format_date('20201205112344', current_format='%Y%m%d%H%M%S')
        self.assertEqual(datetime.strftime(date, '%m/%d/%Y'), actual)

    def test_align_columns(self) -> None:
        columns = [
            ["few char", "many characters", "a little"],
            ["1", "2", "3"],
            ["a longer one", "few char", "many"]
        ]

        expected = textwrap.dedent("""\
            few char         many characters     a little    
            1                2                   3           
            a longer one     few char            many        """)

        self.assertEqual(expected, align_columns(columns))
