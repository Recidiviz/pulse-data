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
"""Tests for big_query_utils.py"""
import unittest

from recidiviz.big_query.big_query_utils import normalize_column_name_for_bq


class BigQueryUtilsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.head_whitespace = "  FIELD_NAME_532"
        self.tail_whitespace = "FIELD_NAME_532  "
        self.non_printable_characters = "FIELD_\x16NAME_532"
        self.name_with_spaces = "FIELD NAME 532"
        self.name_with_chars = "FIELD?NAME*532"
        self.valid_column_name = "FIELD_NAME_532"
        self.column_names = [
            self.head_whitespace,
            self.tail_whitespace,
            self.non_printable_characters,
            self.name_with_spaces,
            self.name_with_chars,
            self.valid_column_name,
        ]

    def test_normalize_column_name_for_bq_lead_whitespace(self) -> None:
        for column_name in self.column_names:
            normalized = normalize_column_name_for_bq(column_name)
            self.assertEqual(normalized, self.valid_column_name)
