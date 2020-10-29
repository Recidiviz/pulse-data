# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for recidiviz.persistence.database.export.export_utils.py."""

import unittest

from recidiviz.persistence.database.export.export_utils import state_code_in_clause, format_region_codes_for_sql, \
    format_columns_for_sql


class ExportUtilsTest(unittest.TestCase):
    """Tests for recidiviz.persistence.database.export.export_utils.py."""

    def setUp(self) -> None:
        self.single_state_code = ['us_va']
        self.multiple_states = ['us_nd', 'US_ID']

    def test_state_code_in_clause(self) -> None:
        """Test that state_code_in_clause returns a well-formatted SQL clause"""
        self.assertEqual(state_code_in_clause(self.single_state_code), "state_code in ('US_VA')")
        self.assertEqual(state_code_in_clause(self.multiple_states), "state_code in ('US_ND','US_ID')")

    def test_format_region_codes_for_sql(self) -> None:
        """Assert that a list of region codes are formatted to a comma delimited string
             to be used for a SQL clause
        """
        self.assertEqual(format_region_codes_for_sql(self.single_state_code), "'US_VA'")
        self.assertEqual(format_region_codes_for_sql(self.multiple_states), "'US_ND','US_ID'")

    def test_format_columns_for_sql(self) -> None:
        """Assert that a list of columns are formatted to a comma delimited string
             to be used for a SQL clause
        """
        self.assertEqual(format_columns_for_sql(['name', 'state_code', 'person_id']), "name,state_code,person_id")
        self.assertEqual(format_columns_for_sql([]), "")

    def test_format_columns_for_sql_with_prefix(self) -> None:
        """Assert that a list of columns are formatted to a comma delimited string
             to be used for a SQL clause
        """
        table_prefix = 'table_prefix'
        expected = "table_prefix.name,table_prefix.state_code,table_prefix.person_id"
        self.assertEqual(format_columns_for_sql(['name', 'state_code', 'person_id'], table_prefix), expected)
        self.assertEqual(format_columns_for_sql([], table_prefix), "")
