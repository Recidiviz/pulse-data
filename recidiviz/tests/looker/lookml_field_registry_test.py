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
"""Tests for LookMLFieldRegistry."""
import unittest

from recidiviz.looker.lookml_field_registry import LookMLFieldRegistry
from recidiviz.looker.lookml_view_field import DimensionLookMLViewField
from recidiviz.looker.lookml_view_field_parameter import LookMLFieldParameter


class TestLookMLFieldRegistry(unittest.TestCase):
    """Tests for LookMLFieldRegistry."""

    def setUp(self) -> None:
        self.default_field = DimensionLookMLViewField(
            field_name="default_field", parameters=[]
        )
        self.registry = LookMLFieldRegistry(default_fields=[self.default_field])

    def test_register_and_get_fields(self) -> None:
        table_id = "test_table"
        field1 = DimensionLookMLViewField(field_name="field1", parameters=[])
        field2 = DimensionLookMLViewField(field_name="field2", parameters=[])
        self.registry.register(table_id, [field1, field2])

        fields = self.registry.get(table_id)
        self.assertIn(field1, fields)
        self.assertIn(field2, fields)
        self.assertIn(self.default_field, fields)

    def test_get_fields_with_no_registration(self) -> None:
        table_id = "non_existent_table"
        fields = self.registry.get(table_id)
        self.assertIn(self.default_field, fields)

    def test_get_fields_override_default(self) -> None:
        table_id = "test_table"
        conflicting_field = DimensionLookMLViewField(
            field_name="default_field",
            parameters=[LookMLFieldParameter.sql("SELECT 1")],
        )
        self.registry.register(table_id, [conflicting_field])

        fields = self.registry.get(table_id)
        self.assertIn(conflicting_field, fields)
        self.assertNotIn(self.default_field, fields)
