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
"""Tests for lookml dashboard builder"""
import unittest
from unittest.mock import MagicMock

from recidiviz.looker.lookml_dashboard import LookMLDashboard
from recidiviz.looker.lookml_dashboard_builder import (
    LookMLDashboardElementMetadata,
    LookMLDashboardElementsProvider,
    SingleExploreLookMLDashboardBuilder,
)
from recidiviz.looker.lookml_dashboard_element import LookMLDashboardElement
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import (
    DimensionLookMLViewField,
    TimeDimensionGroupLookMLViewField,
)


class TestDashboardElementMetadata(unittest.TestCase):
    """Tests for LookMLDashboardElementMetadata."""

    def test_from_view(self) -> None:
        view = LookMLView(
            view_name="test_view",
            fields=[
                DimensionLookMLViewField(field_name="dimension_field", parameters=[]),
                TimeDimensionGroupLookMLViewField.for_date_column(
                    column_name="dimension_group_field"
                ),
            ],
        )
        metadata = LookMLDashboardElementMetadata.from_view(view)
        self.assertEqual(metadata.name, "Test View")
        self.assertEqual(
            metadata.fields,
            ["test_view.dimension_field", "test_view.dimension_group_field_date"],
        )
        self.assertEqual(len(metadata.sort_fields), 1)
        self.assertEqual(
            metadata.sort_fields[0].field, "test_view.dimension_group_field_date"
        )


class TestLookMLDashboardBuilder(unittest.TestCase):
    """Tests for LookMLDashboardBuilder."""

    def setUp(self) -> None:
        self.mock_provider = MagicMock(spec=LookMLDashboardElementsProvider)
        self.mock_provider.build_dashboard_elements.return_value = [
            LookMLDashboardElement.for_table_chart(
                name="Test Element", explore="test_explore"
            )
        ]
        self.builder = SingleExploreLookMLDashboardBuilder(
            dashboard_name="test_dashboard",
            dashboard_title="Test Dashboard",
            explore_name="test_explore",
            filter_fields=["test_view.field1"],
            element_provider=self.mock_provider,
        )

    def test_build(self) -> None:
        dashboard = self.builder.build()
        self.assertIsInstance(dashboard, LookMLDashboard)
        self.assertEqual(dashboard.dashboard_name, "test_dashboard")
        self.assertEqual(len(dashboard.filters), 1)
        self.assertEqual(len(dashboard.elements), 1)
