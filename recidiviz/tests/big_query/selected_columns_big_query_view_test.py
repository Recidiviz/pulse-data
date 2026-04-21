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
"""Tests for SelectedColumnsBigQueryView and SelectedColumnsBigQueryViewBuilder."""
import unittest

from recidiviz.big_query.big_query_view_column import (
    COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
    Date,
    Integer,
    String,
)
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.utils.metadata import local_project_id_override

_COLUMNS = [
    String(
        name="state_code",
        description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
        mode="NULLABLE",
    ),
    Integer(
        name="person_id",
        description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
        mode="NULLABLE",
    ),
    Date(
        name="start_date",
        description=COLUMN_UNDOCUMENTED_PLACEHOLDER_TEXT,
        mode="NULLABLE",
    ),
]

_QUERY_TEMPLATE = "SELECT {columns} FROM `{project_id}.some_dataset.some_table`"


class TestSelectedColumnsBigQueryViewBuilder(unittest.TestCase):
    """Tests for SelectedColumnsBigQueryViewBuilder."""

    def setUp(self) -> None:
        self.builder = SelectedColumnsBigQueryViewBuilder(
            dataset_id="test_dataset",
            view_id="test_view",
            view_query_template=_QUERY_TEMPLATE,
            columns=_COLUMNS,
        )

    def test_columns_are_bigquery_view_columns(self) -> None:
        self.assertEqual(self.builder.columns, _COLUMNS)

    def test_column_names_substituted_into_query_template(self) -> None:
        with local_project_id_override("test-project"):
            view = self.builder.build()
        self.assertIn("state_code", view.view_query)
        self.assertIn("person_id", view.view_query)
        self.assertIn("start_date", view.view_query)
        # Should be comma-separated column names, not BigQueryViewColumn objects
        self.assertNotIn("BigQueryViewColumn", view.view_query)

    def test_schema_derived_from_columns(self) -> None:
        with local_project_id_override("test-project"):
            view = self.builder.build()
        self.assertIsNotNone(view.schema)
        assert view.schema is not None
        self.assertEqual(len(view.schema), len(_COLUMNS))
        schema_cols = [(c.name, c.field_type) for c in view.schema]
        self.assertEqual(
            schema_cols,
            [(c.name, c.field_type) for c in _COLUMNS],
        )

    def test_built_view_columns_match_builder_columns(self) -> None:
        with local_project_id_override("test-project"):
            view = self.builder.build()
        self.assertEqual(view.columns, _COLUMNS)
