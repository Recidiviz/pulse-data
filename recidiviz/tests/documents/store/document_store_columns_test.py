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
"""Tests for document_store_columns.py."""
import inspect
import unittest

from recidiviz.documents.store import document_store_columns
from recidiviz.documents.store.document_store_columns import (
    get_document_store_column_schema,
)


class TestGetDocumentStoreColumnSchema(unittest.TestCase):
    def test_all_column_name_constants_have_schema(self) -> None:
        column_name_constants = {
            name: value
            for name, value in inspect.getmembers(document_store_columns)
            if name.endswith("_COLUMN_NAME") and isinstance(value, str)
        }

        self.assertTrue(
            column_name_constants,
            "Expected at least one _COLUMN_NAME constant in the module",
        )

        for symbol_name, column_name in column_name_constants.items():
            with self.subTest(symbol=symbol_name):
                schema = get_document_store_column_schema(column_name)
                self.assertEqual(schema.name, column_name)
