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
"""Tests for SourceTableConfig"""
import unittest
from unittest.mock import MagicMock, patch

from google.cloud.bigquery import SchemaField, Table

from recidiviz.source_tables.source_table_config import (
    DEFAULT_COLUMN_DESCRIPTION,
    DEFAULT_SOURCE_TABLE_DESCRIPTION,
    SourceTableConfig,
)


@patch("recidiviz.utils.metadata.project_id", MagicMock(return_value="recidiviz-456"))
class TestSourceTableConfigFromTable(unittest.TestCase):
    """Tests for SourceTableConfig.from_table"""

    def _make_table(
        self,
        dataset_id: str,
        table_id: str,
        schema: list[SchemaField],
        description: str | None = None,
    ) -> Table:
        table = Table(f"recidiviz-456.{dataset_id}.{table_id}")
        table.schema = schema
        if description is not None:
            table.description = description
        return table

    def test_from_table_fills_placeholder_column_description(self) -> None:
        """Columns without descriptions get a placeholder description."""
        table = self._make_table(
            dataset_id="my_dataset",
            table_id="my_table",
            schema=[
                SchemaField("id", "INTEGER", mode="REQUIRED"),
                SchemaField("name", "STRING"),
            ],
        )

        config = SourceTableConfig.from_table(table)

        for field in config.schema_fields:
            self.assertEqual(field.description, DEFAULT_COLUMN_DESCRIPTION)

    def test_from_table_preserves_existing_column_description(self) -> None:
        """Columns that already have descriptions keep them."""
        table = self._make_table(
            dataset_id="my_dataset",
            table_id="my_table",
            schema=[
                SchemaField(
                    "id", "INTEGER", mode="REQUIRED", description="The primary key"
                ),
                SchemaField("name", "STRING", description="The user's display name"),
            ],
        )

        config = SourceTableConfig.from_table(table)

        self.assertEqual(config.schema_fields[0].description, "The primary key")
        self.assertEqual(config.schema_fields[1].description, "The user's display name")

    def test_from_table_mixed_descriptions(self) -> None:
        """Some columns have descriptions, some don't."""
        table = self._make_table(
            dataset_id="my_dataset",
            table_id="my_table",
            schema=[
                SchemaField(
                    "id", "INTEGER", mode="REQUIRED", description="The primary key"
                ),
                SchemaField("name", "STRING"),
                SchemaField("email", "STRING", description="User email address"),
                SchemaField("created_at", "TIMESTAMP"),
            ],
        )

        config = SourceTableConfig.from_table(table)

        self.assertEqual(config.schema_fields[0].description, "The primary key")
        self.assertEqual(
            config.schema_fields[1].description, DEFAULT_COLUMN_DESCRIPTION
        )
        self.assertEqual(config.schema_fields[2].description, "User email address")
        self.assertEqual(
            config.schema_fields[3].description, DEFAULT_COLUMN_DESCRIPTION
        )

    def test_from_table_fills_placeholder_table_description(self) -> None:
        """Tables without descriptions get a placeholder description."""
        table = self._make_table(
            dataset_id="my_dataset",
            table_id="my_table",
            schema=[SchemaField("id", "INTEGER")],
        )

        config = SourceTableConfig.from_table(table)

        self.assertEqual(config.description, DEFAULT_SOURCE_TABLE_DESCRIPTION)

    def test_from_table_preserves_existing_table_description(self) -> None:
        """Tables with descriptions keep them."""
        table = self._make_table(
            dataset_id="my_dataset",
            table_id="my_table",
            schema=[SchemaField("id", "INTEGER")],
            description="My important table",
        )

        config = SourceTableConfig.from_table(table)

        self.assertEqual(config.description, "My important table")

    def test_from_table_preserves_field_type_and_mode(self) -> None:
        """Field type and mode are preserved when creating new SchemaField objects."""
        table = self._make_table(
            dataset_id="my_dataset",
            table_id="my_table",
            schema=[
                SchemaField("id", "INTEGER", mode="REQUIRED"),
                SchemaField("name", "STRING", mode="NULLABLE"),
                SchemaField("tags", "STRING", mode="REPEATED"),
            ],
        )

        config = SourceTableConfig.from_table(table)

        self.assertEqual(config.schema_fields[0].field_type, "INTEGER")
        self.assertEqual(config.schema_fields[0].mode, "REQUIRED")
        self.assertEqual(config.schema_fields[1].field_type, "STRING")
        self.assertEqual(config.schema_fields[1].mode, "NULLABLE")
        self.assertEqual(config.schema_fields[2].field_type, "STRING")
        self.assertEqual(config.schema_fields[2].mode, "REPEATED")

    def test_from_table_preserves_nested_fields(self) -> None:
        """Nested (RECORD/STRUCT) sub-fields are preserved."""
        sub_fields = (
            SchemaField("street", "STRING"),
            SchemaField("city", "STRING"),
        )
        table = self._make_table(
            dataset_id="my_dataset",
            table_id="my_table",
            schema=[
                SchemaField("address", "RECORD", fields=sub_fields),
            ],
        )

        config = SourceTableConfig.from_table(table)

        self.assertEqual(len(config.schema_fields), 1)
        self.assertEqual(config.schema_fields[0].name, "address")
        self.assertEqual(len(config.schema_fields[0].fields), 2)
        self.assertEqual(config.schema_fields[0].fields[0].name, "street")
        self.assertEqual(config.schema_fields[0].fields[1].name, "city")
