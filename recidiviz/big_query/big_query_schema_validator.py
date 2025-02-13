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
"""Validates BigQuery schema field names against a given schema map."""
from google.cloud import bigquery


class BigQuerySchemaValidator:
    """Validates schema field names against a given BigQuery schema map."""

    def __init__(self, schema_map: dict[str, list[bigquery.SchemaField]]):
        self.schema_map: dict[str, set[str]] = {
            table: {field.name for field in fields}
            for table, fields in schema_map.items()
        }

    def validate_table_contains_field(self, table: str, field: str) -> str:
        """
        Ensures a field exists in the table schema. Returns the field name if it exists,
        otherwise raises an AttributeError.
        """
        if table not in self.schema_map:
            raise AttributeError(f"Table [{table}] not found in schema.")
        if field not in self.schema_map[table]:
            raise AttributeError(
                f"Field [{field}] not found in [{table}]. Available fields: {self.schema_map[table]}"
            )
        return field

    def validate_table_contains_fields(
        self, table: str, fields: list[str]
    ) -> list[str]:
        """
        Ensures multiple fields exist in the table schema. Returns the list of fields if they exist,
        otherwise raises an AttributeError
        """
        errors = []
        for field in fields:
            try:
                self.validate_table_contains_field(table, field)
            except AttributeError as e:
                errors.append(str(e))
        if errors:
            raise AttributeError("\n".join(errors))

        return fields
