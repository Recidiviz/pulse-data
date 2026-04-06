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
"""Column name constants and schema definitions for document store metadata tables."""
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.enums import SqlTypeNames

PERSON_ID_COLUMN_NAME = "person_id"
PERSON_EXTERNAL_ID_COLUMN_NAME = "person_external_id"
PERSON_EXTERNAL_ID_TYPE_COLUMN_NAME = "person_external_id_type"
STAFF_ID_COLUMN_NAME = "staff_id"
STAFF_EXTERNAL_ID_COLUMN_NAME = "staff_external_id"
STAFF_EXTERNAL_ID_TYPE_COLUMN_NAME = "staff_external_id_type"
DOCUMENT_CONTENTS_ID_COLUMN_NAME = "document_contents_id"
DOCUMENT_TEXT_COLUMN_NAME = "document_text"
DOCUMENT_UPDATE_DATETIME_COLUMN_NAME = "document_update_datetime"
UPLOAD_DATETIME_COLUMN_NAME = "upload_datetime"

_ALL_COLUMN_DEFINITIONS = [
    SchemaField(
        name=PERSON_ID_COLUMN_NAME,
        field_type=SqlTypeNames.INT64.value,
        mode="REQUIRED",
    ),
    SchemaField(
        name=PERSON_EXTERNAL_ID_COLUMN_NAME,
        field_type=SqlTypeNames.STRING.value,
        mode="REQUIRED",
    ),
    SchemaField(
        name=PERSON_EXTERNAL_ID_TYPE_COLUMN_NAME,
        field_type=SqlTypeNames.STRING.value,
        mode="REQUIRED",
    ),
    SchemaField(
        name=STAFF_ID_COLUMN_NAME,
        field_type=SqlTypeNames.INT64.value,
        mode="REQUIRED",
    ),
    SchemaField(
        name=STAFF_EXTERNAL_ID_COLUMN_NAME,
        field_type=SqlTypeNames.STRING.value,
        mode="REQUIRED",
    ),
    SchemaField(
        name=STAFF_EXTERNAL_ID_TYPE_COLUMN_NAME,
        field_type=SqlTypeNames.STRING.value,
        mode="REQUIRED",
    ),
    SchemaField(
        name=DOCUMENT_CONTENTS_ID_COLUMN_NAME,
        field_type=SqlTypeNames.STRING.value,
        mode="NULLABLE",
    ),
    SchemaField(
        name=DOCUMENT_TEXT_COLUMN_NAME,
        field_type=SqlTypeNames.STRING.value,
        mode="NULLABLE",
    ),
    SchemaField(
        name=DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
        field_type=SqlTypeNames.TIMESTAMP.value,
        mode="REQUIRED",
    ),
    SchemaField(
        name=UPLOAD_DATETIME_COLUMN_NAME,
        field_type=SqlTypeNames.TIMESTAMP.value,
        mode="REQUIRED",
    ),
]

_COLUMNS_BY_NAME: dict[str, SchemaField] = {c.name: c for c in _ALL_COLUMN_DEFINITIONS}


# TODO(#68778) Write a test that looks for all symbols ending in _COLUMN_NAME
# and verifies this function returns a valid result for each.
def get_document_store_column_schema(column_name: str) -> SchemaField:
    """Return schema for a standard column that may exist in a document store
    collection metadata table.
    """
    if column_name not in _COLUMNS_BY_NAME:
        raise ValueError(
            f"Unknown document store column: [{column_name}]. "
            f"Known columns: {sorted(_COLUMNS_BY_NAME.keys())}"
        )
    return _COLUMNS_BY_NAME[column_name]
