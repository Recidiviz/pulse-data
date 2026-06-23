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
ROW_CREATE_DATETIME_COLUMN_NAME = "row_create_datetime"
DOCUMENT_LENGTH_BYTES_COLUMN_NAME = "document_length_bytes"
DOCUMENT_UPLOAD_BATCH_NUM_COLUMN_NAME = "document_upload_batch_number"

_ALL_COLUMN_DEFINITIONS = [
    SchemaField(
        name=PERSON_ID_COLUMN_NAME,
        field_type=SqlTypeNames.INT64.value,
        mode="REQUIRED",
        description="Recidiviz internal person_id for the root entity",
    ),
    SchemaField(
        name=PERSON_EXTERNAL_ID_COLUMN_NAME,
        field_type=SqlTypeNames.STRING.value,
        mode="REQUIRED",
        description="External ID for the person associated with this document",
    ),
    SchemaField(
        name=PERSON_EXTERNAL_ID_TYPE_COLUMN_NAME,
        field_type=SqlTypeNames.STRING.value,
        mode="REQUIRED",
        description="Type of the person external ID",
    ),
    SchemaField(
        name=STAFF_ID_COLUMN_NAME,
        field_type=SqlTypeNames.INT64.value,
        mode="REQUIRED",
        description="Recidiviz internal staff_id for the root entity",
    ),
    SchemaField(
        name=STAFF_EXTERNAL_ID_COLUMN_NAME,
        field_type=SqlTypeNames.STRING.value,
        mode="REQUIRED",
        description="External ID for the staff member associated with this document",
    ),
    SchemaField(
        name=STAFF_EXTERNAL_ID_TYPE_COLUMN_NAME,
        field_type=SqlTypeNames.STRING.value,
        mode="REQUIRED",
        description="Type of the staff external ID",
    ),
    SchemaField(
        name=DOCUMENT_CONTENTS_ID_COLUMN_NAME,
        field_type=SqlTypeNames.STRING.value,
        mode="NULLABLE",
        description="SHA256 hash of state_code | document_text, used to identify the document contents in GCS. NULL if the document has been deleted in the source data.",
    ),
    SchemaField(
        name=DOCUMENT_TEXT_COLUMN_NAME,
        field_type=SqlTypeNames.STRING.value,
        mode="NULLABLE",
        description="The generated text content of the document.",
    ),
    SchemaField(
        name=DOCUMENT_UPDATE_DATETIME_COLUMN_NAME,
        field_type=SqlTypeNames.TIMESTAMP.value,
        mode="NULLABLE",
        description="Datetime from the source data indicating when the document was last updated. Null for deleted documents.",
    ),
    SchemaField(
        name=ROW_CREATE_DATETIME_COLUMN_NAME,
        field_type=SqlTypeNames.TIMESTAMP.value,
        mode="REQUIRED",
        description="Datetime when this metadata row was written by the document store process",
    ),
    SchemaField(
        name=DOCUMENT_LENGTH_BYTES_COLUMN_NAME,
        field_type=SqlTypeNames.INT64.value,
        mode="REQUIRED",
        description="Length of the document_text in bytes",
    ),
    SchemaField(
        name=DOCUMENT_UPLOAD_BATCH_NUM_COLUMN_NAME,
        field_type=SqlTypeNames.INT64.value,
        mode="REQUIRED",
        description="Batch number assigned to each document based on cumulative byte size",
    ),
]

_COLUMNS_BY_NAME: dict[str, SchemaField] = {c.name: c for c in _ALL_COLUMN_DEFINITIONS}


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
