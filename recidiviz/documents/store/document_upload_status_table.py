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
"""Schema definition for the document_upload_status table."""
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.enums import SqlTypeNames


class DocumentUploadStatusTable:
    """Defines the schema for the document_upload_status table, which tracks
    the status of document uploads to the document store."""

    table_id = "document_upload_status"

    description = "Tracks the status of document uploads to the document store."

    @staticmethod
    def schema() -> list[SchemaField]:
        return [
            SchemaField(
                name="document_contents_id",
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="SHA256 hash of state_code | document_text",
            ),
            SchemaField(
                name="job_id",
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="The job run id that attempted the upload",
            ),
            SchemaField(
                name="upload_datetime",
                field_type=SqlTypeNames.TIMESTAMP.value,
                mode="REQUIRED",
                description="When the upload was attempted",
            ),
            SchemaField(
                name="status",
                field_type=SqlTypeNames.STRING.value,
                mode="REQUIRED",
                description="SUCCESS or FAILURE",
            ),
            SchemaField(
                name="error_message",
                field_type=SqlTypeNames.STRING.value,
                mode="NULLABLE",
                description="Error details if FAILURE, NULL if SUCCESS",
            ),
        ]
