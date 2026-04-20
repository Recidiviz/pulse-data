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

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    document_store_metadata_dataset_for_region,
)

DOCUMENT_UPLOAD_SUCCESS = "SUCCESS"
DOCUMENT_UPLOAD_FAILURE = "FAILURE"


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
                description=f"{DOCUMENT_UPLOAD_SUCCESS} or {DOCUMENT_UPLOAD_FAILURE}",
            ),
            SchemaField(
                name="error_message",
                field_type=SqlTypeNames.STRING.value,
                mode="NULLABLE",
                description=f"Error details if {DOCUMENT_UPLOAD_FAILURE}, NULL if {DOCUMENT_UPLOAD_SUCCESS}",
            ),
        ]

    @classmethod
    def get_table_address(
        cls, project_id: str, state_code: StateCode
    ) -> ProjectSpecificBigQueryAddress:
        """Returns the BigQuery address for the document_upload_status table."""
        return ProjectSpecificBigQueryAddress(
            project_id=project_id,
            dataset_id=document_store_metadata_dataset_for_region(state_code),
            table_id=cls.table_id,
        )
