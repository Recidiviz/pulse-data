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
"""ExtractionJobSubmittedDocumentMetadata model class."""
import datetime
from typing import Any

import attr

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.document_provider import (
    GCSDocument,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.dataset_config import (
    EXTRACTION_METADATA_DATASET_ID,
)


@attr.define
class ExtractionJobSubmittedDocumentMetadata:
    """Model for rows in extraction_job_submitted_documents table.

    Represents a document that was submitted as part of an extraction job.
    The job_index preserves the order of submission, which is needed for
    positional correlation with batch results (e.g., Vertex AI batch results
    don't include custom_id).
    """

    job_id: str
    document_id: str
    submitted_datetime: datetime.datetime
    job_index: int  # Position in submission order (0-indexed)
    gcs_uri: str
    mime_type: str

    @classmethod
    def metadata_table_address(
        cls, sandbox_dataset_prefix: str | None
    ) -> BigQueryAddress:
        dataset_id = EXTRACTION_METADATA_DATASET_ID
        if sandbox_dataset_prefix:
            dataset_id = BigQueryAddressOverrides.format_sandbox_dataset(
                sandbox_dataset_prefix, dataset_id
            )
        return BigQueryAddress(
            dataset_id=dataset_id,
            table_id="extraction_job_submitted_documents",
        )

    def as_metadata_row(self) -> dict[str, Any]:
        """Row written to the extraction_job_submitted_documents metadata table."""
        return {
            "job_id": self.job_id,
            "document_id": self.document_id,
            "submitted_datetime": self.submitted_datetime,
            "job_index": self.job_index,
            "gcs_uri": self.gcs_uri,
            "mime_type": self.mime_type,
        }

    @classmethod
    def from_metadata_row(
        cls, row: dict[str, Any]
    ) -> "ExtractionJobSubmittedDocumentMetadata":
        """Reconstructs an ExtractionJobSubmittedDocument from a metadata table row."""
        return cls(
            job_id=row["job_id"],
            document_id=row["document_id"],
            submitted_datetime=row["submitted_datetime"],
            job_index=row["job_index"],
            gcs_uri=row["gcs_uri"],
            mime_type=row["mime_type"],
        )

    def to_gcs_document(self) -> GCSDocument:
        """Convert to GCSDocument for request reconstruction."""
        return GCSDocument(
            document_id=self.document_id,
            gcs_file_path=GcsfsFilePath.from_absolute_path(self.gcs_uri),
            mime_type=self.mime_type,
        )
