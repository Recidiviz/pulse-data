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
"""ExtractionJobMetadata model class.

This is a pure data class that stores only IDs and metadata that map directly
to/from the extraction_jobs metadata table. The extractor configuration can be
looked up separately via get_extractor(extractor_id).
"""
import datetime
from typing import Any

import attr

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.dataset_config import (
    EXTRACTION_METADATA_DATASET_ID,
)


@attr.define
class ExtractionJobMetadata:
    """Information about a specific attempt to extract data from a collection of
    documents.

    This is a pure data model - all fields map directly to metadata table columns.
    Job submission logic lives in LLMClient.submit_extraction_job().
    """

    # ID that uniquely identifies a specific job, returned by submit_batch()
    job_id: str

    # When the job was submitted
    start_datetime: datetime.datetime

    # The extractor ID (e.g., US_IX_EMPLOYMENT)
    extractor_id: str

    # Version ID unique to the extractor config (includes prompt hash, model, etc.)
    extractor_version_id: str

    # The state this job is processing documents for
    state_code: StateCode

    # Number of documents submitted for extraction
    num_documents_submitted: int

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
            table_id="extraction_jobs",
        )

    def as_metadata_row(self) -> dict[str, Any]:
        """Row written to the extraction_jobs metadata table."""
        return {
            "job_id": self.job_id,
            "start_datetime": self.start_datetime,
            "extractor_id": self.extractor_id,
            "extractor_version_id": self.extractor_version_id,
            "state_code": self.state_code.value,
            "num_documents_submitted": self.num_documents_submitted,
        }

    @classmethod
    def from_metadata_row(cls, row: dict[str, Any]) -> "ExtractionJobMetadata":
        """Reconstructs an ExtractionJob from a metadata table row."""
        return cls(
            job_id=row["job_id"],
            start_datetime=row["start_datetime"],
            extractor_id=row["extractor_id"],
            extractor_version_id=row["extractor_version_id"],
            state_code=StateCode(row["state_code"]),
            num_documents_submitted=row.get("num_documents_submitted", 0),
        )
