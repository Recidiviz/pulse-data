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
"""ExtractionJobResultMetadata model class."""
import datetime
from typing import Any

import attr

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.dataset_config import (
    EXTRACTION_METADATA_DATASET_ID,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_job_error_type import (
    ExtractionJobErrorType,
)


@attr.define
class ExtractionJobResultMetadata:
    """High level metadata about the results of a single extraction job."""

    # ID that uniquely identifies a specific job
    job_id: str

    # The time we finished processing results for this job
    result_datetime: datetime.datetime

    # Summary info about how many successes / failures in this job.
    num_documents_originally_submitted: int
    num_successful_extractions: int
    num_failed_extractions: int

    error_type: ExtractionJobErrorType | None
    error_message: str | None

    def __attrs_post_init__(self) -> None:
        if self.num_successful_extractions < 0 or self.num_failed_extractions < 0:
            raise ValueError(
                "num_successful_extractions and num_failed_extractions must be non-negative"
            )
        if (
            self.num_successful_extractions + self.num_failed_extractions
            > self.num_documents_originally_submitted
        ):
            raise ValueError(
                "num_successful_extractions + num_failed_extractions must not exceed "
                "num_documents_originally_submitted"
            )
        if self.error_type is None and (
            self.num_successful_extractions + self.num_failed_extractions
            != self.num_documents_originally_submitted
        ):
            raise ValueError(
                "When there is no job-level error, num_successful_extractions + "
                "num_failed_extractions must equal num_documents_originally_submitted"
            )
        if (self.error_type is None) != (self.error_message is None):
            raise ValueError(
                "error_type and error_message must both be set or both be None"
            )

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
            table_id="extraction_job_results",
        )

    def as_metadata_row(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "result_datetime": self.result_datetime,
            "num_documents_originally_submitted": self.num_documents_originally_submitted,
            "num_successful_extractions": self.num_successful_extractions,
            "num_failed_extractions": self.num_failed_extractions,
            "error_type": self.error_type.value if self.error_type else None,
            "error_message": self.error_message,
        }
