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
"""DocumentExtractionResultMetadata model class."""
import datetime
from typing import Any

import attr

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extraction_error_type import (
    DocumentExtractionErrorType,
)


@attr.define
class DocumentExtractionResultMetadata:
    """Per-document extraction outcome. One row per document per extraction attempt."""

    job_id: str
    document_id: str
    extractor_id: str
    extractor_version_id: str
    extraction_datetime: datetime.datetime
    status: str
    result_json: str | None
    error_message: str | None
    error_type: DocumentExtractionErrorType | None

    def __attrs_post_init__(self) -> None:
        if self.status == "SUCCESS":
            if self.result_json is None:
                raise ValueError("result_json must be set when status is SUCCESS")
            if self.error_type is not None or self.error_message is not None:
                raise ValueError(
                    "error_type and error_message must be None when status is SUCCESS"
                )
        else:
            if self.result_json is not None:
                raise ValueError("result_json must be None when status is not SUCCESS")
            if self.error_type is None or self.error_message is None:
                raise ValueError(
                    "error_type and error_message must both be set when status is not SUCCESS"
                )

    RAW_DATASET_ID = "document_extraction_results__raw"

    @staticmethod
    def raw_table_id(state_code: StateCode, collection_name: str) -> str:
        """Returns the table ID for the per-extractor raw results table."""
        return f"{state_code.value.lower()}_{collection_name.lower()}"

    @classmethod
    def raw_table_address(
        cls,
        state_code: StateCode,
        collection_name: str,
        sandbox_dataset_prefix: str | None,
    ) -> BigQueryAddress:
        """Returns the BigQueryAddress for the per-extractor raw results table."""
        dataset_id = cls.RAW_DATASET_ID
        if sandbox_dataset_prefix:
            dataset_id = BigQueryAddressOverrides.format_sandbox_dataset(
                sandbox_dataset_prefix, dataset_id
            )
        return BigQueryAddress(
            dataset_id=dataset_id,
            table_id=cls.raw_table_id(state_code, collection_name),
        )

    def as_metadata_row(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "document_id": self.document_id,
            "extractor_id": self.extractor_id,
            "extractor_version_id": self.extractor_version_id,
            "extraction_datetime": self.extraction_datetime,
            "status": self.status,
            "result_json": self.result_json,
            "error_message": self.error_message,
            "error_type": self.error_type.value if self.error_type else None,
        }
