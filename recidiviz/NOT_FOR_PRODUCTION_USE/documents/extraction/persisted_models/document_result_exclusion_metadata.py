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
"""DocumentResultExclusionMetadata model class."""
import datetime
from typing import Any

import attr

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.extraction_exclusion_type import (
    ExtractionExclusionType,
)


@attr.define
class DocumentResultExclusionMetadata:
    """Captures why a document was excluded from validated extraction output.

    One row per failure reason per document per extraction attempt. A single
    document can have multiple failure rows (e.g., both NOT_RELEVANT and
    LOW_CONFIDENCE).
    """

    job_id: str
    document_id: str
    extractor_id: str
    extractor_version_id: str
    extraction_datetime: datetime.datetime
    exclusion_type: ExtractionExclusionType
    exclusion_details_json: str | None

    EXCLUSIONS_DATASET_ID = "document_extraction_results__exclusions"

    @staticmethod
    def table_id(state_code: StateCode, collection_name: str) -> str:
        return f"{state_code.value.lower()}_{collection_name.lower()}"

    @classmethod
    def table_address(
        cls,
        state_code: StateCode,
        collection_name: str,
        sandbox_dataset_prefix: str | None,
    ) -> BigQueryAddress:
        dataset_id = cls.EXCLUSIONS_DATASET_ID
        if sandbox_dataset_prefix:
            dataset_id = BigQueryAddressOverrides.format_sandbox_dataset(
                sandbox_dataset_prefix, dataset_id
            )
        return BigQueryAddress(
            dataset_id=dataset_id,
            table_id=cls.table_id(state_code, collection_name),
        )

    def as_metadata_row(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "document_id": self.document_id,
            "extractor_id": self.extractor_id,
            "extractor_version_id": self.extractor_version_id,
            "extraction_datetime": self.extraction_datetime,
            "exclusion_type": self.exclusion_type.value,
            "exclusion_details_json": self.exclusion_details_json,
        }
