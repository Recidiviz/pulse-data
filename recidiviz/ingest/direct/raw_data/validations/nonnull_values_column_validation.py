# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Validation to check if a column has only null values"""
from typing import Any, Dict, List

import attr

from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_types import (
    RawDataTableImportBlockingValidation,
    RawDataTableImportBlockingValidationFailure,
    RawDataTableImportBlockingValidationType,
)
from recidiviz.utils.string import StrictStringFormatter

NONNULL_VALUES_CHECK_TEMPLATE = """
SELECT *
FROM {project_id}.{dataset_id}.{table_id}
WHERE {column_name} IS NOT NULL
LIMIT 1
"""


@attr.define
class NonNullValuesColumnValidation(RawDataTableImportBlockingValidation):
    """Validation to check if a column has only null values, runs on all columns unless explicitly exempt."""

    column_name: str
    validation_type: RawDataTableImportBlockingValidationType = (
        RawDataTableImportBlockingValidationType.NONNULL_VALUES
    )

    def build_query(self) -> str:
        return StrictStringFormatter().format(
            NONNULL_VALUES_CHECK_TEMPLATE,
            project_id=self.project_id,
            dataset_id=self.temp_table_address.dataset_id,
            table_id=self.temp_table_address.table_id,
            column_name=self.column_name,
        )

    def get_error_from_results(
        self, results: List[Dict[str, Any]]
    ) -> RawDataTableImportBlockingValidationFailure | None:
        if results:
            # Found at least one nonnull value
            return None
        # Found only null values
        return RawDataTableImportBlockingValidationFailure(
            validation_type=self.validation_type,
            error_msg=f"Found column [{self.column_name}] on raw file [{self.file_tag}] with only null values."
            f"\nValidation query: {self.query}",
        )
