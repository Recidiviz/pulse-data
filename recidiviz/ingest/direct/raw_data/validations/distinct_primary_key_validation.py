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
"""Validation to assert that in a given file that has defined primary keys, all case-insensitive primary keys are distinct"""
from typing import Any, ClassVar

from attrs import define

from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    BaseRawDataImportBlockingValidation,
    RawDataColumnValidationMixin,
    RawDataImportBlockingValidationContext,
    RawDataImportBlockingValidationFailure,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_type import (
    RawDataImportBlockingValidationType,
)
from recidiviz.utils.string import StrictStringFormatter

DISTINCT_PRIMARY_KEY_QUERY_TEMPLATE = """
SELECT {primary_key_cols}
FROM `{project_id}.{dataset_id}.{table_id}`
GROUP BY {primary_key_cols}
HAVING COUNT(*) > 1
LIMIT 1 
"""


@define
class DistinctPrimaryKeyValidation(
    BaseRawDataImportBlockingValidation, RawDataColumnValidationMixin
):
    """Validation that checks that all primary keys in a file are distinct (case insensitive)."""

    VALIDATION_TYPE: ClassVar[
        RawDataImportBlockingValidationType
    ] = RawDataImportBlockingValidationType.DISTINCT_PRIMARY_KEYS
    primary_key_cols: list[str]

    @classmethod
    def create_validation(
        cls, context: RawDataImportBlockingValidationContext
    ) -> "DistinctPrimaryKeyValidation":
        return cls(
            project_id=context.project_id,
            temp_table_address=context.temp_table_address,
            file_tag=context.file_tag,
            primary_key_cols=context.raw_file_config.primary_key_cols,
            state_code=context.state_code,
        )

    @classmethod
    def validation_applies_to_file(
        cls,
        context: RawDataImportBlockingValidationContext,
    ) -> bool:
        return len(
            context.raw_file_config.primary_key_cols
        ) > 0 and not context.raw_file_config.file_is_exempt_from_validation(
            cls.VALIDATION_TYPE
        )

    def build_query(self) -> str:
        return StrictStringFormatter().format(
            DISTINCT_PRIMARY_KEY_QUERY_TEMPLATE,
            project_id=self.project_id,
            dataset_id=self.temp_table_address.dataset_id,
            table_id=self.temp_table_address.table_id,
            primary_key_cols=", ".join(self.primary_key_cols),
        )

    def get_error_from_results(
        self, results: list[dict[str, Any]]
    ) -> RawDataImportBlockingValidationFailure | None:
        if not results:
            return None

        return RawDataImportBlockingValidationFailure(
            validation_type=self.VALIDATION_TYPE,
            validation_query=self.build_query(),
            error_msg=(
                f"Found duplicate primary keys for raw file [{self.file_tag}]"
                f"\nPrimary key columns: {self.primary_key_cols}"
            ),
        )
