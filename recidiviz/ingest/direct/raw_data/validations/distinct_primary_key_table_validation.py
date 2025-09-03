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
from typing import Any

from attrs import define

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataImportBlockingValidationFailure,
    RawDataTableImportBlockingValidation,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_type import (
    RawDataImportBlockingValidationType,
)
from recidiviz.utils.string import StrictStringFormatter

DISTINCT_PRIMARY_KEY_QUERY_TEMPLATE = """
SELECT {select_lower_primary_keys_clause}
FROM `{project_id}.{dataset_id}.{table_id}`
GROUP BY {group_by_clause}
HAVING COUNT(*) > 1
LIMIT 1 
"""


@define
class DistinctPrimaryKeyTableValidation(RawDataTableImportBlockingValidation):
    """Validation that checks that all primary keys in a file are distinct (case insensitive)."""

    primary_key_cols: list[str]

    @classmethod
    def create_table_validation(
        cls,
        *,
        state_code: StateCode,
        file_tag: str,
        project_id: str,
        temp_table_address: BigQueryAddress,
        primary_key_cols: list[str],
    ) -> "DistinctPrimaryKeyTableValidation":
        return cls(
            project_id=project_id,
            temp_table_address=temp_table_address,
            file_tag=file_tag,
            primary_key_cols=primary_key_cols,
            state_code=state_code,
        )

    @staticmethod
    def validation_type() -> RawDataImportBlockingValidationType:
        return RawDataImportBlockingValidationType.DISTINCT_PRIMARY_KEYS

    @staticmethod
    def validation_applies_to_table(file_config: DirectIngestRawFileConfig) -> bool:
        return len(file_config.primary_key_cols) > 0

    def build_query(self) -> str:
        return StrictStringFormatter().format(
            DISTINCT_PRIMARY_KEY_QUERY_TEMPLATE,
            project_id=self.project_id,
            dataset_id=self.temp_table_address.dataset_id,
            table_id=self.temp_table_address.table_id,
            select_lower_primary_keys_clause=", ".join(
                f"LOWER({col}) as lower_{col}" for col in self.primary_key_cols
            ),
            group_by_clause=", ".join(f"lower_{col}" for col in self.primary_key_cols),
        )

    def get_error_from_results(
        self, results: list[dict[str, Any]]
    ) -> RawDataImportBlockingValidationFailure | None:
        if not results:
            return None

        return RawDataImportBlockingValidationFailure(
            validation_type=self.validation_type(),
            validation_query=self.build_query(),
            error_msg=(
                f"Found duplicate primary keys for raw file [{self.file_tag}]"
                f"\nPrimary key columns: {self.primary_key_cols}"
            ),
        )
