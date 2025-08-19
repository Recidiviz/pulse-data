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
import datetime
from typing import Any, Dict, List

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.raw_data.validations.import_blocking_validations_query_runner import (
    RawDataImportBlockingValidationQueryRunner,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataColumnImportBlockingValidation,
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
)
from recidiviz.utils.string import StrictStringFormatter

NONNULL_VALUES_CHECK_TEMPLATE = """
SELECT {column_name}
FROM {project_id}.{dataset_id}.{table_id}
WHERE {column_name} IS NOT NULL
LIMIT 1
"""


@attr.define
class NonNullValuesColumnValidation(RawDataColumnImportBlockingValidation):
    """Validation to check if a column has only null values, runs on all columns unless explicitly exempt."""

    @classmethod
    def create_column_validation(
        cls,
        *,
        file_tag: str,
        project_id: str,
        state_code: StateCode,
        temp_table_address: BigQueryAddress,
        file_upload_datetime: datetime.datetime,
        column: RawTableColumnInfo,
        bq_client: BigQueryClient,
    ) -> "RawDataColumnImportBlockingValidation":
        """Factory method to create a column validation."""
        if not (temp_table_col_name := column.name_at_datetime(file_upload_datetime)):
            raise ValueError(
                f"Column [{column.name}] does not exist at datetime [{file_upload_datetime}]"
            )

        return cls(
            project_id=project_id,
            temp_table_address=temp_table_address,
            column_name=temp_table_col_name,
            file_tag=file_tag,
            state_code=state_code,
            query_runner=RawDataImportBlockingValidationQueryRunner(
                bq_client=bq_client
            ),
        )

    @staticmethod
    def validation_type() -> RawDataImportBlockingValidationType:
        return RawDataImportBlockingValidationType.NONNULL_VALUES

    @staticmethod
    def validation_applies_to_column(
        column: RawTableColumnInfo, raw_file_config: DirectIngestRawFileConfig
    ) -> bool:
        return (
            raw_file_config.always_historical_export
            or column.name in raw_file_config.primary_key_cols
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
    ) -> RawDataImportBlockingValidationFailure | None:
        if results:
            # Found at least one nonnull value
            return None
        # Found only null values
        return RawDataImportBlockingValidationFailure(
            validation_type=self.validation_type(),
            validation_query=self.query,
            error_msg=f"Found column [{self.column_name}] on raw file [{self.file_tag}] with only null values.",
        )

    def run_validation(
        self,
    ) -> RawDataImportBlockingValidationFailure | None:
        """Runs the validation query and returns an error if any non-null values are found."""
        results = self.query_runner.run_query(self.query)
        return self.get_error_from_results(results)
