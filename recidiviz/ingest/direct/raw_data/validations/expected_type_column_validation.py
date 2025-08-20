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
"""Validation to check if a column has values that can't be cast to the expected type."""
from datetime import datetime
from typing import Any, Dict, List, Optional

import attr
from google.cloud.bigquery.enums import StandardSqlTypeNames as BigQueryFieldType

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataColumnImportBlockingValidation,
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
)
from recidiviz.utils.string import StrictStringFormatter

ERROR_MESSAGE_ROW_LIMIT = 10

EXPECTED_TYPE_CHECK_TEMPLATE = """
SELECT DISTINCT {column_name}
FROM {project_id}.{dataset_id}.{table_id}
WHERE {column_name} IS NOT NULL
    AND SAFE_CAST({column_name} AS {type}) IS NULL
    {null_values_filter}
LIMIT {error_message_limit}
"""

COLUMN_TYPE_TO_BIG_QUERY_TYPE = {
    RawTableColumnFieldType.INTEGER: BigQueryFieldType.INT64,
    RawTableColumnFieldType.DATETIME: BigQueryFieldType.STRING,
    RawTableColumnFieldType.STRING: BigQueryFieldType.STRING,
    RawTableColumnFieldType.PERSON_EXTERNAL_ID: BigQueryFieldType.STRING,
    RawTableColumnFieldType.STAFF_EXTERNAL_ID: BigQueryFieldType.STRING,
}


@attr.define
class ExpectedTypeColumnValidation(RawDataColumnImportBlockingValidation):
    """Validation that checks if a column has values that can't be cast to the expected type"""

    column_type: RawTableColumnFieldType
    null_values: Optional[List[str]]

    @classmethod
    def create_column_validation(
        cls,
        *,
        file_tag: str,
        project_id: str,
        state_code: StateCode,
        temp_table_address: BigQueryAddress,
        file_upload_datetime: datetime,
        column: RawTableColumnInfo,
    ) -> "ExpectedTypeColumnValidation":
        if not (temp_table_col_name := column.name_at_datetime(file_upload_datetime)):
            raise ValueError(
                f"Column [{column.name}] does not exist at datetime [{file_upload_datetime}]"
            )
        return cls(
            file_tag=file_tag,
            project_id=project_id,
            temp_table_address=temp_table_address,
            column_name=temp_table_col_name,
            column_type=column.field_type,
            null_values=(
                cls._escape_values_for_query(column.null_values)
                if column.null_values
                else None
            ),
            state_code=state_code,
        )

    @staticmethod
    def validation_type() -> RawDataImportBlockingValidationType:
        return RawDataImportBlockingValidationType.EXPECTED_TYPE

    @staticmethod
    def validation_applies_to_column(
        column: RawTableColumnInfo,
        _raw_file_config: DirectIngestRawFileConfig,
    ) -> bool:
        return (
            COLUMN_TYPE_TO_BIG_QUERY_TYPE.get(column.field_type)
            != BigQueryFieldType.STRING
        )

    def build_query(self) -> str:
        big_query_type = COLUMN_TYPE_TO_BIG_QUERY_TYPE.get(self.column_type)
        if big_query_type is None:
            raise ValueError(
                f"BigQuery type not found for column type {self.column_type.value}"
            )

        if big_query_type == BigQueryFieldType.STRING:
            raise ValueError(
                f"field_type [{self.column_type.value}] has BigQuery type {BigQueryFieldType.STRING.value}, "
                f"expected type validation should not be run on string columns, "
                "as all values can be cast to string."
                f"\nFile tag: [{self.file_tag}], Column: [{self.column_name}]"
            )

        return StrictStringFormatter().format(
            EXPECTED_TYPE_CHECK_TEMPLATE,
            project_id=self.project_id,
            dataset_id=self.temp_table_address.dataset_id,
            table_id=self.temp_table_address.table_id,
            column_name=self.column_name,
            type=big_query_type.value,
            null_values_filter=self._build_null_values_filter(
                self.column_name, self.null_values
            ),
            error_message_limit=ERROR_MESSAGE_ROW_LIMIT,
        )

    def get_error_from_results(
        self, results: List[Dict[str, Any]]
    ) -> RawDataImportBlockingValidationFailure | None:
        if results:
            non_matching_types = [result[self.column_name] for result in results]
            non_matching_prefix = (
                f"First [{len(non_matching_types)}] of many"
                if len(non_matching_types) == ERROR_MESSAGE_ROW_LIMIT
                else f"All [{len(non_matching_types)}]"
            )
            # At least one row found with a value that can't be cast to the expected type
            return RawDataImportBlockingValidationFailure(
                validation_type=self.validation_type(),
                validation_query=self.query,
                error_msg=(
                    f"Found column [{self.column_name}] on raw file [{self.file_tag}]"
                    f" not matching the field_type defined in its configuration YAML."
                    f"\nDefined type: [{self.column_type.value}]."
                    f"\n{non_matching_prefix} values that do not parse: [{', '.join(non_matching_types)}]."
                ),
            )
        # All rows can be cast to the expected type
        return None
