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
"""Validation that checks if any columns in a given file tag have values that can't be cast to their expected type."""
import datetime
from typing import Any, ClassVar

import attr
from google.cloud.bigquery.enums import StandardSqlTypeNames as BigQueryFieldType

from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawTableColumnFieldType,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    BaseRawDataImportBlockingValidation,
    RawDataColumnValidationMixin,
    RawDataImportBlockingValidationContext,
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
)
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type

ERROR_MESSAGE_ROW_LIMIT = 10


EXPECTED_TYPE_TEMPLATE = """
WITH failed_casting AS (
  SELECT
    {case_when_statements}
  FROM `{project_id}.{dataset_id}.{table_id}`
),
aggregated_failures AS (
  SELECT
    {aggregation_statements}
  FROM failed_casting
)
SELECT
  column_name,
  failed_values
FROM aggregated_failures
UNPIVOT (
  failed_values FOR column_name IN ({column_mapping})
)
WHERE ARRAY_LENGTH(failed_values) > 0
"""

COLUMN_TYPE_TO_BIG_QUERY_TYPE = {
    RawTableColumnFieldType.INTEGER: BigQueryFieldType.INT64,
    RawTableColumnFieldType.DATETIME: BigQueryFieldType.STRING,
    RawTableColumnFieldType.STRING: BigQueryFieldType.STRING,
    RawTableColumnFieldType.PERSON_EXTERNAL_ID: BigQueryFieldType.STRING,
    RawTableColumnFieldType.STAFF_EXTERNAL_ID: BigQueryFieldType.STRING,
}


@attr.define
class ExpectedTypeValidation(
    BaseRawDataImportBlockingValidation, RawDataColumnValidationMixin
):
    """Validation that checks if any columns in a given file tag have values that can't be cast to their expected type.
    Validation runs on all columns with a field_type other than string unless explicitly exempt.
    """

    VALIDATION_TYPE: ClassVar[
        RawDataImportBlockingValidationType
    ] = RawDataImportBlockingValidationType.EXPECTED_TYPE
    column_name_to_columns: dict[str, RawTableColumnInfo]

    @classmethod
    def _get_columns_to_validate(
        cls,
        raw_file_config: DirectIngestRawFileConfig,
        file_update_datetime: datetime.datetime,
    ) -> list[RawTableColumnInfo]:
        return [
            col
            for col in cls.get_columns_relevant_for_validation(
                cls.VALIDATION_TYPE, raw_file_config, file_update_datetime
            )
            if COLUMN_TYPE_TO_BIG_QUERY_TYPE.get(col.field_type)
            != BigQueryFieldType.STRING
        ]

    @classmethod
    def create_validation(
        cls, context: RawDataImportBlockingValidationContext
    ) -> "ExpectedTypeValidation":
        columns_to_validate = cls._get_columns_to_validate(
            context.raw_file_config,
            context.file_update_datetime,
        )
        if not columns_to_validate:
            raise ValueError(
                f"No columns requiring expected type validation in [{context.file_tag}]"
            )

        return cls(
            project_id=context.project_id,
            temp_table_address=context.temp_table_address,
            file_tag=context.file_tag,
            state_code=context.state_code,
            column_name_to_columns={
                assert_type(
                    col.name_at_datetime(context.file_update_datetime), str
                ): col
                for col in columns_to_validate
            },
        )

    @classmethod
    def validation_applies_to_file(
        cls, context: RawDataImportBlockingValidationContext
    ) -> bool:
        """Returns True if the file is not exempt from the validation and there is at least one column
        with `field_type: integer` that is not exempt from the validation for the given file config
        """
        return bool(
            cls._get_columns_to_validate(
                context.raw_file_config,
                context.file_update_datetime,
            )
        )

    def build_query(self) -> str:
        formatter = StrictStringFormatter()

        case_when_statements: list[str] = []
        aggregation_statements: list[str] = []
        column_mapping: list[str] = []

        for column_name, column in self.column_name_to_columns.items():
            big_query_type = COLUMN_TYPE_TO_BIG_QUERY_TYPE.get(column.field_type)
            if big_query_type is None:
                raise ValueError(
                    f"BigQuery type not found for column type {column.field_type.value}"
                )

            if big_query_type == BigQueryFieldType.STRING:
                # This case should ideally be filtered out in create_validation,
                # but as a safeguard, we raise an error here.
                raise ValueError(
                    f"field_type [{column.field_type.value}] has BigQuery type {BigQueryFieldType.STRING.value}, "
                    f"expected type validation should not be run on string columns, "
                    "as all values can be cast to string."
                    f"\nFile tag: [{self.file_tag}], Column: [{column_name}]"
                )

            null_values_filter = self.build_null_values_filter(
                column.name, column.null_values
            )

            # Case statement to identify failed casting values
            case_when_statements.append(
                f"""
            CASE
            WHEN {column_name} IS NOT NULL
                AND SAFE_CAST({column_name} AS {big_query_type.value}) IS NULL
                {null_values_filter}
            THEN {column_name}
            ELSE NULL
            END AS {column_name}_failed_value"""
            )

            # Aggregation to collect failed values
            aggregation_statements.append(
                f"""
            ARRAY_AGG(DISTINCT {column_name}_failed_value IGNORE NULLS LIMIT {ERROR_MESSAGE_ROW_LIMIT}) AS {column_name}_failures"""
            )

            # UNPIVOT mapping
            column_mapping.append(f"{column_name}_failures AS '{column_name}'")

        return formatter.format(
            EXPECTED_TYPE_TEMPLATE,
            project_id=self.project_id,
            dataset_id=self.temp_table_address.dataset_id,
            table_id=self.temp_table_address.table_id,
            case_when_statements=",".join(case_when_statements),
            aggregation_statements=",\n    ".join(aggregation_statements),
            column_mapping=", ".join(column_mapping),
        )

    def get_error_from_results(
        self, results: list[dict[str, Any]]
    ) -> RawDataImportBlockingValidationFailure | None:
        if not results:
            return None

        error_msg = (
            f"Found column(s) on raw file [{self.file_tag}] "
            "not matching the field_type defined in its configuration YAML."
        )
        for result in results:
            column_name = result["column_name"]
            non_matching_types = result["failed_values"]
            column = self.column_name_to_columns[column_name]

            non_matching_prefix = (
                f"First [{len(non_matching_types)}] of many"
                if len(non_matching_types) == ERROR_MESSAGE_ROW_LIMIT
                else f"All [{len(non_matching_types)}]"
            )
            error_msg += (
                f"\nColumn name: [{column_name}]"
                f"\nDefined type: [{column.field_type.value}]."
                f"\n{non_matching_prefix} values that do not parse: [{', '.join(non_matching_types)}].\n"
            )

        return RawDataImportBlockingValidationFailure(
            validation_type=self.VALIDATION_TYPE,
            validation_query=self.build_query(),
            error_msg=error_msg,
        )
