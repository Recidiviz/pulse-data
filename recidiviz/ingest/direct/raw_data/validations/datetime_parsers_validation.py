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
"""Validation that checks if any datetime columns in a given file tag have values that don't match any of their datetime parsers."""
import datetime
from typing import Any, ClassVar, Dict, List

import attr

from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
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


DATETIME_PARSERS_TEMPLATE = """
WITH failed_parsing AS (
  SELECT
    {case_when_statements}
  FROM `{project_id}.{dataset_id}.{table_id}`
),
aggregated_failures AS (
  SELECT
    {aggregation_statements}
  FROM failed_parsing
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


@attr.define
class DatetimeParsersValidation(
    BaseRawDataImportBlockingValidation, RawDataColumnValidationMixin
):
    """Validation that checks if any datetime columns in a given file tag have values that don't match any of
    their datetime parsers. Validation runs on all columns with datetime_sql_parsers unless explicitly exempt.
    """

    VALIDATION_TYPE: ClassVar[
        RawDataImportBlockingValidationType
    ] = RawDataImportBlockingValidationType.DATETIME_PARSERS
    column_name_to_columns: dict[str, RawTableColumnInfo]

    def __attrs_post_init__(self) -> None:
        self._ensure_non_empty_parsers()

    def _ensure_non_empty_parsers(self) -> None:
        for column_name, column in self.column_name_to_columns.items():
            if not column.datetime_sql_parsers:
                raise ValueError(
                    f"datetime_sql_parsers for [{column_name}] must not be empty"
                )

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
            if col.datetime_sql_parsers
        ]

    @classmethod
    def create_validation(
        cls, context: RawDataImportBlockingValidationContext
    ) -> "DatetimeParsersValidation":
        columns_to_validate = cls._get_columns_to_validate(
            context.raw_file_config, file_update_datetime=context.file_update_datetime
        )
        if not columns_to_validate:
            raise ValueError(
                f"No columns with configured datetime_sql_parsers in [{context.file_tag}]"
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
        """Returns True if the file is not exempt from the validation and there is at least one column with
        configured datetime_sql_parsers that is not exempt from the validation for the given file config
        """
        return bool(
            cls._get_columns_to_validate(
                context.raw_file_config, context.file_update_datetime
            )
        )

    def build_query(self) -> str:
        formatter = StrictStringFormatter()

        def build_datetime_parser_clause(
            column_name: str, datetime_sql_parsers: list[str]
        ) -> str:
            formatted_parsers = ", ".join(
                [
                    formatter.format(parser, col_name=column_name)
                    for parser in datetime_sql_parsers
                ]
            )
            return f"COALESCE({formatted_parsers}) IS NULL"

        case_when_statements: list[str] = []
        aggregation_statements: list[str] = []
        column_mapping: list[str] = []

        for column_name, column in self.column_name_to_columns.items():
            not_null_clause = f"{column_name} IS NOT NULL"
            datetime_parsers_clause = build_datetime_parser_clause(
                column_name, assert_type(column.datetime_sql_parsers, list)
            )
            null_values_filter = self.build_null_values_filter(
                column.name, column.null_values
            )

            # Case statement to identify failed values
            case_when_statements.append(
                f"""
            CASE
            WHEN {not_null_clause} AND {datetime_parsers_clause} {null_values_filter}
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
            DATETIME_PARSERS_TEMPLATE,
            project_id=self.project_id,
            dataset_id=self.temp_table_address.dataset_id,
            table_id=self.temp_table_address.table_id,
            case_when_statements=",".join(case_when_statements),
            aggregation_statements=",\n    ".join(aggregation_statements),
            column_mapping=", ".join(column_mapping),
        )

    def get_error_from_results(
        self, results: List[Dict[str, Any]]
    ) -> RawDataImportBlockingValidationFailure | None:
        if not results:
            # All datetime values parsed
            return None

        error_msg = (
            f"Found column(s) on raw file [{self.file_tag}] "
            "not matching any of the datetime_sql_parsers defined in its configuration YAML."
        )
        for result in results:
            column_name = result["column_name"]
            unparseable_datetimes = result["failed_values"]
            column = self.column_name_to_columns[column_name]

            unparseable_prefix = (
                f"First [{len(unparseable_datetimes)}] of many"
                if len(unparseable_datetimes) == ERROR_MESSAGE_ROW_LIMIT
                else f"All [{len(unparseable_datetimes)}]"
            )
            error_msg += (
                f"\nColumn name: [{column_name}]"
                f"\nDefined parsers: [{', '.join(assert_type(column.datetime_sql_parsers, list))}]."
                f"\n{unparseable_prefix} values that do not parse: [{', '.join(unparseable_datetimes)}].\n"
            )

        return RawDataImportBlockingValidationFailure(
            validation_type=self.VALIDATION_TYPE,
            validation_query=self.build_query(),
            error_msg=error_msg,
        )
