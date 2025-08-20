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
"""Validation that checks if a datetime column has values that don't match any of its datetime parsers."""
from datetime import datetime
from typing import Any, Dict, List, Optional

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataColumnImportBlockingValidation,
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
)
from recidiviz.utils.string import StrictStringFormatter

ERROR_MESSAGE_ROW_LIMIT = 10

DATETIME_PARSERS_CHECK_TEMPLATE = """
SELECT DISTINCT {column_name}
FROM {project_id}.{dataset_id}.{table_id}
WHERE {column_name} IS NOT NULL
  AND COALESCE({datetime_sql_parsers}) IS NULL
  {null_values_filter}
LIMIT {error_message_limit}
"""


@attr.define
class DatetimeParsersColumnValidation(RawDataColumnImportBlockingValidation):
    """Validation that checks if a datetime column has values that don't match any of its datetime parsers
    validation runs on all columns with datetime_sql_parsers unless explicitly exempt.
    """

    datetime_sql_parsers: List[str]
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
    ) -> "DatetimeParsersColumnValidation":
        if not (temp_table_col_name := column.name_at_datetime(file_upload_datetime)):
            raise ValueError(
                f"Column [{column.name}] does not exist at datetime [{file_upload_datetime}]"
            )
        if not column.datetime_sql_parsers:
            raise ValueError(
                f"datetime_sql_parsers for {column.name} must not be empty"
            )

        return cls(
            project_id=project_id,
            temp_table_address=temp_table_address,
            file_tag=file_tag,
            state_code=state_code,
            column_name=temp_table_col_name,
            datetime_sql_parsers=column.datetime_sql_parsers,
            null_values=(
                cls._escape_values_for_query(column.null_values)
                if column.null_values
                else None
            ),
        )

    @staticmethod
    def validation_type() -> RawDataImportBlockingValidationType:
        return RawDataImportBlockingValidationType.DATETIME_PARSERS

    @staticmethod
    def validation_applies_to_column(
        column: RawTableColumnInfo,
        _raw_file_config: DirectIngestRawFileConfig,
    ) -> bool:
        return column.datetime_sql_parsers is not None

    def build_query(self) -> str:
        if not self.datetime_sql_parsers:
            raise ValueError(
                f"datetime_sql_parsers for {self.column_name} must not be empty"
            )

        formatter = StrictStringFormatter()

        return formatter.format(
            DATETIME_PARSERS_CHECK_TEMPLATE,
            project_id=self.project_id,
            dataset_id=self.temp_table_address.dataset_id,
            table_id=self.temp_table_address.table_id,
            column_name=self.column_name,
            datetime_sql_parsers=", ".join(
                [
                    formatter.format(parser, col_name=self.column_name)
                    for parser in self.datetime_sql_parsers
                ]
            ),
            null_values_filter=self._build_null_values_filter(
                self.column_name, self.null_values
            ),
            error_message_limit=ERROR_MESSAGE_ROW_LIMIT,
        )

    def get_error_from_results(
        self, results: List[Dict[str, Any]]
    ) -> RawDataImportBlockingValidationFailure | None:
        if results:
            unparseable_datetimes = [result[self.column_name] for result in results]
            unparseable_prefix = (
                f"First [{len(unparseable_datetimes)}] of many"
                if len(unparseable_datetimes) == ERROR_MESSAGE_ROW_LIMIT
                else f"All [{len(unparseable_datetimes)}]"
            )
            # At least one datetime value didn't parse correctly
            return RawDataImportBlockingValidationFailure(
                validation_type=self.validation_type(),
                validation_query=self.query,
                error_msg=(
                    f"Found column [{self.column_name}] on raw file [{self.file_tag}] "
                    f"not matching any of the datetime_sql_parsers defined in its configuration YAML."
                    f"\nDefined parsers: [{', '.join(self.datetime_sql_parsers)}]."
                    f"\n{unparseable_prefix} values that do not parse: [{', '.join(unparseable_datetimes)}]."
                ),
            )
        # All datetime values parsed
        return None
