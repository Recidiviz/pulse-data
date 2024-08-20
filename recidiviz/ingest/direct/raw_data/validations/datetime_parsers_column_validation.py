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
from typing import Any, Dict, List

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.ingest.direct.raw_data.raw_file_configs import RawTableColumnInfo
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataColumnImportBlockingValidation,
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
)
from recidiviz.utils.string import StrictStringFormatter

DATETIME_PARSERS_CHECK_TEMPLATE = """
SELECT *
FROM {project_id}.{dataset_id}.{table_id}
WHERE {column_name} IS NOT NULL AND COALESCE({datetime_sql_parsers}) IS NULL
LIMIT 1
"""


@attr.define
class DatetimeParsersColumnValidation(RawDataColumnImportBlockingValidation):
    """Validation that checks if a datetime column has values that don't match any of its datetime parsers
    validation runs on all columns with datetime_sql_parsers unless explicitly exempt.
    """

    datetime_sql_parsers: List[str]
    validation_type: RawDataImportBlockingValidationType = (
        RawDataImportBlockingValidationType.DATETIME_PARSERS
    )

    @classmethod
    def create_column_validation(
        cls,
        file_tag: str,
        project_id: str,
        temp_table_address: BigQueryAddress,
        column: RawTableColumnInfo,
    ) -> "DatetimeParsersColumnValidation":
        if not column.datetime_sql_parsers:
            raise ValueError(
                f"datetime_sql_parsers for {column.name} must not be empty"
            )

        return cls(
            project_id=project_id,
            temp_table_address=temp_table_address,
            file_tag=file_tag,
            column_name=column.name,
            datetime_sql_parsers=column.datetime_sql_parsers,
        )

    @staticmethod
    def validation_applies_to_column(column: RawTableColumnInfo) -> bool:
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
        )

    def get_error_from_results(
        self, results: List[Dict[str, Any]]
    ) -> RawDataImportBlockingValidationFailure | None:
        if results:
            # At least one datetime value didn't parse correctly
            return RawDataImportBlockingValidationFailure(
                validation_type=self.validation_type,
                error_msg=(
                    f"Found column [{self.column_name}] on raw file [{self.file_tag}] "
                    f"not matching any of the datetime_sql_parsers defined in its configuration YAML."
                    f"\nDefined parsers: [{', '.join(self.datetime_sql_parsers)}]."
                    f"\nFirst value that does not parse: [{results[0][self.column_name]}]."
                    f"\nValidation query: {self.query}"
                ),
            )
        # All datetime values parsed
        return None
