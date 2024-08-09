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

from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_types import (
    RawDataTableImportBlockingValidation,
    RawDataTableImportBlockingValidationFailure,
    RawDataTableImportBlockingValidationType,
)
from recidiviz.utils.string import StrictStringFormatter

DATETIME_PARSERS_CHECK_TEMPLATE = """
SELECT *
FROM {project_id}.{dataset_id}.{table_id}
WHERE {column_name} IS NOT NULL AND COALESCE({datetime_sql_parsers}) IS NULL
LIMIT 1
"""


@attr.define
class DatetimeParsersColumnValidation(RawDataTableImportBlockingValidation):
    """Validation that checks if a datetime column has values that don't match any of its datetime parsers
    validation runs on all columns with datetime_sql_parsers unless explicitly exempt.
    """

    column_name: str
    datetime_sql_parsers: List[str]
    validation_type: RawDataTableImportBlockingValidationType = (
        RawDataTableImportBlockingValidationType.DATETIME_PARSERS
    )

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
    ) -> RawDataTableImportBlockingValidationFailure | None:
        if results:
            # At least one datetime value didn't parse correctly
            return RawDataTableImportBlockingValidationFailure(
                validation_type=RawDataTableImportBlockingValidationType.DATETIME_PARSERS,
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
