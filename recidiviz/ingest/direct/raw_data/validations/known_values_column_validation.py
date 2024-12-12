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
"""Validation to check if a column has values that are not one of the known_values supplied in the column config."""
from datetime import datetime
from typing import Any, Dict, List, Optional

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    ColumnEnumValueInfo,
    DirectIngestRawFileConfig,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataColumnImportBlockingValidation,
    RawDataImportBlockingValidationFailure,
    RawDataImportBlockingValidationType,
)
from recidiviz.utils.string import StrictStringFormatter

KNOWN_VALUES_CHECK_TEMPLATE = """
SELECT {column_name}
FROM {project_id}.{dataset_id}.{table_id}
WHERE {column_name} IS NOT NULL
    AND {column_name} NOT IN ({known_values})
    {null_values_filter}
LIMIT 1
"""


@attr.define
class KnownValuesColumnValidation(RawDataColumnImportBlockingValidation):
    """Validation that checks if a column has values that are not one of the known_values supplied in the column config
    validation runs on all columns with supplied known_values unless explicitly exempt.
    """

    known_values: List[str]
    null_values: Optional[List[str]]

    @staticmethod
    def _get_escaped_known_values(known_values: List[ColumnEnumValueInfo]) -> List[str]:
        return [known_value.value.replace("\\", "\\\\") for known_value in known_values]

    @classmethod
    def create_column_validation(
        cls,
        file_tag: str,
        project_id: str,
        temp_table_address: BigQueryAddress,
        file_upload_datetime: datetime,
        column: RawTableColumnInfo,
    ) -> "KnownValuesColumnValidation":
        if not (temp_table_col_name := column.name_at_datetime(file_upload_datetime)):
            raise ValueError(
                f"Column [{column.name}] does not exist at datetime [{file_upload_datetime}]"
            )
        if not column.known_values:
            raise ValueError(f"known_values for {column.name} must not be empty")

        return cls(
            project_id=project_id,
            temp_table_address=temp_table_address,
            file_tag=file_tag,
            column_name=temp_table_col_name,
            known_values=cls._escape_values_for_query(
                [enum.value for enum in column.known_values]
            ),
            null_values=(
                cls._escape_values_for_query(column.null_values)
                if column.null_values
                else None
            ),
        )

    @staticmethod
    def validation_type() -> RawDataImportBlockingValidationType:
        return RawDataImportBlockingValidationType.KNOWN_VALUES

    @staticmethod
    def validation_applies_to_column(
        column: RawTableColumnInfo,
        _raw_file_config: DirectIngestRawFileConfig,
    ) -> bool:
        return column.known_values is not None

    def build_query(self) -> str:
        if not self.known_values:
            raise ValueError(f"known_values for {self.column_name} must not be empty")

        return StrictStringFormatter().format(
            KNOWN_VALUES_CHECK_TEMPLATE,
            project_id=self.project_id,
            dataset_id=self.temp_table_address.dataset_id,
            table_id=self.temp_table_address.table_id,
            column_name=self.column_name,
            known_values=", ".join([f'"{value}"' for value in self.known_values]),
            null_values_filter=self._build_null_values_filter(
                self.column_name, self.null_values
            ),
        )

    def get_error_from_results(
        self, results: List[Dict[str, Any]]
    ) -> RawDataImportBlockingValidationFailure | None:
        if results:
            # At least one row found with a value not in the known_values set
            return RawDataImportBlockingValidationFailure(
                validation_type=self.validation_type(),
                validation_query=self.query,
                error_msg=(
                    f"Found column [{self.column_name}] on raw file [{self.file_tag}] "
                    f"not matching any of the known_values defined in its configuration YAML."
                    f"\nDefined known values: [{', '.join(self.known_values)}]."
                    f"\nFirst value that does not parse: [{results[0][self.column_name]}]."
                ),
            )
        # All rows have values in the known_values set
        return None
