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
from typing import Any, Dict, List

import attr
from google.cloud.bigquery.enums import StandardSqlTypeNames as BigQueryFieldType

from recidiviz.ingest.direct.raw_data.raw_file_configs import RawTableColumnFieldType
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_types import (
    RawDataTableImportBlockingValidation,
    RawDataTableImportBlockingValidationFailure,
    RawDataTableImportBlockingValidationType,
)
from recidiviz.utils.string import StrictStringFormatter

EXPECTED_TYPE_CHECK_TEMPLATE = """
SELECT *
FROM {project_id}.{dataset_id}.{table_id}
WHERE {column_name} IS NOT NULL AND SAFE_CAST({column_name} AS {type}) IS NULL
LIMIT 1
"""

COLUMN_TYPE_TO_BIG_QUERY_TYPE = {
    RawTableColumnFieldType.INTEGER: BigQueryFieldType.INT64,
    RawTableColumnFieldType.DATETIME: BigQueryFieldType.DATETIME,
    RawTableColumnFieldType.STRING: BigQueryFieldType.STRING,
    RawTableColumnFieldType.PERSON_EXTERNAL_ID: BigQueryFieldType.STRING,
    RawTableColumnFieldType.STAFF_EXTERNAL_ID: BigQueryFieldType.STRING,
}


@attr.define
class ExpectedTypeColumnValidation(RawDataTableImportBlockingValidation):
    """Validation that checks if a column has values that can't be cast to the expected type"""

    column_name: str
    column_type: RawTableColumnFieldType
    validation_type: RawDataTableImportBlockingValidationType = (
        RawDataTableImportBlockingValidationType.EXPECTED_TYPE
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
        )

    def get_error_from_results(
        self, results: List[Dict[str, Any]]
    ) -> RawDataTableImportBlockingValidationFailure | None:
        if results:
            # At least one row found with a value that can't be cast to the expected type
            return RawDataTableImportBlockingValidationFailure(
                validation_type=self.validation_type,
                error_msg=(
                    f"Found column [{self.column_name}] on raw file [{self.file_tag}] "
                    f"not matching the field_type defined in its configuration YAML."
                    f"Defined type: [{self.column_type.value}]."
                    f"\nFirst value that does not parse: [{results[0][self.column_name]}]."
                    f"\nValidation query: {self.query}"
                ),
            )
        # All rows can be cast to the expected type
        return None
