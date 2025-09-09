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
"""Validation that checks if any columns in a given file tag have only null values."""
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

NONNULL_VALUES_TEMPLATE = """
SELECT
    column_name,
    all_values_null
FROM (
    SELECT
        {column_checks}
    FROM {project_id}.{dataset_id}.{table_id}
)
UNPIVOT (
    all_values_null FOR column_name IN ({column_mapping})
)
WHERE all_values_null = True
"""


@attr.define
class NonNullValuesValidation(
    BaseRawDataImportBlockingValidation, RawDataColumnValidationMixin
):
    """Validation to check if a column has only null values, runs on all primary key columns
    and all columns in a historical file unless explicitly exempt."""

    VALIDATION_TYPE: ClassVar[
        RawDataImportBlockingValidationType
    ] = RawDataImportBlockingValidationType.NONNULL_VALUES
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
            if raw_file_config.always_historical_export
            or col.name_at_datetime(file_update_datetime)
            in raw_file_config.primary_key_cols
        ]

    @classmethod
    def create_validation(
        cls, context: RawDataImportBlockingValidationContext
    ) -> "NonNullValuesValidation":
        columns_to_validate = cls._get_columns_to_validate(
            context.raw_file_config, file_update_datetime=context.file_update_datetime
        )
        if not columns_to_validate:
            raise ValueError(
                f"No columns requiring non-null validation in [{context.file_tag}]"
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
        """Returns True if the file is not exempt from the validation, is always historical
        or if there is at least one primary key column that is not exempt from the validation
        for the given file config
        """
        return bool(
            cls._get_columns_to_validate(
                context.raw_file_config, context.file_update_datetime
            )
        )

    def build_query(self) -> str:
        column_checks: list[str] = []
        column_mapping: list[str] = []

        for column_name in self.column_name_to_columns.keys():
            column_checks.append(
                f"COUNTIF({column_name} IS NOT NULL) = 0 AS {column_name}_null"
            )
            column_mapping.append(f"{column_name}_null AS '{column_name}'")

        return StrictStringFormatter().format(
            NONNULL_VALUES_TEMPLATE,
            project_id=self.project_id,
            dataset_id=self.temp_table_address.dataset_id,
            table_id=self.temp_table_address.table_id,
            column_checks=",\n        ".join(column_checks),
            column_mapping=", ".join(column_mapping),
        )

    def get_error_from_results(
        self, results: List[Dict[str, Any]]
    ) -> RawDataImportBlockingValidationFailure | None:
        if not results:
            return None

        error_msg = (
            f"Found column(s) on raw file [{self.file_tag}] with only null values."
        )

        for result in results:
            column_name = result["column_name"]
            nonnull_value = result["all_values_null"]
            if nonnull_value:
                error_msg += f"\nColumn name: [{column_name}]"

        return RawDataImportBlockingValidationFailure(
            validation_type=self.VALIDATION_TYPE,
            validation_query=self.build_query(),
            error_msg=error_msg,
        )
