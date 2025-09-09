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
"""Classes for raw table validations."""
import abc
import datetime
from typing import Any, ClassVar, Optional

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import escape_backslashes_for_query
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation_type import (
    RawDataImportBlockingValidationType,
)


@attr.define
class RawDataImportBlockingValidationFailure:
    """Represents a failure encountered while running a RawDataTableImportBlockingValidation"""

    validation_type: RawDataImportBlockingValidationType
    validation_query: str
    error_msg: str

    def __str__(self) -> str:
        return (
            f"Error: {self.error_msg}"
            f"\nValidation type: {self.validation_type.value}"
            f"\nValidation query: {self.validation_query}"
        )


@attr.dataclass
class RawDataImportBlockingValidationContext:
    state_code: StateCode
    file_tag: str
    project_id: str
    temp_table_address: BigQueryAddress
    raw_file_config: DirectIngestRawFileConfig
    file_update_datetime: datetime.datetime
    raw_data_instance: DirectIngestInstance


@attr.define
class BaseRawDataImportBlockingValidation:
    """Interface for a validation to be run on raw data after it has been loaded to a temporary table"""

    VALIDATION_TYPE: ClassVar[RawDataImportBlockingValidationType | None] = None
    state_code: StateCode
    file_tag: str
    project_id: str
    temp_table_address: BigQueryAddress

    @abc.abstractmethod
    def get_error_from_results(
        self, results: list[dict[str, Any]]
    ) -> RawDataImportBlockingValidationFailure | None:
        """Implemented by subclasses to determine if the query results should produce
        an error.
        """

    @abc.abstractmethod
    def build_query(self) -> str:
        """Implemented by subclasses to build the query to run on the temporary table"""

    @classmethod
    @abc.abstractmethod
    def validation_applies_to_file(
        cls,
        context: RawDataImportBlockingValidationContext,
    ) -> bool:
        """Implemented by subclasses to determine if the validation applies to the provided raw data file."""

    @classmethod
    @abc.abstractmethod
    def create_validation(
        cls, context: RawDataImportBlockingValidationContext
    ) -> "BaseRawDataImportBlockingValidation":
        """Creates validation class instance."""


@attr.define
class RawDataColumnValidationMixin:
    """
    Mixin class providing common utility methods for raw data import blocking validations
    that operate on individual columns.
    """

    @staticmethod
    def get_columns_relevant_for_validation(
        validation_type: RawDataImportBlockingValidationType,
        raw_file_config: DirectIngestRawFileConfig,
        file_update_datetime: datetime.datetime,
    ) -> list[RawTableColumnInfo]:
        """
        Returns all columns for a given |raw_file_config| that
        1. Are not exempt from the given |validation_type|
        2. Exist in the raw file config for the given |file_update_datetime|
        3. Still exist in the raw file config

        If the whole file is exempt from the given |validation_type|, returns an empty list
        """
        if raw_file_config.file_is_exempt_from_validation(validation_type):
            return []

        return [
            column
            for column in raw_file_config.current_columns
            if column.exists_at_datetime(file_update_datetime)
            and not raw_file_config.column_is_exempt_from_validation(
                column.name, validation_type
            )
        ]

    @staticmethod
    def build_null_values_filter(
        column_name: str, null_values: Optional[list[str]]
    ) -> str:
        """Builds a SQL filter clause to exclude specified null values from a column."""
        if not null_values:
            return ""

        null_values_str = ", ".join(
            [f'"{value}"' for value in escape_backslashes_for_query(null_values)]
        )
        return f"AND {column_name} NOT IN ({null_values_str})"


class RawDataImportBlockingValidationError(Exception):
    """Raised when one or more pre-import validations fail for a given file tag."""

    def __init__(
        self, file_tag: str, failures: list[RawDataImportBlockingValidationFailure]
    ):
        self.file_tag = file_tag
        self.failures = failures

    def __str__(self) -> str:
        return (
            f"{len(self.failures)} pre-import validation(s) failed for file [{self.file_tag}]."
            f" If you wish [{self.file_tag}] to be permanently excluded from any validation, "
            " please add the validation_type and exemption_reason to import_blocking_validation_exemptions"
            " for a table-wide exemption or to import_blocking_column_validation_exemptions"
            " for a column-specific exemption in the raw file config."
            f"\n{self._get_failure_messages()}"
        )

    def _get_failure_messages(self) -> str:
        return "\n\n".join([str(failure) for failure in self.failures])
