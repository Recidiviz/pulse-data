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
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_pre_import_validation_type import (
    RawDataPreImportValidationType,
)


@attr.define
class RawDataPreImportValidationFailure:
    """Represents a failure encountered while running a RawDataPreImportValidation."""

    validation_type: RawDataPreImportValidationType = attr.ib(
        validator=attr.validators.in_(RawDataPreImportValidationType)
    )
    validation_query: str = attr.ib(validator=attr_validators.is_str)
    error_msg: str = attr.ib(validator=attr_validators.is_str)

    def __str__(self) -> str:
        return (
            f"Error: {self.error_msg}"
            f"\nValidation type: {self.validation_type.value}"
            f"\nValidation query: {self.validation_query}"
        )

    def to_blocking_failure(self) -> "RawDataBlockingValidationFailure":
        return RawDataBlockingValidationFailure(
            validation_type=self.validation_type,
            validation_query=self.validation_query,
            error_msg=self.error_msg,
        )

    def to_non_blocking_failure(self) -> "RawDataNonBlockingValidationFailure":
        return RawDataNonBlockingValidationFailure(
            validation_type=self.validation_type,
            validation_query=self.validation_query,
            error_msg=self.error_msg,
        )

    def to_json(self) -> dict[str, str]:
        return {
            "validation_type": self.validation_type.value,
            "validation_query": self.validation_query,
            "error_msg": self.error_msg,
        }

    @classmethod
    def from_json(cls, data: dict[str, str]) -> "RawDataPreImportValidationFailure":
        return cls(
            validation_type=RawDataPreImportValidationType(data["validation_type"]),
            validation_query=data["validation_query"],
            error_msg=data["error_msg"],
        )


@attr.define
class RawDataBlockingValidationFailure(RawDataPreImportValidationFailure):
    """Validation failure that should block the file import."""


@attr.define
class RawDataNonBlockingValidationFailure(RawDataPreImportValidationFailure):
    """Validation failure that should not block the file import, but should be surfaced to monitoring."""


@attr.dataclass
class RawDataPreImportValidationContext:
    state_code: StateCode
    file_tag: str
    project_id: str
    temp_table_address: BigQueryAddress
    raw_file_config: DirectIngestRawFileConfig
    file_update_datetime: datetime.datetime
    raw_data_instance: DirectIngestInstance


@attr.define
class BaseRawDataPreImportValidation:
    """Interface for a validation to be run on raw data after it has been loaded to a temporary table"""

    VALIDATION_TYPE: ClassVar[RawDataPreImportValidationType | None] = None
    state_code: StateCode
    file_tag: str
    project_id: str
    temp_table_address: BigQueryAddress

    @abc.abstractmethod
    def get_error_from_results(
        self, results: list[dict[str, Any]]
    ) -> RawDataPreImportValidationFailure | None:
        """Implemented by subclasses to determine if the query results should produce
        a validation failure.
        """

    @abc.abstractmethod
    def build_query(self) -> str:
        """Implemented by subclasses to build the query to run on the temporary table"""

    @classmethod
    @abc.abstractmethod
    def validation_applies_to_file(
        cls,
        context: RawDataPreImportValidationContext,
    ) -> bool:
        """Implemented by subclasses to determine if the validation applies to the provided raw data file."""

    @classmethod
    @abc.abstractmethod
    def create_validation(
        cls,
        context: RawDataPreImportValidationContext,
    ) -> "BaseRawDataPreImportValidation":
        """Creates validation class instance."""


@attr.define
class RawDataColumnValidationMixin:
    """
    Mixin class providing common utility methods for raw data pre-import validations
    that operate on individual columns.
    """

    @staticmethod
    def get_columns_relevant_for_validation(
        validation_type: RawDataPreImportValidationType,
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


@attr.define
class RawDataPreImportValidationError(Exception):
    """Raised when one or more blocking pre-import validations fail for a given file tag."""

    file_tag: str = attr.ib(validator=attr_validators.is_str)
    # Validation failures that should block the file import.
    failures: list[RawDataBlockingValidationFailure] = attr.ib(
        validator=attr_validators.is_non_empty_list
    )
    # Validation warnings that should not block the file import but should be surfaced to monitoring.
    warnings: list[RawDataNonBlockingValidationFailure] | None = attr.ib(
        validator=attr_validators.is_opt_list, default=None
    )

    @property
    def error_message(self) -> str:
        # Warnings are surfaced separately from errors, so we only include blocking failures in the error message.
        failure_messages = "\n\n".join([str(failure) for failure in self.failures])
        return (
            f"{len(self.failures)} pre-import validation(s) failed for file [{self.file_tag}]."
            f" If you wish [{self.file_tag}] to be permanently excluded from any validation, "
            " please add the validation_type and exemption_reason to pre_import_validation_exemptions"
            " for a table-wide exemption or to pre_import_column_validation_exemptions"
            " for a column-specific exemption in the raw file config."
            f"\n{failure_messages}"
        )

    def __str__(self) -> str:
        return self.error_message
