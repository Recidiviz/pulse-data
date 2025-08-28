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
from typing import Any, Dict, List, Optional

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    RawTableColumnInfo,
)
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


@attr.define
class RawDataImportBlockingValidation:
    """Interface for a validation to be run on raw data after it has been loaded to a temporary table"""

    state_code: StateCode
    file_tag: str
    project_id: str
    temp_table_address: BigQueryAddress
    query: str = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self.query = self.build_query()

    @staticmethod
    @abc.abstractmethod
    def validation_type() -> RawDataImportBlockingValidationType:
        """Each subclass must define its own validation type."""

    @abc.abstractmethod
    def get_error_from_results(
        self, results: List[Dict[str, Any]]
    ) -> RawDataImportBlockingValidationFailure | None:
        """Implemented by subclasses to determine if the query results should produce
        an error.
        """

    @abc.abstractmethod
    def build_query(self) -> str:
        """Implemented by subclasses to build the query to run on the temporary table"""


@attr.define
class RawDataColumnImportBlockingValidation(RawDataImportBlockingValidation):
    """Interface for a validation to be run on a per-column basis."""

    column_name: str

    @classmethod
    def create_column_validation(
        cls,
        *,
        file_tag: str,
        project_id: str,
        state_code: StateCode,
        temp_table_address: BigQueryAddress,
        file_upload_datetime: datetime.datetime,
        column: RawTableColumnInfo,
    ) -> "RawDataColumnImportBlockingValidation":
        """Factory method to create a column validation."""
        if not (temp_table_col_name := column.name_at_datetime(file_upload_datetime)):
            raise ValueError(
                f"Column [{column.name}] does not exist at datetime [{file_upload_datetime}]"
            )

        return cls(
            project_id=project_id,
            temp_table_address=temp_table_address,
            column_name=temp_table_col_name,
            file_tag=file_tag,
            state_code=state_code,
        )

    @staticmethod
    @abc.abstractmethod
    def validation_applies_to_column(
        column: RawTableColumnInfo,
        raw_file_config: DirectIngestRawFileConfig,
    ) -> bool:
        """Implemented by subclasses to determine if the validation applies to the given column"""

    @staticmethod
    def _escape_values_for_query(values: List[str]) -> List[str]:
        return [value.replace("\\", "\\\\") for value in values]

    @staticmethod
    def _build_null_values_filter(
        column_name: str, null_values: Optional[List[str]]
    ) -> str:
        if null_values:
            null_values_str = ", ".join([f'"{value}"' for value in null_values])
            return f"AND {column_name} NOT IN ({null_values_str})"
        return ""


@attr.define
class RawDataTableImportBlockingValidation(RawDataImportBlockingValidation):
    """Interface for a validation to be run on a per-table basis."""

    @staticmethod
    @abc.abstractmethod
    def validation_applies_to_table(file_config: DirectIngestRawFileConfig) -> bool:
        """Implemented by subclasses to determine if the validation applies to the given table"""


class RawDataImportBlockingValidationError(Exception):
    """Raised when one or more pre-import validations fail for a given file tag."""

    def __init__(
        self, file_tag: str, failures: List[RawDataImportBlockingValidationFailure]
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
