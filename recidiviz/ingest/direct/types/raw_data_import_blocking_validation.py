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
from typing import Any, Dict, List

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
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
    error_msg: str


@attr.define
class RawDataImportBlockingValidation:
    """Interface for a validation to be run on raw data after it has been loaded to a temporary table"""

    file_tag: str
    project_id: str
    temp_table_address: BigQueryAddress
    query: str = attr.ib(init=False)

    def __attrs_post_init__(self) -> None:
        self.query = self.build_query()

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
        file_tag: str,
        project_id: str,
        temp_table_address: BigQueryAddress,
        column: RawTableColumnInfo,
    ) -> "RawDataColumnImportBlockingValidation":
        """Factory method to create a column validation."""
        return cls(
            project_id=project_id,
            temp_table_address=temp_table_address,
            column_name=column.name,
            file_tag=file_tag,
        )

    @staticmethod
    @abc.abstractmethod
    def validation_applies_to_column(column: RawTableColumnInfo) -> bool:
        """Implemented by subclasses to determine if the validation applies to the given column"""


@attr.define
class RawDataTableImportBlockingValidation(RawDataImportBlockingValidation):
    """Interface for a validation to be run on a per-table basis."""

    region_code: str
    raw_data_instance: DirectIngestInstance

    @classmethod
    def create_table_validation(
        cls,
        file_tag: str,
        project_id: str,
        temp_table_address: BigQueryAddress,
        region_code: str,
        raw_data_instance: DirectIngestInstance,
    ) -> "RawDataTableImportBlockingValidation":
        """Factory method to create a table validation."""
        return cls(
            project_id=project_id,
            temp_table_address=temp_table_address,
            file_tag=file_tag,
            region_code=region_code,
            raw_data_instance=raw_data_instance,
        )

    @staticmethod
    @abc.abstractmethod
    def validation_applies_to_table(file_config: DirectIngestRawFileConfig) -> bool:
        """Implemented by subclasses to determine if the validation applies to the given table"""
