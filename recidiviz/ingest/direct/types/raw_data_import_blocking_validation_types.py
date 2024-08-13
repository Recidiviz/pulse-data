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
"""Types for raw table validations."""
import abc
from enum import Enum
from typing import Any, Dict, List

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress


class RawDataTableImportBlockingValidationType(Enum):
    """Types of import-blocking validations to be run on raw data after it has been loaded to a temporary table
    and before appending to the raw data table"""

    # checks that a column contains at least one nonnull value
    NONNULL_VALUES = "NONNULL_VALUES"
    # checks that datetime columns can be parsed using one of their defined datetime_sql_parsers
    DATETIME_PARSERS = "DATETIME_PARSERS"
    # checks that all values in an enum column match one of the defined known_values
    KNOWN_VALUES = "KNOWN_VALUES"
    # checks that all values in a column with a defined type can be cast to that type,
    # excluding string columns which columns are imported as by default
    EXPECTED_TYPE = "EXPECTED_TYPE"
    # checks that for raw data files that are always historical exports
    # the number of rows in the raw data table is stable
    STABLE_HISTORICAL_RAW_DATA_COUNTS = "STABLE_HISTORICAL_RAW_DATA_COUNTS"


@attr.define
class RawDataTableImportBlockingValidationFailure:
    """Represents a failure encountered while running a RawDataTableImportBlockingValidation"""

    validation_type: RawDataTableImportBlockingValidationType
    error_msg: str


@attr.define
class RawDataTableImportBlockingValidation:
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
    ) -> RawDataTableImportBlockingValidationFailure | None:
        """Implemented by subclasses to determine if the query results should produce
        an error.
        """

    @abc.abstractmethod
    def build_query(self) -> str:
        """Implemented by subclasses to build the query to run on the temporary table"""
