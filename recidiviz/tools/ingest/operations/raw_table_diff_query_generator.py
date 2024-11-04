# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Base class for generating a query to compare raw data between tables."""
import abc
import datetime
from typing import Any, Dict, List, Optional, Sequence

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    UPDATE_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


@attr.define
class RawTableDiffQueryResultRow:
    """Base class for a row returned by a diff query"""

    update_datetime: datetime.datetime


@attr.define
class RawTableDiffQueryResult:
    """Represents the results returned by a diff query

    rows_missing_from_src: Rows that are in the comparison table but not in the source table
    rows_missing_from_cmp: Rows that are in the source table but not in the comparison table
    rows_with_differences: Rows that are in both tables but have differences in their values
    """

    rows_missing_from_src: Sequence[RawTableDiffQueryResultRow]
    rows_missing_from_cmp: Sequence[RawTableDiffQueryResultRow]
    rows_with_differences: Sequence[RawTableDiffQueryResultRow]

    @abc.abstractmethod
    def build_result_rows_str(self, limit: Optional[int] = None) -> str:
        """Builds a string representation of the result rows, returning the first `limit` rows for each category
        if a limit is provided, otherwise returns string representation of all rows"""


@attr.define
class RawTableDiffQueryGenerator:
    """Base class for generating a query to compare raw data between tables."""

    src_project_id: str
    src_dataset_id: str
    cmp_project_id: str
    cmp_dataset_id: str
    truncate_update_datetime_col_name: str

    @classmethod
    def create_query_generator(
        cls,
        region_code: str,
        src_project_id: str,
        src_ingest_instance: DirectIngestInstance,
        cmp_project_id: str,
        cmp_ingest_instance: DirectIngestInstance,
        truncate_update_datetime_part: Optional[str] = None,
    ) -> "RawTableDiffQueryGenerator":
        state_code = StateCode(region_code.upper())

        return cls(
            src_project_id=src_project_id,
            src_dataset_id=raw_tables_dataset_for_region(
                state_code=state_code,
                instance=src_ingest_instance,
                sandbox_dataset_prefix=None,
            ),
            cmp_project_id=cmp_project_id,
            cmp_dataset_id=raw_tables_dataset_for_region(
                state_code=state_code,
                instance=cmp_ingest_instance,
                sandbox_dataset_prefix=None,
            ),
            truncate_update_datetime_col_name=cls._get_truncated_datetime_column_name(
                truncate_update_datetime_part
            ),
        )

    @staticmethod
    def _get_dataset_id_for_ingest_instance(
        state_code: StateCode, ingest_instance: DirectIngestInstance
    ) -> str:
        return raw_tables_dataset_for_region(
            state_code=state_code,
            instance=ingest_instance,
            sandbox_dataset_prefix=None,
        )

    @staticmethod
    def _get_truncated_datetime_column_name(
        truncate_update_datetime_part: Optional[str] = None,
    ) -> str:
        return (
            f"DATETIME_TRUNC({UPDATE_DATETIME_COL_NAME}, {truncate_update_datetime_part})"
            if truncate_update_datetime_part
            else UPDATE_DATETIME_COL_NAME
        )

    @abc.abstractmethod
    def generate_query(self, file_tag: str) -> str:
        """Generate the query to compare the tables."""

    @staticmethod
    @abc.abstractmethod
    def parse_query_result(
        query_result: List[Dict[str, Any]]
    ) -> RawTableDiffQueryResult:
        """Converts a list of dictionaries, with each dictionary representing a query result row
        with key=column name, value=value, into a RawTableDiffQueryResult object"""
