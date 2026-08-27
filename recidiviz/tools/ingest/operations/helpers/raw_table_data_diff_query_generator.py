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
# =============================================================================
"""Generates a query to compare raw data between tables."""
import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import attr

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    IS_DELETED_COL_NAME,
    UPDATE_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.ingest.operations.helpers.raw_table_diff_query_generator import (
    RawTableDiffQueryGenerator,
    RawTableDiffQueryResult,
    RawTableDiffQueryResultRow,
)
from recidiviz.utils.string import StrictStringFormatter

QUERY_TEMPLATE = """
WITH src AS (
  SELECT {src_columns}
  FROM `{src_project_id}.{src_raw_data_dataset_id}.{raw_data_table_id}`
  {optional_datetime_filter}
  EXCEPT DISTINCT
  SELECT {cmp_columns}
  FROM `{cmp_project_id}.{cmp_raw_data_dataset_id}.{raw_data_table_id}`
  {optional_datetime_filter}
),
cmp AS (
  SELECT {cmp_columns}
  FROM `{cmp_project_id}.{cmp_raw_data_dataset_id}.{raw_data_table_id}`
  {optional_datetime_filter}
  EXCEPT DISTINCT
  SELECT {src_columns}
  FROM `{src_project_id}.{src_raw_data_dataset_id}.{raw_data_table_id}`
  {optional_datetime_filter}
)
SELECT
    *
FROM src
FULL OUTER JOIN cmp ON
{column_comparison}
ORDER BY src_update_datetime, cmp_update_datetime
LIMIT 500
"""

SRC_COLUMN_PREFIX = "src_"
CMP_COLUMN_PREFIX = "cmp_"


@attr.define
class RawTableDataDiffQueryResultRow(RawTableDiffQueryResultRow):
    """The result of a single row comparison in the raw table data diff query.

    columns_with_differences: A dictionary of column names to tuples of the source and comparison values.
    correct_columns: A dictionary of column names to the correct values."""

    columns_with_differences: Dict[str, Tuple[str, str]]
    correct_columns: Dict[str, str]

    def __str__(self) -> str:
        s = f"update_datetime: {self.update_datetime.isoformat()}\n"
        if self.correct_columns:
            s += ", ".join(
                f"{column_name}: {value}"
                for column_name, value in self.correct_columns.items()
                if column_name != UPDATE_DATETIME_COL_NAME
            )
        if self.columns_with_differences:
            s += "\nCOLUMNS WITH DIFFERENCES:"
            for column_name, value in self.columns_with_differences.items():
                s += (
                    f"\n\t{column_name}:"
                    f"\n\t\tsrc: {value[0]}"
                    f"\n\t\tcmp: {value[1]}"
                )
        return s


@attr.define
class RawTableDataDiffQueryResult(RawTableDiffQueryResult):
    """The result of the raw table data diff query.

    columns_with_differences: All of the distinct columns that had differences between the source and comparison tables.
    rows_missing_from_src: Rows that are in the comparison table but have no primary key match in the source table.
    rows_missing_from_cmp: Rows that are in the source table but have no primary key match in the comparison table.
    rows_with_differences: Rows that are in both tables but have differences in their non-primary key values.
    """

    columns_with_differences: Set[str]

    def build_result_rows_str(self, limit: Optional[int] = None) -> str:
        limit = limit or max(
            len(self.rows_with_differences),
            len(self.rows_missing_from_src),
            len(self.rows_missing_from_cmp),
        )
        s = ""
        if self.columns_with_differences:
            formatted_columns = "\n".join(
                f"\t- {col}" for col in self.columns_with_differences
            )
            s += f"The following {len(self.columns_with_differences)} columns had differences:\n{formatted_columns}\n\n"
        if self.rows_with_differences:
            formatted_rows = "\n".join(
                str(row) for row in self.rows_with_differences[:limit]
            )
            s += f"In the following {len(self.rows_with_differences)} rows:\n{formatted_rows}\n\n"
        if self.rows_missing_from_src:
            formatted_rows = "\n".join(
                str(row) for row in self.rows_missing_from_src[:limit]
            )
            s += f"The following {len(self.rows_missing_from_src)} comparison table rows had no exact primary key match in the source table:\n{formatted_rows}\n\n"
        if self.rows_missing_from_cmp:
            formatted_rows = "\n".join(
                str(row) for row in self.rows_missing_from_cmp[:limit]
            )
            s += f"The following {len(self.rows_missing_from_cmp)} source table rows had no exact primary key match in the comparison table:\n{formatted_rows}\n\n"

        return s


@attr.define
class RawTableDataDiffQueryGenerator(RawTableDiffQueryGenerator):
    """Generates a query to compare raw data between tables.
    Finds comparison table rows that have no exact match in the source table and
    source table rows that have no exact match in the comparison table, then
    attempts to match rows by joining based on primary keys and update_datetime.
    If the file tag has no documented primary key, all columns are used as the primary key.
    If a row in the source table has no exact primary key match in the comparison table,
    it is returned with all of the comparison columns as None, and vice versa.
    The first 500 results are returned ordered by update_datetime."""

    region_raw_file_config: DirectIngestRegionRawFileConfig

    @classmethod
    def create_query_generator(
        cls,
        region_code: str,
        src_project_id: str,
        src_ingest_instance: DirectIngestInstance,
        cmp_project_id: str,
        cmp_ingest_instance: DirectIngestInstance,
        truncate_update_datetime_part: Optional[str] = None,
        start_date_inclusive: Optional[datetime.datetime] = None,
        end_date_exclusive: Optional[datetime.datetime] = None,
    ) -> "RawTableDataDiffQueryGenerator":
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
            region_raw_file_config=DirectIngestRegionRawFileConfig(region_code),
            optional_datetime_filter=cls._get_optional_datetime_filter(
                start_date_inclusive, end_date_exclusive
            ),
        )

    def _build_columns_for_comparison(
        self, prefix: str, file_column_names: List[str]
    ) -> List[str]:
        columns_for_comparison = [
            f"{column} AS {prefix}{column}"
            for column in file_column_names + [IS_DELETED_COL_NAME]
        ]
        columns_for_comparison.append(
            f"{self.truncate_update_datetime_col_name} AS {prefix}{UPDATE_DATETIME_COL_NAME}"
        )

        return columns_for_comparison

    def _build_primary_key_columns(
        self, file_config: DirectIngestRawFileConfig
    ) -> List[str]:
        primary_key_cols = file_config.primary_key_cols or [
            column.name for column in file_config.current_columns
        ]
        primary_key_cols.append(UPDATE_DATETIME_COL_NAME)

        return primary_key_cols

    def generate_query(self, file_tag: str) -> str:
        file_config = self.region_raw_file_config.raw_file_configs[file_tag]
        column_names = [column.name for column in file_config.current_columns]

        return StrictStringFormatter().format(
            QUERY_TEMPLATE,
            src_project_id=self.src_project_id,
            src_raw_data_dataset_id=self.src_dataset_id,
            cmp_project_id=self.cmp_project_id,
            cmp_raw_data_dataset_id=self.cmp_dataset_id,
            raw_data_table_id=file_tag,
            src_columns=", ".join(
                self._build_columns_for_comparison(
                    prefix=SRC_COLUMN_PREFIX, file_column_names=column_names
                )
            ),
            cmp_columns=", ".join(
                self._build_columns_for_comparison(
                    prefix=CMP_COLUMN_PREFIX, file_column_names=column_names
                )
            ),
            column_comparison=" AND ".join(
                f"{SRC_COLUMN_PREFIX}{column} = {CMP_COLUMN_PREFIX}{column}"
                for column in self._build_primary_key_columns(file_config)
            ),
            optional_datetime_filter=self.optional_datetime_filter or "",
        )

    @staticmethod
    def parse_query_result(
        query_result: List[Dict[str, Any]],
    ) -> RawTableDiffQueryResult:
        """Parse the raw query result into a RawTableDataDiffQueryResult"""

        src_missing_cmp: List[RawTableDataDiffQueryResultRow] = []
        cmp_missing_src: List[RawTableDataDiffQueryResultRow] = []
        rows_with_differences: List[RawTableDataDiffQueryResultRow] = []
        columns_with_differences: Set[str] = set()

        for results_dict in query_result:
            src_columns = {
                key[len(SRC_COLUMN_PREFIX) :]: value
                for key, value in results_dict.items()
                if key.startswith(SRC_COLUMN_PREFIX)
            }
            cmp_columns = {
                key[len(CMP_COLUMN_PREFIX) :]: value
                for key, value in results_dict.items()
                if key.startswith(CMP_COLUMN_PREFIX)
            }

            all_src_none = all(value is None for value in src_columns.values())
            all_cmp_none = all(value is None for value in cmp_columns.values())

            if all_src_none:
                cmp_missing_src.append(
                    RawTableDataDiffQueryResultRow(
                        correct_columns=cmp_columns,
                        columns_with_differences={},
                        update_datetime=cmp_columns[UPDATE_DATETIME_COL_NAME],
                    )
                )
            elif all_cmp_none:
                src_missing_cmp.append(
                    RawTableDataDiffQueryResultRow(
                        correct_columns=src_columns,
                        columns_with_differences={},
                        update_datetime=src_columns[UPDATE_DATETIME_COL_NAME],
                    )
                )
            else:
                col_dif = {}
                col_same = {}

                for key in src_columns:
                    src_value = src_columns[key]
                    cmp_value = cmp_columns[key]
                    if src_value != cmp_value:
                        col_dif[key] = (src_value, cmp_value)
                        columns_with_differences.add(key)
                    else:
                        col_same[key] = src_value

                rows_with_differences.append(
                    RawTableDataDiffQueryResultRow(
                        update_datetime=src_columns[UPDATE_DATETIME_COL_NAME],
                        columns_with_differences=col_dif,
                        correct_columns=col_same,
                    )
                )

        return RawTableDataDiffQueryResult(
            rows_missing_from_cmp=src_missing_cmp,
            rows_missing_from_src=cmp_missing_src,
            rows_with_differences=rows_with_differences,
            columns_with_differences=columns_with_differences,
        )
