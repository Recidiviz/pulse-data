# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""
Helper to record the differences between two BigQuery addresses.

When run, creates two tables and returns statistics about the run:
- full comparison table: shows all rows from both views, plus information about whether the row is in
  both views, the original view, and/or the new view
- differences table: shows only the differences between the two tables, excluding rows that match and
  setting matching values to null.

The output tables will be named based on the original dataset / view IDs.

Example full comparison:
state_code | person_id | district | supervision_type | in_both | in_original | in_new
---------- | --------- |--------- | ---------------- | ------- | ----------- | ------
  US_XX    | 123       | D1       | PROBATION        | true    | true        | true
  US_XX    | 456       | D2       | PAROLE           | false   | true        | false
  US_XX    | 456       | D3       | PAROLE           | false   | false       | true
  US_YY    | 789       | D4       | DUAL             | false   | true        | false

Example differences comparison:
state_code | person_id | district.o | district.n | supervision_type.o | supervision_type.n | in_original | in_new
---------- | --------- |----------- | ---------- | ------------------ | ------------------ | ----------- | ------
  US_XX    | 456       | D2         | D3         | null               | null               | true        | true
  US_YY    | 789       | D4         | null       | DUAL               | null               | true        | false

Example statistics (using state_code as a grouping column):
state_code  count_original  count_new  in_both  in_original_not_new  in_new_not_original  new_error_rate  original_error_rate
     US_XX               2          2        1                    1                    1             0.5                  0.5
     US_YY               1          0        0                    0                    1             NaN                  0.0

Note: in the statistics+full output, a row with the same primary key values but different other
values will count as two rows: one in_original and one in_new.
"""

import logging
from typing import Dict, List, Optional, Tuple

import attr
import pandas as pd

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.view_update_manager import (
    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.utils.string import StrictStringFormatter

_POTENTIAL_ID_COLS = [
    "person_id",
    "person_external_id",
    "external_id",
    "state_id",
    "tdoc_id",
    "id",
    "priority",
]

_DATA_TYPES_TO_COMPARE = [
    "BOOL",
    "DATE",
    "DATETIME",
    "FLOAT64",
    "INT64",
    "STRING",
]

_COLUMNS_QUERY = """
    SELECT
      table_name,
      column_name,
      data_type
    FROM `{project_id}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = "{table_name}"
"""

STATS_COUNT_ORIGINAL_COL = "count_original"
STATS_COUNT_NEW_COL = "count_new"
STATS_IN_BOTH_COL = "in_both"
STATS_IN_ORIGINAL_NOT_NEW_COL = "in_original_not_new"
STATS_IN_NEW_NOT_ORIGINAL_COL = "in_new_not_original"
STATS_NEW_ERROR_RATE_COL = "new_error_rate"
STATS_ORIGINAL_ERROR_RATE_COL = "original_error_rate"

_STATS_QUERY_SUMS = f"""
      SUM(IF(in_original,1,0)) AS {STATS_COUNT_ORIGINAL_COL},
      SUM(IF(in_new,1,0)) AS {STATS_COUNT_NEW_COL},
      SUM(IF(in_original AND in_new,1,0)) AS {STATS_IN_BOTH_COL},
      SUM(IF(in_original AND NOT in_new,1,0)) AS {STATS_IN_ORIGINAL_NOT_NEW_COL},
      SUM(IF(not in_original AND in_new,1,0)) AS {STATS_IN_NEW_NOT_ORIGINAL_COL}
"""

_STATS_QUERY_WITH_GROUPING = f"""
    SELECT
      {{grouping_columns}},
      {_STATS_QUERY_SUMS}
    FROM `{{comparison_output_table}}`
    GROUP BY {{group_order}}
    ORDER BY {{group_order}}
"""

_STATS_QUERY_NO_GROUPING = f"""
    SELECT
      {_STATS_QUERY_SUMS}
    FROM `{{comparison_output_table}}`
"""


def format_lines_for_query(lines: List[str], join_str: str = ",\n      ") -> str:
    return join_str.join(lines)


def all_columns_equal_condition(column_names: List[str]) -> str:
    return format_lines_for_query(
        [
            f"TO_JSON_STRING(original_rows.{col})=TO_JSON_STRING(new_rows.{col})"
            for col in column_names
        ],
        join_str="\n      AND ",
    )


def get_comparison_query(
    column_names: List[str], table_name_orig: str, table_name_new: str, id_col: str
) -> str:
    return f"""
    WITH original_rows AS
    (
    SELECT
      {format_lines_for_query(column_names)}
    FROM {table_name_orig}
    )
    ,
    new_rows AS
    (
    SELECT
      {format_lines_for_query(column_names)}
    FROM {table_name_new}
    )

    SELECT
      {format_lines_for_query(
        [
            f"IFNULL(original_rows.{col}, new_rows.{col}) AS {col}"
            for col in column_names
        ]
      )},
      original_rows.{id_col} IS NOT NULL AND new_rows.{id_col} IS NOT NULL AS in_both,
      original_rows.{id_col} IS NOT NULL AS in_original,
      new_rows.{id_col} IS NOT NULL AS in_new
    FROM original_rows
    FULL OUTER JOIN new_rows
    ON
      {all_columns_equal_condition(column_names)}
    """


def get_difference_query(
    column_names: List[str],
    table_name_orig: str,
    table_name_new: str,
    primary_keys: List[str],
) -> str:
    def format_for_difference(col: str, primary_keys: List[str]) -> str:
        if col in primary_keys:
            return col
        return f"IF(original_rows.{col} = new_rows.{col}, NULL, STRUCT(original_rows.{col} AS o, new_rows.{col} AS n)) AS {col}"

    id_col = primary_keys[0]
    return f"""
    SELECT
      {format_lines_for_query([format_for_difference(col, primary_keys) for col in column_names])},
      original_rows.{id_col} IS NOT NULL AS in_original,
      new_rows.{id_col} IS NOT NULL AS in_new
    FROM {table_name_orig} original_rows
    FULL OUTER JOIN {table_name_new} new_rows
    USING(
      {format_lines_for_query(primary_keys)}
    )
    WHERE NOT ({all_columns_equal_condition(column_names)})
"""


def _get_columns_to_compare_and_ignore(
    address: ProjectSpecificBigQueryAddress,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    column_query = StrictStringFormatter().format(
        _COLUMNS_QUERY,
        project_id=address.project_id,
        dataset=address.dataset_id,
        table_name=address.table_id,
    )
    logging.info("Running query:\n%s", column_query)
    column_df = pd.read_gbq(column_query)
    column_df["is_comparable"] = column_df.data_type.isin(_DATA_TYPES_TO_COMPARE)
    return column_df[column_df.is_comparable], column_df[~column_df.is_comparable]


@attr.define(kw_only=True)
class CompareTablesResult:
    full_comparison_address: ProjectSpecificBigQueryAddress
    differences_output_address: ProjectSpecificBigQueryAddress
    ignored_columns_to_types: Dict[str, str]
    comparison_stats_df: pd.DataFrame

    @property
    def count_original(self) -> int:
        return self._get_comparison_val(STATS_COUNT_ORIGINAL_COL)

    @property
    def count_new(self) -> int:
        return self._get_comparison_val(STATS_COUNT_NEW_COL)

    @property
    def count_in_both(self) -> int:
        return self._get_comparison_val(STATS_IN_BOTH_COL)

    def _get_comparison_val(self, col_name: str) -> int:
        count = self.comparison_stats_df.loc[0, col_name]
        if pd.isnull(count):
            return 0
        return count


def compare_table_or_view(
    *,
    address_original: ProjectSpecificBigQueryAddress,
    address_new: ProjectSpecificBigQueryAddress,
    comparison_output_dataset_id: str,
    primary_keys: Optional[List[str]],
    grouping_columns: Optional[List[str]],
) -> CompareTablesResult:
    """Compares the data at two different BigQuery addresses, emits a full comparison
    table and a differences table, and returns a result that includes statistics about
    the number of matching rows in both views, in just the original view, and just the
    new view.
    """
    output_project_id = address_original.project_id
    # Setup + determine which columns we'll be comparing
    bq_client = BigQueryClientImpl(project_id=output_project_id)
    comparison_output_dataset_ref = bq_client.dataset_ref_for_id(
        comparison_output_dataset_id
    )

    bq_client.create_dataset_if_necessary(
        comparison_output_dataset_ref, TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
    )

    column_df, ignored_df = _get_columns_to_compare_and_ignore(address_original)
    new_column_df, _ = _get_columns_to_compare_and_ignore(address_new)
    column_names = column_df["column_name"].tolist()
    new_column_names = new_column_df["column_name"].tolist()
    if column_names != new_column_names:
        raise ValueError(
            f"Original and new datasets comparable columns do not match. "
            f"Original: {column_names}. New: {new_column_names}"
        )

    # If primary keys were not specified, try to find columns that are some form of identifier
    if primary_keys is None:
        logging.warning(
            "--primary_keys was not set, finding a suitable primary key(s) for comparison."
        )
        primary_keys = []
        id_col = next((col for col in _POTENTIAL_ID_COLS if col in column_names), None)
        if id_col is None:
            primary_keys.extend(column_names)
        else:
            primary_keys.append(id_col)
            if "state_code" in column_names:
                primary_keys.append("state_code")
        logging.warning("Using %s as primary keys", primary_keys)

    # Construct a query that tells us which rows are in original, new, or both
    query = get_comparison_query(
        column_names,
        table_name_orig=address_original.format_address_for_query(),
        table_name_new=address_new.format_address_for_query(),
        id_col=primary_keys[0],
    )

    # Create _full table to store results of the above query
    full_comparison_address = ProjectSpecificBigQueryAddress(
        project_id=output_project_id,
        dataset_id=comparison_output_dataset_id,
        table_id=f"{address_original.table_id}_full",
    )

    full_comparison_address.to_str()

    insert_job = bq_client.create_table_from_query_async(
        dataset_id=full_comparison_address.dataset_id,
        table_id=full_comparison_address.table_id,
        query=query,
        overwrite=True,
        use_query_cache=True,
    )
    insert_job.result()

    # Construct query to show only the actual differences between the two tables
    difference_query = get_difference_query(
        column_names,
        table_name_orig=address_original.format_address_for_query(),
        table_name_new=address_new.format_address_for_query(),
        primary_keys=primary_keys,
    )

    # Create _differences table to store results of the above query
    differences_output_address = ProjectSpecificBigQueryAddress(
        project_id=output_project_id,
        dataset_id=comparison_output_dataset_id,
        table_id=f"{address_original.table_id}_differences",
    )
    insert_job = bq_client.create_table_from_query_async(
        dataset_id=differences_output_address.dataset_id,
        table_id=differences_output_address.table_id,
        query=difference_query,
        overwrite=True,
        use_query_cache=True,
    )
    insert_job.result()

    stats_query = (
        StrictStringFormatter().format(
            _STATS_QUERY_WITH_GROUPING,
            comparison_output_table=full_comparison_address.to_str(),
            grouping_columns=format_lines_for_query(grouping_columns),
            group_order=format_lines_for_query(
                [str(i + 1) for i in range(len(grouping_columns))], ", "
            ),
        )
        if grouping_columns
        else StrictStringFormatter().format(
            _STATS_QUERY_NO_GROUPING,
            comparison_output_table=full_comparison_address.to_str(),
        )
    )
    logging.info("Running Query:\n%s", stats_query)
    stats = pd.read_gbq(stats_query)
    stats[STATS_NEW_ERROR_RATE_COL] = (
        stats[STATS_IN_NEW_NOT_ORIGINAL_COL] / stats[STATS_COUNT_NEW_COL]
    )
    stats[STATS_ORIGINAL_ERROR_RATE_COL] = (
        stats[STATS_IN_ORIGINAL_NOT_NEW_COL] / stats[STATS_COUNT_ORIGINAL_COL]
    )

    return CompareTablesResult(
        full_comparison_address=full_comparison_address,
        differences_output_address=differences_output_address,
        comparison_stats_df=stats,
        ignored_columns_to_types={
            row.column_name: row.data_type for row in ignored_df.itertuples()
        },
    )
