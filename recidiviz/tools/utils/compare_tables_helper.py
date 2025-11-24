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
from typing import List, Optional

import attr
import pandas as pd

from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.constants import TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
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

_DIRECTLY_COMPARABLE_DATA_TYPES = [
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


def get_comparison_query(
    column_names: List[str],
    table_name_orig: str,
    table_name_new: str,
    ignore_case: bool,
) -> str:
    column_names_str = ", ".join(column_names)
    json_table = "TO_JSON_STRING(t)" if not ignore_case else "LOWER(TO_JSON_STRING(t))"
    return f"""
    WITH original_rows AS (
      SELECT 
        FARM_FINGERPRINT({json_table}) AS __comparison_key, {column_names_str}
      FROM (SELECT {column_names_str} FROM {table_name_orig}) t
    ),
    new_rows AS (
      SELECT 
        FARM_FINGERPRINT({json_table}) AS __comparison_key, {column_names_str}
      FROM (SELECT {column_names_str} FROM {table_name_new}) t
    )

    SELECT
      COALESCE(original_rows.__comparison_key, new_rows.__comparison_key) AS __comparison_key,
      {format_lines_for_query(
        [
            f"IFNULL(original_rows.{col}, new_rows.{col}) AS {col}"
            for col in column_names
        ]
      )},
      original_rows.__comparison_key IS NOT NULL AND new_rows.__comparison_key IS NOT NULL AS in_both,
      original_rows.__comparison_key IS NOT NULL AS in_original,
      new_rows.__comparison_key IS NOT NULL AS in_new
    FROM original_rows
    FULL OUTER JOIN new_rows
    ON original_rows.__comparison_key = new_rows.__comparison_key
    """


def get_difference_query(
    columns_df: pd.DataFrame,
    full_comparison_table_address: ProjectSpecificBigQueryAddress,
    primary_keys: Optional[List[str]],
    ignore_case: bool,
) -> str:
    """Constructs a query that shows each difference between two tables or views."""
    all_column_names = columns_df["column_name"].tolist()
    directly_comparable_column_names = columns_df[columns_df.is_directly_comparable][
        "column_name"
    ].tolist()
    primary_keys = primary_keys or ["__comparison_key"]

    def format_for_difference(col: str) -> str:
        if col in primary_keys:
            return col

        if col in directly_comparable_column_names:
            col_comparison_str = (
                f"original_rows.{col} = new_rows.{col}"
                if not ignore_case
                else f"LOWER(original_rows.{col}) = LOWER(new_rows.{col})"
            )
        else:
            # If we can't compare the values directly (e.g. for ARRAY types),
            # we convert to JSON first.
            col_comparison_str = (
                f"TO_JSON_STRING(original_rows.{col}) = TO_JSON_STRING(new_rows.{col})"
                if not ignore_case
                else f"LOWER(TO_JSON_STRING(original_rows.{col})) = LOWER(TO_JSON_STRING(new_rows.{col}))"
            )

        return f"IF({col_comparison_str}, NULL, STRUCT(original_rows.{col} AS o, new_rows.{col} AS n)) AS {col}"

    return f"""
    SELECT
      {format_lines_for_query([format_for_difference(col) for col in all_column_names])},
      original_rows.__comparison_key IS NOT NULL AS in_original,
      new_rows.__comparison_key IS NOT NULL AS in_new
    FROM (
      SELECT * EXCEPT(in_original, in_new, in_both)
      FROM {full_comparison_table_address.format_address_for_query()} t
      WHERE in_original AND NOT in_both
    ) original_rows
    FULL OUTER JOIN (
      SELECT * EXCEPT(in_original, in_new, in_both)
      FROM {full_comparison_table_address.format_address_for_query()} t
      WHERE in_new AND NOT in_both
    ) new_rows
    USING(
      {format_lines_for_query(primary_keys)}
    )
    WHERE IFNULL(original_rows.__comparison_key, 0) != IFNULL(new_rows.__comparison_key, 0)
    ORDER BY in_original DESC, in_new DESC
"""


def _get_columns_info_df(
    address: ProjectSpecificBigQueryAddress,
) -> pd.DataFrame:
    column_query = StrictStringFormatter().format(
        _COLUMNS_QUERY,
        project_id=address.project_id,
        dataset=address.dataset_id,
        table_name=address.table_id,
    )
    logging.info("Running query:\n%s", column_query)
    column_df = pd.read_gbq(column_query)
    column_df["is_directly_comparable"] = column_df.data_type.isin(
        _DIRECTLY_COMPARABLE_DATA_TYPES
    )
    return column_df


@attr.define(kw_only=True)
class CompareTablesResult:
    full_comparison_address: ProjectSpecificBigQueryAddress
    differences_output_address: ProjectSpecificBigQueryAddress
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

    @property
    def count_original_not_new(self) -> int:
        return self._get_comparison_val(STATS_IN_ORIGINAL_NOT_NEW_COL)

    @property
    def count_new_not_original(self) -> int:
        return self._get_comparison_val(STATS_IN_NEW_NOT_ORIGINAL_COL)

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
    ignore_case: bool = False,
    ignore_columns: Optional[List[str]] = None,
) -> CompareTablesResult:
    """Compares the data at two different BigQuery addresses, emits a full comparison
    table and a differences table, and returns a result that includes statistics about
    the number of matching rows in both views, in just the original view, and just the
    new view.
    """
    output_project_id = address_original.project_id
    # Setup + determine which columns we'll be comparing
    bq_client = BigQueryClientImpl(project_id=output_project_id)

    # check that original and new addresses are valid
    invalid_addresses: list[BigQueryAddress] = []
    for table_address in [address_original, address_new]:
        project_agnostic_address = table_address.to_project_agnostic_address()
        if not bq_client.table_exists(project_agnostic_address):
            invalid_addresses.append(project_agnostic_address)
    if invalid_addresses:
        raise ValueError(
            f"One or more tables to compare do not exist:\n{BigQueryAddress.addresses_to_str(invalid_addresses, indent_level=2)}"
        )

    bq_client.create_dataset_if_necessary(
        comparison_output_dataset_id,
        default_table_expiration_ms=TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
    )

    columns_df = _get_columns_info_df(address_original)
    all_column_names = columns_df["column_name"].tolist()
    new_columns_df = _get_columns_info_df(address_new)
    new_all_column_names = new_columns_df["column_name"].tolist()

    if ignore_columns:
        for ignore_col in ignore_columns:
            # Raise an error if the ignore column is not found in at least one of the tables
            if (ignore_col not in all_column_names) and (
                ignore_col not in new_all_column_names
            ):
                raise ValueError(
                    f"Column to ignore [{ignore_col}] not found in either table"
                )
            if ignore_col in all_column_names:
                all_column_names.remove(ignore_col)
            if ignore_col in new_all_column_names:
                new_all_column_names.remove(ignore_col)

        # Drop the ignore columns before generating the comparison queries below
        columns_df = columns_df[~columns_df["column_name"].isin(ignore_columns)]

    if sorted(all_column_names) != sorted(new_all_column_names):
        raise ValueError(
            f"Original and new views comparable columns do not match. "
            f"Original: {all_column_names}. New: {new_all_column_names}"
        )

    # If primary keys were not specified, try to find columns that are some form of identifier
    if primary_keys is None:
        logging.warning(
            "--primary_keys was not set, finding a suitable primary key(s) for comparison."
        )
        id_col = next(
            (col for col in _POTENTIAL_ID_COLS if col in all_column_names), None
        )
        if id_col is not None:
            primary_keys = [id_col]
            if "state_code" in all_column_names:
                primary_keys.append("state_code")
        logging.warning("Using %s as primary keys", primary_keys)

    # Construct a query that tells us which rows are in original, new, or both
    query = get_comparison_query(
        all_column_names,
        table_name_orig=address_original.format_address_for_query(),
        table_name_new=address_new.format_address_for_query(),
        ignore_case=ignore_case,
    )

    # Create _full table to store results of the above query
    full_comparison_address = ProjectSpecificBigQueryAddress(
        project_id=output_project_id,
        dataset_id=comparison_output_dataset_id,
        table_id=f"{address_original.table_id}_full",
    )

    bq_client.create_table_from_query(
        address=full_comparison_address.to_project_agnostic_address(),
        query=query,
        overwrite=True,
        use_query_cache=True,
    )

    # Construct query to show only the actual differences between the two tables
    difference_query = get_difference_query(
        columns_df=columns_df,
        full_comparison_table_address=full_comparison_address,
        primary_keys=primary_keys,
        ignore_case=ignore_case,
    )

    # Create _differences table to store results of the above query
    differences_output_address = ProjectSpecificBigQueryAddress(
        project_id=output_project_id,
        dataset_id=comparison_output_dataset_id,
        table_id=f"{address_original.table_id}_differences",
    )
    bq_client.create_table_from_query(
        address=differences_output_address.to_project_agnostic_address(),
        query=difference_query,
        overwrite=True,
        use_query_cache=True,
    )

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
    )
