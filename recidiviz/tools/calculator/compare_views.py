# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Script to record the differences between two views.

When run, creates two tables and prints out statistics about the run:
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
state_code  count_original  count_new  in_both  in_original_not_new  in_new_not_original
     US_XX               2          2        1                    1                    1
     US_YY               1          1        0                    0                    1

Note: in the statistics+full output, a row with the same primary key values but different other
values will count as two rows: one in_original and one in_new.

This can be run with the following command:
    python -m recidiviz.tools.calculator.compare_views \
        --dataset_original DATASET_ORIGINAL \
        --dataset_new DATASET_NEW \
        --view_id VIEW_ID \
        [--view_id_new VIEW_ID_NEW] \
        --output_dataset_prefix OUTPUT_DATASET_PREFIX \
        [--primary_keys PRIMARY_KEYS] \
        [--grouping_columns GROUPING_COLUMNS] \
        [--project_id {recidiviz-staging,recidiviz-123}] \
        [--log_level {DEBUG,INFO,WARNING,ERROR,FATAL,CRITICAL}]

Example usage:
    python -m recidiviz.tools.calculator.compare_views \
        --dataset_original dashboard_views \
        --dataset_new dana_pathways_dashboard_views \
        --view_id supervision_to_prison_transitions_materialized \
        --output_dataset_prefix dana_pathways \
        --grouping_columns state_code

For more info on what each argument does, run:
    python -m recidiviz.tools.calculator.compare_views --help
"""

import argparse
import logging
import sys
from typing import List, Optional, Tuple

import pandas as pd

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.view_update_manager import (
    TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS,
)
from recidiviz.utils.params import str_to_list
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
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
    FROM `{project_id}.{dataset_original}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = "{table_name}"
"""

_STATS_QUERY_SUMS = """
      SUM(IF(in_original,1,0)) AS count_original,
      SUM(IF(in_new,1,0)) AS count_new,
      SUM(IF(in_original AND in_new,1,0)) AS in_both,
      SUM(IF(in_original AND NOT in_new,1,0)) AS in_original_not_new,
      SUM(IF(not in_original AND in_new,1,0)) AS in_new_not_original
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


def get_columns_to_compare_and_ignore(
    project_id: str, dataset: str, table_name: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    column_query = StrictStringFormatter().format(
        _COLUMNS_QUERY,
        project_id=project_id,
        dataset_original=dataset,
        table_name=table_name,
    )
    logging.info("Running query:\n%s", column_query)
    column_df = pd.read_gbq(column_query)
    column_df["is_comparable"] = column_df.data_type.isin(_DATA_TYPES_TO_COMPARE)
    return column_df[column_df.is_comparable], column_df[~column_df.is_comparable]


def compare_view(
    *,
    dataset_original: str,
    dataset_new: str,
    view_id: str,
    view_id_new: Optional[str],
    output_dataset_prefix: str,
    primary_keys: Optional[List[str]],
    grouping_columns: Optional[List[str]],
    project_id: str,
) -> None:
    """Compares two versions of a view, emits a full comparison table and a differences table, and
    prints statistics about the number of rows in both views, the original view, and the new view.
    """
    # Setup + determine which columns we'll be comparing
    if view_id_new is None:
        view_id_new = view_id
    bq_client = BigQueryClientImpl(project_id=project_id)
    comparison_output_dataset_id = (
        f"{output_dataset_prefix}_{dataset_original}_comparison_output"
    )
    comparison_output_dataset_ref = bq_client.dataset_ref_for_id(
        comparison_output_dataset_id
    )

    bq_client.create_dataset_if_necessary(
        comparison_output_dataset_ref, TEMP_DATASET_DEFAULT_TABLE_EXPIRATION_MS
    )

    column_df, ignored_df = get_columns_to_compare_and_ignore(
        project_id, dataset_original, view_id
    )
    new_column_df, _ = get_columns_to_compare_and_ignore(
        project_id, dataset_new, view_id_new
    )
    column_names = column_df["column_name"].tolist()
    if not column_df.equals(new_column_df):
        raise ValueError(
            f"Original and new datasets comparable columns do not match. Original: {column_names}. "
            f"New: {new_column_df['column_name'].tolist()}"
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
    table_name_orig = f"`{project_id}.{dataset_original}.{view_id}`"
    table_name_new = f"`{project_id}.{dataset_new}.{view_id_new}`"
    query = get_comparison_query(
        column_names,
        table_name_orig=table_name_orig,
        table_name_new=table_name_new,
        id_col=primary_keys[0],
    )

    # Create _full table to store results of the above query
    full_table_name = f"{view_id}_full"
    comparison_output_table = (
        f"{project_id}.{comparison_output_dataset_id}.{full_table_name}"
    )
    insert_job = bq_client.create_table_from_query_async(
        dataset_id=comparison_output_dataset_id,
        table_id=full_table_name,
        query=query,
        overwrite=True,
        use_query_cache=True,
    )
    insert_job.result()
    print(f"Full comparison available at `{comparison_output_table}`")

    # Construct query to show only the actual differences between the two tables
    difference_query = get_difference_query(
        column_names,
        table_name_orig=table_name_orig,
        table_name_new=table_name_new,
        primary_keys=primary_keys,
    )

    # Create _differences table to store results of the above query
    differences_table_name = f"{view_id}_differences"
    insert_job = bq_client.create_table_from_query_async(
        dataset_id=comparison_output_dataset_id,
        table_id=differences_table_name,
        query=difference_query,
        overwrite=True,
        use_query_cache=True,
    )
    insert_job.result()
    differences_output_table = (
        f"{project_id}.{comparison_output_dataset_id}.{differences_table_name}"
    )
    print(f"Differences available at `{differences_output_table}`")

    stats_query = (
        StrictStringFormatter().format(
            _STATS_QUERY_WITH_GROUPING,
            comparison_output_table=comparison_output_table,
            grouping_columns=format_lines_for_query(grouping_columns),
            group_order=format_lines_for_query(
                [str(i + 1) for i in range(len(grouping_columns))], ", "
            ),
        )
        if grouping_columns
        else StrictStringFormatter().format(
            _STATS_QUERY_NO_GROUPING,
            comparison_output_table=comparison_output_table,
        )
    )
    logging.info("Running Query:\n%s", stats_query)

    print("\n*****COMPARISON STATS*****")
    stats_result = pd.read_gbq(stats_query)
    print(stats_result.to_string(index=False))
    for row in ignored_df.itertuples():
        logging.warning(
            "Ignored column %s of unsupported type %s", row.column_name, row.data_type
        )


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the arguments needed to call the compare_view function."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dataset_original",
        type=str,
        required=True,
        help="Original dataset to compare",
    )
    parser.add_argument(
        "--dataset_new", type=str, required=True, help="New dataset to compare"
    )
    parser.add_argument("--view_id", type=str, required=True, help="View ID to compare")
    parser.add_argument(
        "--view_id_new",
        type=str,
        help="View ID for the 'new' view in the comparison. If not set, will use the same value as "
        "--view_id.",
    )
    parser.add_argument(
        "--output_dataset_prefix",
        type=str,
        required=True,
        help="Prefix for the comparison output dataset. Output location will be "
        "`{project_id}.{output_dataset_prefix}_{dataset}_comparison_output.view_[full|differences]`",
    )
    parser.add_argument(
        "--primary_keys",
        type=str_to_list,
        help="A comma-separated list of columns that can be used to uniquely identify a row. If "
        "unset, will pick from a hardcoded list of id-like columns, plus state_code if the view "
        "contains that column. If no ID column is found, will compare based on an exact match of "
        "all column values.",
    )
    parser.add_argument(
        "--grouping_columns",
        type=str_to_list,
        help="A comma-separated list of columns to group final statistics by.",
    )
    parser.add_argument(
        "--project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        default=GCP_PROJECT_STAGING,
    )
    parser.add_argument(
        "--log_level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "FATAL", "CRITICAL"],
        default="WARNING",
        help="Log level for the script. Set to INFO or higher to log queries that are run.",
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    known_args, _ = parse_arguments(sys.argv)
    logging.basicConfig(level=known_args.log_level)
    compare_view(
        dataset_original=known_args.dataset_original,
        dataset_new=known_args.dataset_new,
        view_id=known_args.view_id,
        view_id_new=known_args.view_id_new,
        output_dataset_prefix=known_args.output_dataset_prefix,
        primary_keys=known_args.primary_keys,
        grouping_columns=known_args.grouping_columns,
        project_id=known_args.project_id,
    )
