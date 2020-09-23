# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Helpers for building internal consistency queries that sum values across breakdown rows and make sure they match the
metric total rows.
"""
from typing import List, Optional


# TODO(#3839): Simplify this via metric_big_query_view.dimensions
def _metric_totals_table_query(partition_columns: List[str],
                               mutually_exclusive_breakdown_columns: List[str],
                               calculated_columns_to_validate: List[str]) -> str:
    """Builds a sub-table query for a table called metric_totals, which contains one row per unique partition column
    value combination per calculated column to validate, with a value column with the metric value where all breakdown
    columns are 'ALL'.
    """

    single_col_sum_queries = []
    partition_columns_str = ', '.join(partition_columns)

    for column_name in calculated_columns_to_validate:
        single_col_sum_query = \
            f"  SELECT {partition_columns_str}, \'{column_name}\' AS value_name, {column_name} AS value " \
            f"FROM metric_sum_row "

        single_col_sum_queries.append(single_col_sum_query)

    sums_query = "\n  UNION ALL\n".join(single_col_sum_queries)

    where_filter = ' AND '.join([f'{mutually_exclusive_breakdown_column} = \'ALL\''
                                 for mutually_exclusive_breakdown_column in mutually_exclusive_breakdown_columns])

    full_template = f"metric_sum_row AS (\n" \
        f"  SELECT * \n" \
        f"  FROM `{{project_id}}.{{validated_table_dataset_id}}.{{validated_table_id}}`\n" \
        f"  WHERE {where_filter}\n" \
        f"),\n" \
        f"metric_totals AS (\n" \
        f"{sums_query}\n" \
        f")"
    return full_template


def _breakdown_sums_table_query(partition_columns: List[str],
                                mutually_exclusive_breakdown_column: str,
                                calculated_columns_to_validate: List[str]) -> str:
    """Builds a sub-table query for a table which contains one row per unique partition column value combination per
    calculated column to validate, with a value column with the SUM of all rows where where the
    mutually_exclusive_breakdown_column is not 'ALL'.
    """

    rows_table_name = f'{mutually_exclusive_breakdown_column}_breakdown_rows'

    single_col_sum_queries = []
    partition_columns_str = ', '.join(partition_columns)

    for column_name in calculated_columns_to_validate:
        single_col_sum_query = \
            f"  SELECT {partition_columns_str}, \'{column_name}\' AS value_name, SUM({column_name}) AS value " \
            f"FROM {rows_table_name} " \
            f"GROUP BY {partition_columns_str}"

        single_col_sum_queries.append(single_col_sum_query)

    sums_query = "\n  UNION ALL\n".join(single_col_sum_queries)

    full_template = f"{rows_table_name} AS (\n" \
        f"  SELECT * \n" \
        f"  FROM `{{project_id}}.{{validated_table_dataset_id}}.{{validated_table_id}}`\n" \
        f"  WHERE {mutually_exclusive_breakdown_column} != 'ALL'\n" \
        f"),\n" \
        f"{mutually_exclusive_breakdown_column}_breakdown_sums AS (\n" \
        f"{sums_query}\n" \
        f")"
    return full_template


INTERNAL_CONSISTENCY_QUERY_TEMPLATE = """WITH
{metric_totals_table_query_str},
{breakdown_sums_table_queries_str}
SELECT 
    {partition_columns_str_renamed_state_code},
    value_name,
    metric_totals.value AS metric_total,
    {breakdown_select_col_clauses_str}
FROM 
  metric_totals
{breakdown_table_join_clauses_str}
ORDER BY {order_by_cols}
"""


def internal_consistency_query(partition_columns: List[str],
                               mutually_exclusive_breakdown_columns: List[str],
                               calculated_columns_to_validate: List[str],
                               non_mutually_exclusive_breakdown_columns: Optional[List[str]] = None):
    """
    Builds and returns a query that can be used to ensure that the various metric breakdowns sum to the values
    represented in the aggregate ('ALL') columns in each query.

    Args:
        partition_columns: A list of columns that partition the data such that it wouldn't make sense to sum across
            different values of that partition (e.g. columns that are not calculated and also never have 'ALL' as a
            value).
        mutually_exclusive_breakdown_columns: A list of dimensional breakdown columns where only one is not 'ALL' at a
            time. Often the demographic breakdown columns in the query.
        non_mutually_exclusive_breakdown_columns: A list of dimensional breakdown columns where only one is not 'ALL'
            at a time, but where people may be counted towards more than one category. We do not expect the sums
            across non-ALL categories to be equal to the overall metric totals.
        calculated_columns_to_validate: A list of numerical columns whose sum across the breakdown dimensions should
            equal the value in the row where all breakdown dimensions are 'ALL'
    """

    partition_columns_str = ', '.join(partition_columns)

    breakdown_columns = mutually_exclusive_breakdown_columns

    if non_mutually_exclusive_breakdown_columns:
        breakdown_columns += non_mutually_exclusive_breakdown_columns

    metric_totals_table_query_str = _metric_totals_table_query(
        partition_columns=partition_columns,
        mutually_exclusive_breakdown_columns=breakdown_columns,
        calculated_columns_to_validate=calculated_columns_to_validate)

    breakdown_sums_table_queries = []
    breakdown_select_col_clauses = []
    breakdown_table_join_clauses = []
    for col in mutually_exclusive_breakdown_columns:
        breakdown_sums_table_queries.append(
            _breakdown_sums_table_query(partition_columns=partition_columns,
                                        mutually_exclusive_breakdown_column=col,
                                        calculated_columns_to_validate=calculated_columns_to_validate)
        )

        breakdown_select_col_clauses.append(f'{col}_breakdown_sums.value AS {col}_breakdown_sum')

        breakdown_table_join_clause = f"FULL OUTER JOIN\n" \
                                      f"  {col}_breakdown_sums\n" \
                                      f"USING({partition_columns_str}, value_name)"
        breakdown_table_join_clauses.append(breakdown_table_join_clause)

    breakdown_sums_table_queries_str = ',\n'.join(breakdown_sums_table_queries)
    breakdown_select_col_clauses_str = ',\n    '.join(breakdown_select_col_clauses)
    breakdown_table_join_clauses_str = '\n'.join(breakdown_table_join_clauses)

    partition_columns_str_renamed_state_code = ', '.join(
        [col if col != 'state_code' else 'state_code AS region_code' for col in partition_columns])

    return INTERNAL_CONSISTENCY_QUERY_TEMPLATE.format(
        partition_columns_str_renamed_state_code=partition_columns_str_renamed_state_code,
        metric_totals_table_query_str=metric_totals_table_query_str,
        breakdown_sums_table_queries_str=breakdown_sums_table_queries_str,
        breakdown_select_col_clauses_str=breakdown_select_col_clauses_str,
        breakdown_table_join_clauses_str=breakdown_table_join_clauses_str,
        order_by_cols=partition_columns_str
    )
