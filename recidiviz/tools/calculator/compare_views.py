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
"""Script to record the differences between two views. For more details about the
output, see docstring in compare_tables_helper.py.

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
from typing import List, Tuple

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.tools.utils.compare_tables_helper import compare_table_or_view
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.params import str_to_list


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
        "--ignore_columns",
        type=str_to_list,
        help="A comma-separated list of columns where changes should be ignored.",
    )
    parser.add_argument(
        "--ignore_case",
        action="store_true",
        help="Compare all columns case-insensitively",
        default=False,
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


def main() -> None:
    """Executes the main flow of the script."""
    known_args, _ = parse_arguments(sys.argv)
    logging.basicConfig(level=known_args.log_level)
    result = compare_table_or_view(
        address_original=ProjectSpecificBigQueryAddress(
            project_id=known_args.project_id,
            dataset_id=known_args.dataset_original,
            table_id=known_args.view_id,
        ),
        address_new=ProjectSpecificBigQueryAddress(
            project_id=known_args.project_id,
            dataset_id=known_args.dataset_new,
            table_id=known_args.view_id_new or known_args.view_id,
        ),
        comparison_output_dataset_id=(
            f"{known_args.output_dataset_prefix}_{known_args.dataset_original}_comparison_output"
        ),
        primary_keys=known_args.primary_keys,
        grouping_columns=known_args.grouping_columns,
        ignore_case=known_args.ignore_case,
        ignore_columns=known_args.ignore_columns,
    )

    print(
        f"Full comparison available at {result.full_comparison_address.format_address_for_query()}"
    )
    print(
        f"Differences available at {result.differences_output_address.format_address_for_query()}"
    )
    print("\n*****COMPARISON STATS*****")
    print(result.comparison_stats_df.to_string(index=False))


if __name__ == "__main__":
    main()
