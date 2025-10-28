# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
This script compares ingest view results between deployed and sandbox datasets, outputting comparison results to BigQuery tables.

Example usage:
    python -m recidiviz.tools.ingest.operations.compare_ingest_view_results_datasets \
        --state-code US_CA \
        --sandbox-dataset-prefix my_sandbox \
        [--project-id recidiviz-123]
"""
import argparse
import concurrent
import logging
from concurrent import futures

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.big_query.big_query_client import BQ_CLIENT_MAX_POOL_SIZE
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.pipelines.ingest.dataset_config import (
    ingest_view_materialization_results_dataset,
)
from recidiviz.tools.ingest.operations.constants import (
    FILL_CHAR,
    INGEST_VIEW_DIFF_RESULTS_DATASET_ID,
    LINE_SEPARATOR,
    LINE_WIDTH,
)
from recidiviz.tools.utils.compare_tables_helper import (
    CompareTablesResult,
    compare_table_or_view,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING


def _compare_ingest_view_to_sandbox(
    ingest_views_dataset_id: str,
    sandbox_dataset_id: str,
    comparison_output_dataset_id: str,
    view_name: str,
    project_id: str,
) -> CompareTablesResult:
    """Compare an ingest view result between deployed and sandbox datasets.

    Args:
        ingest_views_dataset_id: The dataset ID containing the deployed ingest view results
        sandbox_dataset_id: The dataset ID containing the sandbox ingest view results
        comparison_output_dataset_id: The dataset ID to write comparison results to
        view_name: The name of the specific ingest view result to compare
        project_id: The GCP project ID where both datasets exist

    Returns:
        CompareTablesResult containing comparison statistics and output table addresses
    """

    result = compare_table_or_view(
        address_original=ProjectSpecificBigQueryAddress(
            project_id=project_id,
            dataset_id=ingest_views_dataset_id,
            table_id=view_name,
        ),
        address_new=ProjectSpecificBigQueryAddress(
            project_id=project_id,
            dataset_id=sandbox_dataset_id,
            table_id=view_name,
        ),
        comparison_output_dataset_id=comparison_output_dataset_id,
        primary_keys=None,
        grouping_columns=None,
        ignore_case=False,
        ignore_columns=None,
    )

    return result


def _collect_ingest_view_names(state_code: StateCode) -> list[str]:
    """Collect all ingest view names for the given state.

    Args:
        state_code: The state code to collect ingest view names for

    Returns:
        List of ingest view names
    """
    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value.lower()
    )
    view_collector = DirectIngestViewQueryBuilderCollector(region)

    ingest_view_names = []
    for query_builder in view_collector.get_query_builders():
        ingest_view_names.append(query_builder.ingest_view_name)

    return sorted(ingest_view_names)


def compare_ingest_view_results_to_sandbox(
    *,
    state_code: StateCode,
    sandbox_dataset_prefix: str,
    project_id: str,
    comparison_output_dataset_id: str,
) -> None:
    """Compare all ingest view results between deployed and sandbox datasets.
    Runs comparisons in parallel and logs statistics about differences found.

    Args:
        state_code: The state to compare ingest views for (e.g., StateCode.US_CA)
        sandbox_dataset_prefix: Prefix used for the sandbox dataset name
        project_id: The GCP project ID containing both datasets
        comparison_output_dataset_id: The dataset ID to write comparison results to
    """
    ingest_view_names = _collect_ingest_view_names(state_code)

    ingest_views_dataset_id = ingest_view_materialization_results_dataset(
        state_code=state_code,
    )
    sandbox_dataset_id = ingest_view_materialization_results_dataset(
        state_code=state_code,
        sandbox_dataset_prefix=sandbox_dataset_prefix,
    )

    logging.info(
        "Comparing %d ingest view results for state [%s] between datasets [%s] and [%s]",
        len(ingest_view_names),
        state_code.value,
        ingest_views_dataset_id,
        sandbox_dataset_id,
    )

    comparison_results: dict[str, CompareTablesResult] = {}
    with futures.ThreadPoolExecutor(
        max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
    ) as executor:
        future_to_view_name = {
            executor.submit(
                _compare_ingest_view_to_sandbox,
                ingest_views_dataset_id=ingest_views_dataset_id,
                sandbox_dataset_id=sandbox_dataset_id,
                comparison_output_dataset_id=comparison_output_dataset_id,
                view_name=view_name,
                project_id=project_id,
            ): view_name
            for view_name in ingest_view_names
        }
        for future in concurrent.futures.as_completed(future_to_view_name):
            view_name = future_to_view_name[future]
            try:
                comparison_results[view_name] = future.result()
            except Exception as exc:
                logging.error(
                    "Ingest view comparison generated an exception for view [%s]: %s",
                    view_name,
                    exc,
                )

    differing_views, equal_views = [], []
    for view_name, result in comparison_results.items():
        if result.count_original_not_new == 0 and result.count_new_not_original == 0:
            equal_views.append(view_name)
            continue
        differing_views.append(view_name)
        logging.info(LINE_SEPARATOR)
        logging.info(
            "INGEST VIEW [%s] DIFFERS:"
            "\n  - %d total rows in ingest view results"
            "\n  - %d total rows in sandbox"
            "\n  - %d rows only in ingest view results"
            "\n  - %d rows only in sandbox",
            view_name,
            result.count_original,
            result.count_new,
            result.count_original_not_new,
            result.count_new_not_original,
        )
        logging.info(
            "\nDifferences results table: %s",
            result.differences_output_address.to_str(),
        )

    logging.info(LINE_SEPARATOR)
    logging.info("SUMMARY".ljust(LINE_WIDTH, FILL_CHAR))
    logging.info(LINE_SEPARATOR)
    if not differing_views:
        logging.info("SUCCESS: No differences found across all compared ingest views.")
        logging.info(LINE_SEPARATOR)
        return

    if equal_views:
        logging.info("EQUAL INGEST VIEWS:\n - %s", "\n - ".join(equal_views))
    else:
        logging.info("No equal ingest views found.")
    logging.info(LINE_SEPARATOR)
    logging.info("DIFFERING INGEST VIEWS:\n - %s", "\n - ".join(differing_views))
    logging.info(LINE_SEPARATOR)
    logging.info(
        "Results saved to dataset [%s] in project [%s]",
        comparison_output_dataset_id,
        project_id,
    )
    logging.info(LINE_SEPARATOR)


def _parse_arguments() -> argparse.Namespace:
    """Parse command line arguments for the comparison script."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--state-code",
        type=StateCode,
        required=True,
        help="The state code to compare ingest views for.",
    )
    parser.add_argument(
        "--sandbox-dataset-prefix",
        type=str,
        required=True,
        help="A prefix used for the sandbox dataset.",
    )
    parser.add_argument(
        "--project-id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        default=GCP_PROJECT_STAGING,
        help="The GCP project ID to use for the comparison.",
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point for the comparison script."""
    args = _parse_arguments()
    compare_ingest_view_results_to_sandbox(
        state_code=args.state_code,
        sandbox_dataset_prefix=args.sandbox_dataset_prefix,
        project_id=args.project_id,
        comparison_output_dataset_id=f"{args.sandbox_dataset_prefix}_{INGEST_VIEW_DIFF_RESULTS_DATASET_ID}",
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
