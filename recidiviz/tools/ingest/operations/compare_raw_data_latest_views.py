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
This script compares raw data latest views to a sandbox load of the latest views, outputting comparison results to BigQuery tables.

The script is intended to be used after running load_raw_data_latest_views_to_sandbox.py

Example usage:
    python -m recidiviz.tools.ingest.operations.compare_referenced_raw_data_latest_views \
        --state-code US_OZ \
        --sandbox-dataset-prefix my_sandbox \
        --sandbox-raw-data-instance SECONDARY \
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
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_latest_view_collector import (
    RAW_DATA_LATEST_VIEW_ID_SUFFIX,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder_collector import (
    DirectIngestViewQueryBuilderCollector,
)
from recidiviz.tools.ingest.operations.constants import (
    FILL_CHAR,
    LINE_SEPARATOR,
    LINE_WIDTH,
    RAW_DATA_DIFF_RESULTS_DATASET_ID,
)
from recidiviz.tools.raw_data_reference_reasons_yaml_loader import (
    RawDataReferenceReasonsYamlLoader,
)
from recidiviz.tools.utils.compare_tables_helper import (
    CompareTablesResult,
    compare_table_or_view,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING


def _compare_latest_view_to_sandbox(
    latest_dataset_id: str,
    sandbox_dataset_id: str,
    comparison_output_dataset_id: str,
    view_name: str,
    project_id: str,
) -> CompareTablesResult:
    """Compare a latest view between production and sandbox datasets.

    Args:
        latest_dataset_id: The dataset ID containing the production latest views
        sandbox_dataset_id: The dataset ID containing the sandbox latest views
        comparison_output_dataset_id: The dataset ID to write comparison results to
        view_name: The name of the specific view to compare
        project_id: The GCP project ID where both datasets exist

    Returns:
        CompareTablesResult containing comparison statistics and output table addresses
    """

    result = compare_table_or_view(
        address_original=ProjectSpecificBigQueryAddress(
            project_id=project_id,
            dataset_id=latest_dataset_id,
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


def _collect_file_tags_with_downstream_references(state_code: StateCode) -> set[str]:
    """Collect all raw data file tags that have downstream references.

    Identifies raw data files that are either:
    1. Referenced by ingest views
    2. Have explicit downstream references defined in raw_data_reference_reasons.yaml

    Args:
        state_code: The state code to collect file tags for

    Returns:
        Set of file tags that have downstream dependencies
    """
    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value.lower()
    )
    view_collector = DirectIngestViewQueryBuilderCollector(region)

    referenced_file_tags = set()
    for ingest_view in view_collector.get_query_builders():
        referenced_file_tags.update(ingest_view.raw_data_table_dependency_file_tags)

    raw_data_references = (
        RawDataReferenceReasonsYamlLoader.get_downstream_referencing_views(state_code)
    )
    referenced_file_tags.update(raw_data_references)
    return referenced_file_tags


def _collect_file_tags(state_code: StateCode, referenced_views_only: bool) -> set[str]:
    """Collect all raw data file tags, optionally filtering to only those with downstream references.

    Args:
        state_code: The state code to collect file tags for
        referenced_views_only: If True, only include file tags that have downstream references

    Returns:
        Set of file tags
    """
    if referenced_views_only:
        return _collect_file_tags_with_downstream_references(state_code)

    region_raw_file_config = get_region_raw_file_config(state_code.value)
    return region_raw_file_config.raw_file_tags


def compare_latest_views_to_sandbox(
    *,
    state_code: StateCode,
    sandbox_dataset_prefix: str,
    sandbox_raw_data_instance: DirectIngestInstance,
    project_id: str,
    comparison_output_dataset_id: str,
    referenced_views_only: bool,
) -> None:
    """Compare all referenced latest views between deployed and sandbox datasets.

    This function orchestrates the comparison of raw data latest views that have
    downstream references. It runs comparisons in parallel and logs statistics
    about differences found.

    Args:
        state_code: The state to compare views for (e.g., StateCode.US_CA)
        sandbox_dataset_prefix: Prefix used for the sandbox dataset name
        sandbox_raw_data_instance: The raw data instance used in the sandbox
            (PRIMARY or SECONDARY)
        project_id: The GCP project ID containing both datasets
        comparison_output_dataset_id: The dataset ID to write comparison results to
    """
    referenced_file_tags = _collect_file_tags(state_code, referenced_views_only)

    latest_dataset_id = raw_latest_views_dataset_for_region(
        state_code=state_code,
        instance=DirectIngestInstance.PRIMARY,
    )
    sandbox_dataset_id = raw_latest_views_dataset_for_region(
        state_code=state_code,
        instance=sandbox_raw_data_instance,
        sandbox_dataset_prefix=sandbox_dataset_prefix,
    )

    view_names = [
        f"{file_tag}{RAW_DATA_LATEST_VIEW_ID_SUFFIX}"
        for file_tag in sorted(referenced_file_tags)
    ]
    logging.info(
        "Comparing %d latest views for state [%s] between datasets [%s] and [%s]",
        len(view_names),
        state_code.value,
        latest_dataset_id,
        sandbox_dataset_id,
    )

    comparison_results: dict[str, CompareTablesResult] = {}
    with futures.ThreadPoolExecutor(
        max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
    ) as executor:
        future_to_view_name = {
            executor.submit(
                _compare_latest_view_to_sandbox,
                latest_dataset_id=latest_dataset_id,
                sandbox_dataset_id=sandbox_dataset_id,
                comparison_output_dataset_id=comparison_output_dataset_id,
                view_name=view_name,
                project_id=project_id,
            ): view_name
            for view_name in view_names
        }
        for future in concurrent.futures.as_completed(future_to_view_name):
            view_name = future_to_view_name[future]
            try:
                comparison_results[view_name] = future.result()
            except Exception as exc:
                logging.error(
                    "View comparison generated an exception for view [%s]: %s",
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
            "VIEW [%s] DIFFERS:"
            "\n  - %d total rows in original"
            "\n  - %d total rows in sandbox"
            "\n  - %d rows only in original"
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
        logging.info("SUCCESS: No differences found across all compared views.")
        logging.info(LINE_SEPARATOR)
        return

    if equal_views:
        logging.info("EQUAL VIEWS:\n - %s", "\n - ".join(equal_views))
    else:
        logging.info("No equal views found.")
    logging.info(LINE_SEPARATOR)
    logging.info("DIFFERING VIEWS:\n - %s", "\n - ".join(differing_views))
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
        help="The state code to compare views for.",
    )
    parser.add_argument(
        "--sandbox-dataset-prefix",
        type=str,
        required=True,
        help="A prefix used for the sandbox dataset.",
    )
    parser.add_argument(
        "--sandbox-raw-data-instance",
        type=DirectIngestInstance,
        required=True,
        help="The raw data instance used when loading latest views to the sandbox.",
    )
    parser.add_argument(
        "--project-id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        default=GCP_PROJECT_STAGING,
        help="The GCP project ID to use for the comparison.",
    )
    parser.add_argument(
        "--referenced-views-only",
        action="store_true",
        help="Compare only views for raw data files that are referenced by ingest views",
        default=False,
    )
    return parser.parse_args()


def main() -> None:
    """Main entry point for the comparison script."""
    args = _parse_arguments()
    compare_latest_views_to_sandbox(
        state_code=args.state_code,
        sandbox_dataset_prefix=args.sandbox_dataset_prefix,
        sandbox_raw_data_instance=args.sandbox_raw_data_instance,
        project_id=args.project_id,
        comparison_output_dataset_id=RAW_DATA_DIFF_RESULTS_DATASET_ID,
        referenced_views_only=args.referenced_views_only,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
