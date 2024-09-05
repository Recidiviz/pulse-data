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
"""For a given region, compares the raw data in BigQuery.

By default, ensures that all of the rows that exist in primary exist in secondary in staging,
and visa versa. First checks that each table has the same number of distinct file_ids
and rows for each update datetime. If that is successful, it then checks that the
data is matching in both tables.

Example usage:
python -m recidiviz.tools.ingest.operations.compare_raw_data --region us_mo
python -m recidiviz.tools.ingest.operations.compare_raw_data --region us_tn \
    --source-project-id recidiviz-staging --source-ingest-instance PRIMARY \
    --comparison-project-id recidiviz-123 --comparison-ingest-instance PRIMARY \
    --file-tags file_tag_1,file_tag_2
"""
import argparse
import logging
import sys
from typing import Dict, List

from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.ingest.operations.raw_data_region_diff_query_executor import (
    RawDataRegionDiffQueryExecutor,
)
from recidiviz.tools.ingest.operations.raw_table_diff_query_generator import (
    RawTableDiffQueryResult,
)
from recidiviz.tools.ingest.operations.raw_table_file_counts_diff_query_generator import (
    RawTableFileCountsDiffQueryGenerator,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING

LINE_SEPARATOR = "-" * 100


def _log_file_counts_successes(succeeded_tables: List[str]) -> None:
    if not succeeded_tables:
        return
    logging.info("SUCCESSES - Distinct file counts match for tables:")
    logging.error(LINE_SEPARATOR)
    for file_tag in succeeded_tables:
        logging.info("\t- %s", file_tag)
    logging.info(LINE_SEPARATOR)


def _log_file_counts_failures(
    failed_table_results: Dict[str, RawTableDiffQueryResult]
) -> None:
    if not failed_table_results:
        return
    logging.error("FAILURES - Distinct file counts do not match for tables:")
    logging.error(LINE_SEPARATOR)
    for file_tag, result in failed_table_results.items():
        logging.error("%s:", file_tag)
        logging.error(result)
        logging.error(LINE_SEPARATOR)


def _parse_args() -> argparse.Namespace:
    """Parses command line arguments."""

    def comma_separated_list(value: str) -> List[str]:
        return [item.strip() for item in value.split(",")]

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--region", type=str, required=True, help="E.g. 'us_nd'")
    parser.add_argument(
        "--source-project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        default=GCP_PROJECT_STAGING,
        help="Specifies the project id of the raw data to be compared against.",
    )
    parser.add_argument(
        "--source-ingest-instance",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        default=DirectIngestInstance.PRIMARY,
        help="Specifies the ingest instance of the raw data to be compared against.",
    )
    parser.add_argument(
        "--comparison-project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        default=GCP_PROJECT_STAGING,
        help="Specifies the project id of the raw data to be compared to.",
    )
    parser.add_argument(
        "--comparison-ingest-instance",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        default=DirectIngestInstance.SECONDARY,
        help="Specifies the ingest instance of the raw data to be compared to.",
    )
    parser.add_argument(
        "--truncate-update-datetime",
        type=str,
        choices=["SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH"],
        help="The granularity to truncate the update datetime to before comparing. By "
        "default the full datetime is used.",
    )
    parser.add_argument(
        "--file-tags",
        type=comma_separated_list,
        default=[],
        help="Specifies a comma-separated list of file tags to compare. If not provided, all file tags in the datasets will be compared.",
    )
    args = parser.parse_args()
    return args


def main() -> None:
    """Runs the table comparisons and logs results."""
    args = _parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    logging.info(
        "\nComparing raw data for [%s] between (project=[%s], instance=[%s]) and"
        " (project=[%s], instance=[%s])",
        args.region,
        args.source_project_id,
        args.source_ingest_instance.value,
        args.comparison_project_id,
        args.comparison_ingest_instance.value,
    )
    logging.info(LINE_SEPARATOR)

    query_generator = RawTableFileCountsDiffQueryGenerator.create_query_generator(
        region_code=args.region,
        src_project_id=args.source_project_id,
        src_ingest_instance=args.source_ingest_instance,
        cmp_project_id=args.comparison_project_id,
        cmp_ingest_instance=args.comparison_ingest_instance,
        truncate_update_datetime_part=args.truncate_update_datetime,
    )
    query_executor = RawDataRegionDiffQueryExecutor(
        region_code=args.region,
        project_id=args.source_project_id,
        query_generator=query_generator,
        file_tags=args.file_tags,
    )

    results = query_executor.run_queries()

    _log_file_counts_successes(results.succeeded_tables)
    _log_file_counts_failures(results.failed_table_results)

    # TODO(#32737) implement full raw data comparison

    sys.exit(len(results.failed_table_results))


if __name__ == "__main__":
    main()
