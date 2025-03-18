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
import datetime
import logging
import sys
from typing import Dict, List, Optional

from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.ingest.operations.helpers.raw_data_region_diff_query_executor import (
    RawDataRegionDiffQueryExecutor,
    RawDataRegionQueryResult,
)
from recidiviz.tools.ingest.operations.helpers.raw_table_data_diff_query_generator import (
    RawTableDataDiffQueryGenerator,
)
from recidiviz.tools.ingest.operations.helpers.raw_table_diff_query_generator import (
    RawTableDiffQueryGenerator,
    RawTableDiffQueryResult,
)
from recidiviz.tools.ingest.operations.helpers.raw_table_file_counts_diff_query_generator import (
    RawTableFileCountsDiffQueryGenerator,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.log_helpers import make_log_output_path

LINE_SEPARATOR = "-" * 100

RESULT_ROW_DISPLAY_LIMIT = 10

RAW_DATA_DIFF_RESULTS_DATASET_ID = "raw_data_comparison_output"


def _get_table_name_prefix(
    region_code: str,
    src_project_id: str,
    src_ingest_instance: DirectIngestInstance,
    cmp_project_id: str,
    cmp_ingest_instance: DirectIngestInstance,
) -> str:
    unix_ts = int(datetime.datetime.now(tz=datetime.UTC).timestamp())
    return f"{region_code}_src_{src_project_id}_{src_ingest_instance.value}_cmp_{cmp_project_id}_{cmp_ingest_instance.value}_{unix_ts}_"


def _log_successes(succeeded_tables: List[str]) -> str:
    success_log = ""

    if not succeeded_tables:
        return success_log

    success_log += "\nSUCCESSES"
    success_log += LINE_SEPARATOR
    success_log += "\n"
    for file_tag in succeeded_tables:
        success_log += f"\t- {file_tag}"
    success_log += LINE_SEPARATOR
    logging.info(success_log)
    return success_log


def _log_failures(
    failed_table_results: Dict[str, RawTableDiffQueryResult],
    result_row_display_limit: Optional[int] = None,
) -> str:
    fail_log = ""

    if not failed_table_results:
        return ""

    fail_log += "\nFAILURES"
    fail_log += LINE_SEPARATOR
    fail_log += "\n"
    fail_log += "see log file or bq table for more details"
    logging.error(fail_log)
    for file_tag, result in failed_table_results.items():
        # since these can be so big, only log the file tag with failures
        logging.error("%s:\n", file_tag)
        fail_log += f"{file_tag}: \n"
        fail_log += result.build_result_rows_str(limit=result_row_display_limit)
        fail_log += LINE_SEPARATOR
        fail_log += "\n"
    return fail_log


def _log_results(
    logfile_path: str,
    results: RawDataRegionQueryResult,
    result_row_display_limit: Optional[int] = None,
) -> None:
    success_logs = _log_successes(
        succeeded_tables=results.succeeded_tables,
    )
    failure_logs = _log_failures(
        failed_table_results=results.failed_table_results,
        result_row_display_limit=result_row_display_limit,
    )

    with open(logfile_path, "w", encoding="utf-8") as f:
        f.writelines([success_logs, failure_logs])


def _execute_diff_query(
    query_generator: RawTableDiffQueryGenerator,
    region_code: str,
    project_id: str,
    file_tags: List[str],
    save_to_table: bool = False,
    dataset_id: Optional[str] = None,
    table_name_prefix: Optional[str] = None,
) -> RawDataRegionQueryResult:
    """Create the query generator class, execute the queries, and log the results.
    Returns True if the query returned no differences for all tables, False otherwise.
    """

    query_executor = RawDataRegionDiffQueryExecutor(
        region_code=region_code,
        project_id=project_id,
        query_generator=query_generator,
        file_tags=file_tags,
        save_to_table=save_to_table,
        dataset_id=dataset_id,
        table_name_prefix=table_name_prefix,
    )

    return query_executor.run_queries()


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
    parser.add_argument(
        "--skip-file-counts-check",
        action="store_true",
        help="If set, skips checking that there are the same number of distinct files"
        " in the two datasets.",
    )
    parser.add_argument(
        "--save-to-table",
        action="store_true",
        help="If set, saves the results of the comparison to a table in BigQuery. Tables have a TTL of 7 days.",
    )
    parser.add_argument(
        "--start-date-inclusive",
        type=str,
        help="Inclusive start date in the format YYYY-MM-DD to compare data.",
    )
    parser.add_argument(
        "--end-date-exclusive",
        type=str,
        help="Exclusive end date in the format YYYY-MM-DD to compare data.",
    )
    args = parser.parse_args()
    return args


def main() -> None:
    """Runs the table comparisons and logs results."""
    args = _parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    logging.info(
        "\nComparing raw data for [%s] between src (project=[%s], instance=[%s]) and"
        " cmp (project=[%s], instance=[%s])",
        args.region,
        args.source_project_id,
        args.source_ingest_instance.value,
        args.comparison_project_id,
        args.comparison_ingest_instance.value,
    )
    logging.info(LINE_SEPARATOR)

    path = make_log_output_path("compare_raw_data", region_code=args.region)
    logging.info("writing full logs to [%s]", path)

    file_tags = args.file_tags
    if not args.skip_file_counts_check:
        results = _execute_diff_query(
            query_generator=RawTableFileCountsDiffQueryGenerator.create_query_generator(
                region_code=args.region,
                src_project_id=args.source_project_id,
                src_ingest_instance=args.source_ingest_instance,
                cmp_project_id=args.comparison_project_id,
                cmp_ingest_instance=args.comparison_ingest_instance,
                truncate_update_datetime_part=args.truncate_update_datetime,
                start_date_inclusive=(
                    datetime.datetime.fromisoformat(args.start_date)
                    if args.start_date_inclusive
                    else None
                ),
                end_date_exclusive=(
                    datetime.datetime.fromisoformat(args.end_date)
                    if args.end_date_exclusive
                    else None
                ),
            ),
            region_code=args.region,
            project_id=args.source_project_id,
            file_tags=file_tags,
        )
        _log_results(path, results)
        # Only fully compare the data for tables that have the same number of distinct file_ids
        file_tags = results.succeeded_tables

    results = _execute_diff_query(
        query_generator=RawTableDataDiffQueryGenerator.create_query_generator(
            region_code=args.region,
            src_project_id=args.source_project_id,
            src_ingest_instance=args.source_ingest_instance,
            cmp_project_id=args.comparison_project_id,
            cmp_ingest_instance=args.comparison_ingest_instance,
            truncate_update_datetime_part=args.truncate_update_datetime,
            start_date_inclusive=(
                datetime.datetime.fromisoformat(args.start_date_inclusive)
                if args.start_date_inclusive
                else None
            ),
            end_date_exclusive=(
                datetime.datetime.fromisoformat(args.end_date_exclusive)
                if args.end_date_exclusive
                else None
            ),
        ),
        region_code=args.region,
        project_id=args.source_project_id,
        file_tags=file_tags,
        save_to_table=args.save_to_table,
        dataset_id=RAW_DATA_DIFF_RESULTS_DATASET_ID if args.save_to_table else None,
        table_name_prefix=(
            _get_table_name_prefix(
                region_code=args.region,
                src_project_id=args.source_project_id,
                src_ingest_instance=args.source_ingest_instance,
                cmp_project_id=args.comparison_project_id,
                cmp_ingest_instance=args.comparison_ingest_instance,
            )
            if args.save_to_table
            else None
        ),
    )
    _log_results(
        path,
        results,
        result_row_display_limit=(RESULT_ROW_DISPLAY_LIMIT),
    )
    logging.info(
        "Only the first %d rows of each category are displayed.",
        RESULT_ROW_DISPLAY_LIMIT,
    )
    if args.save_to_table:
        logging.info(
            "To view all results, please check the BigQuery table for each file tag at %s.%s.%s{file_tag}",
            args.source_project_id,
            RAW_DATA_DIFF_RESULTS_DATASET_ID,
            _get_table_name_prefix(
                region_code=args.region,
                src_project_id=args.source_project_id,
                src_ingest_instance=args.source_ingest_instance,
                cmp_project_id=args.comparison_project_id,
                cmp_ingest_instance=args.comparison_ingest_instance,
            ),
        )
    else:
        logging.info(
            "To view all results, rerun with the --save-to-table flag to save to the full results to a Big Query table."
        )

    sys.exit(0 if not results.failed_table_results else 1)


if __name__ == "__main__":
    main()
