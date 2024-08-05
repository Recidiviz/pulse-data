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
"""For a given state, compares the raw data in BigQuery between projects.

By default, ensures that all of the rows that exist in staging also exist in production.
It only validates the converse if the --exact flag is supplied. We do not always expect
all data in production to be imported in staging (e.g. in MO where data is only dropped
in production each week).

Example usage:
python -m recidiviz.tools.ingest.operations.compare_raw_data_between_projects --region us_mo
python -m recidiviz.tools.ingest.operations.compare_raw_data_between_projects --region us_tn \
    --source-project-id recidiviz-staging --source-ingest-instance PRIMARY \
    --comparison-project-id recidiviz-staging --comparison-ingest-instance SECONDARY \
    --exact
"""

import argparse
import datetime
import logging
import sys
from typing import Dict, List, Optional, Tuple

from google.cloud import bigquery, exceptions

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_constants import IS_DELETED_COL_NAME
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.string import StrictStringFormatter

RECIDIVIZ_COLUMNS = [
    IS_DELETED_COL_NAME,
]

COMPARISON_TEMPLATE = """
WITH compared AS (
  SELECT {columns}, {datetime_column} AS update_datetime
  FROM `{source_project_id}.{source_raw_data_dataset_id}.{raw_data_table_id}`
  EXCEPT DISTINCT
  SELECT {columns}, {datetime_column} AS update_datetime
  FROM `{comparison_project_id}.{comparison_raw_data_dataset_id}.{raw_data_table_id}`
)
SELECT update_datetime, COUNT(*) as num_missing_rows
FROM compared
GROUP BY update_datetime
ORDER BY update_datetime
"""


def compare_raw_data_between_projects(
    region_code: str,
    source_project_id: str,
    source_ingest_instance: DirectIngestInstance,
    comparison_project_id: str,
    comparison_ingest_instance: DirectIngestInstance,
    truncate_update_datetime_part: Optional[str] = None,
) -> List[str]:
    """Compares the raw data between specified projects and instances for given region."""
    logging.info(
        "**** Ensuring all raw data for [%s] in (project=[%s], instance=[%s]) also exists in "
        "(project=[%s], instance=[%s]) ****",
        region_code.upper(),
        source_project_id,
        source_ingest_instance.value,
        comparison_project_id,
        comparison_ingest_instance.value,
    )

    raw_file_config = DirectIngestRegionRawFileConfig(region_code)

    source_bq_client = BigQueryClientImpl(project_id=source_project_id)
    source_dataset_id = raw_tables_dataset_for_region(
        state_code=StateCode(region_code.upper()),
        instance=source_ingest_instance,
        sandbox_dataset_prefix=None,
    )
    comparison_dataset_id = raw_tables_dataset_for_region(
        state_code=StateCode(region_code.upper()),
        instance=comparison_ingest_instance,
        sandbox_dataset_prefix=None,
    )

    query_jobs: Dict[str, bigquery.QueryJob] = {}
    for file_tag, file_config in raw_file_config.raw_file_configs.items():
        source_table_address = BigQueryAddress(
            dataset_id=source_dataset_id, table_id=file_tag
        )
        if (
            not source_bq_client.table_exists(source_table_address)
            or file_config.is_undocumented
        ):
            continue

        columns = ", ".join([column.name for column in file_config.documented_columns])
        columns += ", " + ", ".join(RECIDIVIZ_COLUMNS)

        query_job = source_bq_client.run_query_async(
            query_str=StrictStringFormatter().format(
                COMPARISON_TEMPLATE,
                source_project_id=source_project_id,
                source_raw_data_dataset_id=source_dataset_id,
                comparison_project_id=comparison_project_id,
                comparison_raw_data_dataset_id=comparison_dataset_id,
                raw_data_table_id=file_tag,
                columns=columns,
                datetime_column=(
                    f"DATETIME_TRUNC(update_datetime, {truncate_update_datetime_part})"
                    if truncate_update_datetime_part is not None
                    else "update_datetime"
                ),
            ),
            use_query_cache=True,
        )
        query_jobs[file_tag] = query_job

    table_column_width = min(
        max(len(tag) for tag in raw_file_config.raw_file_configs), 30
    )

    failed_tables: List[str] = []
    for file_tag in sorted(raw_file_config.raw_file_tags):
        justified_name = file_tag.ljust(table_column_width)

        if file_tag not in query_jobs:
            # This file did not exist in the project that is the source of truth.
            continue

        query_job = query_jobs[file_tag]
        try:
            rows = query_job.result()
        except exceptions.NotFound:
            logging.warning(
                "%s | Missing table %s.%s.%s",
                justified_name,
                comparison_project_id,
                comparison_dataset_id,
                file_tag,
            )
            failed_tables.append(file_tag)
            continue

        counts: List[Tuple[datetime.datetime, int]] = [row.values() for row in rows]

        if counts:
            logging.warning(
                "%s | Missing data in the %s.%s table",
                justified_name,
                comparison_project_id,
                comparison_ingest_instance,
            )
            for update_datetime, num_missing in counts:
                logging.warning("\t%ss: %d", update_datetime.isoformat(), num_missing)
            failed_tables.append(file_tag)
        else:
            logging.info(
                "%s | (project=[%s], instance=[%s]) contains all of the data from (project=[%s], instance=[%s])",
                justified_name,
                comparison_project_id,
                comparison_ingest_instance.value,
                source_project_id,
                source_ingest_instance.value,
            )

    return failed_tables


def output_failed_tables(
    failed_tables: List[str],
    source_project_id: str,
    source_ingest_instance: DirectIngestInstance,
    comparison_project_id: str,
    comparison_ingest_instance: DirectIngestInstance,
) -> None:
    if failed_tables:
        logging.error(
            "FAILURE - The following tables had data in (project=[%s], instance=[%s]), that was not in "
            "(project=[%s], instance=[%s]).",
            source_project_id,
            source_ingest_instance.value,
            comparison_project_id,
            comparison_ingest_instance.value,
        )
        for table in failed_tables:
            logging.error("- %s", table)
    else:
        logging.info(
            "None of the tables had data in (project=[%s], instance=[%s]) that was not in "
            "(project=[%s], instance=[%s])",
            source_project_id,
            source_ingest_instance.value,
            comparison_project_id,
            comparison_ingest_instance.value,
        )


def main() -> None:
    """Runs the comparison and logs final results."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--region", type=str, required=True, help="E.g. 'us_nd'")
    parser.add_argument(
        "--exact",
        action="store_true",
        help="If set, also performs the converse comparison, ensuring that all data in "
        "production also exists in staging.",
    )
    parser.add_argument(
        "--source-project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        default=GCP_PROJECT_PRODUCTION,
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
        default=DirectIngestInstance.PRIMARY,
        help="Specifies the ingest instance of the raw data to be compared to.",
    )
    parser.add_argument(
        "--truncate-update-datetime",
        type=str,
        choices=["SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH"],
        help="The granularity to truncate the update datetime to before comparing. By "
        "default the full datetime is used.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    source_to_comparison_failed_tables = compare_raw_data_between_projects(
        region_code=args.region,
        source_project_id=args.source_project_id,
        source_ingest_instance=args.source_ingest_instance,
        comparison_project_id=args.comparison_project_id,
        comparison_ingest_instance=args.comparison_ingest_instance,
        truncate_update_datetime_part=args.truncate_update_datetime,
    )

    comparison_to_source_failed_tables = []
    if args.exact:
        # Reverse the source and comparison project ID + instance
        comparison_to_source_failed_tables = compare_raw_data_between_projects(
            region_code=args.region,
            source_project_id=args.comparison_project_id,
            source_ingest_instance=args.comparison_ingest_instance,
            comparison_project_id=args.source_project_id,
            comparison_ingest_instance=args.source_ingest_instance,
            truncate_update_datetime_part=args.truncate_update_datetime,
        )

    logging.info("*****" * 20)

    logging.info(
        "RESULTS: Comparing (project=[%s], instance=[%s]) vs. (project=[%s], instance=[%s])",
        args.source_project_id,
        args.source_ingest_instance.value,
        args.comparison_project_id,
        args.comparison_ingest_instance.value,
    )
    output_failed_tables(
        source_to_comparison_failed_tables,
        source_project_id=args.source_project_id,
        source_ingest_instance=args.source_ingest_instance,
        comparison_project_id=args.comparison_project_id,
        comparison_ingest_instance=args.comparison_ingest_instance,
    )
    if args.exact:
        logging.info(
            "RESULTS: Comparing (project=[%s], instance=[%s]) vs. (project=[%s], instance=[%s])",
            args.comparison_project_id,
            args.comparison_ingest_instance.value,
            args.source_project_id,
            args.source_ingest_instance.value,
        )
        output_failed_tables(
            comparison_to_source_failed_tables,
            source_project_id=args.comparison_project_id,
            source_ingest_instance=args.comparison_ingest_instance,
            comparison_project_id=args.source_project_id,
            comparison_ingest_instance=args.source_ingest_instance,
        )
    if (
        not source_to_comparison_failed_tables
        and not comparison_to_source_failed_tables
    ):
        logging.info("SUCCESS - All raw data was present.")
    sys.exit(
        len(source_to_comparison_failed_tables)
        + len(comparison_to_source_failed_tables)
    )


if __name__ == "__main__":
    main()
