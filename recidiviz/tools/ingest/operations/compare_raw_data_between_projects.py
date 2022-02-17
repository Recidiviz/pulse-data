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
python -m recidiviz.tools.ingest.operations.compare_raw_data_between_projects --region us_pa --exact
python -m recidiviz.tools.ingest.operations.compare_raw_data_between_projects --region us_mo
"""


import argparse
import datetime
import logging
import sys
from typing import Dict, List, Tuple

from google.cloud import bigquery, exceptions

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_tables_dataset_for_region,
)
from recidiviz.utils import environment
from recidiviz.utils.string import StrictStringFormatter

COMPARISON_TEMPLATE = """
WITH compared AS (
  SELECT {columns}, update_datetime
  FROM `{source_project_id}.{raw_data_dataset_id}.{raw_data_table_id}`
  EXCEPT DISTINCT
  SELECT {columns}, update_datetime
  FROM `{comparison_project_id}.{raw_data_dataset_id}.{raw_data_table_id}`
)
SELECT update_datetime, COUNT(*) as num_missing_rows
FROM compared
GROUP BY update_datetime
ORDER BY update_datetime
"""


def compare_raw_data_between_projects(
    region_code: str,
    source_project_id: str = environment.GCP_PROJECT_STAGING,
    comparison_project_id: str = environment.GCP_PROJECT_PRODUCTION,
) -> List[str]:
    """Compares the raw data between staging and production for a given region."""
    logging.info(
        "**** Ensuring all raw data for [%s] in [%s] also exists in [%s] ****",
        region_code.upper(),
        source_project_id,
        comparison_project_id,
    )

    raw_file_config = DirectIngestRegionRawFileConfig(region_code)

    bq_client = BigQueryClientImpl(project_id=source_project_id)
    dataset_id = raw_tables_dataset_for_region(region_code, sandbox_dataset_prefix=None)
    source_dataset = bq_client.dataset_ref_for_id(dataset_id)

    query_jobs: Dict[str, bigquery.QueryJob] = {}
    for file_tag, file_config in raw_file_config.raw_file_configs.items():
        if (
            not bq_client.table_exists(source_dataset, file_tag)
            or file_config.is_undocumented
            or not file_config.primary_key_cols
        ):
            continue

        columns = ", ".join([column.name for column in file_config.available_columns])

        query_job = bq_client.run_query_async(
            query_str=StrictStringFormatter().format(
                COMPARISON_TEMPLATE,
                source_project_id=source_project_id,
                comparison_project_id=comparison_project_id,
                raw_data_dataset_id=dataset_id,
                raw_data_table_id=file_tag,
                columns=columns,
            )
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
                dataset_id,
                file_tag,
            )
            failed_tables.append(file_tag)
            continue

        counts: List[Tuple[datetime.datetime, int]] = [row.values() for row in rows]

        if counts:
            logging.warning(
                "%s | Missing data in the %s table",
                justified_name,
                comparison_project_id,
            )
            for update_datetime, num_missing in counts:
                logging.warning("\t%ss: %d", update_datetime.isoformat(), num_missing)
            failed_tables.append(file_tag)
        else:
            logging.info(
                "%s | %s contains all of the data from %s",
                justified_name,
                comparison_project_id,
                source_project_id,
            )

    return failed_tables


def output_failed_tables(
    failed_tables: List[str], source_project_id: str, comparison_project_id: str
) -> None:
    if failed_tables:
        logging.error(
            "FAILURE - The following tables had data in %s that was not in %s.",
            source_project_id,
            comparison_project_id,
        )
        for table in failed_tables:
            logging.error("- %s", table)


def main() -> None:
    """Runs the comparison and logs final results."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--region", type=str, required=True, help="E.g. 'us_nd'")
    parser.add_argument(
        "--exact",
        type=bool,
        nargs="?",
        const=True,
        default=False,
        help="If set, also performs the converse comparison, ensuring that all data in "
        "production also exists in staging.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    production_failed_tables = compare_raw_data_between_projects(
        region_code=args.region,
        source_project_id=environment.GCP_PROJECT_STAGING,
        comparison_project_id=environment.GCP_PROJECT_PRODUCTION,
    )

    staging_failed_tables = []
    if args.exact:
        staging_failed_tables = compare_raw_data_between_projects(
            region_code=args.region,
            source_project_id=environment.GCP_PROJECT_PRODUCTION,
            comparison_project_id=environment.GCP_PROJECT_STAGING,
        )

    logging.info("*****" * 20)

    output_failed_tables(
        production_failed_tables,
        source_project_id=environment.GCP_PROJECT_STAGING,
        comparison_project_id=environment.GCP_PROJECT_PRODUCTION,
    )
    output_failed_tables(
        staging_failed_tables,
        source_project_id=environment.GCP_PROJECT_PRODUCTION,
        comparison_project_id=environment.GCP_PROJECT_STAGING,
    )
    if not production_failed_tables and not staging_failed_tables:
        logging.info("SUCCESS - All raw data was present.")
    sys.exit(len(production_failed_tables) + len(staging_failed_tables))


if __name__ == "__main__":
    main()
