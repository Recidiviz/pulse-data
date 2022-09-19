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

"""Exports data from the state dataset in BigQuery to CSV files uploaded to a bucket in Google Cloud Storage.

Example usage (run from `pipenv shell`):

python -m recidiviz.export.state.state_bq_table_export_to_csv --project-id recidiviz-staging \
--dry-run True --target-bucket recidiviz-staging-jessex-test --state-code US_PA
"""


import argparse
import datetime
import logging
from typing import cast

from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClientImpl, ExportQueryConfig
from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET
from recidiviz.export.state.state_bq_table_export_utils import (
    state_table_export_query_str,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath, GcsfsDirectoryPath

from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool


def gcs_export_directory(
    bucket_name: str, today: datetime.date, state_code: str
) -> GcsfsDirectoryPath:
    """Returns a GCS directory to export files into, of the format:
    gs://{bucket_name}/ingested_state_data/{state_code}/{YYYY}/{MM}/{DD}
    """
    path = GcsfsDirectoryPath.from_bucket_and_blob_name(
        bucket_name=bucket_name,
        blob_name=f"ingested_state_data/{state_code}/{today.year:04}/{today.month:02}/{today.day:02}/",
    )
    return cast(GcsfsDirectoryPath, path)


def run_export(dry_run: bool, state_code: str, target_bucket: str) -> None:
    """Performs the export operation, exporting rows for the given state codes from the tables from the state dataset
    in the given project to CSV files with the same names as the tables to the given GCS bucket."""
    today = datetime.date.today()

    big_query_client = BigQueryClientImpl()
    dataset_ref = big_query_client.dataset_ref_for_id(STATE_BASE_DATASET)
    if not big_query_client.dataset_exists(dataset_ref):
        raise ValueError(f"Dataset {dataset_ref.dataset_id} does not exist")

    tables = big_query_client.list_tables(dataset_ref.dataset_id)

    export_configs = []
    for table in tables:
        logging.info("******************************")
        export_query = state_table_export_query_str(table, [state_code.upper()])
        logging.info(export_query)

        if not export_query:
            continue

        export_dir = gcs_export_directory(target_bucket, today, state_code.lower())
        export_file_name = f"{table.table_id}_{today.isoformat()}_export.csv"
        file = GcsfsFilePath.from_directory_and_file_name(export_dir, export_file_name)
        output_uri = file.uri()

        export_config = ExportQueryConfig(
            query=export_query,
            query_parameters=[],
            intermediate_dataset_id="export_temporary_tables",
            intermediate_table_name=f"{dataset_ref.dataset_id}_{table.table_id}_{state_code.lower()}",
            output_uri=output_uri,
            output_format=bigquery.DestinationFormat.CSV,
        )
        export_configs.append(export_config)
        if dry_run:
            logging.info(
                "[DRY RUN] Created export configuration to export table to GCS: %s",
                export_config,
            )
        else:
            logging.info(
                "Created export configuration to export table to GCS: %s", export_config
            )

    if dry_run:
        logging.info("[DRY RUN] Exporting [%d] tables to GCS", len(export_configs))
    else:
        logging.info("Exporting [%d] tables to GCS", len(export_configs))
        big_query_client.export_query_results_to_cloud_storage(
            export_configs, print_header=True
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--project-id",
        required=True,
        help="The id for this particular project, E.g. 'recidiviz-123'",
    )

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs script in dry-run mode, only prints the operations it would perform.",
    )

    parser.add_argument(
        "--target-bucket",
        required=True,
        help="The target Google Cloud Storage bucket to export data to, e.g. recidiviz-123-some-data",
    )

    parser.add_argument(
        "--state-code", required=True, help="The state code to export data for"
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    with local_project_id_override(args.project_id):
        run_export(
            dry_run=args.dry_run,
            state_code=args.state_code,
            target_bucket=args.target_bucket,
        )
