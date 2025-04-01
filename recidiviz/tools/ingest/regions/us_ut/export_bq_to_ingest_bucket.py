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
"""Script for exporting data from a set of BQ tables to an UT's ingest bucket.


Example usage:

# to copy all files w/ today as the update_datetime

python -m recidiviz.tools.ingest.regions.us_ut.export_bq_to_ingest_bucket \
    --destination-project-id recidiviz-staging \
    --dry-run True

# to copy one file w/ a date

python -m recidiviz.tools.ingest.regions.us_ut.export_bq_to_ingest_bucket \
    --destination-project-id recidiviz-staging \
    --update-date 2024-01-01 \
    --source-table-ids [table_1] \
    --dry-run True



"""
import argparse
import concurrent.futures
import datetime
import logging
import sys

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_raw_file_name,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
    get_region_raw_file_config,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

US_UT_INGEST_PROJECT_ID = "recidiviz-ingest-us-ut"
US_UT_INGEST_MIRROR_DATASET_ID = "ut_udc_dw_dev_recidiviz"
MAX_EXPORT_THREADS = 8
LOGGING_WIDTH = 80
FILL_CHAR = "#"


@attr.define(kw_only=True)
class BigQueryTableToRawDataFileTagExportManager:
    """Class that manages the logic for exporting a single BigQuery Table to a single
    ingest-ready CSV file.
    """

    big_query_address: BigQueryAddress
    destination_path: GcsfsFilePath

    def export(self, *, bq_client: BigQueryClient, dry_run: bool) -> None:
        """Executes the export of |big_query_address| to |destination_path|."""

        if dry_run:
            logging.info(
                "\t [DRY RUN] would export [%s] -> [%s]",
                self.big_query_address.to_str(),
                self.destination_path.abs_path(),
            )
        else:
            logging.info(
                "\t Exporting [%s] -> [%s]",
                self.big_query_address.to_str(),
                self.destination_path.abs_path(),
            )
            export_job = bq_client.export_table_to_cloud_storage_async(
                source_table_address=self.big_query_address,
                destination_uri=self.destination_path.uri(),
                destination_format=bigquery.DestinationFormat.CSV,
                print_header=True,
            )
            if not export_job:
                raise ValueError(
                    f"Table {self.big_query_address.to_str()} does not exist"
                )

            export_job.result()

    @classmethod
    def from_address_dir_and_datetime(
        cls,
        *,
        big_query_address: BigQueryAddress,
        destination_directory: GcsfsDirectoryPath,
        update_datetime: datetime.datetime,
        is_sharded: bool,
    ) -> "BigQueryTableToRawDataFileTagExportManager":
        output_file_path = GcsfsFilePath.from_directory_and_file_name(
            destination_directory,
            to_normalized_unprocessed_raw_file_name(
                big_query_address.table_id, dt=update_datetime
            ),
        )

        return cls(
            big_query_address=big_query_address,
            destination_path=(
                output_file_path.sharded() if is_sharded else output_file_path
            ),
        )


@attr.define(kw_only=True)
class BigQueryToIngestBucketExportManager:
    """Class for managing exports of a set of big query addresses to the provided
    destination directory.
    """

    # info about how to access bq
    address_to_update_datetime: dict[BigQueryAddress, datetime.datetime]

    # info about where to write the data to
    state_code: StateCode
    raw_data_instance: DirectIngestInstance

    @property
    def region_config(self) -> DirectIngestRegionRawFileConfig:
        return get_region_raw_file_config(self.state_code.value)

    @property
    def ingest_bucket(self) -> GcsfsDirectoryPath:
        return gcsfs_direct_ingest_bucket_for_state(
            region_code=self.state_code.value.lower(),
            ingest_instance=self.raw_data_instance,
        )

    def should_table_be_sharded(self, file_tag: str) -> bool:
        if file_tag not in self.region_config.raw_file_tags:
            return False

        return self.region_config.raw_file_configs[file_tag].is_chunked_file

    def export(self, *, bq_client: BigQueryClient, dry_run: bool) -> None:
        """Executes the export of |source_big_query_addresses| to |destination_directory|."""

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=MAX_EXPORT_THREADS
        ) as executor:

            futures_to_address = {
                executor.submit(
                    BigQueryTableToRawDataFileTagExportManager.from_address_dir_and_datetime(
                        big_query_address=address,
                        destination_directory=self.ingest_bucket,
                        update_datetime=update_datetime,
                        is_sharded=self.should_table_be_sharded(address.table_id),
                    ).export,
                    bq_client=bq_client,
                    dry_run=dry_run,
                ): address
                for address, update_datetime in self.address_to_update_datetime.items()
            }

        successful_exports: list[BigQueryAddress] = []
        failed_exports: list[Exception] = []
        for future in concurrent.futures.as_completed(futures_to_address):
            try:
                future.result()
                successful_exports.append(futures_to_address[future])
            except Exception as e:
                failed_exports.append(e)

        logging.info("".center(LOGGING_WIDTH, FILL_CHAR))
        logging.info("  RESULTS  ".center(LOGGING_WIDTH, FILL_CHAR))
        logging.info("".center(LOGGING_WIDTH, FILL_CHAR))
        if successful_exports:
            logging.info(
                "Successfully imported [%s] tables: \n %s",
                len(successful_exports),
                "\n".join(f"\t- {export.to_str()}" for export in successful_exports),
            )
            logging.info("".center(LOGGING_WIDTH, FILL_CHAR))
        if failed_exports:
            raise ExceptionGroup(
                "Exports failed with the following messages: ", failed_exports
            )

    @classmethod
    def from_table_ids(
        cls,
        *,
        state_code: StateCode,
        raw_data_instance: DirectIngestInstance,
        source_dataset_id: str,
        table_ids: list[str],
        update_datetime: datetime.datetime,
    ) -> "BigQueryToIngestBucketExportManager":
        return BigQueryToIngestBucketExportManager(
            address_to_update_datetime={
                BigQueryAddress(
                    dataset_id=source_dataset_id, table_id=table_id
                ): update_datetime
                for table_id in table_ids
            },
            state_code=state_code,
            raw_data_instance=raw_data_instance,
        )


def export_bq_to_ingest_bucket(
    *,
    state_code: StateCode,
    source_project_id: str,
    source_dataset_id: str,
    source_table_ids: list[str] | None,
    destination_raw_data_instance: DirectIngestInstance,
    update_datetime: datetime.datetime,
    dry_run: bool,
) -> None:
    """Executes the export of bq tables to the ingest bucket for |state_code| and
    |destination_raw_data_instance|.
    """

    source_project_bq_client = BigQueryClientImpl(project_id=source_project_id)
    if not source_table_ids:
        source_table_ids = [
            table.table_id
            for table in source_project_bq_client.list_tables(source_dataset_id)
        ]

    tables_to_export = "\n".join([f"\t - {table}" for table in source_table_ids])

    prompt_for_confirmation(
        f"Found [{len(source_table_ids)}] tables to export ? \n {tables_to_export}. \n Proceed w/ dry_run: {dry_run}?"
    )

    export_manager = BigQueryToIngestBucketExportManager.from_table_ids(
        source_dataset_id=source_dataset_id,
        table_ids=source_table_ids,
        state_code=state_code,
        raw_data_instance=destination_raw_data_instance,
        update_datetime=update_datetime,
    )

    export_manager.export(bq_client=source_project_bq_client, dry_run=dry_run)


def _create_parser() -> argparse.ArgumentParser:
    """Builds an argument parser for this script."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--update-datetime",
        type=str,
        required=False,
        help=(
            "ISO-formatted date or datetime to be used as the update_datetime of generated "
            "ingest-ready raw data files. If no value is provided, the current datetime is used."
        ),
    )

    parser.add_argument(
        "--source-table-ids",
        default=[],
        nargs="+",
        type=str,
        required=False,
    )

    parser.add_argument(
        "--destination-project-id",
        type=str,
        required=False,
        choices=[GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING],
        default=GCP_PROJECT_STAGING,
    )

    parser.add_argument(
        "--destination-raw-data-instance",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        default=DirectIngestInstance.PRIMARY,
        required=False,
    )

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs script in dry-run mode, only prints the operations it would perform.",
    )

    return parser


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    args = _create_parser().parse_args()

    with local_project_id_override(args.destination_project_id):
        export_bq_to_ingest_bucket(
            state_code=StateCode.US_UT,
            source_project_id=US_UT_INGEST_PROJECT_ID,
            source_dataset_id=US_UT_INGEST_MIRROR_DATASET_ID,
            source_table_ids=args.source_table_ids,
            destination_raw_data_instance=args.destination_raw_data_instance,
            update_datetime=(
                datetime.datetime.now(tz=datetime.UTC)
                if args.update_datetime_str is None
                else datetime.datetime.fromisoformat(args.update_datetime_str)
            ),
            dry_run=args.dry_run,
        )
