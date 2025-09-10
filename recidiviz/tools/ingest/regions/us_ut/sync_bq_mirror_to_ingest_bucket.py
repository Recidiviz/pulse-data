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
"""Script for syncing our BQ mirror of Utah's BQ instance to our UT ingest bucket

Every day, a manually defined scheduled query `ut-udc-dw-dev-to-recidiviz-ingest-us-ut-daily-sync` runs
in the `recidiviz-ingest-us-ut` BQ project that copies all data from their "Stage" BQ
dataset into a new BQ dataset, recording each table's last modified time in
`metadata.table_sync_tracker` in order to be used as the table update_datetime. For a
more detailed description & diagram, see go/ut-bq-mirror!

This script will ~~
    1) determine the most up-to-date mirrored dataset to read from
    2) determine which tables we want to export based on the file tags we have defined
       for utah and fetch the last modified time for each one
    3) export!!

    
python -m recidiviz.tools.ingest.regions.us_ut.sync_bq_mirror_to_ingest_bucket \
    --destination-project-id recidiviz-staging \
    --destination-raw-data-instance SECONDARY \
    --dry-run True

python -m recidiviz.tools.ingest.regions.us_ut.sync_bq_mirror_to_ingest_bucket \
    --destination-project-id recidiviz-staging \
    --table-ids [table_1,table_2,table_3] \
    --dry-run True

if you're running from w/in a cloud run job, use something like 

python -m recidiviz.tools.ingest.regions.us_ut.sync_bq_mirror_to_ingest_bucket \
    --dry-run False
    

"""
import argparse
import datetime
import logging
import sys
from enum import Enum

import attr
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.big_query.big_query_utils import bq_query_job_result_to_list_of_row_dicts
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.ingest.regions.us_ut.export_bq_to_ingest_bucket import (
    BigQueryToIngestBucketExportManager,
)
from recidiviz.utils.environment import (
    GCP_PROJECT_PRODUCTION,
    GCP_PROJECT_STAGING,
    in_gcp,
)
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool, str_to_list
from recidiviz.utils.string import StrictStringFormatter

US_UT_INGEST_PROJECT_ID = "recidiviz-ingest-us-ut"
US_UT_INGEST_METADATA_DATASET_ID = "metadata"
US_UT_SYNC_TRACKER = BigQueryAddress(
    dataset_id=US_UT_INGEST_METADATA_DATASET_ID, table_id="sync_tracker"
)
US_UT_TABLE_SYNC_TRACKER = BigQueryAddress(
    dataset_id=US_UT_INGEST_METADATA_DATASET_ID, table_id="table_sync_tracker"
)

LATEST_SYNC_RUN_QUERY = """
SELECT
    sync_status,
    sync_start_datetime
FROM `{project_id}.{sync_tracker}`
QUALIFY (
    -- get most recent started sync
    RANK() OVER (ORDER BY sync_start_datetime DESC) = 1 
    -- and get most recent status row from that sync
    AND ROW_NUMBER() OVER (PARTITION BY sync_start_datetime ORDER BY row_write_datetime DESC) = 1
);"""


LATEST_COMPLETED_SYNC_TABLES_QUERY = """SELECT
  dest_dataset,
  dest_table,
  source_last_modified_time
FROM `{project_id}.{sync_table_tracker}`
WHERE sync_start_datetime = DATETIME('{sync_datetime_str}')
"""


class SyncStatus(Enum):
    """Status enum & utils for the BQ-to-BQ sync."""

    COMPLETED = "COMPLETED"
    ERROR = "ERROR"
    STARTED = "STARTED"

    @classmethod
    def error_message_for_status(cls, status: "SyncStatus") -> str:
        if status == cls.ERROR:
            return (
                "The most recent sync from Utah's BigQuery to our BigQuery failed; see "
                "go/ut-bq-mirror for how and where that job is configured."
            )
        if status == cls.STARTED:
            return (
                "The most recent sync from Utah's BigQuery to our BigQuery is still in "
                "progress; please wait until it is completed before exporting."
            )

        raise ValueError(f"Cannot generate error message for status: [{status}]")


@attr.define(kw_only=True)
class CandidateExportTable:
    """Utility class for parsing table and datetime info."""

    address: BigQueryAddress
    update_datetime: datetime.datetime = attr.field(
        validator=attr_validators.is_utc_timezone_aware_datetime
    )

    @classmethod
    def from_bq_dict(cls, result_dict: dict[str, str]) -> "CandidateExportTable":
        return CandidateExportTable(
            address=BigQueryAddress(
                dataset_id=result_dict["dest_dataset"],
                table_id=result_dict["dest_table"],
            ),
            update_datetime=datetime.datetime.fromtimestamp(
                # python wants timestamp in seconds, no ms so we make the conversion
                float(result_dict["source_last_modified_time"]) / 1000,
                tz=datetime.UTC,
            ),
        )


def _get_latest_sync_query(project_id: str) -> str:
    return StrictStringFormatter().format(
        LATEST_SYNC_RUN_QUERY,
        project_id=project_id,
        sync_tracker=US_UT_SYNC_TRACKER.to_str(),
    )


def _get_latest_sync_tables_query(sync_datetime_str: str) -> str:
    return StrictStringFormatter().format(
        LATEST_COMPLETED_SYNC_TABLES_QUERY,
        project_id=US_UT_INGEST_PROJECT_ID,
        sync_table_tracker=US_UT_TABLE_SYNC_TRACKER.to_str(),
        sync_datetime_str=sync_datetime_str,
    )


@attr.define(kw_only=True)
class SyncBigQueryMirrorManager:
    """Class for managing exporting the most recent successful dataset sync."""

    source_project_id: str
    table_ids: set[str]
    state_code: StateCode
    raw_data_instance: DirectIngestInstance

    def _get_latest_valid_sync_datetime(self, client: BigQueryClient) -> str:
        last_status_result = client.run_query_async(
            query_str=_get_latest_sync_query(self.source_project_id),
            use_query_cache=False,
        ).result()

        last_status_row = one(
            bq_query_job_result_to_list_of_row_dicts(last_status_result)
        )

        current_sync_status = SyncStatus(last_status_row["sync_status"].upper())

        if current_sync_status != SyncStatus.COMPLETED:
            raise ValueError(SyncStatus.error_message_for_status(current_sync_status))

        logging.info(
            "Most recent sync at [%s] was successful",
            last_status_row["sync_start_datetime"],
        )
        return last_status_row["sync_start_datetime"]

    def _get_latest_sync_tables(
        self, client: BigQueryClient, sync_datetime_str: str
    ) -> list[CandidateExportTable]:
        query_result = client.run_query_async(
            query_str=_get_latest_sync_tables_query(sync_datetime_str),
            use_query_cache=False,
        ).result()

        all_candidates = [
            CandidateExportTable.from_bq_dict(result_dict)
            for result_dict in bq_query_job_result_to_list_of_row_dicts(query_result)
        ]

        logging.info(
            "Found [%s] total candidate tables for sync at [%s]",
            len(all_candidates),
            sync_datetime_str,
        )

        return all_candidates

    def _get_candidates_from_latest_sync(self) -> list[CandidateExportTable]:
        """Fetches all tables from the most recent sync."""
        source_bq_client = BigQueryClientImpl(project_id=self.source_project_id)

        latest_sync_datetime_str = self._get_latest_valid_sync_datetime(
            source_bq_client
        )

        all_candidates = self._get_latest_sync_tables(
            source_bq_client, sync_datetime_str=latest_sync_datetime_str
        )

        if len(all_candidates) == 0:
            raise ValueError(
                "Found no valid tables to export; this likely means either Utah's internal "
                "export process failed, our process ran before their finished, or we no "
                "longer have the correct permissions to query the dataset. See "
                "go/ut-bq-mirror for more context on how the job is configured, how to "
                "check for the latest run dates, and who to contact from Utah if things "
                "look wrong."
            )

        file_tag_candidates = [
            candidate
            for candidate in all_candidates
            if candidate.address.table_id in self.table_ids
        ]

        logging.info(
            "Found [%s] valid file tags of the [%s] candidates tables",
            len(file_tag_candidates),
            len(all_candidates),
        )

        return file_tag_candidates

    def sync(self, *, dry_run: bool) -> None:
        candidates_to_sync = self._get_candidates_from_latest_sync()

        # we might not receive all file tags each time -- if we don't, stale raw data
        # tables will alert us to these tables being stale which feels more
        # in line with the severity of the issue
        if missing_tables := self.table_ids - set(
            candidate.address.table_id for candidate in candidates_to_sync
        ):
            logging.error(
                "Could not find the following tables: \n %s \nin the most recent sync; continuing w/ those we found .....",
                "\n".join(f"\t-{table}" for table in missing_tables),
            )

        source_bq_client = BigQueryClientImpl(project_id=self.source_project_id)
        export_manager = BigQueryToIngestBucketExportManager(
            state_code=self.state_code,
            raw_data_instance=self.raw_data_instance,
            address_to_update_datetime={
                table.address: table.update_datetime for table in candidates_to_sync
            },
        )

        export_manager.export(bq_client=source_bq_client, dry_run=dry_run)


def sync_bq_mirror_to_ingest_bucket(
    *,
    destination_raw_data_instance: DirectIngestInstance,
    table_ids: set[str],
    dry_run: bool,
) -> None:

    table_ids_set = (
        get_region_raw_file_config(StateCode.US_UT.value).raw_file_tags
        if not table_ids
        else set(table_ids)
    )

    sync_manager = SyncBigQueryMirrorManager(
        source_project_id=US_UT_INGEST_PROJECT_ID,
        state_code=StateCode.US_UT,
        raw_data_instance=destination_raw_data_instance,
        table_ids=table_ids_set,
    )
    sync_manager.sync(dry_run=dry_run)


def _create_parser() -> argparse.ArgumentParser:
    """Builds an argument parser for this script."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--destination-project-id",
        type=str,
        required=False,
        choices=[GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING],
        default=None,
    )

    parser.add_argument(
        "--destination-raw-data-instance",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        default=DirectIngestInstance.PRIMARY,
        required=False,
    )

    parser.add_argument(
        "--table-ids",
        type=str_to_list,
        required=False,
        help="Comma separated list of table ids to import, if they are present. If no "
        "tables are provided, will default to all file tags.",
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

    if in_gcp():

        if args.destination_project_id:
            raise ValueError(
                "Please do not set a destination_project when running in GCP; we will use the execution env to determine the project to use."
            )

        sync_bq_mirror_to_ingest_bucket(
            destination_raw_data_instance=args.destination_raw_data_instance,
            table_ids=args.table_ids,
            dry_run=args.dry_run,
        )
    else:
        if args.destination_project_id is None:
            raise ValueError("You must provide a valid destination project!")

        with local_project_id_override(args.destination_project_id):
            sync_bq_mirror_to_ingest_bucket(
                destination_raw_data_instance=args.destination_raw_data_instance,
                table_ids=args.table_ids,
                dry_run=args.dry_run,
            )
