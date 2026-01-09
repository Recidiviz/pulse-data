# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
Script for copying all files in production storage for a region to state storage for a
region. Should be used when we want to sync raw data between prod and staging. Will
almost always be followed by `move_raw_state_files_from_storage` to actually move
the files to the ingest bucket for processing.

When run in dry-run mode (the default), will only log copies, but will not execute them.

Example usage:

uv run python -m recidiviz.tools.ingest.operations.copy_raw_state_files_between_projects \
    --region us_tn --source-project-id recidiviz-123  --source-raw-data-instance PRIMARY \
    --destination-project-id recidiviz-staging --destination-raw-data-instance SECONDARY \
    --start-date-bound 2022-03-24 --dry-run True
"""
import argparse
import logging

from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_storage_directory_path_for_state,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.ingest.operations.helpers.operate_on_raw_storage_directories_controller import (
    IngestFilesOperationType,
    OperateOnRawStorageDirectoriesController,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.params import str_to_bool


# TODO(##37517) make start_date_bound -> state_datetime_bound and make it datetime | None instead, etc
def copy_raw_state_files(
    *,
    region: str,
    source_project_id: str,
    source_raw_data_instance: DirectIngestInstance,
    destination_project_id: str,
    destination_raw_data_instance: DirectIngestInstance,
    start_date_bound: str | None = None,
    end_date_bound: str | None = None,
    file_tag_filters: list[str] | None = None,
    dry_run: bool = True,
    skip_confirmation: bool = False,
) -> None:
    """Copies raw state files between projects.

    Args:
        region: Region code (e.g., 'us_nd')
        source_project_id: GCP project to copy from
        source_raw_data_instance: DirectIngestInstance to copy from
        destination_project_id: GCP project to copy to
        destination_raw_data_instance: DirectIngestInstance to copy to
        start_date_bound: Optional lower bound date (inclusive). E.g. 2019-09-23
        end_date_bound: Optional upper bound date (inclusive). E.g. 2019-09-23
        file_tag_filters: Optional list of file tags to filter for
        dry_run: If True, only prints what would be copied without executing
    """
    source_region_storage_dir_path = (
        gcsfs_direct_ingest_storage_directory_path_for_state(
            region_code=region,
            ingest_instance=source_raw_data_instance,
            project_id=source_project_id,
        )
    )
    destination_region_storage_dir_path = (
        gcsfs_direct_ingest_storage_directory_path_for_state(
            region_code=region,
            ingest_instance=destination_raw_data_instance,
            project_id=destination_project_id,
        )
    )

    OperateOnRawStorageDirectoriesController.create_controller(
        region_code=region,
        operation_type=IngestFilesOperationType.COPY,
        source_region_storage_dir_path=source_region_storage_dir_path,
        destination_region_storage_dir_path=destination_region_storage_dir_path,
        file_tags=file_tag_filters or [],
        start_date_bound=start_date_bound,
        end_date_bound=end_date_bound,
        dry_run=dry_run,
        skip_confirmation=skip_confirmation,
    ).run()


def main() -> None:
    """Executes the main flow of the script."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("--region", required=True, help="E.g. 'us_nd'")

    parser.add_argument(
        "--source-project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project against which to run this script.",
        required=True,
    )

    parser.add_argument(
        "--source-raw-data-instance",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        help="Used to identify which instance the raw data should be copied from.",
        required=True,
    )

    parser.add_argument(
        "--destination-project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project against which to run this script.",
        required=True,
    )

    parser.add_argument(
        "--destination-raw-data-instance",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        help="Used to identify which instance the raw data should be copied to.",
        required=True,
    )

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs copy in dry-run mode, only prints the file copies it would do.",
    )

    parser.add_argument(
        "--start-date-bound",
        help="The lower bound date to start from, inclusive. For partial copying of ingested files. "
        "E.g. 2019-09-23.",
    )

    parser.add_argument(
        "--end-date-bound",
        help="The upper bound date to end at, inclusive. For partial copying of ingested files. "
        "E.g. 2019-09-23.",
    )
    parser.add_argument(
        "--file-tag-filters",
        required=False,
        default=[],
        nargs="+",
        help="Space-separated list of file tags to filter for. "
        "If neither file-tag-filters or file-tag-regex is set, will move all files.",
    )

    args = parser.parse_args()

    copy_raw_state_files(
        region=args.region,
        source_project_id=args.source_project_id,
        source_raw_data_instance=args.source_raw_data_instance,
        destination_project_id=args.destination_project_id,
        destination_raw_data_instance=args.destination_raw_data_instance,
        start_date_bound=args.start_date_bound,
        end_date_bound=args.end_date_bound,
        file_tag_filters=args.file_tag_filters,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
