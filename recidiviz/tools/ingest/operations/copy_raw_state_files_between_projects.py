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
Script for copying all files in production storage for a region to state storage for a region. Should be used when we
want to rerun ingest for a state in staging.

When run in dry-run mode (the default), will only log copies, but will not execute them.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.ingest.operations.copy_raw_state_files_between_projects \
    --region us_tn --source-project-id recidiviz-123 \
    --destination-project-id recidiviz-staging --start-date-bound 2022-03-24 --dry-run True
"""
import argparse
import logging

from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_storage_directory_path_for_state,
)
from recidiviz.ingest.direct.gcs.file_type import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.ingest.operations.operate_on_storage_ingest_files_controller import (
    IngestFilesOperationType,
    OperateOnStorageIngestFilesController,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.params import str_to_bool


def main() -> None:
    """Executes the main flow of the script."""
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
        "--destination-project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        help="Used to select which GCP project against which to run this script.",
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

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    source_region_storage_dir_path = gcsfs_direct_ingest_storage_directory_path_for_state(
        region_code=args.region,
        # Raw files are only ever stored in the PRIMARY storage bucket
        ingest_instance=DirectIngestInstance.PRIMARY,
        project_id=args.source_project_id,
    )
    destination_region_storage_dir_path = gcsfs_direct_ingest_storage_directory_path_for_state(
        region_code=args.region,
        # Raw files are only ever stored in the PRIMARY storage bucket
        ingest_instance=DirectIngestInstance.PRIMARY,
        project_id=args.destination_project_id,
    )
    OperateOnStorageIngestFilesController(
        region_code=args.region,
        operation_type=IngestFilesOperationType.COPY,
        source_region_storage_dir_path=source_region_storage_dir_path,
        destination_region_storage_dir_path=destination_region_storage_dir_path,
        file_type_to_operate_on=GcsfsDirectIngestFileType.RAW_DATA,
        start_date_bound=args.start_date_bound,
        end_date_bound=args.end_date_bound,
        file_tag_filters=[],
        dry_run=args.dry_run,
    ).run()


if __name__ == "__main__":
    main()
