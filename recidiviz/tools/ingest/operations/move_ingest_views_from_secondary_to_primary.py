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
Script for moving all files in production storage for a region to state storage for a region.
Should be used when a rerun in the SECONDARY instance has completed and we are copying the data
to the PRIMARY instance.

When run in dry-run mode (the default), will only log moves, but will not execute them.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.ingest.operations.move_ingest_views_from_secondary_to_primary \
    --region us_nd --project-id recidiviz-staging --dry-run True
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
        "--project-id",
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

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    source_region_storage_dir_path = (
        gcsfs_direct_ingest_storage_directory_path_for_state(
            region_code=args.region,
            ingest_instance=DirectIngestInstance.SECONDARY,
            project_id=args.project_id,
        )
    )
    destination_region_storage_dir_path = (
        gcsfs_direct_ingest_storage_directory_path_for_state(
            region_code=args.region,
            ingest_instance=DirectIngestInstance.PRIMARY,
            project_id=args.project_id,
        )
    )
    OperateOnStorageIngestFilesController(
        region_code=args.region,
        operation_type=IngestFilesOperationType.MOVE,
        source_region_storage_dir_path=source_region_storage_dir_path,
        destination_region_storage_dir_path=destination_region_storage_dir_path,
        file_type_to_operate_on=GcsfsDirectIngestFileType.INGEST_VIEW,
        start_date_bound=None,
        end_date_bound=None,
        file_tag_filters=[],
        dry_run=args.dry_run,
    ).run()


if __name__ == "__main__":
    main()
