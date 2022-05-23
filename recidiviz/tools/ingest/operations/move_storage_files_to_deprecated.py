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
"""
Script for moving files in state storage to the deprecated folder in state storage.

Example path transformation:
gs://recidiviz-123-direct-ingest-state-storage/us_nd/raw/2019/08/12/
unprocessed_2019-08-12T00:00:00:000000_raw_docstars_contacts.csv ->
gs://recidiviz-123-direct-ingest-state-storage/us_nd/deprecated/deprecated_on_2020-07-22/raw/2019/08/12/
unprocessed_2019-08-12T00:00:00:000000_raw_docstars_contacts.csv

When run in dry-run mode (the default), will log the move of each file, but will not execute them.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.ingest.operations.move_storage_files_to_deprecated \
    --file-type raw --region us_nd --start-date-bound  2019-08-12 \
    --end-date-bound 2019-08-17 --project-id recidiviz-staging \
    --ingest-instance PRIMARY \
    --dry-run True \
    [--file-tag-filters "docstars_contacts elite_offenders"]

"""
import argparse
import logging
import os
from datetime import date
from typing import List, Optional

from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_storage_directory_path_for_state,
)
from recidiviz.ingest.direct.gcs.file_type import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema.operations.schema import (
    DirectIngestRawFileMetadata,
)
from recidiviz.tools.ingest.operations.operate_on_storage_ingest_files_controller import (
    IngestFilesOperationType,
    OperateOnStorageIngestFilesController,
)
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool


class MoveFilesToDeprecatedController:
    """Class with functionality to move files to deprecated folder with proper formatting."""

    def __init__(
        self,
        *,
        file_type: GcsfsDirectIngestFileType,
        region_code: str,
        ingest_instance: DirectIngestInstance,
        start_date_bound: Optional[str],
        end_date_bound: Optional[str],
        dry_run: bool,
        project_id: str,
        file_tag_filters: List[str],
    ):
        self.file_type = file_type
        self.region_code = region_code
        self.start_date_bound = start_date_bound
        self.end_date_bound = end_date_bound
        self.dry_run = dry_run
        self.file_tag_filters = file_tag_filters
        self.project_id = project_id

        if (
            self.file_type == GcsfsDirectIngestFileType.RAW_DATA
            and ingest_instance != DirectIngestInstance.PRIMARY
        ):
            raise ValueError(
                f"Raw files are only ever handled in the PRIMARY ingest instance. "
                f"Instead, found ingest_instance [{ingest_instance}]."
            )

        self.region_storage_dir_path = (
            gcsfs_direct_ingest_storage_directory_path_for_state(
                region_code=region_code,
                ingest_instance=ingest_instance,
                project_id=self.project_id,
            )
        )

        self.deprecated_region_storage_dir_path = (
            GcsfsDirectoryPath.from_dir_and_subdir(
                self.region_storage_dir_path,
                os.path.join(
                    "deprecated",
                    f"deprecated_on_{date.today()}",
                ),
            )
        )

    def run(self) -> None:
        """Main function that will execute the move to deprecated."""

        if self.file_type == GcsfsDirectIngestFileType.RAW_DATA:
            prompt_for_confirmation(
                "You have chosen to deprecate RAW_DATA type files. It is relatively "
                "rare that this should happen - generally only if we have received bad "
                "data from the state. \nAre you sure you want to proceed?",
                dry_run=self.dry_run,
            )

            prompt_for_confirmation(
                f"All associated rows in the BigQuery dataset "
                f"`{self.region_code.lower()}_raw_data` must be deleted before moving "
                f"these files to a deprecated location.\nHave you already done so?",
                dry_run=self.dry_run,
            )

        # TODO(#3666): Update this script to make updates to our Operations db and
        #  BigQuery (if necessary). For now we print these messages to check if
        #  appropriate data has been deleted from operations db.
        if self.file_type == GcsfsDirectIngestFileType.RAW_DATA:
            operations_table = DirectIngestRawFileMetadata.__tablename__
        else:
            raise ValueError(f"Unexpected file type [{self.file_type}].")

        prompt_for_confirmation(
            f"All associated rows from our postgres table `{operations_table}` "
            "must be deleted or marked as invalidated before moving these files to a deprecated "
            "location.\nHave you already done so?",
            dry_run=self.dry_run,
        )

        OperateOnStorageIngestFilesController(
            region_code=self.region_code,
            operation_type=IngestFilesOperationType.MOVE,
            source_region_storage_dir_path=self.region_storage_dir_path,
            destination_region_storage_dir_path=self.deprecated_region_storage_dir_path,
            file_type_to_operate_on=self.file_type,
            start_date_bound=self.start_date_bound,
            end_date_bound=self.end_date_bound,
            file_tag_filters=self.file_tag_filters,
            dry_run=self.dry_run,
        ).run()


def parse_arguments() -> argparse.Namespace:
    """Runs the move_state_files_to_deprecated script."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--file-type",
        required=True,
        choices=[file_type.value for file_type in GcsfsDirectIngestFileType],
        help="Defines whether we should move raw files or generated ingest_view files",
    )

    parser.add_argument("--region", required=True, help="E.g. 'us_nd'")

    parser.add_argument(
        "--ingest-instance",
        required=True,
        choices=[instance.value for instance in DirectIngestInstance],
        help="Defines which ingest instance we should be deprecating files for.",
    )

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs move in dry-run mode, only prints the file moves it would do.",
    )

    parser.add_argument(
        "--start-date-bound",
        help="The lower bound date to start from, inclusive. For partial moving of ingested files. "
        "E.g. 2019-09-23.",
    )

    parser.add_argument(
        "--end-date-bound",
        help="The upper bound date to end at, inclusive. For partial moving of ingested files. "
        "E.g. 2019-09-23.",
    )

    parser.add_argument(
        "--project-id", help="The id for this particular project, E.g. 'recidiviz-123'"
    )

    parser.add_argument(
        "--file-tag-filters",
        required=False,
        default=[],
        nargs="+",
        help="List of file tags to filter for. If not set, will move all files.",
    )

    return parser.parse_args()


def main() -> None:
    """Runs the move_state_files_to_deprecated script."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    args = parse_arguments()

    MoveFilesToDeprecatedController(
        file_type=GcsfsDirectIngestFileType(args.file_type),
        region_code=args.region,
        ingest_instance=DirectIngestInstance(args.ingest_instance),
        start_date_bound=args.start_date_bound,
        end_date_bound=args.end_date_bound,
        project_id=args.project_id,
        dry_run=args.dry_run,
        file_tag_filters=args.file_tag_filters,
    ).run()


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        main()
