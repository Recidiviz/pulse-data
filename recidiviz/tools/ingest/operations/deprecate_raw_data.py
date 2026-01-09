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
Script for deprecating imported raw data. This script will :
(1) move state storage to the deprecated folder in state storage
    Example path transformation:
        gs://recidiviz-123-direct-ingest-state-storage/us_nd/raw/2019/08/12/
            processed_2019-08-12T00:00:00:000000_raw_docstars_contacts.csv ->
        gs://recidiviz-123-direct-ingest-state-storage/us_nd/deprecated/deprecated_on_2020-07-22/raw/2019/08/12/
            processed_2019-08-12T00:00:00:000000_raw_docstars_contacts.csv

(2) deprecate matching raw data files in the operations database
(3) delete matching raw data rows out of BigQuery 

When run in dry-run mode (the default), will log the move of each file, but will not execute them.
You can use EITHER --file-tag-filters OR --file-tag-regex, but NOT BOTH.
Note that --file-tag-regex is not a traditional regex, and needs to adhere to https://cloud.google.com/storage/docs/wildcards.

Example usage:

uv run python -m recidiviz.tools.ingest.operations.deprecate_raw_data \
    --project-id recidiviz-staging \
    --region us_nd \
    --ingest-instance PRIMARY \
    --start-date-bound  2019-08-12 \
    --end-date-bound 2019-08-17 \
    --dry-run True \
    --skip-prompts False \
    [--file-tag-filters docstars_contacts elite_offenders] \
    [--file-tag-regex COMS_]

"""
import argparse
import logging
from datetime import date
from typing import List, Optional

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_deprecated_storage_directory_path_for_state,
    gcsfs_direct_ingest_storage_directory_path_for_state,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.tools.ingest.operations.helpers.delete_bq_raw_table_rows_controller import (
    DeleteBQRawTableRowsController,
)
from recidiviz.tools.ingest.operations.helpers.invalidate_operations_db_files_controller import (
    InvalidateOperationsDBFilesController,
    ProcessingStatusFilterType,
)
from recidiviz.tools.ingest.operations.helpers.operate_on_raw_storage_directories_controller import (
    IngestFilesOperationType,
    OperateOnRawStorageDirectoriesController,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool


class MoveFilesToDeprecatedController:
    """Class with functionality to move files to deprecated folder with proper formatting."""

    def __init__(
        self,
        *,
        region_code: str,
        ingest_instance: DirectIngestInstance,
        start_date_bound: Optional[str],
        end_date_bound: Optional[str],
        dry_run: bool,
        project_id: str,
        file_tag_filters: List[str],
        file_tag_regex: Optional[str],
        skip_prompts: bool,
    ):
        self.region_code = region_code
        self.start_date_bound = start_date_bound
        self.end_date_bound = end_date_bound
        self.dry_run = dry_run
        self.file_tag_filters = file_tag_filters
        self.file_tag_regex = file_tag_regex
        self.project_id = project_id
        self.skip_prompts = skip_prompts
        self.ingest_instance = ingest_instance

        self.region_storage_dir_path = (
            gcsfs_direct_ingest_storage_directory_path_for_state(
                region_code=region_code,
                ingest_instance=ingest_instance,
                project_id=self.project_id,
            )
        )

        self.deprecated_region_storage_dir_path = (
            gcsfs_direct_ingest_deprecated_storage_directory_path_for_state(
                region_code=region_code,
                ingest_instance=ingest_instance,
                deprecated_on_date=date.today(),
                project_id=self.project_id,
            )
        )

    def run(self) -> None:
        """Main function that will execute the move to deprecated."""

        if not self.skip_prompts:
            prompt_for_confirmation(
                "You have chosen to deprecate RAW DATA files. It is relatively "
                "rare that this should happen - generally only if we have received bad "
                "data from the state. \nAre you sure you want to proceed?",
                dry_run=self.dry_run,
            )

        invalidated_files = InvalidateOperationsDBFilesController.create_controller(
            project_id=self.project_id,
            state_code=StateCode(self.region_code.upper()),
            ingest_instance=self.ingest_instance,
            file_tag_filters=self.file_tag_filters,
            file_tag_regex=self.file_tag_regex,
            start_date_bound=self.start_date_bound,
            end_date_bound=self.end_date_bound,
            dry_run=self.dry_run,
            skip_prompts=self.skip_prompts,
            # we only want to invalidate files that have been processed -- if there are
            # file tags that are pending import (i.e. not yet imported or have failed)
            # to import, we want to be sure those are not unintentionally affected by
            # this operation
            processing_status_filter=ProcessingStatusFilterType.PROCESSED_ONLY,
        ).run()

        if invalidated_files:
            DeleteBQRawTableRowsController(
                bq_client=BigQueryClientImpl(project_id=self.project_id),
                state_code=StateCode(self.region_code.upper()),
                ingest_instance=self.ingest_instance,
                dry_run=self.dry_run,
                skip_prompts=self.skip_prompts,
            ).run(file_tag_to_file_ids_to_delete=invalidated_files.file_tag_to_file_ids)

        OperateOnRawStorageDirectoriesController.create_controller(
            region_code=self.region_code,
            operation_type=IngestFilesOperationType.MOVE,
            source_region_storage_dir_path=self.region_storage_dir_path,
            destination_region_storage_dir_path=self.deprecated_region_storage_dir_path,
            start_date_bound=self.start_date_bound,
            end_date_bound=self.end_date_bound,
            file_tags=self.file_tag_filters,
            file_tag_regex=self.file_tag_regex,
            dry_run=self.dry_run,
        ).run()


# TODO(##37517) make start_date_bound -> state_datetime_bound and make it datetime | None instead, etc
def parse_arguments() -> argparse.Namespace:
    """Parses and validates the arguments for the deprecate_raw_data script."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
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
        help="Space-separated list of file tags to filter for. "
        "If neither file-tag-filters or file-tag-regex is set, will move all files.",
    )

    parser.add_argument(
        "--file-tag-regex",
        required=False,
        default=None,
        help="A pattern to match against file tags. Note that this is not a traditional regex, "
        "and needs to adhere to https://cloud.google.com/storage/docs/wildcards. "
        "If neither file-tag-filters or file-tag-regex is set, will move all files.",
    )

    parser.add_argument(
        "--skip-prompts",
        default=False,
        type=str_to_bool,
        help="Skip confirmation prompts, if true. This should only be used in the "
        "context of a flashing checklist.",
    )

    parser.add_argument(
        "--debug", default=False, type=str_to_bool, help="Sets log level to DEBUG."
    )

    args = parser.parse_args()
    if args.file_tag_filters and args.file_tag_regex:
        raise ValueError(
            "MUST USE EITHER --file-tag-filter OR --file-tag-regex, NOT BOTH"
        )
    return args


def main() -> None:
    """Runs the move_state_files_to_deprecated script."""
    args = parse_arguments()
    logging.getLogger().setLevel(logging.DEBUG if args.debug else logging.INFO)
    with local_project_id_override(args.project_id):
        with cloudsql_proxy_control.connection(schema_type=SchemaType.OPERATIONS):
            MoveFilesToDeprecatedController(
                region_code=args.region,
                ingest_instance=DirectIngestInstance(args.ingest_instance),
                start_date_bound=args.start_date_bound,
                end_date_bound=args.end_date_bound,
                project_id=args.project_id,
                dry_run=args.dry_run,
                file_tag_filters=args.file_tag_filters,
                file_tag_regex=args.file_tag_regex,
                skip_prompts=args.skip_prompts,
            ).run()


if __name__ == "__main__":
    main()
