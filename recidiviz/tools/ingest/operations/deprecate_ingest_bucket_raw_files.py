# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
Script for deprecating raw files in the ingest bucket for a given state and ingest instance.
Moves files to deprecated state storage and invalidates any relevant rows in the operations db.
Runs in dry-run mode by default.

Example path transformation:
gs://recidiviz-staging-direct-ingest-state-us-nd/unprocessed_2019-08-12T00:00:00:000000_raw_docstars_contacts.csv ->
gs://recidiviz-staging-direct-ingest-state-storage/us_nd/deprecated/deprecated_on_2020-07-22/unprocessed_2019-08-12T00:00:00:000000_raw_docstars_contacts.csv

Example usage:

uv run python -m recidiviz.tools.ingest.operations.deprecate_ingest_bucket_raw_files \
    --project-id recidiviz-staging \
    --state-code US_ND \
    --ingest-instance PRIMARY \
    --start-datetime-inclusive 2019-08-12 \
    [--dry-run False] \
    [--file-tag-filters docstars_contacts elite_offenders]

python -m recidiviz.tools.ingest.operations.deprecate_ingest_bucket_raw_files \
    --project-id recidiviz-staging \
    --state-code US_ND \
    --ingest-instance PRIMARY \
    --start-datetime-inclusive 2019-08-12T12:00:00Z \
    --end-datetime-exclusive 2019-08-12T18:00:00Z \
    --file-tag-filters docstars_contacts elite_offenders
"""
import argparse
import datetime
import logging

from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import parse_opt_datetime_maybe_add_tz
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.tools.ingest.operations.helpers.invalidate_operations_db_files_controller import (
    InvalidateOperationsDBFilesController,
    ProcessingStatusFilterType,
)
from recidiviz.tools.ingest.operations.helpers.move_ingest_bucket_raw_files_to_deprecated_controller import (
    MoveIngestBucketRawFilesToDeprecatedController,
)
from recidiviz.tools.postgres.cloudsql_proxy_control import cloudsql_proxy_control
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool


def _parse_arguments() -> argparse.Namespace:
    """Parses and validates the arguments."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--state-code",
        help="The state we should be deprecating files for.",
        type=StateCode,
        choices=list(StateCode),
        required=True,
    )

    parser.add_argument(
        "--ingest-instance",
        help="Defines which ingest instance we should be deprecating files for.",
        type=DirectIngestInstance,
        choices=list(DirectIngestInstance),
        required=True,
    )

    parser.add_argument(
        "--project-id",
        help="The id for this particular project, E.g. 'recidiviz-123'",
        required=True,
    )

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Defaults to True. If set to False, will actually move the files and invalidate the rows.",
    )

    parser.add_argument(
        "--start-datetime-inclusive",
        help="The lower bound datetime to start from (inclusive). If a date format is "
        "specified (e.g. 2019-09-23), it will be converted into a datetime with 0s filled "
        "in; otherwise, please specify a valid ISO datetime (e.g. 2019-09-03T12:00:00Z)",
    )

    parser.add_argument(
        "--end-datetime-exclusive",
        help="The upper bound datetime to end at (exclusive). If a date format is "
        "specified (e.g. 2019-09-23), it will be converted into a datetime with 0s filled "
        "in; otherwise, please specify a valid ISO datetime (e.g. 2019-09-03T12:00:00Z)",
    )

    parser.add_argument(
        "--file-tag-filters",
        required=False,
        default=[],
        nargs="+",
        help="List of file tags to filter for. If not set, will move all files.",
    )

    return parser.parse_args()


def main(
    *,
    project_id: str,
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    start_datetime_inclusive: datetime.datetime | None,
    end_datetime_exclusive: datetime.datetime | None,
    file_tag_filters: list[str],
    dry_run: bool,
) -> None:
    """Move files matching the given criteria from the ingest bucket to deprecated storage,
    and invalidate any relevant rows from the metadata tables in the operations db.
    """
    if (
        start_datetime_inclusive
        and end_datetime_exclusive
        and start_datetime_inclusive > end_datetime_exclusive
    ):
        raise ValueError(
            f"The start date bound [{start_datetime_inclusive}] must be less than or equal to the end date bound [{end_datetime_exclusive}]."
        )

    (
        successful_gcsfs_file_paths,
        failed_gcsfs_file_paths,
    ) = MoveIngestBucketRawFilesToDeprecatedController.create_controller(
        state_code=state_code,
        project_id=project_id,
        ingest_instance=ingest_instance,
        start_datetime_inclusive=start_datetime_inclusive,
        end_datetime_exclusive=end_datetime_exclusive,
        file_tag_filters=file_tag_filters,
        dry_run=dry_run,
    ).run()

    if successful_gcsfs_file_paths:
        InvalidateOperationsDBFilesController.create_controller(
            project_id=project_id,
            state_code=state_code,
            ingest_instance=ingest_instance,
            normalized_filenames_filter=[
                f.file_name for f in successful_gcsfs_file_paths
            ],
            dry_run=dry_run,
            # only invalidate file entries that have not been successfully imported --
            # if there are files stuck in the ingest bucket that have been processed
            # it is likely the move failed after import
            processing_status_filter=ProcessingStatusFilterType.UNPROCESSED_ONLY,
        ).run()

    if failed_gcsfs_file_paths:
        logging.error(
            "Failed to deprecate the following files:%s"
            "\nFiles that failed to move were not invalidated in the operations db.",
            "\n    ".join([f.file_name for f in failed_gcsfs_file_paths]),
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    args = _parse_arguments()

    with local_project_id_override(args.project_id):
        with cloudsql_proxy_control.connection(schema_type=SchemaType.OPERATIONS):
            main(
                project_id=args.project_id,
                state_code=args.state_code,
                ingest_instance=args.ingest_instance,
                start_datetime_inclusive=parse_opt_datetime_maybe_add_tz(
                    args.start_datetime_inclusive, tz_to_add=datetime.UTC
                ),
                end_datetime_exclusive=parse_opt_datetime_maybe_add_tz(
                    args.end_datetime_exclusive, tz_to_add=datetime.UTC
                ),
                file_tag_filters=args.file_tag_filters,
                dry_run=args.dry_run,
            )
