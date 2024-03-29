# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Temporary script to copy the most recent and least recent files for every file tag associated with the passed
in state and project id from primary storage bucket to secondary ingest bucket.

    Usage:
        python -m recidiviz.tools.ingest.one_offs.copy_least_and_most_recent_files_from_primary_storage_to_secondary_ingest_bucket --dry-run True --project-id=recidiviz-staging --state-code=US_TN
"""
import argparse
import datetime
from collections import defaultdict
from typing import Dict, List

from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_file_path_from_normalized_path,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
    gcsfs_direct_ingest_storage_directory_path_for_state,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tools.gsutil_shell_helpers import gsutil_cp, gsutil_ls, gsutil_mv
from recidiviz.tools.ingest.one_offs.clear_redundant_raw_data_on_bq import (
    get_raw_file_configs_for_state,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool


def print_paths_list(label: str, file_list: List[GcsfsFilePath]) -> None:
    if len(file_list) == 1:
        print(f"\t* {label}: '{file_list[0].uri()}'")
    else:
        print(f"\t* {label}:")
        for path in file_list:
            print(f"\t\t* '{path.uri()}'")


def identify_files_to_copy_over(
    state_code: StateCode, project_id: str
) -> List[GcsfsFilePath]:
    """Identify most and least recent files on GCS in primary storage."""
    raw_file_configs: Dict[
        str, DirectIngestRawFileConfig
    ] = get_raw_file_configs_for_state(state_code)

    paths_to_copy_over: List[GcsfsFilePath] = []
    primary_storage_dir = gcsfs_direct_ingest_storage_directory_path_for_state(
        region_code=state_code.value,
        ingest_instance=DirectIngestInstance.PRIMARY,
        project_id=project_id,
    )
    raw_data_path = GcsfsDirectoryPath.from_dir_and_subdir(primary_storage_dir, "raw")
    raw_file_search_uri = GcsfsDirectoryPath.from_dir_and_subdir(
        raw_data_path, "**/*.*"
    ).uri()
    print("Collecting all storage file paths...")
    all_file_paths = [
        GcsfsFilePath.from_absolute_path(uri) for uri in gsutil_ls(raw_file_search_uri)
    ]

    all_paths_by_tag_by_date: Dict[
        str, Dict[datetime.date, List[GcsfsFilePath]]
    ] = defaultdict(lambda: defaultdict(list))
    for path in all_file_paths:
        parts = filename_parts_from_path(path)
        all_paths_by_tag_by_date[parts.file_tag][
            parts.utc_upload_datetime.date()
        ].append(path)

    for file_tag in raw_file_configs.keys():
        paths_by_date = all_paths_by_tag_by_date.get(file_tag)
        if not paths_by_date:
            print(f"Found no files on GCS that matched file_tag={file_tag}. Skipping.")
            continue
        dates_with_data = sorted(paths_by_date.keys())
        if len(dates_with_data) == 1:
            print(
                f"Found one date with files on GCS that matched file_tag={file_tag}. Adding:"
            )
            paths = paths_by_date[dates_with_data[0]]
            paths_to_copy_over.extend(paths)
            print_paths_list("Added", paths)
        else:
            print(
                f"Found multiple dates with files on GCS that matched file_tag={file_tag}. Identifying most and least recent."
            )
            most_recent = paths_by_date[dates_with_data[-1]]
            least_recent = paths_by_date[dates_with_data[0]]

            paths_to_copy_over.extend(most_recent)
            paths_to_copy_over.extend(least_recent)

            print_paths_list("Most recent", most_recent)
            print_paths_list("Least recent", least_recent)

    return paths_to_copy_over


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs script in dry-run mode, only prints the operations it would perform.",
    )

    parser.add_argument("--state-code", type=StateCode, required=True)

    parser.add_argument(
        "--project-id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )

    return parser


def copy_paths_from_primary_storage_to_secondary_storage(
    dry_run: bool, state_code: StateCode, paths_to_copy: List[GcsfsFilePath]
) -> List[GcsfsFilePath]:
    storage_paths: List[GcsfsFilePath] = []
    for path in paths_to_copy:
        path_uri = path.uri()
        # Change the bucket but reuse the relative path
        new_path = GcsfsFilePath.from_directory_and_file_name(
            gcsfs_direct_ingest_storage_directory_path_for_state(
                region_code=state_code.value,
                ingest_instance=DirectIngestInstance.SECONDARY,
            ),
            path.blob_name,
        )
        new_path_uri = new_path.uri()
        storage_paths.append(GcsfsFilePath.from_absolute_path(new_path_uri))
        log_statement = f"gsutil_cp to COPY files from primary storage to secondary storage:\n\t* from_path='{path_uri}'\n\t* to_path='{new_path_uri}'"
        if not dry_run:
            print(f"Running {log_statement}")
            gsutil_cp(from_path=path_uri, to_path=new_path_uri)
        else:
            print(f"Would run {log_statement}")
    return storage_paths


def move_paths_from_secondary_storage_to_secondary_ingest(
    dry_run: bool,
    paths_to_move: List[GcsfsFilePath],
    project_id: str,
    state_code: StateCode,
) -> None:
    for path in paths_to_move:
        # Change the bucket but reuse the relative path
        path_uri = path.uri()
        new_path_uri = to_normalized_unprocessed_file_path_from_normalized_path(
            GcsfsFilePath.from_directory_and_file_name(
                gcsfs_direct_ingest_bucket_for_state(
                    region_code=state_code.value,
                    ingest_instance=DirectIngestInstance.SECONDARY,
                    project_id=project_id,
                ),
                # We only want the name of the file.
                path.file_name,
            ).uri()
        )
        log_statement = f"gsutil_mv to MOVE files from secondary storage to secondary ingest,\n\t* from_path='{path_uri}'\n\t* to_path='{new_path_uri}'"
        if not dry_run:
            print(f"Running {log_statement}")
            gsutil_mv(from_path=path_uri, to_path=new_path_uri)
        else:
            print(f"Would run {log_statement}")


# TODO(#21429): Delete this script once ingest is in dataflow and we have more gracefully handled entity deletion.
if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        args = create_parser().parse_args()

        gcs_paths_to_copy_over = identify_files_to_copy_over(
            state_code=args.state_code, project_id=args.project_id
        )
        secondary_storage_paths = copy_paths_from_primary_storage_to_secondary_storage(
            dry_run=args.dry_run,
            state_code=args.state_code,
            paths_to_copy=gcs_paths_to_copy_over,
        )
        move_paths_from_secondary_storage_to_secondary_ingest(
            dry_run=args.dry_run,
            paths_to_move=secondary_storage_paths,
            project_id=args.project_id,
            state_code=args.state_code,
        )
