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
"""Helpers for calling gsutil commands inside of Python scripts."""
import logging
import os
from typing import List, Optional

from recidiviz.common.date import is_between_date_strs_inclusive, is_date_str
from recidiviz.tools.utils.script_helpers import run_command


def is_empty_response(e: RuntimeError) -> bool:
    return "CommandException: One or more URLs matched no objects." in str(
        e
    ) or "CommandException: No URLs matched" in str(e)


GSUTIL_DEFAULT_TIMEOUT_SEC = 60 * 20  # 20 minutes


def gsutil_ls(
    gs_path: str, directories_only: bool = False, allow_empty: bool = False
) -> List[str]:
    """Returns list of paths returned by 'gsutil ls <gs_path>.
    E.g.
    gsutil_ls('gs://recidiviz-123-state-storage') ->
        ['gs://recidiviz-123-state-storage/us_nd']

    See more documentation here:
    https://cloud.google.com/storage/docs/gsutil/commands/ls
    """
    flags = ""
    if directories_only:
        if "**" in gs_path:
            raise ValueError(
                "Double-wildcard searches are not compatible with the -d flag and "
                "will return paths other than directories."
            )
        flags = "-d"

    command = f'gsutil ls {flags} "{gs_path}"'
    try:
        res = run_command(
            command, assert_success=True, timeout_sec=GSUTIL_DEFAULT_TIMEOUT_SEC
        )
    except RuntimeError as e:
        if allow_empty and is_empty_response(e):
            return []
        raise e

    return [p for p in res.splitlines() if p != gs_path]


# See https://github.com/GoogleCloudPlatform/gsutil/issues/464#issuecomment-633334888
_GSUTIL_PARALLEL_COMMAND_OPTIONS = (
    "-q -m -o GSUtil:parallel_process_count=1 -o GSUtil:parallel_thread_count=24"
)


def gsutil_cp(from_path: str, to_path: str, allow_empty: bool = False) -> None:
    """Copies a file/files via 'gsutil cp'.

    See more documentation here:
    https://cloud.google.com/storage/docs/gsutil/commands/cp
    """
    command = f'gsutil {_GSUTIL_PARALLEL_COMMAND_OPTIONS} cp "{from_path}" "{to_path}"'
    try:
        run_command(
            command, assert_success=True, timeout_sec=GSUTIL_DEFAULT_TIMEOUT_SEC
        )
    except RuntimeError as e:
        if not allow_empty or not is_empty_response(e):
            raise e


def gsutil_mv(from_path: str, to_path: str, allow_empty: bool = False) -> None:
    """Moves a file/files via 'gsutil mv'.

    See more documentation here:
    https://cloud.google.com/storage/docs/gsutil/commands/mv
    """

    command = f'gsutil {_GSUTIL_PARALLEL_COMMAND_OPTIONS} mv "{from_path}" "{to_path}"'
    try:
        run_command(
            command, assert_success=True, timeout_sec=GSUTIL_DEFAULT_TIMEOUT_SEC
        )
    except RuntimeError as e:
        if not allow_empty or not is_empty_response(e):
            raise e


def _date_str_from_date_subdir_path(date_subdir_path: str) -> str:
    """Returns the date in ISO format corresponding to the storage subdir path."""
    parts = date_subdir_path.rstrip("/").split("/")
    return f"{parts[-3]}-{parts[-2]}-{parts[-1]}"


def gsutil_get_storage_subdirs_containing_raw_files(
    storage_bucket_path: str,
    upper_bound_date: Optional[str],
    lower_bound_date: Optional[str],
    file_tag_filters: Optional[List[str]] = None,
) -> List[str]:
    """Returns all subdirs containing files in the provided |storage_bucket_path| for a given
    region."""
    # We search with a double wildcard and then filter in python because it is much
    # faster than doing `gs://{storage_bucket_path}/raw/*/*/*/`
    all_files_wildcard = f"gs://{storage_bucket_path}/raw/**"
    paths = gsutil_ls(all_files_wildcard)

    all_subdirs = {os.path.dirname(path) for path in paths}

    subdirs_containing_files = []
    for subdir in all_subdirs:
        subdir_date = _date_str_from_date_subdir_path(subdir)
        if is_date_str(subdir_date) and is_between_date_strs_inclusive(
            upper_bound_date=upper_bound_date,
            lower_bound_date=lower_bound_date,
            date_of_interest=subdir_date,
        ):
            if file_tag_filters:
                found_matches = False
                for file_tag_filter in file_tag_filters:
                    path = subdir + f"/*{file_tag_filter}*"
                    try:
                        located_matching_files = "\n\t * ".join(gsutil_ls(path))
                        logging.info(
                            "Found the following files matching the file_tag_filter='%s' in the GCS "
                            "subdirectory='%s':\n\t * %s",
                            file_tag_filter,
                            subdir,
                            located_matching_files,
                        )
                        found_matches = True
                    except Exception:
                        logging.info(
                            "Files matching the file_tag_filter='%s' were not present in the "
                            "GCS subdirectory='%s'",
                            file_tag_filter,
                            subdir,
                        )
                if found_matches:
                    subdirs_containing_files.append(subdir)
            else:
                # If there are no filters, then automatically append the subdirectory, since it is within the date range
                subdirs_containing_files.append(subdir)

    return sorted(subdirs_containing_files)
