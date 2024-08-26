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
from multiprocessing.pool import ThreadPool
from typing import List, Optional, Set, Tuple

from tqdm import tqdm

from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.common.date import is_between_date_strs_inclusive, is_date_str
from recidiviz.tools.utils.script_helpers import RunCommandUnsuccessful, run_command


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

    command = f'gsutil {_GSUTIL_PARALLEL_COMMAND_OPTIONS} ls {flags} "{gs_path}"'
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
    logging.debug(command)
    try:
        run_command(
            command, assert_success=True, timeout_sec=GSUTIL_DEFAULT_TIMEOUT_SEC
        )
    except RuntimeError as e:
        if (
            not allow_empty
            or not is_empty_response(e)
            or isinstance(e, RunCommandUnsuccessful)
        ):
            raise e


def gsutil_mv(from_path: str, to_path: str, allow_empty: bool = False) -> None:
    """Moves a file/files via 'gsutil mv'.

    See more documentation here:
    https://cloud.google.com/storage/docs/gsutil/commands/mv
    """

    command = f'gsutil {_GSUTIL_PARALLEL_COMMAND_OPTIONS} mv "{from_path}" "{to_path}"'
    logging.debug(command)
    try:
        run_command(
            command, assert_success=True, timeout_sec=GSUTIL_DEFAULT_TIMEOUT_SEC
        )
    except RuntimeError as e:
        logging.debug(str(e))
        if (
            not allow_empty
            or not is_empty_response(e)
            or isinstance(e, RunCommandUnsuccessful)
        ):
            raise e


def _date_str_from_date_subdir_path(date_subdir_path: str) -> str:
    """Returns the date in ISO format corresponding to the storage subdir path."""
    parts = date_subdir_path.rstrip("/").split("/")
    return f"{parts[-3]}-{parts[-2]}-{parts[-1]}"


def _subdir_is_in_date_range(
    subdir: str, upper_bound_date: Optional[str], lower_bound_date: Optional[str]
) -> bool:
    """Returns True if the given subdirectory path has a date between the given bounds."""
    subdir_date = _date_str_from_date_subdir_path(subdir)
    return is_date_str(subdir_date) and is_between_date_strs_inclusive(
        upper_bound_date=upper_bound_date,
        lower_bound_date=lower_bound_date,
        date_of_interest=subdir_date,
    )


def _get_subdirs_in_date_range(
    raw_data_path: GcsfsDirectoryPath,
    upper_bound_date: Optional[str],
    lower_bound_date: Optional[str],
) -> Set[str]:
    output = set()
    for path in gsutil_ls(raw_data_path.wildcard_path().uri()):
        subdir = os.path.dirname(path)
        if _subdir_is_in_date_range(subdir, upper_bound_date, lower_bound_date):
            output.add(subdir)
    return output


def _get_filters(
    file_tag_filters: Optional[List[str]],
    file_tag_regex: Optional[str],
) -> List[str]:
    if file_tag_filters and file_tag_regex:
        raise ValueError("Cannot have both file_tag_filter and file_tag_regex")
    if file_tag_regex:
        return [file_tag_regex]
    if file_tag_filters:
        return file_tag_filters
    return []


def gsutil_get_storage_subdirs_containing_raw_files(
    storage_bucket_path: GcsfsDirectoryPath,
    upper_bound_date: Optional[str],
    lower_bound_date: Optional[str],
    file_tag_filters: Optional[List[str]] = None,
    file_tag_regex: Optional[str] = None,
) -> List[str]:
    """Returns all subdirs containing files in the provided |storage_bucket_path| for a given
    region."""
    # We search with a double wildcard and then filter in python because it is much
    # faster than doing `gs://{storage_bucket_path}/raw/*/*/*/`
    filters = _get_filters(file_tag_filters, file_tag_regex)
    raw_data_path = GcsfsDirectoryPath.from_dir_and_subdir(storage_bucket_path, "raw")
    subdirs_in_date_range = _get_subdirs_in_date_range(
        raw_data_path, upper_bound_date, lower_bound_date
    )
    # We return all subdirectories in the date range if there are no filters for file tags.
    if not any(filters):
        return sorted(list(subdirs_in_date_range))

    subdirs_to_search = subdirs_in_date_range
    subdirs_containing_files: Set[str] = set()
    thread_pool = ThreadPool(processes=16)

    for file_tag_filter in filters:
        progress = tqdm(
            desc=f"Searching for [{file_tag_filter}] in [{len(subdirs_to_search)}] subdirs...",
            total=len(subdirs_to_search),
        )

        paths_to_search = [
            (
                subdir,
                file_tag_filter,
                subdirs_containing_files,
                progress,
            )
            for subdir in subdirs_to_search
        ]
        thread_pool.map(_parallel_get_storage_subdirs, paths_to_search)
        progress.close()

        # if we find a single match for a filter inside of a subdir, we've already marked
        # it as containing files so we dont need to revisit
        subdirs_to_search = subdirs_to_search - subdirs_containing_files

        if not subdirs_to_search:
            break

    return sorted(list(subdirs_containing_files))


def _parallel_get_storage_subdirs(args: Tuple[str, str, Set[str], tqdm]) -> None:
    subdir, file_filter, subdirs_containing_files, progress = args
    path = subdir + f"/*{file_filter}*"
    results = gsutil_ls(path, allow_empty=True)
    # if we find any results, add them to |args.subdirs_containing_files|
    if results:
        subdirs_containing_files.add(subdir)

    progress.update(1)
