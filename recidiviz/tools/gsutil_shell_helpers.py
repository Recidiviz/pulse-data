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
"""Helpers for running gcloud storage CLI commands from Python scripts.

TODO(AUR-235): the gsutil_ prefix on these function names is historical as they were
migrated off gsutil ahead of its removal from the Google Cloud CLI in March 2027;
rename them and their ~48 call sites to match.
"""
import logging
import os
from multiprocessing.pool import ThreadPool
from typing import List, Optional, Set, Tuple

from tqdm import tqdm

from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.common.date import is_between_date_strs_inclusive, is_date_str
from recidiviz.tools.utils.script_helpers import RunCommandUnsuccessful, run_command

# Neither CLI gives "nothing matched" its own exit code, so this has to match on
# stderr text. gsutil and `gcloud storage` word it differently, and they differ
# differently per verb. Measured against a real bucket on 2026-09-16:
#
#   verb  gsutil                                     gcloud storage
#   ls    CommandException: One or more URLs         ERROR: (gcloud.storage.ls) One or more
#         matched no objects.                        URLs matched no objects.
#   cp    CommandException: No URLs matched: <url>   ERROR: (gcloud.storage.cp) The following
#   mv                                               URLs matched no objects or files:
#
# For `ls` the two share a substring. For `cp` and `mv` they share nothing at
# all, so dropping the gsutil wording alone would not have been enough. Both
# tools' markers stay listed: gsutil is only removed from the gcloud CLI package
# after March 2027, so a contributor on an older SDK still sees the old text.
_EMPTY_RESPONSE_MARKERS = (
    # Shared by `gsutil ls` and `gcloud storage ls`.
    "One or more URLs matched no objects.",
    # `gsutil cp` and `gsutil mv`.
    "CommandException: No URLs matched",
    # `gcloud storage` cp, mv, and objects update.
    "The following URLs matched no objects or files:",
)


def is_empty_response(e: RuntimeError) -> bool:
    message = str(e)
    return any(marker in message for marker in _EMPTY_RESPONSE_MARKERS)


GSUTIL_DEFAULT_TIMEOUT_SEC = 60 * 20  # 20 minutes


def gsutil_ls(
    gs_path: str,
    *,
    directories_only: bool = False,
    files_only: bool = False,
    allow_empty: bool = False,
) -> List[str]:
    """Returns list of paths returned by 'gcloud storage ls <gs_path>'.
    E.g.
    gsutil_ls('gs://recidiviz-123-state-storage') ->
        ['gs://recidiviz-123-state-storage/us_nd']

    See more documentation here:
    https://cloud.google.com/sdk/gcloud/reference/storage/ls
    """
    if directories_only and files_only:
        raise ValueError("files_only and directories_only cannot both be True.")

    if directories_only:
        if "**" in gs_path:
            raise ValueError(
                "Double-wildcard searches are not compatible with directories_only "
                "and will return paths other than directories."
            )
        return _ls_directories(gs_path, allow_empty=allow_empty)

    paths = _run_ls(gs_path, allow_empty=allow_empty)
    if files_only:
        return [p for p in paths if not p.endswith("/")]

    return paths


def _run_ls(gs_path: str, *, allow_empty: bool) -> List[str]:
    """Runs `gcloud storage ls` and returns the object and prefix paths it printed."""
    command = f'gcloud storage ls "{gs_path}"'
    try:
        res = run_command(
            command, assert_success=True, timeout_sec=GSUTIL_DEFAULT_TIMEOUT_SEC
        )
    except RuntimeError as e:
        if allow_empty and is_empty_response(e):
            return []
        raise e

    # When a wildcard matches a prefix, `gcloud storage ls` descends into it and
    # announces each one with a header line that ends in "/:", followed by a blank
    # line. An object name can never end in "/", so dropping those two shapes only
    # ever removes formatting, never a real result.
    return [p for p in res.splitlines() if p and p != gs_path and not p.endswith("/:")]


def _ls_directories(gs_path: str, *, allow_empty: bool) -> List[str]:
    """Returns only the immediate subdirectories of |gs_path|.

    `gsutil ls -d "gs://bucket/prefix/*"` printed one line per matched prefix.
    `gcloud storage ls` has no -d flag, and given the same wildcard it descends one
    level into every match and prints the contents instead. Callers index that list
    (see get_last_stored_date_for_states), so a naive swap returns a FILE where a
    directory is expected, with no error. Verified against a real bucket on
    2026-09-16.

    Listing the plain prefix, with no wildcard, reproduces the old output exactly,
    so that is what this does.
    """
    prefix = gs_path[:-1] if gs_path.endswith("*") else gs_path
    if not prefix.endswith("/"):
        prefix = f"{prefix}/"

    return [p for p in _run_ls(prefix, allow_empty=allow_empty) if p.endswith("/")]


def gsutil_cp(from_path: str, to_path: str, allow_empty: bool = False) -> None:
    """Copies a file/files via 'gcloud storage cp'.

    See more documentation here:
    https://cloud.google.com/sdk/gcloud/reference/storage/cp
    """
    command = f'gcloud storage cp "{from_path}" "{to_path}"'
    logging.debug(command)
    try:
        run_command(
            command, assert_success=True, timeout_sec=GSUTIL_DEFAULT_TIMEOUT_SEC
        )
    except RuntimeError as e:
        if allow_empty and is_empty_response(e):
            return
        raise e


def gsutil_mv(from_path: str, to_path: str, allow_empty: bool = False) -> None:
    """Moves a file/files via 'gcloud storage mv'.

    See more documentation here:
    https://cloud.google.com/sdk/gcloud/reference/storage/mv
    """

    command = f'gcloud storage mv "{from_path}" "{to_path}"'
    logging.debug(command)
    try:
        run_command(
            command, assert_success=True, timeout_sec=GSUTIL_DEFAULT_TIMEOUT_SEC
        )
    except RuntimeError as e:
        logging.debug(str(e))
        if allow_empty and is_empty_response(e):
            return
        raise e


def gcloud_storage_rm(
    path: str,
    *,
    force_delete_contents: bool = False,
    allow_empty_response: bool = False,
) -> None:
    """Deletes an object, folder, or bucket via 'gcloud storage rm'. Delete will fail if the bucket or folder is not empty,
    unless force_delete_contents is set to True.

    It's recommended going forward to use gcloud storage instead of gsutil
    https://cloud.google.com/storage/docs/gsutil#should-you-use

    Args:
        path: The path to the object, folder, or bucket to delete, prefixed with 'gs://'.
        force_delete_contents: If True, deletes the contents of the folder or bucket before deleting the folder or bucket.
        allow_empty_response: If True, allows the command to return an empty response without raising an error, for example
            if the path has already been deleted.
    """

    command = f'gcloud storage rm "{path}"'
    command += " --recursive" if force_delete_contents else ""

    logging.debug(command)
    try:
        run_command(
            command, assert_success=True, timeout_sec=GSUTIL_DEFAULT_TIMEOUT_SEC
        )
    except RuntimeError as e:
        logging.debug(str(e))
        if (
            not allow_empty_response
            or not is_empty_response(e)
            or isinstance(e, RunCommandUnsuccessful)
        ):
            raise e


def gcloud_storage_rsync_command(
    directory: str, gcs_uri: str, *, dry_run: bool = False
) -> list[str]:
    args = ["gcloud", "storage", "rsync", directory, gcs_uri]

    if dry_run:
        args.append("--dry-run")

    return args


def gcloud_storage_rsync_airflow_command(
    directory: str, gcs_uri: str, *, use_gsutil: bool = False, dry_run: bool = False
) -> list[str]:
    """Returns the options for rsyncing a composer environment"""
    if use_gsutil:
        return [
            "gsutil",
            # Enable multiprocessing
            "-m",
            "rsync",
            # Sync subdirectories
            "-r",
            # Ignore mtime of files and use checksums to determine if files are different
            "-c",
            # Delete files that are no longer in the source manifest
            "-d",
            "-x",
            "airflow_monitoring\\.py",
            directory,
            gcs_uri,
        ]

    base_command = gcloud_storage_rsync_command(directory, gcs_uri, dry_run=dry_run)
    additional_options = [
        # Sync subdirectories
        "--recursive",
        # Ignore mtime of files and use checksums to determine if files are different
        "--checksums-only",
        # Delete files that are no longer in the source manifest
        "--delete-unmatched-destination-objects",
        # airflow_monitoring.py is managed by Cloud Composer, do not delete it
        "--exclude=airflow_monitoring\\.py",
    ]
    return base_command + additional_options


def _date_str_from_date_subdir_path(date_subdir_path: str) -> str:
    """Returns the date in ISO format corresponding to the storage subdir path."""
    parts = date_subdir_path.rstrip("/").split("/")
    return f"{parts[-3]}-{parts[-2]}-{parts[-1]}"


# TODO(#37517) make start_date_bound -> state_datetime_bound and make it datetime | None instead
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


# TODO(#45991) deprecate in favor of get_storage_directories_containing_raw_files
# TODO(#37517) need to make these date instead of string and then have the callees convert
# from datetime to date and truncate start_datetime to date and bump end datetime
# exclusive to next day, if it's not padded with 0s
def gsutil_get_storage_subdirs_containing_raw_files(
    storage_bucket_path: GcsfsDirectoryPath,
    upper_bound_date: Optional[str],
    lower_bound_date: Optional[str],
    file_filters: Optional[List[str]] = None,
) -> List[str]:
    """Returns all subdirs containing files in the provided |storage_bucket_path| for a given
    region.

    Args:
        storage_bucket_path: The GCS path to the storage bucket to search for subdirectories.
        upper_bound_date: The upper bound date to search for subdirectories, inclusive.
        lower_bound_date: The lower bound date to search for subdirectories, inclusive.
        file_filters: A list of filters to search for in the subdirectories. Must adhere to URI
            wildcard conventions https://cloud.google.com/storage/docs/wildcards.
    """
    # We search with a double wildcard and then filter in python because it is much
    # faster than doing `gs://{storage_bucket_path}/raw/*/*/*/`
    raw_data_path = GcsfsDirectoryPath.from_dir_and_subdir(storage_bucket_path, "raw")
    subdirs_in_date_range = _get_subdirs_in_date_range(
        raw_data_path, upper_bound_date, lower_bound_date
    )
    # We return all subdirectories in the date range if there are no filters for file tags.
    if not file_filters or not any(file_filters):
        return sorted(list(subdirs_in_date_range))

    subdirs_to_search = subdirs_in_date_range
    subdirs_containing_files: Set[str] = set()
    thread_pool = ThreadPool(processes=16)

    for file_tag_filter in file_filters:
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
    path = subdir + f"/{file_filter}"
    results = gsutil_ls(path, allow_empty=True)
    # if we find any results, add them to |args.subdirs_containing_files|
    if results:
        subdirs_containing_files.add(subdir)

    progress.update(1)
