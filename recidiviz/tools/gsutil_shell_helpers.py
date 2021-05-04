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
import os
import subprocess
from typing import List, Optional

from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
)
from recidiviz.common.date import is_date_str, is_between_date_strs_inclusive


def gsutil_ls(gs_path: str, directories_only: bool = False) -> List[str]:
    """Returns list of paths returned by 'gsutil ls <gs_path>.
    E.g.
    gsutil_ls('gs://recidiviz-123-state-storage') ->
        ['gs://recidiviz-123-state-storage/us_nd']

    See more documentation here:
    https://cloud.google.com/storage/docs/gsutil/commands/ls
    """
    res = subprocess.run(
        f'gsutil ls "{gs_path}"',
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )

    if res.stderr:
        raise ValueError(res.stderr.decode("utf-8"))

    result_paths = [p for p in res.stdout.decode("utf-8").splitlines() if p != gs_path]

    if not directories_only:
        return result_paths

    return [p for p in result_paths if p.endswith("/")]


# See https://github.com/GoogleCloudPlatform/gsutil/issues/464#issuecomment-633334888
_GSUTIL_PARALLEL_COMMAND_OPTIONS = (
    "-q -m -o GSUtil:parallel_process_count=1 -o GSUtil:parallel_thread_count=24"
)


def gsutil_cp(from_path: str, to_path: str) -> None:
    """Copies a file/files via 'gsutil cp'.

    See more documentation here:
    https://cloud.google.com/storage/docs/gsutil/commands/cp
    """
    command = f'gsutil {_GSUTIL_PARALLEL_COMMAND_OPTIONS} cp "{from_path}" "{to_path}"'
    res = subprocess.run(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )

    if res.stderr:
        raise ValueError(res.stderr.decode("utf-8"))


def gsutil_mv(from_path: str, to_path: str) -> None:
    """Moves a file/files via 'gsutil mv'.

    See more documentation here:
    https://cloud.google.com/storage/docs/gsutil/commands/mv
    """

    res = subprocess.run(
        f'gsutil {_GSUTIL_PARALLEL_COMMAND_OPTIONS} mv "{from_path}" "{to_path}"',
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )

    if res.stderr:
        raise ValueError(res.stderr.decode("utf-8"))


def _date_str_from_date_subdir_path(date_subdir_path: str) -> str:
    """Returns the date in ISO format corresponding to the storage subdir path."""
    parts = date_subdir_path.rstrip("/").split("/")
    return f"{parts[-3]}-{parts[-2]}-{parts[-1]}"


def _dfs_get_date_subdirs(paths_to_search: List[str], depth: int = 0) -> List[str]:
    """Traverses down through year/month/day subdirectories to contain list of all date subdirectories that contain
    files for a given day."""
    if depth == 3:
        return [
            p
            for p in paths_to_search
            if is_date_str(_date_str_from_date_subdir_path(p))
        ]

    date_subdirs = []
    for p in paths_to_search:
        sub_paths = gsutil_ls(p, directories_only=True)
        date_subdirs.extend(_dfs_get_date_subdirs(sub_paths, depth=depth + 1))

    return date_subdirs


def gsutil_get_storage_subdirs_containing_file_types(
    storage_bucket_path: str,
    file_type: GcsfsDirectIngestFileType,
    upper_bound_date: Optional[str],
    lower_bound_date: Optional[str],
) -> List[str]:
    """Returns all subdirs containing files of type |file_type| in the provided |storage_bucket_path| for a given
    region."""
    subdirs = gsutil_ls(f"gs://{storage_bucket_path}", directories_only=True)

    subdirs_containing_files = []
    for outer_subdir_path in subdirs:
        outer_subdir_name = os.path.basename(os.path.normpath(outer_subdir_path))
        if outer_subdir_name == file_type.value:
            date_subdirs = _dfs_get_date_subdirs([outer_subdir_path])

            for date_path in date_subdirs:
                if is_between_date_strs_inclusive(
                    upper_bound_date=upper_bound_date,
                    lower_bound_date=lower_bound_date,
                    date_of_interest=_date_str_from_date_subdir_path(date_path),
                ):
                    subdirs_containing_files.append(date_path)

    return subdirs_containing_files
