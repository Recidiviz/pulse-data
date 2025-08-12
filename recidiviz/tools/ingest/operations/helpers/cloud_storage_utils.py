# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Helpers for working with cloud storage buckets in scripts"""
import datetime
from enum import Enum, auto
from functools import lru_cache, partial

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.common.date import is_between_date_strs_inclusive
from recidiviz.utils.future_executor import map_fn_with_results


class _StorageDirectoryLevel(Enum):
    """Our direct ingest cloud storage buckets are structured like:

    <bucket-name>/raw/<year>/<month>/<day>/

    When we are searching for files based on a date, this enum helps represent and reason
    about what level in the date folders we are in.
    """

    YEAR = auto()
    MONTH = auto()
    DAY = auto()

    def between_dates_for_level(
        self,
        path: GcsfsDirectoryPath,
        lower_bound_date_inclusive: datetime.date | None,
        upper_bound_date_inclusive: datetime.date | None,
    ) -> bool:
        """For the provided level, determines if |path| is between |lower_bound_date_inclusive|
        and |upper_bound_date_inclusive|.
        """
        lower_bound_date_part_inclusive = self.truncate_date_for_level(
            level=self, date_to_trunc=lower_bound_date_inclusive
        )
        upper_bound_date_part_inclusive = self.truncate_date_for_level(
            level=self, date_to_trunc=upper_bound_date_inclusive
        )
        path_parts = path.relative_path.rstrip("/").split("/")
        match self:
            case _StorageDirectoryLevel.YEAR:
                return is_between_date_strs_inclusive(
                    date_of_interest=path_parts[-1],
                    upper_bound_date=upper_bound_date_part_inclusive,
                    lower_bound_date=lower_bound_date_part_inclusive,
                )
            case _StorageDirectoryLevel.MONTH:
                return is_between_date_strs_inclusive(
                    date_of_interest=f"{path_parts[-2]}-{path_parts[-1]}",
                    upper_bound_date=upper_bound_date_part_inclusive,
                    lower_bound_date=lower_bound_date_part_inclusive,
                )
            case _StorageDirectoryLevel.DAY:
                return is_between_date_strs_inclusive(
                    date_of_interest=f"{path_parts[-3]}-{path_parts[-2]}-{path_parts[-1]}",
                    upper_bound_date=upper_bound_date_part_inclusive,
                    lower_bound_date=lower_bound_date_part_inclusive,
                )

    @staticmethod
    @lru_cache(maxsize=6)
    def truncate_date_for_level(
        *, level: "_StorageDirectoryLevel", date_to_trunc: datetime.date | None
    ) -> str | None:
        if date_to_trunc is None:
            return None
        match level:
            case _StorageDirectoryLevel.YEAR:
                return date_to_trunc.strftime("%Y")
            case _StorageDirectoryLevel.MONTH:
                return date_to_trunc.strftime("%Y-%m")
            case _StorageDirectoryLevel.DAY:
                return date_to_trunc.strftime("%Y-%m-%d")


def _filter_candidate_paths_by_dates(
    *,
    level: _StorageDirectoryLevel,
    candidate_paths: list[GcsfsDirectoryPath],
    lower_bound_date_inclusive: datetime.date | None,
    upper_bound_date_inclusive: datetime.date | None,
) -> list[GcsfsDirectoryPath]:
    """Filters the |candidate_paths| by |lower_bound_date_inclusive| and
    |upper_bound_date_inclusive|, using the knowledge that we are at |level| in the
    storage directory hierarchy.
    """
    if not upper_bound_date_inclusive and not lower_bound_date_inclusive:
        return candidate_paths

    return [
        path
        for path in candidate_paths
        if level.between_dates_for_level(
            path,
            lower_bound_date_inclusive=lower_bound_date_inclusive,
            upper_bound_date_inclusive=upper_bound_date_inclusive,
        )
    ]


def _get_subdirs_in_opt_date_range(
    fs: GCSFileSystem,
    storage_bucket_path: GcsfsDirectoryPath,
    lower_bound_date_inclusive: datetime.date | None,
    upper_bound_date_inclusive: datetime.date | None,
) -> list[GcsfsDirectoryPath]:
    """Returns a list of all possible subdirectory paths below |storage_bucket_path|
    that are within |lower_bound_date_inclusive| and |upper_bound_date_inclusive|.
    """

    paths_to_search = [
        GcsfsDirectoryPath.from_dir_and_subdir(storage_bucket_path, "raw/")
    ]
    for level in [
        _StorageDirectoryLevel.YEAR,
        _StorageDirectoryLevel.MONTH,
        _StorageDirectoryLevel.DAY,
    ]:
        _paths_for_next_level = []
        for path in paths_to_search:
            candidate_paths = fs.list_directories(path)
            filtered_paths = _filter_candidate_paths_by_dates(
                level=level,
                candidate_paths=candidate_paths,
                lower_bound_date_inclusive=lower_bound_date_inclusive,
                upper_bound_date_inclusive=upper_bound_date_inclusive,
            )
            _paths_for_next_level.extend(filtered_paths)
        paths_to_search = _paths_for_next_level

    return paths_to_search


def _search_directory(
    candidate_directory: GcsfsDirectoryPath, *, fs: GCSFileSystem, file_filter_glob: str
) -> bool:
    """Searches |candidate_directory| using |file_filter_glob|, returning a boolean
    for if any results were returned.
    """
    match_glob = f"{candidate_directory.relative_path.rstrip('/')}/{file_filter_glob}"
    return (
        len(fs.ls(bucket_name=candidate_directory.bucket_name, match_glob=match_glob))
        > 0
    )


def _get_subdirs_with_file_filters(
    fs: GCSFileSystem,
    candidate_directories: list[GcsfsDirectoryPath],
    file_filter_globs: list[str],
) -> list[GcsfsDirectoryPath]:
    """Filters out items in |candidate_directories| that don't have at least one match
    returned by each |file_filter_globs| glob pattern.
    """

    subdirectories_to_be_searched = candidate_directories
    subdirs_containing_files: list[GcsfsDirectoryPath] = []

    for file_filter_glob in file_filter_globs:
        result = map_fn_with_results(
            work_items=subdirectories_to_be_searched,
            work_fn=partial(
                _search_directory, fs=fs, file_filter_glob=file_filter_glob
            ),
            overall_timeout_sec=60,  # 1 minute
            single_work_item_timeout_sec=60 * 60,  # 1 hour
        )

        if result.exceptions:
            raise ExceptionGroup(
                f"Found errors while searching for subdirectories matching [{file_filter_glob}]",
                [e[1] for e in result.exceptions],
            )

        subdirectories_to_be_searched = []
        # if we find a single match for a filter inside of a subdir, we've already marked
        # it as containing files so we dont need to revisit
        for subdirectory, found_match in result.successes:
            if found_match:
                subdirs_containing_files.append(subdirectory)
            else:
                subdirectories_to_be_searched.append(subdirectory)

    return subdirs_containing_files


def get_storage_directories_containing_raw_files(
    fs: GCSFileSystem,
    storage_bucket_path: GcsfsDirectoryPath,
    lower_bound_date_inclusive: datetime.date | None,
    upper_bound_date_inclusive: datetime.date | None,
    file_filter_globs: list[str] | None = None,
) -> list[GcsfsDirectoryPath]:
    """Returns all subdirs containing files in the provided |storage_bucket_path|, which
    we assume conforms to our standard storage directory layout

    Args:
        storage_bucket_path (GcsfsDirectoryPath): The GCS path to the storage bucket to
            search for subdirectories.
        lower_bound_date (date | None): The lower bound date to search for directories,
            inclusive.
        upper_bound_date (date | None): The upper bound date to search for directories,
            inclusive.
        file_filters: A list of filters to search for in the subdirectories. Must adhere
            to gcs match glob conventions, see
            https://cloud.google.com/storage/docs/json_api/v1/objects/list#list-object-glob
    """

    subdirs_in_date_range = _get_subdirs_in_opt_date_range(
        fs=fs,
        storage_bucket_path=storage_bucket_path,
        lower_bound_date_inclusive=lower_bound_date_inclusive,
        upper_bound_date_inclusive=upper_bound_date_inclusive,
    )
    if not file_filter_globs:
        return subdirs_in_date_range

    return _get_subdirs_with_file_filters(
        fs, subdirs_in_date_range, file_filter_globs=file_filter_globs
    )
