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
"""Class that handles logic for deciding which file should be processed next
for a given region, given the desired file ordering.
"""

import datetime
from typing import List, Dict, Optional, Set

import pytz

from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsIngestArgs,
    filename_parts_from_path,
    GcsfsDirectIngestFileType,
)
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsFilePath,
    GcsfsBucketPath,
)


class GcsfsDirectIngestJobPrioritizer:
    """Class that handles logic for deciding which file should be processed next
    for a given directory, given the desired file ordering.
    """

    def __init__(
        self,
        fs: DirectIngestGCSFileSystem,
        ingest_bucket_path: GcsfsBucketPath,
        file_tag_rank_list: List[str],
    ):
        self.fs = fs
        self.ingest_bucket_path = ingest_bucket_path
        self.ranks_by_file_tag: Dict[str, str] = self._build_ranks_by_file_tag(
            file_tag_rank_list
        )

    def get_next_job_args(
        self, date_str: Optional[str] = None
    ) -> Optional[GcsfsIngestArgs]:
        """Returns args for the next job to run based on the files currently
        in cloud storage.

        Args:
            date_str: (string) If not None, this function will only return jobs
                for files uploaded on the specified date.
        """
        next_file_path = self._get_next_valid_unprocessed_file_path(date_str)
        if not next_file_path:
            return None

        return GcsfsIngestArgs(
            ingest_time=datetime.datetime.now(tz=pytz.UTC), file_path=next_file_path
        )

    def are_next_args_expected(self, next_args: GcsfsIngestArgs) -> bool:
        """Returns True if the provided args are the args we expect to run next,
        i.e. there are no other files with different file tags we expect to
        see uploaded on this day before we process the specified file.
        """

        date_str = filename_parts_from_path(next_args.file_path).date_str
        expected_next_sort_key_prefix = self._get_expected_next_sort_key_prefix_for_day(
            date_str
        )

        if not expected_next_sort_key_prefix:
            # If we get here, we've seen all the files we expect for a day, so
            # |next_args| represent an extra and/or duplicate file which it's ok
            # to run after everything else.
            return True

        args_sort_key_prefix = self._sort_key_for_file_path(
            next_args.file_path, prefix_only=True
        )

        if not args_sort_key_prefix:
            raise ValueError(f"No known sort key prefix for args [{next_args}]")

        return args_sort_key_prefix <= expected_next_sort_key_prefix

    def are_more_jobs_expected_for_day(self, date_str: str) -> bool:
        """Returns True if we still expect to run more jobs today, based on
        the processed files currently in the ingest bucket.
        """
        return self._get_expected_next_sort_key_prefix_for_day(date_str) is not None

    def _get_next_valid_unprocessed_file_path(
        self, date_str: Optional[str]
    ) -> Optional[GcsfsFilePath]:
        """Returns the path of the unprocessed file in the ingest cloud storage
        bucket that should be processed next.
        """
        if date_str:
            unprocessed_paths = self.fs.get_unprocessed_file_paths_for_day(
                self.ingest_bucket_path,
                date_str,
                GcsfsDirectIngestFileType.INGEST_VIEW,
            )
        else:
            unprocessed_paths = self.fs.get_unprocessed_file_paths(
                self.ingest_bucket_path, GcsfsDirectIngestFileType.INGEST_VIEW
            )

        if not unprocessed_paths:
            return None

        keys_and_paths = []
        for unprocessed_path in unprocessed_paths:
            sort_key = self._sort_key_for_file_path(unprocessed_path, prefix_only=False)
            if sort_key:
                keys_and_paths.append((sort_key, unprocessed_path))

        if not keys_and_paths:
            return None

        sorted_keys_and_paths = sorted(keys_and_paths)
        return sorted_keys_and_paths[0][1]

    def _get_expected_next_sort_key_prefix_for_day(
        self, date_str: str
    ) -> Optional[str]:
        """Returns a sort key that excludes the timestamp/filename_suffix term,
        which describes the next file we expect to see on a given day.
        """
        all_expected = self._get_expected_sort_key_prefixes_for_day(date_str)
        processed = self._get_already_processed_sort_key_prefixes_for_day(date_str)
        to_be_processed = all_expected.difference(processed)
        if not to_be_processed:
            return None

        return sorted(to_be_processed)[0]

    def _get_expected_sort_key_prefixes_for_day(self, date_str: str) -> Set[str]:
        """Returns a set of sort keys without the timestamp/filename_suffix
        term, which describe the files we expect to see on a given day.
        """
        datetime_for_day = datetime.datetime.fromisoformat(date_str)

        sort_keys: Set[str] = set()
        for file_tag in self.ranks_by_file_tag.keys():
            sort_key = self._sort_key(
                datetime_for_day, file_tag, file_name_suffix=None, prefix_only=True
            )
            if not sort_key:
                raise ValueError(f"Unexpected null sort key for file_tag [{file_tag}]")
            sort_keys.add(sort_key)

        return sort_keys

    def _get_already_processed_sort_key_prefixes_for_day(
        self, date_str: str
    ) -> Set[str]:
        """Returns a set of sort keys without the timestamp/filename_suffix
        term, which describe the set of files that have already been processed.
        This set is built for comparison against the set of expected sort keys.
        """
        already_processed_paths = self.fs.get_processed_file_paths_for_day(
            self.ingest_bucket_path,
            date_str,
            file_type_filter=GcsfsDirectIngestFileType.INGEST_VIEW,
        )

        sort_keys: Set[str] = set()
        for path in already_processed_paths:
            sort_key = self._sort_key_for_file_path(path, prefix_only=True)
            if not sort_key:
                raise ValueError(f"Unexpected null sort key for path [{path}]")
            sort_keys.add(sort_key)

        return sort_keys

    def _sort_key_for_file_path(
        self, file_path: GcsfsFilePath, prefix_only: bool
    ) -> Optional[str]:
        """Returns a sort key that will allow us to prioritize when to run an
        ingest job for a file at the given path.

        See |self._sort_key()| for more info.

        Returns:
            The derived sort key for this file.
        """

        parts = filename_parts_from_path(file_path)
        return self._sort_key(
            parts.utc_upload_datetime,
            parts.file_tag,
            parts.filename_suffix,
            prefix_only,
        )

    def _sort_key(
        self,
        utc_upload_datetime: datetime.datetime,
        file_tag: str,
        file_name_suffix: Optional[str],
        prefix_only: bool,
    ) -> Optional[str]:
        """Returns a sort key that will allow us to prioritize when to run an
        ingest job for a file with the given information.

        Sort keys take the form:
            <date term>_<file tag rank>((_<file_name_suffix>)_<timestamp_term>)
        For example: "2019-08-08_00001_001historical_121314567890" OR
            "2019-08-08_00001" if |prefix_only| is false.

        Args:
            utc_upload_datetime: (datetime.datetime) The upload time of the
                file, with millisecond precision.
            file_tag: (string) The file tag - must be a value in
                self.ranks_by_file_tag.
            prefix_only: (bool) Whether to include the timestamp term
                in the sort key. Setting this to false allows us to equate sort
                keys for files with the same tag on a given day, even if they've
                come in with different timestamps.

        Returns:
            The derived sort key.
        """

        if file_tag not in self.ranks_by_file_tag:
            return None

        date_str = utc_upload_datetime.date().isoformat()
        file_tag_rank_str = self.ranks_by_file_tag[file_tag]

        sort_key_parts = [date_str, file_tag_rank_str]
        if not prefix_only:
            utc_time_as_rank_str = utc_upload_datetime.strftime("%H%M%S%f")
            if file_name_suffix:
                sort_key_parts += [file_name_suffix]

            sort_key_parts += [utc_time_as_rank_str]
        return "_".join(sort_key_parts)

    @staticmethod
    def _num_as_rank_str(i: Optional[int]) -> str:
        """Returns a number as a sortable string with up to 5 leading zeroes."""
        return str(i if i else 0).zfill(5)

    def _build_ranks_by_file_tag(self, file_tag_rank_list: List[str]) -> Dict[str, str]:
        return dict(
            {
                (tag, self._num_as_rank_str(i))
                for i, tag in enumerate(file_tag_rank_list)
            }
        )
