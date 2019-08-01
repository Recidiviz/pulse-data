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
"""Controller for parsing and persisting a file in the GCS filesystem."""
import abc
import datetime
import logging
import os
from collections import defaultdict
from typing import Optional, List, Dict, Sequence

import attr

from recidiviz import IngestInfo
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import \
    BaseDirectIngestController
from recidiviz.ingest.direct.controllers.direct_ingest_types import \
    IngestArgs, \
    ArgsPriorityQueue
from recidiviz.ingest.direct.controllers.gcsfs_factory import GcsfsFactory
from recidiviz.ingest.direct.errors import DirectIngestError, \
    DirectIngestErrorType


@attr.s(frozen=True)
class GcsfsIngestArgs(IngestArgs):
    file_path: str = attr.ib()
    storage_bucket: str = attr.ib()


class GcsfsDirectIngestController(BaseDirectIngestController[GcsfsIngestArgs,
                                                             str]):
    """Controller for parsing and persisting a file in the GCS filesystem."""

    _MAX_STORAGE_FILE_RENAME_TRIES = 10

    def __init__(self, region_name: str, system_level: SystemLevel):
        super().__init__(region_name, system_level)
        self.fs = GcsfsFactory.build()
        # TODO(1628): If we re-deploy, this will get wiped - probably just need
        # to always read all the files from disk every time we want to decide
        # what to do next (only relevant incide the GCSFS controllers) and
        # also trigger a job on startup (all controllers?).
        self.pending_jobs_queue = ArgsPriorityQueue(
            sort_key_gen=self._sort_key_for_args)

        self.processed_by_date_str: Dict[str, List[GcsfsIngestArgs]] = \
            defaultdict(list)

        self.ranks_by_file_tag: Dict[str, str] = self._build_ranks_by_file_tag()

    def _num_as_rank_str(self, i: Optional[int]) -> str:
        """Returns a number as a sortable string with up to 5 leading zeroes.
        """
        return str(i if i else 0).zfill(5)

    def _build_ranks_by_file_tag(self) -> Dict[str, str]:
        return dict({(tag, self._num_as_rank_str(i))
                     for i, tag in enumerate(self._get_file_tag_rank_list())})

    def queue_ingest_job(self, args: GcsfsIngestArgs):
        logging.info("Queueing ingest job [%s]. Queue size = [%s]",
                     self._job_tag(args),
                     self.pending_jobs_queue.size())
        self.pending_jobs_queue.push(args)
        self.run_next_ingest_job_if_necessary_or_schedule_wait(args)

    def _get_next_job_args(self, _) -> Optional[GcsfsIngestArgs]:
        return self.pending_jobs_queue.peek()

    def _wait_time_sec_for_next_args(self, args: GcsfsIngestArgs) -> int:
        expected_next_sort_key = self._get_expected_next_sort_key_for_day(
            args.ingest_time)

        args_sort_key = self._sort_key_for_args(args)

        if args_sort_key <= expected_next_sort_key:
            # Run job immediately
            print(f'Running {args_sort_key} - expected_next_sort_key = '
                  f'{expected_next_sort_key}')
            return 0

        # Otherwise wait for a backoff period
        now = datetime.datetime.now()

        # TODO(1628): MAKE THIS BACKOFF PERIOD LONGER
        five_sec_from_ingest_time = \
            datetime.datetime.now() + datetime.timedelta(seconds=5)

        wait_time = max((five_sec_from_ingest_time - now).seconds, 0)
        print(f'Waiting {wait_time} sec for {args_sort_key}')
        return wait_time

    def _get_expected_next_sort_key_for_day(
            self,
            ingest_time: datetime.datetime):
        all_expected = set(self._get_expected_sort_keys_for_day(ingest_time))
        processed = set(
            self._get_already_processed_sort_keys_for_day(ingest_time))

        to_be_processed = all_expected.difference(processed)
        if not to_be_processed:
            return None

        return sorted(to_be_processed)[0]

    def _get_expected_sort_keys_for_day(self,
                                        ingest_time: datetime.datetime):
        # Note: typically we only expect one file of a given type on a given day
        seq_num = 0
        return [self._sort_key(ingest_time, file_tag, seq_num)
                for file_tag, rank_str in self.ranks_by_file_tag.items()]

    def _get_already_processed_sort_keys_for_day(
            self, ingest_time: datetime.datetime):
        already_processed_paths = \
            self._get_already_processed_file_paths_for_day(ingest_time)
        return [self._sort_key_for_file_path(ingest_time, path)
                for path in already_processed_paths]

    def _get_already_processed_file_paths_for_day(
            self,
            ingest_time: datetime.datetime) -> List[str]:
        # TODO(1628): Implement by reading from storage bucket (hack) or some
        #  temp holding folder inside the normal ingest bucket (would have to
        #  write code that later does cleanup to move to actual cold storage
        #  for the previous day once we start processing for the next day).

        args_list: Sequence[GcsfsIngestArgs] = \
            self.processed_by_date_str.get(ingest_time.date().isoformat(), [])

        return [args.file_path for args in args_list]

    def _sort_key_for_args(self, args: GcsfsIngestArgs) -> str:
        # By default, order by time job is queued
        return self._sort_key_for_file_path(args.ingest_time, args.file_path)

    def _sort_key_for_file_path(self,
                                ingest_time: datetime.datetime,
                                file_path: str) -> str:
        # TODO(1628): Parse ingest time and sequence number out of file path
        #  once we are modifying file names in the cloud function.
        return self._sort_key(ingest_time, self.file_tag(file_path), 0)

    def _sort_key(self,
                  ingest_time: datetime.datetime,
                  file_tag: str,
                  seq_num: int):
        date_str = ingest_time.date().isoformat()
        file_tag_rank_str = self.ranks_by_file_tag[file_tag]
        file_seq_rank_str = self._num_as_rank_str(seq_num)
        return f'{date_str}_{file_tag_rank_str}_{file_seq_rank_str}'

    def _job_tag(self, args: GcsfsIngestArgs) -> str:
        return f'{self.region.region_code}/{self.file_name(args.file_path)}:' \
            f'{args.ingest_time}'

    def _read_contents(self, args: GcsfsIngestArgs) -> str:
        if not args.file_path:
            raise DirectIngestError(
                msg=f"File path not set for job [{self._job_tag(args)}]",
                error_type=DirectIngestErrorType.INPUT_ERROR)

        with self.fs.open(args.file_path) as fp:
            return fp.read().decode('utf-8')

    @abc.abstractmethod
    def _get_file_tag_rank_list(self) -> List[str]:
        pass

    @abc.abstractmethod
    def _parse(self,
               args: GcsfsIngestArgs,
               contents: str) -> IngestInfo:
        pass

    def _storage_path(self,
                      storage_bucket: str,
                      ingest_time: datetime.datetime,
                      file_name: str) -> str:
        ingest_date_str = ingest_time.date().isoformat()

        def _build_storage_path(storage_file_name: str):
            return os.path.join(storage_bucket,
                                self.region.region_code,
                                ingest_date_str,
                                storage_file_name)

        storage_path = _build_storage_path(file_name)

        tries = 0
        while self.fs.exists(storage_path):
            if tries >= self._MAX_STORAGE_FILE_RENAME_TRIES:
                raise DirectIngestError(
                    msg=f"Too many versions of file [{file_name}] stored in "
                    f"bucket for date {ingest_date_str}",
                    error_type=DirectIngestErrorType.CLEANUP_ERROR
                )
            file_tag, file_extension = file_name.split('.')
            updated_file_name = f'{file_tag}_{tries}.{file_extension}'
            logging.warning(
                "Desired storage path %s already exists, updating name to %s.",
                storage_path, updated_file_name)
            storage_path = _build_storage_path(updated_file_name)

        return storage_path

    def _do_cleanup(self, args: GcsfsIngestArgs):
        self.pending_jobs_queue.pop()
        date_str = args.ingest_time.date().isoformat()
        self.processed_by_date_str[date_str].append(args)

        if not args.storage_bucket:
            raise DirectIngestError(
                msg=f"No storage bucket for job [{self._job_tag(args)}]",
                error_type=DirectIngestErrorType.INPUT_ERROR)

        file_name = self.file_name(args.file_path)
        if not file_name:
            raise DirectIngestError(
                msg=f"No file name for job [{self._job_tag(args)}]",
                error_type=DirectIngestErrorType.INPUT_ERROR)

        storage_path = self._storage_path(args.storage_bucket,
                                          args.ingest_time,
                                          file_name)

        logging.info("Moving file %s to storage path %s",
                     args.file_path,
                     storage_path)
        self.fs.mv(args.file_path, storage_path)

    @staticmethod
    def file_name(file_path: Optional[str]) -> Optional[str]:
        if not file_path:
            return None

        _, file_name = os.path.split(file_path)
        return file_name

    def file_tag(self,
                 file_path: Optional[str]) -> str:
        # TODO(2058): Right now this function assumes that paths will take the
        #  form /path/to/file/{file_tag}.csv. We will eventually need to handle
        #  file names that include dates or which have numbers from upload
        #  conflicts.
        file_name = self.file_name(file_path)
        if not file_name:
            raise DirectIngestError(
                msg=f"No file name for path [{file_path}]",
                error_type=DirectIngestErrorType.INPUT_ERROR)

        return file_name.split('.')[0]
