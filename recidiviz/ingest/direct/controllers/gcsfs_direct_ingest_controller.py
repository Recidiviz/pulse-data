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
from typing import Optional, List, Iterable

from recidiviz import IngestInfo
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import \
    BaseDirectIngestController
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_job_prioritizer \
    import GcsfsDirectIngestJobPrioritizer
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    GcsfsIngestArgs, filename_parts_from_path, \
    gcsfs_direct_ingest_storage_directory_path_for_region, \
    gcsfs_direct_ingest_directory_path_for_region
from recidiviz.ingest.direct.controllers.gcsfs_factory import GcsfsFactory
from recidiviz.ingest.direct.errors import DirectIngestError, \
    DirectIngestErrorType


class GcsfsDirectIngestController(BaseDirectIngestController[GcsfsIngestArgs,
                                                             Iterable[str]]):
    """Controller for parsing and persisting a file in the GCS filesystem."""

    _MAX_STORAGE_FILE_RENAME_TRIES = 10
    _MAX_PROCESS_JOB_WAIT_TIME_SEC = 300

    def __init__(self,
                 region_name: str,
                 system_level: SystemLevel,
                 ingest_directory_path: Optional[str] = None,
                 storage_directory_path: Optional[str] = None):
        super().__init__(region_name, system_level)
        self.fs = GcsfsFactory.build()

        if ingest_directory_path:
            self.ingest_directory_path = ingest_directory_path
        else:
            self.ingest_directory_path = \
                gcsfs_direct_ingest_directory_path_for_region(region_name,
                                                              system_level)
        if storage_directory_path:
            self.storage_directory_path = storage_directory_path
        else:
            self.storage_directory_path = \
                gcsfs_direct_ingest_storage_directory_path_for_region(
                    region_name, system_level)

        self.file_prioritizer = \
            GcsfsDirectIngestJobPrioritizer(
                self.fs,
                self.ingest_directory_path,
                self._get_file_tag_rank_list())

    # ============== #
    # JOB SCHEDULING #
    # ============== #

    @abc.abstractmethod
    def _get_file_tag_rank_list(self) -> List[str]:
        pass

    def _get_next_job_args(self) -> Optional[GcsfsIngestArgs]:
        return self.file_prioritizer.get_next_job_args()

    def _wait_time_sec_for_next_args(self, args: GcsfsIngestArgs) -> int:
        if self.file_prioritizer.are_next_args_expected(args):
            # Run job immediately
            return 0

        now = datetime.datetime.utcnow()
        file_upload_time: datetime.datetime = \
            filename_parts_from_path(args.file_path).utc_upload_datetime

        max_wait_from_file_upload_time = \
            file_upload_time + datetime.timedelta(
                seconds=self._MAX_PROCESS_JOB_WAIT_TIME_SEC)

        if max_wait_from_file_upload_time <= now:
            wait_time = 0
        else:
            wait_time = (max_wait_from_file_upload_time - now).seconds

        logging.info("Waiting [%s] sec for [%s]",
                     wait_time, self._job_tag(args))
        return wait_time

    def _on_job_scheduled(self, ingest_args: GcsfsIngestArgs):
        pass

    # =================== #
    # SINGLE JOB RUN CODE #
    # =================== #

    def _job_tag(self, args: GcsfsIngestArgs) -> str:
        return f'{self.region.region_code}/{self.file_name(args.file_path)}:' \
            f'{args.ingest_time}'

    def _read_contents(self, args: GcsfsIngestArgs) -> Optional[Iterable[str]]:
        if not args.file_path:
            raise DirectIngestError(
                msg=f"File path not set for job [{self._job_tag(args)}]",
                error_type=DirectIngestErrorType.INPUT_ERROR)

        if not self.fs.exists(args.file_path):
            logging.info(
                "File path [%s] no longer exists - might have already been "
                "processed or deleted", args.file_path)
            return None

        # TODO(1840): Turn this into a generator that only reads / yields lines
        #  one at a time so we don't hold entire large files in memory. NOTE:
        #  calling fp.readLine() does a GET request every time, so this impl
        #  would have to be smarter about calling read() in chunks.
        with self.fs.open(args.file_path) as fp:
            logging.info(
                "Opened path [%s] - now reading contents.", args.file_path)
            binary_contents = fp.read()
            logging.info(
                "Finished reading binary contents for path [%s], now decoding.",
                args.file_path)

        return binary_contents.decode('utf-8').splitlines()

    @abc.abstractmethod
    def _are_contents_empty(self,
                            contents: Iterable[str]) -> bool:
        pass

    @abc.abstractmethod
    def _parse(self,
               args: GcsfsIngestArgs,
               contents: Iterable[str]) -> IngestInfo:
        pass

    def _do_cleanup(self, args: GcsfsIngestArgs):
        self.fs.mv_path_to_processed_path(args.file_path)

        parts = filename_parts_from_path(args.file_path)
        directory_path, _ = os.path.split(args.file_path)

        next_args_for_day = \
            self.file_prioritizer.get_next_job_args(date_str=parts.date_str)

        # TODO(1628): Consider moving all files to storage after a whole day has
        #  passed, even if there are still expected files?
        day_complete = not next_args_for_day and not \
            self.file_prioritizer.are_more_jobs_expected_for_day(parts.date_str)

        if day_complete:
            logging.info(
                "All expected files found for day [%s]. Moving to storage.",
                parts.date_str)
            self.fs.mv_paths_from_date_to_storage(directory_path,
                                                  parts.date_str,
                                                  self.storage_directory_path)

    @staticmethod
    def file_name(file_path: Optional[str]) -> Optional[str]:
        if not file_path:
            return None

        _, file_name = os.path.split(file_path)
        return file_name

    @staticmethod
    def file_tag(file_path: str) -> str:
        return filename_parts_from_path(file_path).file_tag
