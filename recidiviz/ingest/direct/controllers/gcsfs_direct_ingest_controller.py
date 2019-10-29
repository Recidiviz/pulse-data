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
from typing import Optional, List, Iterable

from recidiviz import IngestInfo
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import \
    BaseDirectIngestController
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import \
    to_normalized_unprocessed_file_path, SPLIT_FILE_SUFFIX
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_job_prioritizer \
    import GcsfsDirectIngestJobPrioritizer
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    GcsfsIngestArgs, filename_parts_from_path, \
    gcsfs_direct_ingest_storage_directory_path_for_region, \
    gcsfs_direct_ingest_directory_path_for_region
from recidiviz.ingest.direct.controllers.gcsfs_factory import GcsfsFactory
from recidiviz.ingest.direct.controllers.gcsfs_path import \
    GcsfsFilePath, GcsfsDirectoryPath
from recidiviz.ingest.direct.errors import DirectIngestError, \
    DirectIngestErrorType


class GcsfsDirectIngestController(BaseDirectIngestController[GcsfsIngestArgs,
                                                             Iterable[str]]):
    """Controller for parsing and persisting a file in the GCS filesystem."""

    _MAX_STORAGE_FILE_RENAME_TRIES = 10
    _DEFAULT_MAX_PROCESS_JOB_WAIT_TIME_SEC = 300
    _FILE_SPLIT_LINE_LIMIT = 2500

    def __init__(self,
                 region_name: str,
                 system_level: SystemLevel,
                 ingest_directory_path: Optional[str] = None,
                 storage_directory_path: Optional[str] = None,
                 max_delay_sec_between_files: Optional[int] = None):
        super().__init__(region_name, system_level)
        self.fs = GcsfsFactory.build()
        self.max_delay_sec_between_files = max_delay_sec_between_files

        if not ingest_directory_path:
            ingest_directory_path = \
                gcsfs_direct_ingest_directory_path_for_region(region_name,
                                                              system_level)
        self.ingest_directory_path = \
            GcsfsDirectoryPath.from_absolute_path(ingest_directory_path)

        if not storage_directory_path:
            storage_directory_path = \
                gcsfs_direct_ingest_storage_directory_path_for_region(
                    region_name, system_level)

        self.storage_directory_path = \
            GcsfsDirectoryPath.from_absolute_path(storage_directory_path)

        self.file_prioritizer = \
            GcsfsDirectIngestJobPrioritizer(
                self.fs,
                self.ingest_directory_path,
                self._get_file_tag_rank_list())

        self.file_split_line_limit = self._FILE_SPLIT_LINE_LIMIT

    # ================= #
    # NEW FILE HANDLING #
    # ================= #
    def handle_file(self, path: GcsfsFilePath, start_ingest: bool):
        """Called when a single new file is added to an ingest bucket (may also
        be called as a result of a rename).

        May be called from any worker/queue.
        """
        if self.fs.is_processed_file(path):
            logging.info("File [%s] is already processed, returning.",
                         path.abs_path())
            return

        if self.fs.is_normalized_file_path(path):
            parts = filename_parts_from_path(path)
            if parts.is_file_split and \
                    parts.file_split_size and \
                    parts.file_split_size <= self.file_split_line_limit:
                self.kick_scheduler(just_finished_job=False)
                logging.info("File [%s] is already normalized and split split "
                             "with correct size, kicking scheduler.",
                             path.abs_path())
                return

        logging.info("Creating cloud task to schedule next job.")
        self.cloud_task_manager.create_direct_ingest_handle_new_files_task(
            region=self.region,
            can_start_ingest=start_ingest)

    def handle_new_files(self, can_start_ingest: bool):
        """Searches the ingest directory for new/unprocessed files. Normalizes
        file names and splits files as necessary, schedules the next ingest job
        if allowed.


        Should only be called from the scheduler queue.
        """
        unnormalized_paths = self.fs.get_unnormalized_file_paths(
            self.ingest_directory_path)

        for path in unnormalized_paths:
            logging.info("File [%s] is not yet seen, normalizing.",
                         path.abs_path())
            self.fs.mv_path_to_normalized_path(path)

        if unnormalized_paths:
            logging.info(
                "Normalized at least one path - returning, will handle "
                "normalized files separately.")
            # Normalizing file paths will cause the cloud function that calls
            # this function to be re-triggered.
            return

        unprocessed_paths = self.fs.get_unprocessed_file_paths(
            self.ingest_directory_path)

        did_split = False
        for path in unprocessed_paths:
            if self._split_file_if_necessary(path):
                did_split = True

        if did_split:
            logging.info(
                "Split at least one path - returning, will handle split "
                "files separately.")
            # Writing new split files to storage will cause the cloud function
            # that calls this function to be re-triggered.
            return

        if can_start_ingest and unprocessed_paths:
            self.schedule_next_ingest_job_or_wait_if_necessary(
                just_finished_job=False)

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

        max_delay_sec = self.max_delay_sec_between_files \
            if self.max_delay_sec_between_files is not None \
            else self._DEFAULT_MAX_PROCESS_JOB_WAIT_TIME_SEC
        max_wait_from_file_upload_time = \
            file_upload_time + datetime.timedelta(seconds=max_delay_sec)

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
        return f'{self.region.region_code}/{args.file_path.file_name}:' \
            f'{args.ingest_time}'

    def _read_contents(self, args: GcsfsIngestArgs) -> Optional[Iterable[str]]:
        if not args.file_path:
            raise DirectIngestError(
                msg=f"File path not set for job [{self._job_tag(args)}]",
                error_type=DirectIngestErrorType.INPUT_ERROR)

        return self._read_file_contents(args.file_path)

    def _read_file_contents(
            self,
            file_path: GcsfsFilePath) -> Optional[Iterable[str]]:
        if not self.fs.exists(file_path):
            logging.info(
                "File path [%s] no longer exists - might have already been "
                "processed or deleted", file_path)
            return None

        # TODO(1840): Turn this into a generator that only reads / yields lines
        #  one at a time so we don't hold entire large files in memory. NOTE:
        #  this would require implementing a fs function to read a Cloud Storage
        #  file to a temp file on disk.

        logging.info(
            'Getting storage_client with bucket [%s] and filepath [%s] '
            '(time: [%s])',
            file_path.bucket_name,
            file_path.blob_name,
            datetime.datetime.now().isoformat())
        logging.info(
            "Opening path [%s] and reading contents (time: [%s]).",
            file_path, datetime.datetime.now().isoformat())
        binary_contents = self.fs.download_as_string(file_path)
        logging.info(
            "Finished reading binary contents for path [%s] (time: [%s]), "
            "now decoding.",
            file_path, datetime.datetime.now().isoformat())

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

    def _split_file_if_necessary(self, path: GcsfsFilePath):
        """Checks if the given file needs to be split according to this
        controller's |file_split_line_limit|.
        """
        parts = filename_parts_from_path(path)

        if parts.file_tag not in self._get_file_tag_rank_list():
            logging.info("File tag [%s] for path [%s] not in rank list - "
                         "not splitting.",
                         parts.file_tag,
                         path.abs_path())
            return False

        if parts.is_file_split and \
                parts.file_split_size and \
                parts.file_split_size <= self.file_split_line_limit:
            logging.info("File [%s] already split with size [%s].",
                         path.abs_path(), parts.file_split_size)
            return False

        file_contents = self._read_file_contents(path)

        if not file_contents:
            logging.info("File [%s] has no rows - not splitting.",
                         path.abs_path())
            return False

        if self._can_proceed_with_ingest_for_contents(file_contents):
            logging.info("No need to split file path [%s].", path.abs_path())
            return False

        logging.info("Proceeding to file splitting for path [%s].",
                     path.abs_path())

        self._split_file(path, file_contents)
        return True

    def _create_split_file_path(self,
                                original_file_path: GcsfsFilePath,
                                output_dir: GcsfsDirectoryPath,
                                split_num: int) -> GcsfsFilePath:
        parts = filename_parts_from_path(original_file_path)

        rank_str = str(split_num + 1).zfill(3)
        existing_suffix = \
            f'_{parts.filename_suffix}' if parts.filename_suffix else ''
        updated_file_name = (
            f'{parts.file_tag}{existing_suffix}_{rank_str}'
            f'_{SPLIT_FILE_SUFFIX}_size{self.file_split_line_limit}'
            f'.{parts.extension}')
        return GcsfsFilePath.from_directory_and_file_name(
            output_dir,
            to_normalized_unprocessed_file_path(updated_file_name,
                                                dt=parts.utc_upload_datetime))

    @abc.abstractmethod
    def _split_file(self,
                    path: GcsfsFilePath,
                    file_contents: Iterable[str]) -> None:
        """Should be implemented by subclasses to split files that are too large
        and write the new split files to Google Cloud Storage.
        """

    def _do_cleanup(self, args: GcsfsIngestArgs):
        self.fs.mv_path_to_processed_path(args.file_path)

        parts = filename_parts_from_path(args.file_path)
        self._move_processed_files_to_storage_as_necessary(
            last_processed_date_str=parts.date_str)

    def _is_last_job_for_day(self, args: GcsfsIngestArgs) -> bool:
        """Returns True if the file handled in |args| is the last file for that
        upload date."""
        parts = filename_parts_from_path(args.file_path)
        upload_date, date_str = parts.utc_upload_datetime, parts.date_str
        more_jobs_expected = \
            self.file_prioritizer.are_more_jobs_expected_for_day(date_str)
        if more_jobs_expected:
            return False
        next_job_args = self.file_prioritizer.get_next_job_args(date_str)
        if next_job_args:
            next_job_date = filename_parts_from_path(
                next_job_args.file_path).utc_upload_datetime
            return next_job_date > upload_date
        return True

    def _move_processed_files_to_storage_as_necessary(
            self, last_processed_date_str: str):
        next_args = self.file_prioritizer.get_next_job_args()

        should_move_last_processed_date = False
        if not next_args:
            are_more_jobs_expected = \
                self.file_prioritizer.are_more_jobs_expected_for_day(
                    last_processed_date_str)
            if not are_more_jobs_expected:
                should_move_last_processed_date = True
        else:
            next_date_str = \
                filename_parts_from_path(next_args.file_path).date_str
            if next_date_str < last_processed_date_str:
                logging.info("Found a file [%s] from a date previous to our "
                             "last processed date - not moving anything to "
                             "storage.")
                return

            # If there are still more to process on this day, do not move files
            # from this day.
            should_move_last_processed_date = \
                next_date_str != last_processed_date_str

        self.fs.mv_processed_paths_before_date_to_storage(
            self.ingest_directory_path,
            self.storage_directory_path,
            last_processed_date_str,
            include_bound=should_move_last_processed_date)

    @staticmethod
    def file_tag(file_path: GcsfsFilePath) -> str:
        return filename_parts_from_path(file_path).file_tag
