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
from typing import Optional, List

from recidiviz import IngestInfo
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import \
    BaseDirectIngestController
from recidiviz.ingest.direct.controllers.direct_ingest_file_metadata_manager import \
    BigQueryDirectIngestFileMetadataManager
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import \
    to_normalized_unprocessed_file_path, SPLIT_FILE_SUFFIX, to_normalized_unprocessed_file_path_from_normalized_path, \
    GcsfsFileContentsHandle
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import DirectIngestRawFileImportManager
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_job_prioritizer \
    import GcsfsDirectIngestJobPrioritizer
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    GcsfsIngestArgs, filename_parts_from_path, \
    gcsfs_direct_ingest_storage_directory_path_for_region, \
    gcsfs_direct_ingest_directory_path_for_region, GcsfsDirectIngestFileType, GcsfsRawDataBQImportArgs, \
    GcsfsIngestViewExportArgs
from recidiviz.ingest.direct.controllers.gcsfs_factory import GcsfsFactory
from recidiviz.ingest.direct.controllers.gcsfs_path import \
    GcsfsFilePath, GcsfsDirectoryPath
from recidiviz.ingest.direct.direct_ingest_controller_utils import check_is_region_launched_in_env


class GcsfsDirectIngestController(
        BaseDirectIngestController[GcsfsIngestArgs, GcsfsFileContentsHandle]):
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

        ingest_job_file_type_filter = \
            GcsfsDirectIngestFileType.INGEST_VIEW \
            if self.region.is_raw_vs_ingest_file_name_detection_enabled() else None
        self.file_prioritizer = \
            GcsfsDirectIngestJobPrioritizer(
                self.fs,
                self.ingest_directory_path,
                self._get_file_tag_rank_list(),
                ingest_job_file_type_filter)

        self.file_split_line_limit = self._FILE_SPLIT_LINE_LIMIT

        self.file_metadata_manager = BigQueryDirectIngestFileMetadataManager(
            region_code=self.region.region_code,
            big_query_client=BigQueryClientImpl())

        self.raw_file_import_manager = DirectIngestRawFileImportManager(
            region=self.region,
            fs=self.fs,
            ingest_directory_path=self.ingest_directory_path)

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

            if self.region.is_raw_vs_ingest_file_name_detection_enabled():
                if parts.file_type == GcsfsDirectIngestFileType.RAW_DATA or (
                        parts.file_type == GcsfsDirectIngestFileType.INGEST_VIEW and
                        self.region.are_ingest_view_exports_enabled_in_env()
                ):
                    self.file_metadata_manager.register_new_file(path)

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
        unnormalized_paths = self.fs.get_unnormalized_file_paths(self.ingest_directory_path)

        unnormalized_path_file_type = GcsfsDirectIngestFileType.RAW_DATA \
            if self.region.is_raw_vs_ingest_file_name_detection_enabled() else GcsfsDirectIngestFileType.UNSPECIFIED

        for path in unnormalized_paths:
            logging.info("File [%s] is not yet seen, normalizing.",
                         path.abs_path())
            self.fs.mv_path_to_normalized_path(path, file_type=unnormalized_path_file_type)

        if unnormalized_paths:
            logging.info(
                "Normalized at least one path - returning, will handle "
                "normalized files separately.")
            # Normalizing file paths will cause the cloud function that calls
            # this function to be re-triggered.
            return

        if self._schedule_any_pre_ingest_tasks():
            return

        ingest_file_type_filter = GcsfsDirectIngestFileType.INGEST_VIEW \
            if self.region.is_raw_vs_ingest_file_name_detection_enabled() else None

        unprocessed_paths = self.fs.get_unprocessed_file_paths(self.ingest_directory_path,
                                                               file_type_filter=ingest_file_type_filter)

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

    def do_raw_data_import(self, data_import_args: GcsfsRawDataBQImportArgs) -> None:
        """Process a raw incoming file by importing it to BQ, tracking it in our metadata tables, and moving it to
        storage on completion.
        """
        check_is_region_launched_in_env(self.region)
        if not self.region.are_raw_data_bq_imports_enabled_in_env():
            raise ValueError(f'Raw data imports not enabled for region [{self.region.region_code}]')

        if not self.fs.exists(data_import_args.raw_data_file_path):
            logging.warning(
                "File path [%s] no longer exists - might have already been "
                "processed or deleted", data_import_args.raw_data_file_path)
            return

        file_metadata = self.file_metadata_manager.get_file_metadata(data_import_args.raw_data_file_path)

        if file_metadata.processed_time:
            logging.warning('File [%s] is already marked as processed. Skipping file processing.',
                            data_import_args.raw_data_file_path.file_name)
            return

        self.raw_file_import_manager.import_raw_file_to_big_query(data_import_args.raw_data_file_path,
                                                                  file_metadata)

        if not self.region.are_ingest_view_exports_enabled_in_env():
            # TODO(3162) This is a stopgap measure for regions that have only partially launched. Delete once SQL
            #  pre-processing is enabled for all direct ingest regions.
            parts = filename_parts_from_path(data_import_args.raw_data_file_path)
            ingest_file_tags = self._get_file_tag_rank_list()

            if parts.file_tag in ingest_file_tags:
                self.fs.copy(
                    data_import_args.raw_data_file_path,
                    GcsfsFilePath.from_absolute_path(to_normalized_unprocessed_file_path_from_normalized_path(
                        data_import_args.raw_data_file_path.abs_path(),
                        file_type_override=GcsfsDirectIngestFileType.INGEST_VIEW
                    ))
                )

        # TODO(3020): Technically moving to the processed path here will trigger the handle_new_files() call we need to
        # now check for ingest view export jobs - consider doing something more explicit, intentional here.
        processed_path = self.fs.mv_path_to_processed_path(data_import_args.raw_data_file_path)
        self.file_metadata_manager.mark_file_as_processed(path=data_import_args.raw_data_file_path,
                                                          metadata=file_metadata,
                                                          processed_time=datetime.datetime.now())

        self.fs.mv_path_to_storage(processed_path, self.storage_directory_path)

    def do_ingest_view_export(self, ingest_view_export_args: GcsfsIngestViewExportArgs) -> None:
        check_is_region_launched_in_env(self.region)
        if not self.region.are_ingest_view_exports_enabled_in_env():
            raise ValueError(f'Ingest view exports not enabled for region [{self.region.region_code}]')

        # TODO(3020): Implement ingest view export
        # TODO(3020): Write rows for new files to the metadata table when we export here
        raise ValueError('Unimplemented!')

    # ============== #
    # JOB SCHEDULING #
    # ============== #

    def _schedule_any_pre_ingest_tasks(self) -> bool:
        """Schedules any tasks related to SQL preprocessing of new files in preparation for ingest of those files into
        our Postgres database.

        Returns True if any jobs were scheduled or if there were already any pre-ingest jobs scheduled. Returns False if
        there are no remaining ingest jobs to schedule and it is safe to proceed with ingest.
        """
        if self._schedule_raw_data_import_tasks():
            return True
        # TODO(3020): We have logic to ensure that we wait 10 min for all files to upload properly before moving on to
        #  ingest. We probably actually need this to happen between raw data import and ingest view export steps - if we
        #  haven't seen all files yet and most recent raw data file came in sometime in the last 10 min, we should wait
        #  to do view exports.
        if self._schedule_ingest_view_export_tasks():
            return True
        return False

    def _schedule_raw_data_import_tasks(self) -> bool:
        if not self.region.are_raw_data_bq_imports_enabled_in_env():
            return False

        queue_info = self.cloud_task_manager.get_bq_import_export_queue_info(self.region)

        did_schedule = False
        tasks_to_schedule = [GcsfsRawDataBQImportArgs(path)
                             for path in self.raw_file_import_manager.get_unprocessed_raw_files_to_import()]
        for task_args in tasks_to_schedule:
            if not queue_info.has_task_already_scheduled(task_args):
                self.cloud_task_manager.create_direct_ingest_raw_data_import_task(self.region, task_args)
                did_schedule = True

        return queue_info.has_raw_data_import_jobs_queued() or did_schedule

    def _get_ingest_view_export_task_args(self) -> List[GcsfsIngestViewExportArgs]:
        if not self.region.are_ingest_view_exports_enabled_in_env():
            raise ValueError(f'Ingest view exports not enabled for region [{self.region.region_code}]')

        # TODO(3020): Implement actual checking for new export jobs - use file metadata tables to figure out last export
        #  date and current export date to process.

        return []

    def _schedule_ingest_view_export_tasks(self) -> bool:
        if not self.region.are_ingest_view_exports_enabled_in_env():
            return False

        queue_info = self.cloud_task_manager.get_bq_import_export_queue_info(self.region)
        if queue_info.has_ingest_view_export_jobs_queued():
            # Since we schedule all export jobs at once, after all raw files have been processed, we wait for all of the
            # export jobs to be done before checking if we need to schedule more.
            # TODO(3020): Write a test that early-returns here if we already have ingest view export tasks queues
            return True

        did_schedule = False
        tasks_to_schedule = self._get_ingest_view_export_task_args()
        for task_args in tasks_to_schedule:
            if not queue_info.has_task_already_scheduled(task_args):
                self.cloud_task_manager.create_direct_ingest_ingest_view_export_task(self.region, task_args)
                did_schedule = True

        # TODO(3020): Test that hits this block with newly scheduled tasks
        # TODO(3020): Test that hits this block without newly scheduled tasks
        return did_schedule

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

    def _get_contents_handle(
            self, args: GcsfsIngestArgs) -> Optional[GcsfsFileContentsHandle]:
        return self._get_contents_handle_from_path(args.file_path)

    def _get_contents_handle_from_path(
            self, path: GcsfsFilePath) -> Optional[GcsfsFileContentsHandle]:
        return self.fs.download_to_temp_file(path)

    @abc.abstractmethod
    def _are_contents_empty(self,
                            contents_handle: GcsfsFileContentsHandle) -> bool:
        pass

    def _can_proceed_with_ingest_for_contents(
            self,
            contents_handle: GcsfsFileContentsHandle):
        return self._are_contents_empty(contents_handle) or \
               self._file_meets_file_line_limit(contents_handle)

    @abc.abstractmethod
    def _file_meets_file_line_limit(
            self, contents_handle: GcsfsFileContentsHandle) -> bool:
        """Subclasses should implement to determine whether the file meets the
        expected line limit"""

    @abc.abstractmethod
    def _parse(self,
               args: GcsfsIngestArgs,
               contents_handle: GcsfsFileContentsHandle) -> IngestInfo:
        pass

    def _split_file_if_necessary(self, path: GcsfsFilePath):
        """Checks if the given file needs to be split according to this
        controller's |file_split_line_limit|.
        """
        parts = filename_parts_from_path(path)

        if self.region.is_raw_vs_ingest_file_name_detection_enabled() and \
                parts.file_type != GcsfsDirectIngestFileType.INGEST_VIEW:
            raise ValueError(f'Should not be attempting to split files other than ingest view files, found path with '
                             f'file type: {parts.file_type}')

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

        file_contents_handle = self._get_contents_handle_from_path(path)

        if not file_contents_handle:
            logging.info("File [%s] has no rows - not splitting.",
                         path.abs_path())
            return False

        if self._can_proceed_with_ingest_for_contents(file_contents_handle):
            logging.info("No need to split file path [%s].", path.abs_path())
            return False

        logging.info("Proceeding to file splitting for path [%s].",
                     path.abs_path())

        self._split_file(path, file_contents_handle)
        return True

    def _create_split_file_path(self,
                                original_file_path: GcsfsFilePath,
                                output_dir: GcsfsDirectoryPath,
                                split_num: int) -> GcsfsFilePath:
        parts = filename_parts_from_path(original_file_path)

        rank_str = str(split_num + 1).zfill(5)
        updated_file_name = (
            f'{parts.stripped_file_name}_{rank_str}'
            f'_{SPLIT_FILE_SUFFIX}_size{self.file_split_line_limit}'
            f'.{parts.extension}')

        file_type = GcsfsDirectIngestFileType.INGEST_VIEW \
            if self.region.is_raw_vs_ingest_file_name_detection_enabled() else GcsfsDirectIngestFileType.UNSPECIFIED

        return GcsfsFilePath.from_directory_and_file_name(
            output_dir,
            to_normalized_unprocessed_file_path(updated_file_name,
                                                file_type=file_type,
                                                dt=parts.utc_upload_datetime))

    @abc.abstractmethod
    def _split_file(self,
                    path: GcsfsFilePath,
                    file_contents_handle: GcsfsFileContentsHandle) -> None:
        """Should be implemented by subclasses to split files that are too large
        and write the new split files to Google Cloud Storage.
        """

    def _do_cleanup(self, args: GcsfsIngestArgs):
        self.fs.mv_path_to_processed_path(args.file_path)

        if self.region.are_ingest_view_exports_enabled_in_env():
            raise ValueError('Must implement metadata update to mark as processed here.')

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

        # Note: at this point, we expect RAW file type files to already have been moved once they were imported to BQ.
        file_type_to_move = GcsfsDirectIngestFileType.INGEST_VIEW \
            if self.region.is_raw_vs_ingest_file_name_detection_enabled() else None

        self.fs.mv_processed_paths_before_date_to_storage(
            self.ingest_directory_path,
            self.storage_directory_path,
            file_type_filter=file_type_to_move,
            date_str_bound=last_processed_date_str,
            include_bound=should_move_last_processed_date)

    @staticmethod
    def file_tag(file_path: GcsfsFilePath) -> str:
        return filename_parts_from_path(file_path).file_tag
