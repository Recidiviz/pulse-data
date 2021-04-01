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
import logging
from typing import Optional, List

from recidiviz import IngestInfo
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.cloud_storage.gcs_file_system import GcsfsFileContentsHandle
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import (
    BaseDirectIngestController,
)
from recidiviz.ingest.direct.controllers.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_file_path,
    SPLIT_FILE_SUFFIX,
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.controllers.direct_ingest_ingest_view_export_manager import (
    DirectIngestIngestViewExportManager,
)
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileImportManager,
)
from recidiviz.ingest.direct.controllers.direct_ingest_view_collector import (
    DirectIngestPreProcessedIngestViewCollector,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_job_prioritizer import (
    GcsfsDirectIngestJobPrioritizer,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsIngestArgs,
    filename_parts_from_path,
    GcsfsDirectIngestFileType,
    GcsfsRawDataBQImportArgs,
    GcsfsIngestViewExportArgs,
    gcsfs_direct_ingest_temporary_output_directory_path,
    gcsfs_direct_ingest_storage_directory_path_for_region,
)
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsFilePath,
    GcsfsDirectoryPath,
    GcsfsBucketPath,
)
from recidiviz.ingest.direct.controllers.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestFileMetadataManager,
)
from recidiviz.ingest.direct.direct_ingest_controller_utils import (
    check_is_region_launched_in_env,
)
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestIngestFileMetadata,
)
from recidiviz.utils import trace


class GcsfsDirectIngestController(
    BaseDirectIngestController[GcsfsIngestArgs, GcsfsFileContentsHandle]
):
    """Controller for parsing and persisting a file in the GCS filesystem."""

    _MAX_STORAGE_FILE_RENAME_TRIES = 10
    _DEFAULT_MAX_PROCESS_JOB_WAIT_TIME_SEC = 300
    _INGEST_FILE_SPLIT_LINE_LIMIT = 2500

    def __init__(
        self,
        ingest_bucket_path: GcsfsBucketPath,
    ):
        super().__init__(DirectIngestInstance.for_ingest_bucket(ingest_bucket_path))
        self.fs = DirectIngestGCSFileSystem(GcsfsFactory.build())
        self.ingest_bucket_path = ingest_bucket_path
        self.storage_directory_path = (
            gcsfs_direct_ingest_storage_directory_path_for_region(
                region_code=self.region_code(),
                system_level=self.system_level,
                ingest_instance=self.ingest_instance,
            )
        )

        self.temp_output_directory_path = (
            gcsfs_direct_ingest_temporary_output_directory_path()
        )

        self.file_prioritizer = GcsfsDirectIngestJobPrioritizer(
            self.fs,
            self.ingest_bucket_path,
            self.get_file_tag_rank_list(),
        )

        self.ingest_file_split_line_limit = self._INGEST_FILE_SPLIT_LINE_LIMIT

        self.file_metadata_manager = PostgresDirectIngestFileMetadataManager(
            region_code=self.region.region_code,
            ingest_database_name=self.ingest_database_key.db_name,
        )

        self.raw_file_import_manager = DirectIngestRawFileImportManager(
            region=self.region,
            fs=self.fs,
            ingest_bucket_path=self.ingest_bucket_path,
            temp_output_directory_path=self.temp_output_directory_path,
            big_query_client=BigQueryClientImpl(),
        )

        self.ingest_view_export_manager = DirectIngestIngestViewExportManager(
            region=self.region,
            fs=self.fs,
            output_bucket_name=self.ingest_bucket_path.bucket_name,
            file_metadata_manager=self.file_metadata_manager,
            big_query_client=BigQueryClientImpl(),
            view_collector=DirectIngestPreProcessedIngestViewCollector(
                self.region, self.get_file_tag_rank_list()
            ),
            launched_file_tags=self.get_file_tag_rank_list(),
        )

    # ================= #
    # NEW FILE HANDLING #
    # ================= #
    def handle_file(self, path: GcsfsFilePath, start_ingest: bool) -> None:
        """Called when a single new file is added to an ingest bucket (may also
        be called as a result of a rename).

        May be called from any worker/queue.
        """
        if self.fs.is_processed_file(path):
            logging.info("File [%s] is already processed, returning.", path.abs_path())
            return

        if self.fs.is_normalized_file_path(path):
            parts = filename_parts_from_path(path)

            if (
                parts.is_file_split
                and parts.file_split_size
                and parts.file_split_size <= self.ingest_file_split_line_limit
            ):
                self.kick_scheduler(just_finished_job=False)
                logging.info(
                    "File [%s] is already normalized and split split "
                    "with correct size, kicking scheduler.",
                    path.abs_path(),
                )
                return

        logging.info("Creating cloud task to schedule next job.")
        self.cloud_task_manager.create_direct_ingest_handle_new_files_task(
            region=self.region, can_start_ingest=start_ingest
        )

    def _register_all_new_paths_in_metadata(self, paths: List[GcsfsFilePath]) -> None:
        for path in paths:
            if not self.file_metadata_manager.has_file_been_discovered(path):
                self.file_metadata_manager.mark_file_as_discovered(path)

    @trace.span
    def handle_new_files(self, can_start_ingest: bool) -> None:
        """Searches the ingest directory for new/unprocessed files. Normalizes
        file names and splits files as necessary, schedules the next ingest job
        if allowed.


        Should only be called from the scheduler queue.
        """
        if not can_start_ingest and self.region.is_ingest_launched_in_env():
            raise ValueError(
                "The can_start_ingest flag should only be used for regions where ingest is not yet launched in a "
                "particular environment. If we want to be able to selectively pause ingest processing for a state, we "
                "will first have to build a config that is respected by both the /ensure_all_file_paths_normalized "
                "endpoint and any cloud functions that trigger ingest."
            )

        unnormalized_paths = self.fs.get_unnormalized_file_paths(
            self.ingest_bucket_path
        )

        for path in unnormalized_paths:
            logging.info("File [%s] is not yet seen, normalizing.", path.abs_path())
            self.fs.mv_path_to_normalized_path(
                path, file_type=GcsfsDirectIngestFileType.RAW_DATA
            )

        if unnormalized_paths:
            logging.info(
                "Normalized at least one path - returning, will handle "
                "normalized files separately."
            )
            # Normalizing file paths will cause the cloud function that calls
            # this function to be re-triggered.
            return

        if not can_start_ingest:
            logging.warning(
                "Ingest not configured to start post-file normalization - returning."
            )
            return

        check_is_region_launched_in_env(self.region)

        unprocessed_ingest_view_paths = self.fs.get_unprocessed_file_paths(
            self.ingest_bucket_path,
            file_type_filter=GcsfsDirectIngestFileType.INGEST_VIEW,
        )
        unprocessed_raw_paths = self.fs.get_unprocessed_file_paths(
            self.ingest_bucket_path,
            file_type_filter=GcsfsDirectIngestFileType.RAW_DATA,
        )
        self._register_all_new_paths_in_metadata(unprocessed_raw_paths)

        self._register_all_new_paths_in_metadata(unprocessed_ingest_view_paths)

        unprocessed_paths = unprocessed_raw_paths + unprocessed_ingest_view_paths
        did_split = False
        for path in unprocessed_ingest_view_paths:
            if self._split_file_if_necessary(path):
                did_split = True

        if did_split:
            post_split_unprocessed_ingest_view_paths = (
                self.fs.get_unprocessed_file_paths(
                    self.ingest_bucket_path,
                    file_type_filter=GcsfsDirectIngestFileType.INGEST_VIEW,
                )
            )
            self._register_all_new_paths_in_metadata(
                post_split_unprocessed_ingest_view_paths
            )

            logging.info(
                "Split at least one path - returning, will handle split "
                "files separately."
            )
            # Writing new split files to storage will cause the cloud function
            # that calls this function to be re-triggered.
            return

        if unprocessed_paths:
            self.schedule_next_ingest_job(just_finished_job=False)

    def do_raw_data_import(self, data_import_args: GcsfsRawDataBQImportArgs) -> None:
        """Process a raw incoming file by importing it to BQ, tracking it in our metadata tables, and moving it to
        storage on completion.
        """
        check_is_region_launched_in_env(self.region)

        if not self.fs.exists(data_import_args.raw_data_file_path):
            logging.warning(
                "File path [%s] no longer exists - might have already been "
                "processed or deleted",
                data_import_args.raw_data_file_path,
            )
            self.kick_scheduler(just_finished_job=True)
            return

        file_metadata = self.file_metadata_manager.get_file_metadata(
            data_import_args.raw_data_file_path
        )

        if file_metadata.processed_time:
            logging.warning(
                "File [%s] is already marked as processed. Skipping file processing.",
                data_import_args.raw_data_file_path.file_name,
            )
            self.kick_scheduler(just_finished_job=True)
            return

        self.raw_file_import_manager.import_raw_file_to_big_query(
            data_import_args.raw_data_file_path, file_metadata
        )

        processed_path = self.fs.mv_path_to_processed_path(
            data_import_args.raw_data_file_path
        )
        self.file_metadata_manager.mark_file_as_processed(
            path=data_import_args.raw_data_file_path
        )

        self.fs.mv_path_to_storage(processed_path, self.storage_directory_path)
        self.kick_scheduler(just_finished_job=True)

    def do_ingest_view_export(
        self, ingest_view_export_args: GcsfsIngestViewExportArgs
    ) -> None:
        check_is_region_launched_in_env(self.region)
        did_export = self.ingest_view_export_manager.export_view_for_args(
            ingest_view_export_args
        )
        if (
            not did_export
            or not self.file_metadata_manager.get_ingest_view_metadata_pending_export()
        ):
            logging.info("Creating cloud task to schedule next job.")
            self.cloud_task_manager.create_direct_ingest_handle_new_files_task(
                region=self.region, can_start_ingest=True
            )

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
            logging.info("Found pre-ingest raw data import tasks to schedule.")
            return True
        # TODO(#3020): We used to have logic to ensure that we wait 10 min for all files
        #  to upload properly before moving on to ingest. We probably actually need this
        #  to happen between raw data import and ingest view export steps - if we
        #  haven't seen all files yet and most recent raw data file came in sometime in
        #  the last 10 min, we should wait to do view exports.
        if self._schedule_ingest_view_export_tasks():
            logging.info("Found pre-ingest view export tasks to schedule.")
            return True
        return False

    def _schedule_raw_data_import_tasks(self) -> bool:
        queue_info = self.cloud_task_manager.get_bq_import_export_queue_info(
            self.region
        )

        did_schedule = False
        tasks_to_schedule = [
            GcsfsRawDataBQImportArgs(path)
            for path in self.raw_file_import_manager.get_unprocessed_raw_files_to_import()
        ]
        for task_args in tasks_to_schedule:
            # If the file path has not actually been discovered by the metadata manager yet, it likely was just added
            # and a subsequent call to handle_files will register it and trigger another call to this function so we can
            # schedule the appropriate job.
            discovered = self.file_metadata_manager.has_file_been_discovered(
                task_args.raw_data_file_path
            )
            # If the file path has been processed, but still in the GCS bucket, it's likely due
            # to either a manual move or an accidental duplicate uploading. In either case, we
            # trust the database to have the source of truth.
            processed = self.file_metadata_manager.has_file_been_processed(
                task_args.raw_data_file_path
            )
            if processed:
                logging.warning(
                    "File [%s] is already marked as processed. Skipping file processing.",
                    task_args.raw_data_file_path,
                )
            if (
                discovered
                and not processed
                and not queue_info.has_task_already_scheduled(task_args)
            ):
                self.cloud_task_manager.create_direct_ingest_raw_data_import_task(
                    self.region, task_args
                )
                did_schedule = True

        return queue_info.has_raw_data_import_jobs_queued() or did_schedule

    def _schedule_ingest_view_export_tasks(self) -> bool:
        """Schedules all pending ingest view export tasks for launched ingest view tags, if they have not been
        scheduled. If tasks are scheduled or are still running, returns True. Otherwise, if it's safe to proceed with
        next steps of ingest, returns False."""

        queue_info = self.cloud_task_manager.get_bq_import_export_queue_info(
            self.region
        )
        if queue_info.has_ingest_view_export_jobs_queued():
            # Since we schedule all export jobs at once, after all raw files have been processed, we wait for all of the
            # export jobs to be done before checking if we need to schedule more.
            return True

        did_schedule = False
        tasks_to_schedule = (
            self.ingest_view_export_manager.get_ingest_view_export_task_args()
        )

        rank_list = self.get_file_tag_rank_list()
        ingest_view_name_rank = {
            ingest_view_name: i for i, ingest_view_name in enumerate(rank_list)
        }

        # Filter out views that aren't in ingest view tags.
        filtered_tasks_to_schedule = []
        for args in tasks_to_schedule:
            if args.ingest_view_name not in ingest_view_name_rank:
                logging.warning(
                    "Skipping ingest view task export for [%s] - not in controller ingest tags.",
                    args.ingest_view_name,
                )
                continue
            filtered_tasks_to_schedule.append(args)

        tasks_to_schedule = filtered_tasks_to_schedule

        # Sort by tag order and export datetime
        tasks_to_schedule.sort(
            key=lambda args: (
                ingest_view_name_rank[args.ingest_view_name],
                args.upper_bound_datetime_to_export,
            )
        )

        for task_args in tasks_to_schedule:
            if not queue_info.has_task_already_scheduled(task_args):
                self.cloud_task_manager.create_direct_ingest_ingest_view_export_task(
                    self.region, task_args
                )
                did_schedule = True

        return did_schedule

    @classmethod
    @abc.abstractmethod
    def get_file_tag_rank_list(cls) -> List[str]:
        pass

    def _get_next_job_args(self) -> Optional[GcsfsIngestArgs]:
        args = self.file_prioritizer.get_next_job_args()

        if not args:
            return None

        discovered = self.file_metadata_manager.has_file_been_discovered(args.file_path)

        if not discovered:
            # If the file path has not actually been discovered by the controller yet, it likely was just added and a
            # subsequent call to handle_files will register it and trigger another call to this function so we can
            # schedule the appropriate job.
            logging.info(
                "Found args [%s] for a file that has not been discovered by the metadata manager yet - not scheduling.",
                args,
            )
            return None

        return args

    def _on_job_scheduled(self, ingest_args: GcsfsIngestArgs) -> None:
        pass

    # =================== #
    # SINGLE JOB RUN CODE #
    # =================== #

    def _job_tag(self, args: GcsfsIngestArgs) -> str:
        return (
            f"{self.region.region_code}/{args.file_path.file_name}:"
            f"{args.ingest_time}"
        )

    def _get_contents_handle(
        self, args: GcsfsIngestArgs
    ) -> Optional[GcsfsFileContentsHandle]:
        return self._get_contents_handle_from_path(args.file_path)

    def _get_contents_handle_from_path(
        self, path: GcsfsFilePath
    ) -> Optional[GcsfsFileContentsHandle]:
        return self.fs.download_to_temp_file(path)

    @abc.abstractmethod
    def _are_contents_empty(
        self, args: GcsfsIngestArgs, contents_handle: GcsfsFileContentsHandle
    ) -> bool:
        pass

    def _can_proceed_with_ingest_for_contents(
        self, args: GcsfsIngestArgs, contents_handle: GcsfsFileContentsHandle
    ) -> bool:
        parts = filename_parts_from_path(args.file_path)
        return self._are_contents_empty(
            args, contents_handle
        ) or not self._must_split_contents(parts.file_type, args.file_path)

    def _must_split_contents(
        self, file_type: GcsfsDirectIngestFileType, path: GcsfsFilePath
    ) -> bool:
        if file_type == GcsfsDirectIngestFileType.RAW_DATA:
            return False

        return not self._file_meets_file_line_limit(
            self.ingest_file_split_line_limit, path
        )

    @abc.abstractmethod
    def _file_meets_file_line_limit(self, line_limit: int, path: GcsfsFilePath) -> bool:
        """Subclasses should implement to determine whether the file meets the
        expected line limit"""

    @abc.abstractmethod
    def _parse(
        self, args: GcsfsIngestArgs, contents_handle: GcsfsFileContentsHandle
    ) -> IngestInfo:
        pass

    def _should_split_file(self, path: GcsfsFilePath) -> bool:
        """Returns a handle to the contents of this path if this file should be split, None otherwise."""
        parts = filename_parts_from_path(path)

        if parts.file_type != GcsfsDirectIngestFileType.INGEST_VIEW:
            raise ValueError(
                f"Should not be attempting to split files other than ingest view files, found path with "
                f"file type: {parts.file_type}"
            )

        if parts.file_tag not in self.get_file_tag_rank_list():
            logging.info(
                "File tag [%s] for path [%s] not in rank list - not splitting.",
                parts.file_tag,
                path.abs_path(),
            )
            return False

        if (
            parts.is_file_split
            and parts.file_split_size
            and parts.file_split_size <= self.ingest_file_split_line_limit
        ):
            logging.info(
                "File [%s] already split with size [%s].",
                path.abs_path(),
                parts.file_split_size,
            )
            return False

        return self._must_split_contents(parts.file_type, path)

    @trace.span
    def _split_file_if_necessary(self, path: GcsfsFilePath) -> bool:
        """Checks if the given file needs to be split according to this controller's |file_split_line_limit|.

        Returns True if the file was split, False if splitting was not necessary.
        """

        should_split = self._should_split_file(path)
        if not should_split:
            logging.info("No need to split file path [%s].", path.abs_path())
            return False

        logging.info("Proceeding to file splitting for path [%s].", path.abs_path())

        original_metadata = self.file_metadata_manager.get_file_metadata(path)

        output_dir = GcsfsDirectoryPath.from_file_path(path)

        split_contents_paths = self._split_file(path)
        upload_paths = []
        for i, split_contents_path in enumerate(split_contents_paths):
            upload_path = self._create_split_file_path(path, output_dir, split_num=i)

            logging.info(
                "Copying split [%s] to direct ingest directory at path [%s].",
                i,
                upload_path.abs_path(),
            )

            upload_paths.append(upload_path)
            try:
                self.fs.mv(split_contents_path, upload_path)
            except Exception as e:
                logging.error(
                    "Threw error while copying split files from temp bucket - attempting to clean up before rethrowing."
                    " [%s]",
                    e,
                )
                for p in upload_paths:
                    self.fs.delete(p)
                raise e

        # We wait to register files with metadata manager until all files have been successfully copied to avoid leaving
        # the metadata manager in an inconsistent state.
        if not isinstance(original_metadata, DirectIngestIngestFileMetadata):
            raise ValueError("Attempting to split a non-ingest view type file")

        logging.info(
            "Registering [%s] split files with the metadata manager.",
            len(upload_paths),
        )

        for upload_path in upload_paths:
            ingest_file_metadata = (
                self.file_metadata_manager.register_ingest_file_split(
                    original_metadata, upload_path
                )
            )
            self.file_metadata_manager.mark_ingest_view_exported(ingest_file_metadata)

        self.file_metadata_manager.mark_file_as_processed(path)

        logging.info(
            "Done splitting file [%s] into [%s] paths, moving it to storage.",
            path.abs_path(),
            len(split_contents_paths),
        )

        self.fs.mv_path_to_storage(path, self.storage_directory_path)

        return True

    def _create_split_file_path(
        self,
        original_file_path: GcsfsFilePath,
        output_dir: GcsfsDirectoryPath,
        split_num: int,
    ) -> GcsfsFilePath:
        parts = filename_parts_from_path(original_file_path)

        rank_str = str(split_num + 1).zfill(5)
        updated_file_name = (
            f"{parts.stripped_file_name}_{rank_str}"
            f"_{SPLIT_FILE_SUFFIX}_size{self.ingest_file_split_line_limit}"
            f".{parts.extension}"
        )

        return GcsfsFilePath.from_directory_and_file_name(
            output_dir,
            to_normalized_unprocessed_file_path(
                updated_file_name,
                file_type=parts.file_type,
                dt=parts.utc_upload_datetime,
            ),
        )

    @abc.abstractmethod
    def _split_file(self, path: GcsfsFilePath) -> List[GcsfsFilePath]:
        """Should be implemented by subclasses to split a file accessible via the provided path into multiple
        files and upload those files to GCS. Returns the list of upload paths."""

    def _do_cleanup(self, args: GcsfsIngestArgs) -> None:
        self.fs.mv_path_to_processed_path(args.file_path)

        self.file_metadata_manager.mark_file_as_processed(args.file_path)

        parts = filename_parts_from_path(args.file_path)
        self._move_processed_files_to_storage_as_necessary(
            last_processed_date_str=parts.date_str
        )

    def _is_last_job_for_day(self, args: GcsfsIngestArgs) -> bool:
        """Returns True if the file handled in |args| is the last file for that
        upload date."""
        parts = filename_parts_from_path(args.file_path)
        upload_date, date_str = parts.utc_upload_datetime, parts.date_str
        more_jobs_expected = self.file_prioritizer.are_more_jobs_expected_for_day(
            date_str
        )
        if more_jobs_expected:
            return False
        next_job_args = self.file_prioritizer.get_next_job_args(date_str)
        if next_job_args:
            next_job_date = filename_parts_from_path(
                next_job_args.file_path
            ).utc_upload_datetime
            return next_job_date > upload_date
        return True

    def _move_processed_files_to_storage_as_necessary(
        self, last_processed_date_str: str
    ) -> None:
        """Moves files that have already been ingested/processed, up to and including the given date, into storage,
        if there is nothing more left to ingest/process, i.e. we are not expecting more files."""
        next_args = self.file_prioritizer.get_next_job_args()

        should_move_last_processed_date = False
        if not next_args:
            are_more_jobs_expected = (
                self.file_prioritizer.are_more_jobs_expected_for_day(
                    last_processed_date_str
                )
            )
            if not are_more_jobs_expected:
                should_move_last_processed_date = True
        else:
            next_date_str = filename_parts_from_path(next_args.file_path).date_str
            if next_date_str < last_processed_date_str:
                logging.info(
                    "Found a file [%s] from a date previous to our "
                    "last processed date - not moving anything to "
                    "storage."
                )
                return

            # If there are still more to process on this day, do not move files
            # from this day.
            should_move_last_processed_date = next_date_str != last_processed_date_str

        # Note: at this point, we expect RAW file type files to already have been moved once they were imported to BQ.
        self.fs.mv_processed_paths_before_date_to_storage(
            self.ingest_bucket_path,
            self.storage_directory_path,
            file_type_filter=GcsfsDirectIngestFileType.INGEST_VIEW,
            date_str_bound=last_processed_date_str,
            include_bound=should_move_last_processed_date,
        )

    @staticmethod
    def file_tag(file_path: GcsfsFilePath) -> str:
        return filename_parts_from_path(file_path).file_tag
