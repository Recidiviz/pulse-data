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

"""Functionality to perform direct ingest.
"""
import abc
import datetime
import logging
from typing import Generic, Optional

from recidiviz import IngestInfo
from recidiviz.cloud_storage.gcs_pseudo_lock_manager import (
    GCSPseudoLockManager,
    GCS_TO_POSTGRES_INGEST_RUNNING_LOCK_NAME,
    GCSPseudoLockAlreadyExists,
)
from recidiviz.common.ingest_metadata import (
    IngestMetadata,
    SystemLevel,
)
from recidiviz.ingest.direct.controllers.direct_ingest_types import (
    ContentsHandleType,
    IngestArgsType,
)
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import (
    DirectIngestCloudTaskManagerImpl,
)
from recidiviz.ingest.direct.direct_ingest_controller_utils import (
    check_is_region_launched_in_env,
)
from recidiviz.ingest.direct.errors import DirectIngestError, DirectIngestErrorType
from recidiviz.ingest.ingestor import Ingestor
from recidiviz.ingest.scrape import ingest_utils
from recidiviz.persistence import persistence
from recidiviz.persistence.database.bq_refresh.bq_refresh_utils import (
    postgres_to_bq_lock_name_for_schema,
)
from recidiviz.persistence.database.schema_utils import (
    SchemaType,
)
from recidiviz.utils import regions, trace


class BaseDirectIngestController(Ingestor, Generic[IngestArgsType, ContentsHandleType]):
    """Parses and persists individual-level info from direct ingest partners."""

    def __init__(self, region_name: str, system_level: SystemLevel):
        """Initialize the controller.

        Args:
            region_name: (str) the name of the region to be collected.
        """

        self.region = regions.get_region(region_name, is_direct_ingest=True)
        self.system_level = system_level
        self.cloud_task_manager = DirectIngestCloudTaskManagerImpl()
        self.lock_manager = GCSPseudoLockManager()

    # ============== #
    # JOB SCHEDULING #
    # ============== #
    def kick_scheduler(self, just_finished_job: bool) -> None:
        logging.info("Creating cloud task to schedule next job.")
        self.cloud_task_manager.create_direct_ingest_scheduler_queue_task(
            region=self.region, just_finished_job=just_finished_job, delay_sec=0
        )

    def schedule_next_ingest_job(self, just_finished_job: bool) -> None:
        """Creates a cloud task to run the next ingest job. Depending on the
        next job's IngestArgs, we either post a task to direct/scheduler/ if
        a wait_time is specified or direct/process_job/ if we can run the next
        job immediately."""
        check_is_region_launched_in_env(self.region)

        if self._schedule_any_pre_ingest_tasks():
            logging.info("Found pre-ingest tasks to schedule - returning.")
            return

        if self.lock_manager.is_locked(self.ingest_process_lock_for_region()):
            logging.info("Direct ingest is already locked on region [%s]", self.region)
            return

        process_job_queue_info = self.cloud_task_manager.get_process_job_queue_info(
            self.region
        )
        if process_job_queue_info.size() and not just_finished_job:
            logging.info(
                "Already running job [%s] - will not schedule another job for "
                "region [%s]",
                process_job_queue_info.task_names[0],
                self.region.region_code,
            )
            return

        next_job_args = self._get_next_job_args()

        if not next_job_args:
            logging.info(
                "No more jobs to run for region [%s] - returning",
                self.region.region_code,
            )
            return

        if process_job_queue_info.is_task_queued(self.region, next_job_args):
            logging.info(
                "Already have task queued for next job [%s] - returning.",
                self._job_tag(next_job_args),
            )
            return

        if self.lock_manager.is_locked(
            postgres_to_bq_lock_name_for_schema(self.system_level.schema_type())
        ) or self.lock_manager.is_locked(
            postgres_to_bq_lock_name_for_schema(SchemaType.OPERATIONS)
        ):
            logging.info(
                "Postgres to BigQuery export is running, cannot run ingest - returning"
            )
            return

        logging.info(
            "Creating cloud task to run job [%s]", self._job_tag(next_job_args)
        )
        self.cloud_task_manager.create_direct_ingest_process_job_task(
            region=self.region, ingest_args=next_job_args
        )
        self._on_job_scheduled(next_job_args)

    def _schedule_any_pre_ingest_tasks(self) -> bool:
        """Schedules any tasks related to SQL preprocessing of new files in preparation for ingest of those files into
        our Postgres database.

        Returns True if any jobs were scheduled or if there were already any pre-ingest jobs scheduled. Returns False if
        there are no remaining ingest jobs to schedule and it is safe to proceed with ingest.
        """
        if self._schedule_raw_data_import_tasks():
            logging.info("Found pre-ingest raw data import tasks to schedule.")
            return True
        # TODO(#3020): We have logic to ensure that we wait 10 min for all files to upload properly before moving on to
        #  ingest. We probably actually need this to happen between raw data import and ingest view export steps - if we
        #  haven't seen all files yet and most recent raw data file came in sometime in the last 10 min, we should wait
        #  to do view exports.
        if self._schedule_ingest_view_export_tasks():
            logging.info("Found pre-ingest view export tasks to schedule.")
            return True
        return False

    def _schedule_raw_data_import_tasks(self) -> bool:
        return False

    def _schedule_ingest_view_export_tasks(self) -> bool:
        return False

    @abc.abstractmethod
    def _get_next_job_args(self) -> Optional[IngestArgsType]:
        """Should be overridden to return args for the next ingest job, or
        None if there is nothing to process."""

    @abc.abstractmethod
    def _on_job_scheduled(self, ingest_args: IngestArgsType) -> None:
        """Called from the scheduler queue when an individual direct ingest job
        is scheduled.
        """

    def _wait_time_sec_for_next_args(self, _: IngestArgsType) -> int:
        """Should be overwritten to return the number of seconds we should
        wait to try to run another job, given the args of the next job we would
        run if we had to run a job right now. This gives controllers the ability
        to back off if we want to attempt to enforce an ingest order for files
        that might all be uploaded around the same time, but in inconsistent
        order.
        """
        return 0

    # =================== #
    # SINGLE JOB RUN CODE #
    # =================== #
    def default_job_lock_timeout_in_seconds(self) -> int:
        """This method can be overridden by subclasses that need more (or less)
        time to process jobs to completion, but by default enforces a
        one hour timeout on locks.

        Jobs may take longer than the alotted time, but if they do so, they
        will de facto relinquish their hold on the acquired lock."""
        return 3600

    def run_ingest_job_and_kick_scheduler_on_completion(
        self, args: IngestArgsType
    ) -> None:
        check_is_region_launched_in_env(self.region)

        if self.lock_manager.is_locked(
            postgres_to_bq_lock_name_for_schema(self.system_level.schema_type())
        ) or self.lock_manager.is_locked(
            postgres_to_bq_lock_name_for_schema(SchemaType.OPERATIONS)
        ):
            logging.warning(
                "Postgres to BigQuery export is running, can not run ingest"
            )
            raise GCSPseudoLockAlreadyExists(
                "Postgres to BigQuery export is running, can not run ingest"
            )

        with self.lock_manager.using_lock(
            self.ingest_process_lock_for_region(),
            expiration_in_seconds=self.default_job_lock_timeout_in_seconds(),
        ):
            should_schedule = self._run_ingest_job(args)

        if should_schedule:
            self.kick_scheduler(just_finished_job=True)
            logging.info("Done running task. Returning.")

    def _run_ingest_job(self, args: IngestArgsType) -> bool:
        """
        Runs the full ingest process for this controller - reading and parsing
        raw input data, transforming it to our schema, then writing to the
        database.
        Returns:
            True if we should try to schedule the next job on completion. False,
             otherwise.
        """
        check_is_region_launched_in_env(self.region)

        start_time = datetime.datetime.now()
        logging.info("Starting ingest for ingest run [%s]", self._job_tag(args))

        contents_handle = self._get_contents_handle(args)

        if contents_handle is None:
            logging.warning(
                "Failed to get contents handle for ingest run [%s] - " "returning.",
                self._job_tag(args),
            )
            # If the file no-longer exists, we do want to kick the scheduler
            # again to pick up the next file to run. We expect this to happen
            # occasionally as a race when the scheduler picks up a file before
            # it has been properly moved.
            return True

        if not self._can_proceed_with_ingest_for_contents(args, contents_handle):
            logging.warning(
                "Cannot proceed with contents for ingest run [%s] - returning.",
                self._job_tag(args),
            )
            # If we get here, we've failed to properly split a file picked up
            # by the scheduler. We don't want to schedule a new job after
            # returning here, otherwise we'll get ourselves in a loop where we
            # continually try to schedule this file.
            return False

        logging.info(
            "Successfully read contents for ingest run [%s]", self._job_tag(args)
        )

        if not self._are_contents_empty(args, contents_handle):
            self._parse_and_persist_contents(args, contents_handle)
        else:
            logging.warning(
                "Contents are empty for ingest run [%s] - skipping parse and "
                "persist steps.",
                self._job_tag(args),
            )

        self._do_cleanup(args)

        duration_sec = (datetime.datetime.now() - start_time).total_seconds()
        logging.info(
            "Finished ingest in [%s] sec for ingest run [%s].",
            str(duration_sec),
            self._job_tag(args),
        )

        return True

    @trace.span
    def _parse_and_persist_contents(
        self, args: IngestArgsType, contents_handle: ContentsHandleType
    ) -> None:
        """
        Runs the full ingest process for this controller for files with
        non-empty contents.
        """
        ingest_info = self._parse(args, contents_handle)
        if not ingest_info:
            raise DirectIngestError(
                error_type=DirectIngestErrorType.PARSE_ERROR,
                msg="No IngestInfo after parse.",
            )

        logging.info(
            "Successfully parsed data for ingest run [%s]", self._job_tag(args)
        )

        ingest_info_proto = ingest_utils.convert_ingest_info_to_proto(ingest_info)

        logging.info(
            "Successfully converted ingest_info to proto for ingest " "run [%s]",
            self._job_tag(args),
        )

        ingest_metadata = self._get_ingest_metadata(args)
        persist_success = persistence.write(ingest_info_proto, ingest_metadata)

        if not persist_success:
            raise DirectIngestError(
                error_type=DirectIngestErrorType.PERSISTENCE_ERROR,
                msg="Persist step failed",
            )

        logging.info("Successfully persisted for ingest run [%s]", self._job_tag(args))

    def _get_ingest_metadata(self, args: IngestArgsType) -> IngestMetadata:
        return IngestMetadata(
            self.region.region_code,
            self.region.jurisdiction_id,
            args.ingest_time,
            self.get_enum_overrides(),
            self.system_level,
        )

    def ingest_process_lock_for_region(self) -> str:
        return (
            GCS_TO_POSTGRES_INGEST_RUNNING_LOCK_NAME + self.region.region_code.upper()
        )

    @abc.abstractmethod
    def _job_tag(self, args: IngestArgsType) -> str:
        """Should be overwritten to return a (short) string tag to identify an
        ingest run in logs.
        """

    @abc.abstractmethod
    def _get_contents_handle(
        self, args: IngestArgsType
    ) -> Optional[ContentsHandleType]:
        """Should be overridden by subclasses to return a handle to the contents
        that can return an iterator over the contents and also manages cleanup
        of resources once we are done with the contents.
        Will return None if the contents could not be read (i.e. if they no
        longer exist).
        """

    @abc.abstractmethod
    def _are_contents_empty(
        self, args: IngestArgsType, contents_handle: ContentsHandleType
    ) -> bool:
        """Should be overridden by subclasses to return True if the contents
        for the given args should be considered "empty" and not parsed. For
        example, a CSV might have a single header line but no actual data.
        """

    @abc.abstractmethod
    def _parse(
        self, args: IngestArgsType, contents_handle: ContentsHandleType
    ) -> IngestInfo:
        """Should be overridden by subclasses to parse raw ingested contents
        into an IngestInfo object.
        """

    @abc.abstractmethod
    def _do_cleanup(self, args: IngestArgsType) -> None:
        """Should be overridden by subclasses to do any necessary cleanup in the
        ingested source once contents have been successfully persisted.
        """

    @abc.abstractmethod
    def _can_proceed_with_ingest_for_contents(
        self, args: IngestArgsType, contents_handle: ContentsHandleType
    ) -> bool:
        """Given a pointer to the contents, can the controller continue with
        ingest.
        """
