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
from recidiviz.ingest.direct.direct_ingest_cloud_task_manager import \
    DirectIngestCloudTaskManagerImpl
from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.ingest.direct.controllers.direct_ingest_types import \
    ContentsType, IngestArgsType
from recidiviz.ingest.direct.errors import DirectIngestError, \
    DirectIngestErrorType
from recidiviz.ingest.ingestor import Ingestor
from recidiviz.ingest.scrape import ingest_utils
from recidiviz.persistence import persistence
from recidiviz.utils import regions


class BaseDirectIngestController(Ingestor,
                                 Generic[IngestArgsType, ContentsType]):
    """Parses and persists individual-level info from direct ingest partners.
    """

    def __init__(self, region_name, system_level: SystemLevel):
        """Initialize the controller.

        Args:
            region_name: (str) the name of the region to be collected.
        """

        self.region = regions.get_region(region_name, is_direct_ingest=True)
        self.system_level = system_level
        self.cloud_task_manager = DirectIngestCloudTaskManagerImpl()

    # ============== #
    # JOB SCHEDULING #
    # ============== #
    def kick_scheduler(self, just_finished_job: bool):
        logging.info("Creating cloud task to schedule next job.")
        self.cloud_task_manager.create_direct_ingest_scheduler_queue_task(
            region=self.region,
            just_finished_job=just_finished_job,
            delay_sec=0)

    def schedule_next_ingest_job_or_wait_if_necessary(self,
                                                      just_finished_job: bool):
        """Creates a cloud task to run the next ingest job. Depending on the
        next job's IngestArgs, we either post a task to direct/scheduler/ if
        a wait_time is specified or direct/process_job/ if we can run the next
        job immediately."""
        process_job_queue_info = \
            self.cloud_task_manager.get_process_job_queue_info(self.region)
        if process_job_queue_info.size() and not just_finished_job:
            logging.info(
                "Already running job [%s] - will not schedule another job for "
                "region [%s]",
                process_job_queue_info.task_names[0],
                self.region.region_code)
            return

        next_job_args = self._get_next_job_args()

        if not next_job_args:
            logging.info("No more jobs to run for region [%s] - returning",
                         self.region.region_code)
            return

        if process_job_queue_info.is_task_queued(self.region, next_job_args):
            logging.info(
                "Already have task queued for next job [%s] - returning.",
                self._job_tag(next_job_args))
            return

        wait_time_sec = self._wait_time_sec_for_next_args(next_job_args)
        logging.info("Found next ingest job to run [%s] with wait time [%s].",
                     self._job_tag(next_job_args), wait_time_sec)

        if wait_time_sec:
            scheduler_queue_info = \
                self.cloud_task_manager.get_scheduler_queue_info(self.region)
            if scheduler_queue_info.size() <= 1:
                logging.info(
                    "Creating cloud task to fire timer in [%s] seconds",
                    wait_time_sec)
                self.cloud_task_manager. \
                    create_direct_ingest_scheduler_queue_task(
                        region=self.region,
                        just_finished_job=False,
                        delay_sec=wait_time_sec)
            else:
                logging.info(
                    "[%s] tasks already in the scheduler queue for region "
                    "[%s] - not queueing another task.",
                    str(scheduler_queue_info.size), self.region.region_code)
        else:
            logging.info(
                "Creating cloud task to run job [%s]",
                self._job_tag(next_job_args))
            self.cloud_task_manager.create_direct_ingest_process_job_task(
                region=self.region,
                ingest_args=next_job_args)
            self._on_job_scheduled(next_job_args)

    @abc.abstractmethod
    def _get_next_job_args(self) -> Optional[IngestArgsType]:
        """Should be overridden to return args for the next ingest job, or
        None if there is nothing to process."""

    @abc.abstractmethod
    def _on_job_scheduled(self, ingest_args: IngestArgsType):
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
    def run_ingest_job_and_kick_scheduler_on_completion(self,
                                                        args: IngestArgsType):
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
        start_time = datetime.datetime.now()
        logging.info("Starting ingest for ingest run [%s]", self._job_tag(args))

        contents = self._read_contents(args)

        if contents is None:
            logging.warning(
                "Failed to read contents for ingest run [%s] - returning.",
                self._job_tag(args))
            # If the file no-longer exists, we do want to kick the scheduler
            # again to pick up the next file to run. We expect this to happen
            # occasionally as a race when the scheduler picks up a file before
            # it has been properly moved.
            return True

        if not self._can_proceed_with_ingest_for_contents(contents):
            logging.warning(
                "Cannot proceed with contents for ingest run [%s] - returning.",
                self._job_tag(args))
            # If we get here, we've failed to properly split a file picked up
            # by the scheduler. We don't want to scheduler a new job after
            # returning here, otherwise we'll get ourselves in a loop where we
            # continually try to schedule this file.
            return False

        logging.info("Successfully read contents for ingest run [%s]",
                     self._job_tag(args))

        if not self._are_contents_empty(contents):
            self._parse_and_persist_contents(args, contents)
        else:
            logging.warning(
                "Contents are empty for ingest run [%s] - skipping parse and "
                "persist steps.", self._job_tag(args))

        self._do_cleanup(args)

        duration_sec = (datetime.datetime.now() - start_time).total_seconds()
        logging.info("Finished ingest in [%s] sec for ingest run [%s].",
                     str(duration_sec), self._job_tag(args))

        return True

    def _parse_and_persist_contents(self,
                                    args: IngestArgsType,
                                    contents: ContentsType):
        """
        Runs the full ingest process for this controller for files with
        non-empty contents.
        """
        ingest_info = self._parse(args, contents)
        if not ingest_info:
            raise DirectIngestError(
                error_type=DirectIngestErrorType.PARSE_ERROR,
                msg="No IngestInfo after parse."
            )

        logging.info("Successfully parsed data for ingest run [%s]",
                     self._job_tag(args))

        ingest_info_proto = \
            ingest_utils.convert_ingest_info_to_proto(ingest_info)

        logging.info("Successfully converted ingest_info to proto for ingest "
                     "run [%s]", self._job_tag(args))

        ingest_metadata = self._get_ingest_metadata(args)
        persist_success = persistence.write(ingest_info_proto, ingest_metadata)

        if not persist_success:
            raise DirectIngestError(
                error_type=DirectIngestErrorType.PERSISTENCE_ERROR,
                msg="Persist step failed")

        logging.info("Successfully persisted for ingest run [%s]",
                     self._job_tag(args))

    def _get_ingest_metadata(self, args: IngestArgsType) -> IngestMetadata:
        return IngestMetadata(self.region.region_code,
                              self.region.jurisdiction_id,
                              args.ingest_time,
                              self.get_enum_overrides(),
                              self.system_level)

    @abc.abstractmethod
    def _job_tag(self, args: IngestArgsType) -> str:
        """Should be overwritten to return a (short) string tag to identify an
        ingest run in logs.
        """

    @abc.abstractmethod
    def _read_contents(self, args: IngestArgsType) -> Optional[ContentsType]:
        """Should be overridden by subclasses to read contents that should be
        ingested into the format supported by this controller.

        Will return None if the contents could not be read (i.e. if they no
        longer exist).
        """

    @abc.abstractmethod
    def _are_contents_empty(self,
                            contents: ContentsType) -> bool:
        """Should be overridden by subclasses to return True if the contents
        should be considered "empty" and not parsed. For example, a CSV might
        have a single header line but no actual data.
        """

    @abc.abstractmethod
    def _parse(self,
               args: IngestArgsType,
               contents: ContentsType) -> IngestInfo:
        """Should be overridden by subclasses to parse raw ingested contents
        into an IngestInfo object.
        """

    @abc.abstractmethod
    def _do_cleanup(self, args: IngestArgsType):
        """Should be overridden by subclasses to do any necessary cleanup in the
        ingested source once contents have been successfully persisted.
        """

    @abc.abstractmethod
    def _can_proceed_with_ingest_for_contents(self, contents: ContentsType):
        """ Given the contents read from the file, can the controller continue
        with ingest.
        """
