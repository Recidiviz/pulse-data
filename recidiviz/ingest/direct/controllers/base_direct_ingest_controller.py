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
        in_progress_job_name = \
            self.cloud_task_manager.in_progress_process_job_name(
                self.region)

        if in_progress_job_name and not just_finished_job:
            logging.info(
                "Already running job [%s] - will not schedule another job for "
                "region [%s]",
                in_progress_job_name,
                self.region.region_code)
            return

        next_job_args = self._get_next_job_args()

        if not next_job_args:
            logging.info("No more jobs to run for region [%s] - returning",
                         self.region.region_code)
            return

        wait_time_sec = self._wait_time_sec_for_next_args(next_job_args)
        logging.info("Found next ingest job to run [%s] with wait time [%s].",
                     self._job_tag(next_job_args), wait_time_sec)

        if wait_time_sec:
            schedule_queue_size = \
                self.cloud_task_manager.scheduler_queue_size(self.region)
            if schedule_queue_size:
                logging.info(
                    "Creating cloud task to fire timer in [%s] seconds",
                    wait_time_sec)
                self.cloud_task_manager.\
                    create_direct_ingest_scheduler_queue_task(
                        region=self.region,
                        just_finished_job=False,
                        delay_sec=wait_time_sec)
            else:
                logging.info(
                    "[%s] tasks already in the scheduler queue for region "
                    "[%s] - not queueing another task.",
                    str(schedule_queue_size), self.region.region_code)
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
        self._run_ingest_job(args)
        self.kick_scheduler(just_finished_job=True)
        logging.info("Done running task. Returning from "
                     "run_ingest_job_and_kick_scheduler_on_completion")

    def _run_ingest_job(self, args: IngestArgsType):
        """
        Runs the full ingest process for this controller - reading and parsing
        raw input data, transforming it to our schema, then writing to the
        database.
        """
        logging.info("Starting ingest for ingest run [%s]", self._job_tag(args))

        contents = self._read_contents(args)

        if contents is None:
            logging.warning(
                "Failed to read contents for ingest run [%s] - returning.",
                self._job_tag(args))
            return

        logging.info("Successfully read contents for ingest run [%s]",
                     self._job_tag(args))

        if not self._are_contents_empty(contents):
            self._parse_and_persist_contents(args, contents)
        else:
            logging.warning(
                "Contents are empty for ingest run [%s] - skipping parse and "
                "persist steps.", self._job_tag(args))

        self._do_cleanup(args)

        logging.info("Finished ingest for ingest run [%s]",
                     self._job_tag(args))

    def _parse_and_persist_contents(self,
                                    args: IngestArgsType,
                                    contents: ContentsType):
        """
        Runs the full ingest process for this controller for files with
        non-empty contents.
        """
        ingest_info = self._parse(args, contents)
        # TODO(1738): implement retry on fail.
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

        ingest_metadata = IngestMetadata(self.region.region_code,
                                         self.region.jurisdiction_id,
                                         args.ingest_time,
                                         self.get_enum_overrides(),
                                         self.system_level)
        persist_success = persistence.write(ingest_info_proto, ingest_metadata)

        if not persist_success:
            raise DirectIngestError(
                error_type=DirectIngestErrorType.PERSISTENCE_ERROR,
                msg="Persist step failed")

        logging.info("Successfully persisted for ingest run [%s]",
                     self._job_tag(args))

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
