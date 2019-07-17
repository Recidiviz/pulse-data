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
from typing import TypeVar, Generic

import attr

from recidiviz import IngestInfo
from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.ingest.ingestor import Ingestor
from recidiviz.ingest.direct.errors import DirectIngestError, \
    DirectIngestErrorType
from recidiviz.ingest.scrape import ingest_utils
from recidiviz.persistence import persistence
from recidiviz.utils import regions


@attr.s(frozen=True)
class IngestArgs:
    ingest_time: datetime.datetime = attr.ib()


ContentsType = TypeVar('ContentsType')
IngestArgsType = TypeVar('IngestArgsType', bound=IngestArgs)


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

    def run_ingest(self, args: IngestArgsType):
        """
        Runs the full ingest process for this controller - reading and parsing
        raw input data, transforming it to our schema, then writing to the
        database.
        """

        logging.info("Starting ingest for ingest run [%s]",
                     self._job_tag(args))
        contents = self._read_contents(args)

        logging.info("Successfully read contents for ingest run [%s]",
                     self._job_tag(args))

        ingest_info = self._parse(args, contents)

        # TODO #1738 implement retry on fail.
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

        self._do_cleanup(args)

        logging.info("Finished ingest for ingest run [%s]",
                     self._job_tag(args))

    @abc.abstractmethod
    def _job_tag(self, args: IngestArgsType) -> str:
        """Should be overwritten to return a (short) string tag to identify an
        ingest run in logs.
        """

    @abc.abstractmethod
    def _read_contents(self, args: IngestArgsType) -> ContentsType:
        """Should be overridden by subclasses to read contents that should be
        ingested into the format supported by this controller.
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
