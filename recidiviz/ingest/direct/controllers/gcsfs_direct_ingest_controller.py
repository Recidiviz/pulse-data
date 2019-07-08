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
import os

import attr
from gcsfs import GCSFileSystem

from recidiviz import IngestInfo
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import \
    BaseDirectIngestController, IngestArgs
from recidiviz.ingest.direct.errors import DirectIngestError, \
    DirectIngestErrorType


@attr.s(frozen=True)
class GcsfsIngestArgs(IngestArgs):
    file_path = attr.ib()


class GcsfsDirectIngestController(BaseDirectIngestController[GcsfsIngestArgs,
                                                             str]):
    """Controller for parsing and persisting a file in the GCS filesystem."""

    def __init__(self, region_name: str,
                 system_level: SystemLevel,
                 fs: GCSFileSystem):
        super().__init__(region_name, system_level)
        self.fs = fs

    def _job_tag(self, args: GcsfsIngestArgs) -> str:
        return f'{self.region.region_code}/{self.file_name(args.file_path)}:' \
            f'{args.ingest_time}'

    def _read_contents(self, args: GcsfsIngestArgs) -> str:
        with self.fs.open(args.file_path) as fp:
            return fp.read().decode('utf-8')

    @abc.abstractmethod
    def _parse(self,
               args: GcsfsIngestArgs,
               contents: str) -> IngestInfo:
        pass

    def _do_cleanup(self, args: GcsfsIngestArgs):
        # TODO(2059): Move file to cold storage in case we need to reprocess for
        #  some reason.
        pass

    @staticmethod
    def file_name(file_path: str):
        _, file_name = os.path.split(file_path)
        return file_name

    def file_tag(self, file_path: str) -> str:
        # TODO(2058): Right now this function assumes that paths will take the
        #  form /path/to/file/{file_tag}.csv. We will eventually need to handle
        #  file names that include dates or which have numbers from upload
        #  conflicts.
        file_name = self.file_name(file_path)
        if not isinstance(file_name, str):
            raise DirectIngestError(
                msg=f"Unexpected type for filename: {type(file_name)}",
                error_type=DirectIngestErrorType.INPUT_ERROR)

        return file_name.split('.')[0]
