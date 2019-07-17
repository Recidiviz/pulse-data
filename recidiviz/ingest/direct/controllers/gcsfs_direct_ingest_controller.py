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
import os
from datetime import datetime
from typing import Optional

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
    file_path: Optional[str] = attr.ib()
    storage_bucket: Optional[str] = attr.ib()


class GcsfsDirectIngestController(BaseDirectIngestController[GcsfsIngestArgs,
                                                             str]):
    """Controller for parsing and persisting a file in the GCS filesystem."""

    _MAX_STORAGE_FILE_RENAME_TRIES = 10

    def __init__(self, region_name: str,
                 system_level: SystemLevel,
                 fs: GCSFileSystem):
        super().__init__(region_name, system_level)
        self.fs = fs

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
    def _parse(self,
               args: GcsfsIngestArgs,
               contents: str) -> IngestInfo:
        pass

    def _storage_path(self, storage_bucket: str,
                      ingest_time: datetime,
                      file_name: str) -> str:
        ingest_date_str = ingest_time.date().strftime('%Y-%m-%d')

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

    def file_tag(self, args: GcsfsIngestArgs) -> str:
        # TODO(2058): Right now this function assumes that paths will take the
        #  form /path/to/file/{file_tag}.csv. We will eventually need to handle
        #  file names that include dates or which have numbers from upload
        #  conflicts.
        file_name = self.file_name(args.file_path)
        if not file_name:
            raise DirectIngestError(
                msg=f"No file name for job [{self._job_tag(args)}]",
                error_type=DirectIngestErrorType.INPUT_ERROR)

        return file_name.split('.')[0]
