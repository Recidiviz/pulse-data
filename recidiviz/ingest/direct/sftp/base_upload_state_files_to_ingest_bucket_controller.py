# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Class for uploading files from a given filesystem to a region's ingest bucket"""
import abc
import datetime
import logging
import os
from multiprocessing.pool import ThreadPool
from typing import List, Optional, Tuple

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.common.results import MultiRequestResultWithSkipped
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
    to_normalized_unprocessed_raw_file_path,
)
from recidiviz.ingest.direct.gcs.directory_path_utils import (
    gcsfs_direct_ingest_bucket_for_state,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class UploadStateFilesToIngestBucketDelegate:
    """Delegate responsible for determining how a raw file upload should interact with
    other platform processes.
    """

    @abc.abstractmethod
    def should_pause_processing(self) -> bool:
        """Returns whether we should pause any automatic processing of new files before
        performing this upload. Will return False if the scheduler queue is already paused.
        """

    @abc.abstractmethod
    def pause_processing(self) -> None:
        """Pauses (or instructs the user to pause) any automatic processing that might
        happen to the files we're about to upload.
        """

    @abc.abstractmethod
    def unpause_processing(self) -> None:
        """Unpauses automatic processing of the files we're about to upload."""


class BaseUploadStateFilesToIngestBucketController:
    """Base class for uploading files from a filesystem to a region's ingest bucket."""

    SUPPORTED_EXTENSIONS: List[str] = [".csv", ".txt", ".zip"]

    def __init__(
        self,
        paths_with_timestamps: List[Tuple[str, datetime.datetime]],
        project_id: str,
        region: str,
        delegates: List[UploadStateFilesToIngestBucketDelegate],
        destination_bucket_override: Optional[GcsfsBucketPath] = None,
    ):
        self.paths_with_timestamps = paths_with_timestamps
        self.project_id = project_id
        self.region = region.lower()
        self.delegates = delegates

        self.gcsfs = DirectIngestGCSFileSystem(GcsfsFactory.build())

        # Raw data uploads always default to primary ingest bucket
        self.destination_ingest_bucket = (
            destination_bucket_override
            or gcsfs_direct_ingest_bucket_for_state(
                region_code=region,
                ingest_instance=DirectIngestInstance.PRIMARY,
                project_id=self.project_id,
            )
        )

        self.uploaded_files: List[str] = []
        self.skipped_files: List[str] = []
        self.unable_to_upload_files: List[str] = []

    @abc.abstractmethod
    def _copy_to_ingest_bucket(
        self,
        path: str,
        full_file_upload_path: GcsfsFilePath,
    ) -> None:
        """Copies a file from the filesystem to the GCS bucket. Should be overridden based on the filesystem the path is in."""

    def _upload_file(self, path_with_timestamp: Tuple[str, datetime.datetime]) -> None:
        path, timestamp = path_with_timestamp
        normalized_file_name = os.path.basename(
            to_normalized_unprocessed_raw_file_path(path, dt=timestamp)
        )
        full_file_upload_path = GcsfsFilePath.from_directory_and_file_name(
            self.destination_ingest_bucket, normalized_file_name
        )
        self._copy_to_ingest_bucket(path, full_file_upload_path)

    def _is_supported_extension(self, path: str) -> bool:
        _, ext = os.path.splitext(path)
        if not ext or ext not in self.SUPPORTED_EXTENSIONS:
            logging.info("Skipping file [%s] - invalid extension", path)
            return False
        return True

    @abc.abstractmethod
    def get_paths_to_upload(self) -> List[Tuple[str, datetime.datetime]]:
        """Returns the appropriate paths to upload and the proper associated timestamp that
        it is to be normalized with. Should be overridden based on the filesystem the paths are in.
        """

    def upload_files(
        self,
        paths_with_timestamps_to_upload: List[Tuple[str, datetime.datetime]],
        thread_pool: ThreadPool,
    ) -> None:
        thread_pool.map(self._upload_file, paths_with_timestamps_to_upload)

    def do_upload(self) -> MultiRequestResultWithSkipped[str, str, str]:
        """Perform upload to ingest bucket."""

        # SFTP download writes to primary instance bucket, but we will need to pause
        # the scheduler queue if it isn't already paused
        should_pause = [
            delegate.should_pause_processing() for delegate in self.delegates
        ]
        try:
            # We pause and unpause scheduler queue to prevent races where ingest views begin
            # to generate in the middle of a raw file upload.
            for idx, delegate in enumerate(self.delegates):
                if should_pause[idx]:
                    delegate.pause_processing()
            upload_result = self._do_upload_inner()
        finally:
            for idx, delegate in enumerate(self.delegates):
                if should_pause[idx]:
                    delegate.unpause_processing()
        return upload_result

    def _do_upload_inner(self) -> MultiRequestResultWithSkipped[str, str, str]:
        """Internal helper for uploading to an ingest bucket. Should be called while
        ingest is paused.
        """
        logging.info(
            "Uploading raw files to the %s ingest bucket [%s] for project [%s].",
            self.region,
            self.destination_ingest_bucket.uri(),
            self.project_id,
        )

        paths_with_timestamps_to_upload = self.get_paths_to_upload()
        logging.info(
            "Found %s items to upload to ingest bucket [%s]",
            len(paths_with_timestamps_to_upload),
            self.destination_ingest_bucket.uri(),
        )

        thread_pool = ThreadPool(processes=12)

        self.upload_files(paths_with_timestamps_to_upload, thread_pool)

        thread_pool.close()
        thread_pool.join()

        logging.info(
            "Upload complete, successfully uploaded %s files to ingest bucket [%s], "
            "could not upload %s files, skipped %s files",
            len(self.uploaded_files),
            self.destination_ingest_bucket.uri(),
            len(self.unable_to_upload_files),
            len(self.skipped_files),
        )

        return MultiRequestResultWithSkipped(
            successes=self.uploaded_files,
            failures=self.unable_to_upload_files,
            skipped=self.skipped_files,
        )
