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
from mimetypes import guess_type
from multiprocessing.pool import ThreadPool
from typing import List, Optional, Tuple

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.common.results import MultiRequestResultWithSkipped
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
    to_normalized_unprocessed_file_path,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.ingest.direct.controllers.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
    gcsfs_direct_ingest_bucket_for_region,
)
from recidiviz.ingest.direct.controllers.postgres_direct_ingest_file_metadata_manager import (
    PostgresDirectIngestRawFileMetadataManager,
)


class UploadStateFilesToIngestBucketDelegate:
    @abc.abstractmethod
    def should_pause_processing(self) -> bool:
        """Returns whether we should pause any automatic processing of new files before
        performing this upload. Will return False if processing is already pause.
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

    SUPPORTED_EXTENSIONS: List[str] = [".csv", ".txt"]

    def __init__(
        self,
        paths_with_timestamps: List[Tuple[str, datetime.datetime]],
        project_id: str,
        region: str,
        delegate: UploadStateFilesToIngestBucketDelegate,
        destination_bucket_override: Optional[GcsfsBucketPath] = None,
    ):
        self.paths_with_timestamps = paths_with_timestamps
        self.project_id = project_id
        self.region = region.lower()
        self.delegate = delegate

        self.gcsfs = DirectIngestGCSFileSystem(GcsfsFactory.build())

        # Raw data uploads always default to primary ingest bucket
        self.destination_ingest_bucket = (
            destination_bucket_override
            or gcsfs_direct_ingest_bucket_for_region(
                region_code=region,
                system_level=SystemLevel.STATE,
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
            to_normalized_unprocessed_file_path(
                path, file_type=GcsfsDirectIngestFileType.RAW_DATA, dt=timestamp
            )
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
        it is to be normalized with. Should be overridden based on the filesystem the paths are in."""

    def upload_files(
        self,
        paths_with_timestamps_to_upload: List[Tuple[str, datetime.datetime]],
        thread_pool: ThreadPool,
    ) -> None:
        thread_pool.map(self._upload_file, paths_with_timestamps_to_upload)

    def do_upload(self) -> MultiRequestResultWithSkipped[str, str, str]:
        """Perform upload to ingest bucket."""

        # SFTP download writes to primary instance bucket
        should_pause = self.delegate.should_pause_processing()
        try:
            # We pause and unpause ingest to prevent races where ingest views begin
            # to generate in the middle of a raw file upload.
            if should_pause:
                self.delegate.pause_processing()
            upload_result = self._do_upload_inner()
        finally:
            if should_pause:
                self.delegate.unpause_processing()
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


class DeployedUploadStateFilesToIngestBucketDelegate(
    UploadStateFilesToIngestBucketDelegate
):
    def __init__(self, region_code: str, ingest_instance: DirectIngestInstance) -> None:
        self.ingest_status_manager = DirectIngestInstanceStatusManager(
            region_code=region_code, ingest_instance=ingest_instance
        )

    def should_pause_processing(self) -> bool:
        return not self.ingest_status_manager.is_instance_paused()

    def pause_processing(self) -> None:
        self.ingest_status_manager.pause_instance()

    def unpause_processing(self) -> None:
        self.ingest_status_manager.unpause_instance()


class UploadStateFilesToIngestBucketController(
    BaseUploadStateFilesToIngestBucketController
):
    """Uploads files that already exist in GCS to a region's ingest bucket. Must be used
    in the context of a deployed app engine instance.
    """

    def __init__(
        self,
        paths_with_timestamps: List[Tuple[str, datetime.datetime]],
        project_id: str,
        region_code: str,
        gcs_destination_path: Optional[GcsfsBucketPath] = None,
    ):
        ingest_instance = DirectIngestInstance.PRIMARY
        super().__init__(
            paths_with_timestamps=paths_with_timestamps,
            project_id=project_id,
            region=region_code,
            delegate=DeployedUploadStateFilesToIngestBucketDelegate(
                region_code=region_code, ingest_instance=ingest_instance
            ),
            destination_bucket_override=gcs_destination_path,
        )
        self.postgres_direct_ingest_file_metadata_manager = (
            PostgresDirectIngestRawFileMetadataManager(
                region_code,
                ingest_instance.database_key_for_state(
                    StateCode(region_code.upper())
                ).db_name,
            )
        )

    def _copy_to_ingest_bucket(
        self,
        path: str,
        full_file_upload_path: GcsfsFilePath,
    ) -> None:
        """Moves a file within GCS to the appropriate bucket if it has not already been deemed
        processed or discovered by the file metadata manager.

        We check both processed and discovered because a file may be discovered and awaiting to be
        ingested, so we will not re-upload. We check processed because a file may have already been
        ingested, but has been deleted from the bucket."""
        if not self.postgres_direct_ingest_file_metadata_manager.has_raw_file_been_discovered(
            full_file_upload_path
        ) and not self.postgres_direct_ingest_file_metadata_manager.has_raw_file_been_processed(
            full_file_upload_path
        ):
            try:
                mimetype, _ = guess_type(os.path.basename(path))
                self.gcsfs.mv(
                    src_path=GcsfsFilePath.from_absolute_path(path),
                    dst_path=full_file_upload_path,
                )
                self.gcsfs.set_content_type(
                    full_file_upload_path, mimetype if mimetype else "text/plain"
                )
                logging.info("Copied %s -> %s", path, full_file_upload_path.uri())
                self.uploaded_files.append(path)
            except BaseException as e:
                logging.warning(
                    "Could not copy %s -> %s due to error %s",
                    path,
                    full_file_upload_path.uri(),
                    e.args,
                )
                self.unable_to_upload_files.append(path)
        else:
            logging.info(
                "Skipping %s -> %s, due to %s already being processed",
                path,
                full_file_upload_path.uri(),
                full_file_upload_path.uri(),
            )
            self.skipped_files.append(path)

    def get_paths_to_upload(self) -> List[Tuple[str, datetime.datetime]]:
        """Returns the appropriate paths to upload and the proper associated timestamp that
        it is to be normalized with. Skips any files that are not properly supported."""
        path_candidates = []
        for path, timestamp in self.paths_with_timestamps:
            if self.gcsfs.is_dir(path):
                directory = GcsfsDirectoryPath.from_absolute_path(path)
                files_in_directory = self.gcsfs.ls_with_blob_prefix(
                    bucket_name=directory.bucket_name,
                    blob_prefix=directory.relative_path,
                )
                for file in files_in_directory:
                    if self._is_supported_extension(file.abs_path()):
                        path_candidates.append((file.abs_path(), timestamp))
                    else:
                        self.skipped_files.append(file.abs_path())
            elif self.gcsfs.is_file(path):
                file = GcsfsFilePath.from_absolute_path(path)
                if self._is_supported_extension(file.abs_path()):
                    path_candidates.append((file.abs_path(), timestamp))
                else:
                    self.skipped_files.append(file.abs_path())
            else:
                logging.warning(
                    "Could not indicate %s as a directory or a file in %s. Skipping",
                    path,
                    self.destination_ingest_bucket.uri(),
                )
                self.unable_to_upload_files.append(path)
                continue
        return path_candidates
