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
"""Class for uploading files from a local filesystem to a region's ingest bucket"""
import datetime
import logging
import os
from mimetypes import guess_type
from multiprocessing.pool import ThreadPool
from typing import Optional, List, Tuple

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import to_normalized_unprocessed_file_path, \
    DirectIngestGCSFileSystem
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    gcsfs_direct_ingest_directory_path_for_region, \
    GcsfsDirectIngestFileType

# TODO(#6013): refactor recidiviz.tools.UploadStateFilesToIngestBucketController to inherit from this class


class UploadStateFilesToIngestBucketController:
    """Class for uploading files from a local filesystem to a region's ingest bucket."""
    SUPPORTED_EXTENSIONS: List[str] = [".csv", ".txt"]

    def __init__(self,
                 paths_with_timestamps: List[Tuple[str, datetime.datetime]],
                 project_id: str,
                 region: str,
                 gcs_destination_path: Optional[str] = None):
        self.paths_with_timestamps = paths_with_timestamps
        self.project_id = project_id
        self.region = region.lower()

        self.gcsfs = DirectIngestGCSFileSystem(GcsfsFactory.build())
        self.gcs_destination_path = GcsfsDirectoryPath.from_absolute_path(
            gcsfs_direct_ingest_directory_path_for_region(region, SystemLevel.STATE, project_id=self.project_id)
        ) if gcs_destination_path is None else GcsfsDirectoryPath.from_absolute_path(gcs_destination_path)
        self.uploaded_files: List[str] = []
        self.unable_to_upload_files: List[str] = []

    def _copy_to_ingest_bucket(self, path: str, full_file_upload_path: GcsfsFilePath) -> None:
        try:
            mimetype, _ = guess_type(os.path.basename(path))
            self.gcsfs.mv(src_path=GcsfsFilePath.from_absolute_path(path),
                          dst_path=full_file_upload_path)
            self.gcsfs.set_content_type(full_file_upload_path, mimetype if mimetype else "text/plain")
            logging.info("Copied %s -> %s", path, full_file_upload_path.uri())
            self.uploaded_files.append(path)
        except BaseException as e:
            logging.warning("Could not copy %s -> %s due to error %s", path, full_file_upload_path.uri(), e.args)
            self.unable_to_upload_files.append(path)

    def _upload_file(self, path_with_timestamp: Tuple[str, datetime.datetime]) -> None:
        path, timestamp = path_with_timestamp
        normalized_file_name = os.path.basename(
            to_normalized_unprocessed_file_path(path,
                                                file_type=GcsfsDirectIngestFileType.RAW_DATA,
                                                dt=timestamp))
        full_file_upload_path = GcsfsFilePath.from_directory_and_file_name(self.gcs_destination_path,
                                                                           normalized_file_name)
        self._copy_to_ingest_bucket(path, full_file_upload_path)

    def get_paths_to_upload(self) -> List[Tuple[str, datetime.datetime]]:
        """Returns the appropriate paths to upload and the proper associated timestamp that
        it is to be normalized with. Skips any files that are not properly supported."""
        path_candidates = []
        for path, timestamp in self.paths_with_timestamps:
            if self.gcsfs.is_dir(path):
                directory = GcsfsDirectoryPath.from_absolute_path(path)
                files_in_directory = self.gcsfs.ls_with_blob_prefix(
                    bucket_name=directory.bucket_name,
                    blob_prefix=directory.relative_path
                )
                for file in files_in_directory:
                    path_candidates.append((file.abs_path(), timestamp))
            elif self.gcsfs.is_file(path):
                file = GcsfsFilePath.from_absolute_path(path)
                path_candidates.append((file.abs_path(), timestamp))
            else:
                logging.warning("Could not indicate %s as a directory or a file in %s. Skipping",
                                path,
                                self.gcs_destination_path.uri())
                self.unable_to_upload_files.append(path)
                continue

        result = []
        for path, timestamp in path_candidates:
            _, ext = os.path.splitext(path)
            if not ext or ext not in self.SUPPORTED_EXTENSIONS:
                logging.info("Skipping file [%s] - invalid extension %s", path, ext)
                continue
            result.append((path, timestamp))

        return result

    def upload_files(
            self,
            paths_with_timestamps_to_upload: List[Tuple[str, datetime.datetime]],
            thread_pool: ThreadPool) -> None:
        thread_pool.map(self._upload_file, paths_with_timestamps_to_upload)

    def do_upload(self) -> Tuple[List[str], List[str]]:
        """Perform upload to ingest bucket."""
        logging.info("Uploading raw files to the %s ingest bucket [%s] for project [%s].",
                     self.region, self.gcs_destination_path.uri(), self.project_id)

        paths_with_timestamps_to_upload = self.get_paths_to_upload()
        logging.info("Found %s items to upload to ingest bucket [%s]", len(
            paths_with_timestamps_to_upload), self.gcs_destination_path.uri())

        thread_pool = ThreadPool(processes=12)

        self.upload_files(paths_with_timestamps_to_upload, thread_pool)

        thread_pool.close()
        thread_pool.join()

        logging.info("Upload complete, successfully uploaded %s files to ingest bucket [%s], "
                     "could not upload %s files",
                     len(self.uploaded_files), self.gcs_destination_path.uri(), len(self.unable_to_upload_files))

        return self.uploaded_files, self.unable_to_upload_files
