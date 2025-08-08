# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""An implementation of the GCSFileSystem built on top of a real GCSFileSystem."""
import io
import logging
import os
import tempfile
import uuid
from contextlib import contextmanager
from io import TextIOWrapper
from typing import IO, Any, Dict, Iterator, List, Optional, Union
from zipfile import ZipFile, is_zipfile

from google.api_core import retry
from google.cloud import storage
from google.cloud.exceptions import NotFound

from recidiviz.cloud_storage.gcs_file_system import (
    BYTES_CONTENT_TYPE,
    GCSBlobDoesNotExistError,
    GCSFileSystem,
)
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
    GcsfsPath,
)
from recidiviz.cloud_storage.verifiable_bytes_reader import VerifiableBytesReader
from recidiviz.common.io.file_contents_handle import FileContentsHandle
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
from recidiviz.common.io.zip_file_contents_handle import ZipFileContentsHandle
from recidiviz.common.retry_predicate import google_api_retry_predicate


class GcsfsFileNotCopied(Exception):
    """Raised when a copy google API operation has completed without there being a file
    created at the destination path.
    """


class GcsfsFileNotDeleted(Exception):
    """Raised when a delete Google API operation has completed without hte file being
    deleted from the supposedly deleted path.
    """


def gcsfs_copy_retry(exc: Exception) -> bool:
    return google_api_retry_predicate(exc) or isinstance(exc, GcsfsFileNotCopied)


def gcsfs_delete_retry(exc: Exception) -> bool:
    return google_api_retry_predicate(exc) or isinstance(exc, GcsfsFileNotDeleted)


def generate_random_temp_path(filename: Optional[str] = None) -> str:
    temp_dir = os.path.join(tempfile.gettempdir(), "gcs_temp_files")
    os.makedirs(temp_dir, exist_ok=True)

    return os.path.join(temp_dir, filename if filename else str(uuid.uuid4()))


def unzip(
    fs: GCSFileSystem, zip_file_path: GcsfsFilePath, destination_dir: GcsfsDirectoryPath
) -> List[GcsfsFilePath]:
    """Uses the provided fs to unzip the zip file at the provided |zip_file_path| and
    write all internal files into the provided |destination_dir|. Returns the list of
    unzipped paths generated.
    """
    logging.info("Downloading zip file [%s] as bytes", zip_file_path.uri())
    downloaded_bytes = fs.download_as_bytes(zip_file_path)
    logging.info("Finished downloading zip file [%s] as bytes", zip_file_path.uri())
    zipbytes = io.BytesIO(downloaded_bytes)
    if not is_zipfile(zipbytes):
        raise ValueError(
            f"Path [{zip_file_path.uri()}] is not a zip file. Cannot unzip."
        )

    new_paths = []
    with ZipFile(zipbytes, "r") as z:
        for i, content_filename in enumerate(z.namelist()):
            new_path = GcsfsFilePath.from_directory_and_file_name(
                destination_dir, content_filename
            )
            logging.info(
                "[%s of %s] Uploading unzipped file %s to %s",
                i,
                len(z.namelist()),
                content_filename,
                new_path.uri(),
            )
            fs.upload_from_contents_handle_stream(
                path=new_path,
                contents_handle=ZipFileContentsHandle(content_filename, z),
                content_type=BYTES_CONTENT_TYPE,
            )
            new_paths.append(new_path)
    logging.info("Finished uploading unzipped files")
    return new_paths


class GCSFileSystemImpl(GCSFileSystem):
    """An implementation of the GCSFileSystem built on top of a real GCSFileSystem."""

    def __init__(self, client: storage.Client):
        self.storage_client = client

    @retry.Retry(predicate=google_api_retry_predicate)
    def exists(self, path: Union[GcsfsBucketPath, GcsfsFilePath]) -> bool:
        bucket = self.storage_client.bucket(path.bucket_name)
        if isinstance(path, GcsfsBucketPath):
            return bucket.exists(self.storage_client)

        if isinstance(path, GcsfsFilePath):
            blob = bucket.get_blob(path.blob_name)
            if not blob:
                return False
            return blob.exists(self.storage_client)

        raise ValueError(f"Unexpected path type [{type(path)}]")

    def _get_blob(self, path: GcsfsFilePath) -> storage.Blob:
        try:
            bucket = self.storage_client.bucket(path.bucket_name)
            blob = bucket.get_blob(path.blob_name)
        except NotFound as error:
            raise GCSBlobDoesNotExistError(
                f"Blob at [{path.uri()}] does not exist"
            ) from error

        if not blob:
            logging.warning(
                "Blob at [%s] does not exist - might have already been deleted",
                path.uri(),
            )

            raise GCSBlobDoesNotExistError(f"Blob at [{path.uri()}] does not exist")

        return blob

    @retry.Retry(predicate=google_api_retry_predicate)
    def get_file_size(self, path: GcsfsFilePath) -> Optional[int]:
        try:
            blob = self._get_blob(path)
            return blob.size
        except GCSBlobDoesNotExistError:
            return None

    @retry.Retry(predicate=google_api_retry_predicate)
    def get_crc32c(self, path: GcsfsFilePath) -> Optional[str]:
        try:
            blob = self._get_blob(path)
            return blob.crc32c
        except GCSBlobDoesNotExistError:
            return None

    @retry.Retry(predicate=google_api_retry_predicate)
    def get_metadata(self, path: GcsfsFilePath) -> Optional[Dict[str, Any]]:
        try:
            blob = self._get_blob(path)
            return blob.metadata
        except GCSBlobDoesNotExistError:
            return None

    def clear_metadata(self, path: GcsfsFilePath) -> None:
        try:
            blob = self._get_blob(path)
            blob.metadata = None
            blob.patch()
        except GCSBlobDoesNotExistError:
            pass

    @retry.Retry(predicate=google_api_retry_predicate)
    def update_metadata(
        self,
        path: GcsfsFilePath,
        new_metadata: Dict[str, str],
    ) -> None:
        try:
            blob = self._get_blob(path)
            # keys in new_metadata must match keys in the metadata to overwrite data.
            # if new_metadata keys do not match the existing keys the old keys and values will still exist.
            blob.metadata = new_metadata
            blob.patch()
        except GCSBlobDoesNotExistError:
            pass

    @retry.Retry(predicate=google_api_retry_predicate)
    def mv_file_to_directory_safe(
        self, src_path: GcsfsFilePath, dst_directory: GcsfsDirectoryPath
    ) -> GcsfsFilePath:
        dst_path = GcsfsFilePath.from_directory_and_file_name(
            dst_directory, src_path.file_name
        )

        if self.exists(dst_path):
            raise ValueError(f"Destination path [{dst_path.abs_path()}] already exists")

        self.mv(src_path, dst_path)
        return dst_path

    @retry.Retry(predicate=gcsfs_copy_retry)
    def copy(self, src_path: GcsfsFilePath, dst_path: GcsfsPath) -> None:
        src_blob = self._get_blob(src_path)

        dst_bucket = self.storage_client.bucket(dst_path.bucket_name)

        if isinstance(dst_path, GcsfsFilePath):
            dst_file_path = dst_path
        elif isinstance(dst_path, GcsfsDirectoryPath):
            dst_file_path = GcsfsFilePath.from_directory_and_file_name(
                dst_path, src_path.file_name
            )
        else:
            raise ValueError(f"Unexpected path type [{type(dst_path)}]")

        dest_blob = dst_bucket.blob(dst_file_path.blob_name)

        # We copy using rewrite instead of copy_blob to avoid this error: 'Copy spanning
        # locations and/or storage classes could not complete within 30 seconds. Please
        # use the Rewrite method
        # (https://cloud.google.com/storage/docs/json_api/v1/objects/rewrite) instead.'
        # Solution here adapted from:
        # https://objectpartners.com/2021/01/19/rewriting-files-in-google-cloud-storage/
        rewrite_token = None
        try:
            while True:
                rewrite_token, bytes_rewritten, bytes_to_rewrite = dest_blob.rewrite(
                    src_blob, token=rewrite_token
                )

                if not rewrite_token:
                    logging.info(
                        "Finished copying [%s] bytes to path: %s",
                        bytes_to_rewrite,
                        dst_path.uri(),
                    )
                    break

                logging.info(
                    "Copied [%s] of [%s] bytes to path: %s",
                    bytes_rewritten,
                    bytes_to_rewrite,
                    dst_path.uri(),
                )
        except Exception as e:
            logging.error(
                "File rewrite failed. Attempting to delete partially written file: %s",
                dst_path.uri(),
            )
            dest_blob_path = GcsfsPath.from_blob(blob=dest_blob)
            if not isinstance(dest_blob_path, GcsfsFilePath):
                logging.error(
                    "Destination is not a GcsfsFilePath, got %s", dest_blob_path
                )
                raise e

            self.delete(dest_blob_path)
            raise e

        # TODO(#45956) consider using preconditions here
        if not self.exists(dst_file_path):
            raise GcsfsFileNotCopied(
                f"After file copy, no destination file found at [{dst_path.uri()}]"
            )

    @retry.Retry(predicate=gcsfs_delete_retry)
    def delete(self, path: GcsfsFilePath) -> None:
        if not isinstance(path, GcsfsFilePath):
            raise ValueError(f"Unexpected path type [{type(path)}]")

        try:
            blob = self._get_blob(path)
        except GCSBlobDoesNotExistError:
            logging.info("Did not delete path because it did not exist: %s", path.uri())
            return

        # TODO(#45956) consider using preconditions here to make sure we are deleting
        # the blob we think we are
        blob.delete(self.storage_client)

        if self.exists(path):
            raise GcsfsFileNotDeleted(
                f"Attempted to delete path, but blob still exists at [{path.uri()}]"
            )

        logging.info("Finished deleting path: %s", path.uri())

    def mv(self, src_path: GcsfsFilePath, dst_path: GcsfsPath) -> None:
        self.copy(src_path, dst_path)
        self.delete(src_path)

    @retry.Retry(predicate=google_api_retry_predicate)
    def download_to_temp_file(
        self, path: GcsfsFilePath, retain_original_filename: bool = False
    ) -> Optional[LocalFileContentsHandle]:
        temp_file_path = generate_random_temp_path(
            path.file_name if retain_original_filename else None
        )

        try:
            blob = self._get_blob(path)

            logging.info(
                "Started download of file [{%s}] to local file [%s].",
                path.abs_path(),
                temp_file_path,
            )
            blob.download_to_filename(temp_file_path)
            logging.info(
                "Completed download of file [{%s}] to local file [%s].",
                path.abs_path(),
                temp_file_path,
            )
            return LocalFileContentsHandle(temp_file_path)
        except GCSBlobDoesNotExistError:
            return None

    @retry.Retry(predicate=google_api_retry_predicate)
    def download_as_string(self, path: GcsfsFilePath, encoding: str = "utf-8") -> str:
        blob = self._get_blob(path)

        return blob.download_as_bytes().decode(encoding)

    @retry.Retry(predicate=google_api_retry_predicate)
    def download_as_bytes(self, path: GcsfsFilePath) -> bytes:
        blob = self._get_blob(path)
        return blob.download_as_bytes()

    @retry.Retry(predicate=google_api_retry_predicate)
    def upload_from_string(
        self, path: GcsfsFilePath, contents: str, content_type: str
    ) -> None:
        bucket = self.storage_client.bucket(path.bucket_name)
        bucket.blob(path.blob_name).upload_from_string(
            contents, content_type=content_type
        )

    @retry.Retry(predicate=google_api_retry_predicate)
    def upload_from_contents_handle_stream(
        self,
        path: GcsfsFilePath,
        contents_handle: FileContentsHandle,
        content_type: str,
        timeout: int = 60,
        metadata: Optional[Dict[str, str]] = None,
    ) -> None:
        bucket = self.storage_client.bucket(path.bucket_name)
        with contents_handle.open("rb") as file_stream:
            blob = bucket.blob(path.blob_name)
            if metadata:
                blob.metadata = metadata
            blob.upload_from_file(
                file_stream, content_type=content_type, timeout=timeout
            )

    @retry.Retry(predicate=google_api_retry_predicate)
    def ls(
        self, bucket_name: str, *, blob_prefix: str | None = None
    ) -> List[Union[GcsfsDirectoryPath, GcsfsFilePath]]:
        blobs = self.storage_client.list_blobs(bucket_name, prefix=blob_prefix)
        return [GcsfsPath.from_blob(blob) for blob in blobs]

    @retry.Retry(predicate=google_api_retry_predicate)
    def set_content_type(self, path: GcsfsFilePath, content_type: str) -> None:
        blob = self._get_blob(path)
        blob.content_type = content_type
        blob.patch()

    @retry.Retry(predicate=google_api_retry_predicate)
    def is_dir(self, path: str) -> bool:
        try:
            directory = GcsfsDirectoryPath.from_absolute_path(path)
            # If the directory is empty, has_dir will have 1 entry, which is the Blob representing the directory
            # Otherwise, if the directory doesn't exist on GCS, has_dir will return an empty list
            has_dir = self.ls(
                bucket_name=directory.bucket_name, blob_prefix=directory.relative_path
            )
            return len(has_dir) > 0
        except ValueError:
            return False

    @retry.Retry(predicate=google_api_retry_predicate)
    def is_file(self, path: str) -> bool:
        try:
            file = GcsfsFilePath.from_absolute_path(path)
            return self.exists(file)
        except ValueError:
            return False

    @contextmanager
    def open(
        self,
        path: GcsfsFilePath,
        mode: str = "r",
        chunk_size: Optional[int] = None,
        encoding: Optional[str] = None,
        verifiable: bool = True,
    ) -> Iterator[IO]:

        blob = self._get_blob(path)

        if verifiable:
            # if we are wanting to verify the download of text, we need to first wrap it
            # in a bytes reader and then convert it to a text stream
            needs_text_wrapper = mode in ("r", "rt")

            with blob.open("rb", chunk_size=chunk_size) as f:
                verifiable_reader = VerifiableBytesReader(f, name=path.uri())
                try:
                    if needs_text_wrapper:
                        yield TextIOWrapper(buffer=verifiable_reader, encoding=encoding)
                    else:
                        yield verifiable_reader
                finally:
                    verifiable_reader.verify_crc32c(blob.crc32c)
        else:
            with blob.open(mode=mode, chunk_size=chunk_size, encoding=encoding) as f:
                yield f

    @retry.Retry(predicate=google_api_retry_predicate)
    def rename_blob(self, path: GcsfsFilePath, new_path: GcsfsFilePath) -> None:
        """Renames a blob."""
        blob = self._get_blob(path)
        bucket = self.storage_client.bucket(path.bucket_name)

        try:
            bucket.rename_blob(blob, new_path.abs_path())
        except GCSBlobDoesNotExistError:
            logging.info(
                "Could not rename blob [%s], file does not exist on path [%s].",
                blob.name,
                path,
            )
            return

        logging.info("Blob [%s] renamed to [%s]", blob.name, new_path.abs_path())

    @retry.Retry(predicate=google_api_retry_predicate)
    def unzip(
        self: GCSFileSystem,
        zip_file_path: GcsfsFilePath,
        destination_dir: GcsfsDirectoryPath,
    ) -> List[GcsfsFilePath]:
        return unzip(self, zip_file_path, destination_dir)
