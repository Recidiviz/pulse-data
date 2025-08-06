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
"""An abstraction for manipulating files on the Google Cloud Storage File System"""

import abc
from contextlib import contextmanager
from typing import IO, Dict, Iterator, List, Optional, Union

from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
    GcsfsPath,
)
from recidiviz.common.io.file_contents_handle import FileContentsHandle
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle

BYTES_CONTENT_TYPE = "application/octet-stream"


class GCSBlobDoesNotExistError(ValueError):
    pass


class GCSFileSystem:
    """An abstraction for manipulating files on the Google Cloud Storage File System"""

    @abc.abstractmethod
    def mv(self, src_path: GcsfsFilePath, dst_path: GcsfsPath) -> None:
        """Moves object from bucket 1 to bucket 2 with optional rename. Note:
        this is *not* an atomic move - there is a failure case where you'd end
        up with a copied version of the file at |dst_path| but it has not been
        deleted from the original location.
        """

    @abc.abstractmethod
    def mv_file_to_directory_safe(
        self, src_path: GcsfsFilePath, dst_directory: GcsfsDirectoryPath
    ) -> GcsfsFilePath:
        """Moves |src_path| to |dst_directory|, throwing an error if the |src_path|'s
        file name alread exists within |dst_directory|.
        """

    @abc.abstractmethod
    def copy(self, src_path: GcsfsFilePath, dst_path: GcsfsPath) -> None:
        """Copies object at |src_path| to |dst_path|."""

    @abc.abstractmethod
    def delete(self, path: GcsfsFilePath) -> None:
        """Idempotent deletion of the object at |path|."""

    @abc.abstractmethod
    def exists(self, path: Union[GcsfsBucketPath, GcsfsFilePath]) -> bool:
        """Returns True if the object exists in the fs, False otherwise."""

    @abc.abstractmethod
    def get_file_size(self, path: GcsfsFilePath) -> Optional[int]:
        """Returns the file size of the object if it exists in the fs, None otherwise."""

    @abc.abstractmethod
    def get_crc32c(self, path: GcsfsFilePath) -> Optional[str]:
        """Returns the base64 encoded string representation of the big-endian ordered
        CRC32C checksum for the specified file if it exists in the fs; otherwise, it
        returns None.
        """

    @abc.abstractmethod
    def get_metadata(self, path: GcsfsFilePath) -> Optional[Dict[str, str]]:
        """Returns the metadata for the object at the given path if it exists in the fs,
        returning None if it does not exits. Returns Dict[str, str] instead of Dict[str, Any]
        because all values of the dictionary are typed casted to strings.
        """

    def clear_metadata(self, path: GcsfsFilePath) -> None:
        """Clears all of the custom metadata and sets it to None at the given path if
        it exists in the fs. Will not throw if the path does not exist.
        """

    @abc.abstractmethod
    def update_metadata(
        self,
        path: GcsfsFilePath,
        new_metadata: Dict[str, str],
    ) -> None:
        """Updates the custom metadata for the object at the given path if it exists in
        the fs. If there are preexisting keys in the metadata that match the new_metadata
        keys those keys will be overriden. If custom metadata has keys that new_metadata
        does not those keys will still exist in the custom metadata. To clear preexisiting
        keys not in new_metadata call clear_metadata() before updating.

        Required to pass in Dict[str, str] since gcs appears to just type cast non string
        values of dicts to strings. Recommended to call json.dumps() prior to calling
        update_metadata.
        """

    @abc.abstractmethod
    def download_as_string(self, path: GcsfsFilePath, encoding: str = "utf-8") -> str:
        """Downloads object contents from the given path to a string, decoding it from
        the specified `encoding` (default UTF-8)
        """

    @abc.abstractmethod
    def download_as_bytes(self, path: GcsfsFilePath) -> bytes:
        """Downloads object contents from the given path to bytes."""

    @abc.abstractmethod
    def download_to_temp_file(
        self, path: GcsfsFilePath, retain_original_filename: bool = False
    ) -> Optional[LocalFileContentsHandle]:
        """Generates a new file in a temporary directory on the local file
        system (App Engine VM when in prod/staging), and downloads file contents
        from the provided GCS path into that file, returning a handle to temp
        file on the local App Engine VM file system, or None if the GCS file is
        not found.
        """

    @abc.abstractmethod
    def upload_from_string(
        self, path: GcsfsFilePath, contents: str, content_type: str
    ) -> None:
        """Uploads string contents to a file path."""

    @abc.abstractmethod
    def upload_from_contents_handle_stream(
        self,
        path: GcsfsFilePath,
        contents_handle: FileContentsHandle,
        content_type: str,
        timeout: int = 60,
        metadata: Optional[Dict[str, str]] = None,
    ) -> None:
        """Uploads contents in handle via a file stream to a file path."""

    @abc.abstractmethod
    def ls_with_blob_prefix(
        self, bucket_name: str, blob_prefix: str
    ) -> List[Union[GcsfsDirectoryPath, GcsfsFilePath]]:
        """Returns absolute paths of objects in the bucket with the given |relative_path|."""

    @abc.abstractmethod
    def set_content_type(self, path: GcsfsFilePath, content_type: str) -> None:
        """Allows for the content type of a certain file path to be reset."""

    @abc.abstractmethod
    def is_dir(self, path: str) -> bool:
        """Returns whether the given path exists and is a GcsfsDirectoryPath or not."""

    @abc.abstractmethod
    def is_file(self, path: str) -> bool:
        """Returns whether the given path exists and is a GcsfsFilePath or not."""

    @contextmanager
    @abc.abstractmethod
    def open(
        self,
        path: GcsfsFilePath,
        mode: str = "r",
        chunk_size: Optional[int] = None,
        encoding: Optional[str] = None,
        verifiable: bool = True,
    ) -> Iterator[IO]:
        """Returns a file handler for the given |path|.

        If the file is opened in read mode, contents is streamed from GCS for reduced
        memory consumption. If |verifiable| is True and all of the contents are read,
        will verify that the checksum is correct (or raises).

        |verifiable| can only be set to True when in a read-like mode.
        """

    @abc.abstractmethod
    def rename_blob(self, path: GcsfsFilePath, new_path: GcsfsFilePath) -> None:
        """Renames the blob on the GCS File System"""

    @abc.abstractmethod
    def unzip(
        self, zip_file_path: GcsfsFilePath, destination_dir: GcsfsDirectoryPath
    ) -> List[GcsfsFilePath]:
        """Unzips the zip file at the provided |zip_file_path| and writes all internal
        files into the provided |destination_dir|. Returns the list of unzipped paths
        generated.
        """
