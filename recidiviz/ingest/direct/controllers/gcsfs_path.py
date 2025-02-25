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
"""
Class representing path information about buckets/objects in
Google Cloud Storage.
"""
import abc
import os
from urllib.parse import unquote

import attr
from google.cloud import storage


@attr.s(frozen=True)
class GcsfsPath:
    """
    Abstract class representing path information about buckets/objects in
    Google Cloud Storage.
    """

    # Name of the bucket for this path in Cloud Storage
    bucket_name: str = attr.ib()

    @abc.abstractmethod
    def abs_path(self) -> str:
        """Builds full path as a string."""


@attr.s(frozen=True)
class GcsfsDirectoryPath(GcsfsPath):
    """Represents path information about a "directory" in Google Cloud
    Storage. This might either be an actual bucket path, or a bucket with a
    relative "directory" path, which is really just a filter on all file objects
    (blobs) in the bucket with names with that prefix.
    """
    relative_path: str = attr.ib(default='')

    def abs_path(self) -> str:
        return os.path.join(self.bucket_name, self.relative_path)

    @classmethod
    def from_absolute_path(cls, path_str: str) -> 'GcsfsDirectoryPath':
        split = path_str.split('/', 1)

        if not split:
            raise ValueError(f"No items in split of path [{path_str}]")

        if len(split) == 1 or not split[1]:
            return GcsfsBucketPath(bucket_name=unquote(split[0]))

        relative_path = split[1]
        _, last_dir_part = os.path.split(relative_path)

        if '.' in last_dir_part:
            raise ValueError(
                f'Found unexpected last dir part [{last_dir_part}]')

        return GcsfsDirectoryPath(
            bucket_name=unquote(split[0]),
            relative_path=unquote(split[1])
        )

    @classmethod
    def from_file_path(cls, path: 'GcsfsFilePath'):
        return GcsfsDirectoryPath(bucket_name=path.bucket_name,
                                  relative_path=path.relative_dir)


@attr.s(frozen=True)
class GcsfsBucketPath(GcsfsDirectoryPath):
    """Represents a path to a bucket in Google Cloud Storage."""
    def abs_path(self) -> str:
        return self.bucket_name

    @classmethod
    def from_bucket(cls, bucket: storage.Bucket) -> 'GcsfsBucketPath':
        return GcsfsBucketPath(bucket_name=unquote(bucket.name))


@attr.s(frozen=True)
class GcsfsFilePath(GcsfsPath):
    """Represents path information about an actual file (blob) in Google Cloud
    Storage. The coordinates for this file are the bucket name and the blob
    name, which is the full relative path from the bucket.
    """

    # Relative path to a blob (file) in Cloud Storage.
    blob_name: str = attr.ib()

    @property
    def relative_dir(self) -> str:
        return os.path.split(self.blob_name)[0]

    @property
    def file_name(self) -> str:
        return os.path.split(self.blob_name)[1]

    def abs_path(self) -> str:
        return os.path.join(self.bucket_name, self.blob_name)

    @classmethod
    def from_blob(cls, blob: storage.Blob) -> 'GcsfsFilePath':
        return GcsfsFilePath(
            bucket_name=unquote(blob.bucket.name),
            blob_name=unquote(blob.name))

    @classmethod
    def with_new_file_name(cls,
                           path: 'GcsfsFilePath',
                           new_file_name: str) -> 'GcsfsFilePath':
        relative_dir = os.path.split(path.blob_name)[0]
        new_blob_name = os.path.join(relative_dir, new_file_name)
        return GcsfsFilePath(bucket_name=path.bucket_name,
                             blob_name=new_blob_name)

    @classmethod
    def from_directory_and_file_name(cls,
                                     dir_path: 'GcsfsDirectoryPath',
                                     file_name: str):
        relative_dir = dir_path.relative_path
        new_blob_name = os.path.join(relative_dir, file_name)
        return GcsfsFilePath(bucket_name=dir_path.bucket_name,
                             blob_name=new_blob_name)

    @classmethod
    def from_absolute_path(cls, path_str: str) -> 'GcsfsFilePath':
        split = path_str.split('/', 1)

        if len(split) != 2:
            raise ValueError(
                f"Split of [{path_str}] has unexpected [{len(split)}] items")

        if not split[1]:
            raise ValueError(
                f"Relative path of of [{path_str}] unexpectedly empty.")

        return GcsfsFilePath(bucket_name=unquote(split[0]),
                             blob_name=unquote(split[1]))
