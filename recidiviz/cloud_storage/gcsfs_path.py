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
Generic class representing path information about buckets/objects in
Google Cloud Storage.
"""
import abc
import os
import re
from typing import Union
from urllib.parse import unquote

import attr
from google.cloud import storage


def strip_forward_slash(string: str) -> str:
    if string.startswith("/"):
        return string[1:]
    return string


def normalize_relative_path(relative_path: str) -> str:
    no_slash_relative_path = relative_path.rstrip("/")
    return f"{no_slash_relative_path}/" if relative_path else ""


@attr.s(frozen=True)
class GcsfsPath:
    """
    Abstract class representing path information about buckets/objects in
    Google Cloud Storage.
    """

    PATH_REGEX = re.compile(r"(gs://)?(.+)")

    # Name of the bucket for this path in Cloud Storage
    bucket_name: str = attr.ib()

    def __attrs_post_init__(self) -> None:
        if "/" in self.bucket_name:
            raise ValueError(
                f"Bucket name includes a relative path: [{self.bucket_name}]"
            )

    @property
    def bucket_path(self) -> "GcsfsBucketPath":
        return GcsfsBucketPath(self.bucket_name)

    @abc.abstractmethod
    def abs_path(self) -> str:
        """Builds full path as a string."""

    def uri(self) -> str:
        """Builds a Google Cloud Storage URI (e.g. the absolute path with 'gs://' prepended."""
        return f"gs://{self.abs_path()}"

    @classmethod
    def from_blob(
        cls, blob: storage.Blob
    ) -> Union["GcsfsFilePath", "GcsfsDirectoryPath"]:
        # storage.Blob and storage.Bucket names are url-escaped, we want to
        # convert back to unescaped strings.
        return GcsfsPath.from_bucket_and_blob_name(
            bucket_name=unquote(blob.bucket.name), blob_name=unquote(blob.name)
        )

    @classmethod
    def from_bucket_and_blob_name(
        cls, bucket_name: str, blob_name: str
    ) -> Union["GcsfsFilePath", "GcsfsDirectoryPath"]:
        if blob_name.endswith("/"):
            return GcsfsDirectoryPath(bucket_name=bucket_name, relative_path=blob_name)

        return GcsfsFilePath(bucket_name=bucket_name, blob_name=blob_name)


@attr.s(frozen=True)
class GcsfsDirectoryPath(GcsfsPath):
    """Represents path information about a "directory" in Google Cloud
    Storage. This might either be an actual bucket path, or a bucket with a
    relative "directory" path, which is really just a filter on all file objects
    (blobs) in the bucket with names with that prefix.
    """

    relative_path: str = attr.ib(default="", converter=strip_forward_slash)

    def abs_path(self) -> str:
        return os.path.join(self.bucket_name, self.relative_path)

    @classmethod
    def from_absolute_path(cls, path_str: str) -> "GcsfsDirectoryPath":
        match = re.match(cls.PATH_REGEX, path_str)

        if not match:
            raise ValueError(f"Path [{path_str}] does not match expected pattern")

        split = match.group(2).split("/", 1)

        if not split:
            raise ValueError(f"No items in split of path [{path_str}]")

        if len(split) == 1 or not split[1]:
            return GcsfsBucketPath(bucket_name=unquote(split[0]))

        relative_path = split[1]
        _, last_dir_part = os.path.split(relative_path)

        if "." in last_dir_part:
            raise ValueError(f"Found unexpected last dir part [{last_dir_part}]")

        return GcsfsDirectoryPath(
            bucket_name=unquote(split[0]),
            relative_path=normalize_relative_path(unquote(split[1])),
        )

    @classmethod
    def from_file_path(cls, path: "GcsfsFilePath") -> "GcsfsDirectoryPath":
        return GcsfsDirectoryPath(
            bucket_name=path.bucket_name, relative_path=path.relative_dir
        )

    @classmethod
    def from_dir_and_subdir(
        cls, dir_path: "GcsfsDirectoryPath", subdir: str
    ) -> "GcsfsDirectoryPath":
        relative_path = os.path.join(dir_path.relative_path, subdir)
        return GcsfsDirectoryPath(
            bucket_name=dir_path.bucket_name, relative_path=relative_path
        )


@attr.s(frozen=True)
class GcsfsBucketPath(GcsfsDirectoryPath):
    """Represents a path to a bucket in Google Cloud Storage."""

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        if self.relative_path:
            raise ValueError(
                f"Bucket relative path must be empty. Found [{self.relative_path}]."
            )

    def abs_path(self) -> str:
        return self.bucket_name

    @classmethod
    def from_bucket(cls, bucket: storage.Bucket) -> "GcsfsBucketPath":
        return GcsfsBucketPath(bucket_name=unquote(bucket.name))


@attr.s(frozen=True)
class GcsfsFilePath(GcsfsPath):
    """Represents path information about an actual file (blob) in Google Cloud
    Storage. The coordinates for this file are the bucket name and the blob
    name, which is the full relative path from the bucket.
    """

    # Relative path to a blob (file) in Cloud Storage.
    blob_name: str = attr.ib(converter=strip_forward_slash)

    @property
    def relative_dir(self) -> str:
        return normalize_relative_path(os.path.dirname(self.blob_name))

    @property
    def file_name(self) -> str:
        return os.path.basename(self.blob_name)

    def abs_path(self) -> str:
        return os.path.join(self.bucket_name, self.blob_name)

    @classmethod
    def with_new_file_name(
        cls, path: "GcsfsFilePath", new_file_name: str
    ) -> "GcsfsFilePath":
        relative_dir = os.path.split(path.blob_name)[0]
        new_blob_name = os.path.join(relative_dir, new_file_name)
        return GcsfsFilePath(bucket_name=path.bucket_name, blob_name=new_blob_name)

    @classmethod
    def from_directory_and_file_name(
        cls, dir_path: "GcsfsDirectoryPath", file_name: str
    ) -> "GcsfsFilePath":
        relative_dir = dir_path.relative_path
        new_blob_name = os.path.join(relative_dir, file_name)
        return GcsfsFilePath(bucket_name=dir_path.bucket_name, blob_name=new_blob_name)

    @classmethod
    def from_absolute_path(cls, path_str: str) -> "GcsfsFilePath":
        match = re.match(cls.PATH_REGEX, path_str)

        if not match:
            raise ValueError(f"Path [{path_str}] does not match expected pattern")

        split = match.group(2).split("/", 1)

        if len(split) != 2:
            raise ValueError(
                f"Split of [{path_str}] has unexpected [{len(split)}] items"
            )

        if not split[1]:
            raise ValueError(f"Relative path of of [{path_str}] unexpectedly empty.")

        return GcsfsFilePath(bucket_name=unquote(split[0]), blob_name=unquote(split[1]))
