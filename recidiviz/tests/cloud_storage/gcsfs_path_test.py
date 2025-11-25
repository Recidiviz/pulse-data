# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for classes in gcsfs_path.py"""
import re
import unittest

from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
    GcsfsPath,
)
from recidiviz.utils.types import assert_type


class TestGcsfsPath(unittest.TestCase):
    """Tests for classes in gcsfs_path.py"""

    def test_file_path_extensions(self) -> None:
        path = GcsfsFilePath.from_absolute_path("gs://recidiviz-456-bucket/my_file.txt")
        self.assertEqual("txt", path.extension)
        self.assertFalse(path.has_zip_extension)

        path = GcsfsFilePath.from_absolute_path(
            "gs://recidiviz-456-bucket/path/to/my_file.txt"
        )
        self.assertEqual("txt", path.extension)
        self.assertFalse(path.has_zip_extension)

        path = GcsfsFilePath.from_absolute_path(
            "gs://recidiviz-456-bucket/path/to/my_file.zip"
        )
        self.assertEqual("zip", path.extension)
        self.assertTrue(path.has_zip_extension)

        path = GcsfsFilePath.from_absolute_path(
            "gs://recidiviz-456-bucket/path/to/my_file.ZIP"
        )
        self.assertEqual("ZIP", path.extension)
        # File system extensions are case-sensitive - this is not considered a ZIP file.
        self.assertFalse(path.has_zip_extension)

    def test_base_file_name(self) -> None:
        path = GcsfsFilePath.from_absolute_path("gs://recidiviz-456-bucket/my_file.txt")

        self.assertEqual("my_file.txt", path.file_name)
        self.assertEqual("my_file", path.base_file_name)

    def test_overload_construction(self) -> None:
        file_path = "this/is/a/file/path.csv"
        folder_path = "this/is/a/folder/path/"

        # for GcsfsFilePath, a file path will succeed and always return a GcsfsFilePath
        # but a folder path will throw an error
        assert_type(
            GcsfsFilePath.from_bucket_and_blob_name("bucket", file_path), GcsfsFilePath
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Expected GcsFilePath to not end with a \'\/\', but found \[.*\]\. If this is a directory, please use GcsfsDirectoryPath\.",
        ):
            GcsfsFilePath.from_bucket_and_blob_name("bucket", folder_path)

        # for GcsfsDirectoryPath, a folder path will succeed and always return a
        # GcsfsDirectoryPath but a file path will throw an error
        assert_type(
            GcsfsDirectoryPath.from_bucket_and_blob_name("bucket", folder_path),
            GcsfsDirectoryPath,
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Expected the last part of GcsfsDirectoryPath to not be a file, but found \[.*\] If this is a file, please use GcsfsFilePath.",
        ):
            GcsfsDirectoryPath.from_bucket_and_blob_name("bucket", file_path)

        # for GcsfsPath, we dont know what kind of path it is so it will bifurcate --
        # a folder path will succeed and return a GcsfsDirectoryPath and a file
        # path will succeed and return a GcsfsFilePath
        assert_type(
            GcsfsPath.from_bucket_and_blob_name("bucket", folder_path),
            GcsfsDirectoryPath,
        )
        assert_type(
            GcsfsPath.from_bucket_and_blob_name("bucket", file_path),
            GcsfsFilePath,
        )

    def test_invariants(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Expected GcsFilePath to not end with a \'\/\', but found \[.*\]\. If this is a directory, please use GcsfsDirectoryPath\.",
        ):
            GcsfsFilePath(bucket_name="bucket", blob_name="this/is/a/path/")

        GcsfsFilePath(bucket_name="bucket", blob_name="this/is/a/blob/a")

        with self.assertRaisesRegex(
            ValueError,
            re.escape("Bucket relative path must be empty. Found [aaaaaaa]."),
        ):
            GcsfsBucketPath(bucket_name="bucket", relative_path="aaaaaaa")

        GcsfsBucketPath(bucket_name="bucket", relative_path="")

        with self.assertRaisesRegex(
            ValueError,
            r"Expected the last part of GcsfsDirectoryPath to not be a file, but found \[.*\] If this is a file, please use GcsfsFilePath.",
        ):
            GcsfsDirectoryPath(
                bucket_name="bucket", relative_path="this/is/a/path/to/file.csv"
            )

        GcsfsDirectoryPath(bucket_name="bucket", relative_path="this/is/a/path/")

    def test_cloud_console_link_for_gcs_path_simple_file(self) -> None:
        """Test cloud console link for a simple file in the bucket root."""
        path = GcsfsFilePath(bucket_name="my-bucket", blob_name="file.txt")
        expected_url = (
            "https://console.cloud.google.com/storage/browser/my-bucket/file.txt"
        )
        self.assertEqual(expected_url, path.cloud_console_link_for_gcs_path())

    def test_cloud_console_link_for_gcs_path_simple_directory(self) -> None:
        """Test cloud console link for a directory."""
        path = GcsfsDirectoryPath(bucket_name="my-bucket")
        expected_url = "https://console.cloud.google.com/storage/browser/my-bucket/"
        self.assertEqual(expected_url, path.cloud_console_link_for_gcs_path())

    def test_cloud_console_link_for_gcs_path_deeply_nested(self) -> None:
        """Test cloud console link for a deeply nested file path."""
        path = GcsfsFilePath.from_absolute_path(
            "gs://recidiviz-456-bucket/path/to/very/deeply/nested/file.parquet"
        )
        expected_url = "https://console.cloud.google.com/storage/browser/recidiviz-456-bucket/path/to/very/deeply/nested/file.parquet"
        self.assertEqual(expected_url, path.cloud_console_link_for_gcs_path())
