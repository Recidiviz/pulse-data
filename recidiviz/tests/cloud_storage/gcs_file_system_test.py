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
"""Tests for the GCSFileSystem."""
from unittest import TestCase

from google.api_core import exceptions
from google.cloud import storage
from google.cloud.storage import Blob, Bucket
from mock import create_autospec
from mock.mock import call, patch

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystemImpl
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath


def no_retries_predicate(_exception: Exception) -> bool:
    return False


class TestGcsFileSystem(TestCase):
    """Tests for the GCSFileSystem."""

    def setUp(self) -> None:
        self.mock_storage_client = create_autospec(storage.Client)
        self.fs = GCSFileSystemImpl(self.mock_storage_client)

    def test_retry(self) -> None:
        mock_bucket = create_autospec(Bucket)
        mock_bucket.exists.return_value = True
        # Client first raises a Gateway timeout, then returns a normal bucket.
        self.mock_storage_client.get_bucket.side_effect = [
            exceptions.GatewayTimeout("Exception"),
            mock_bucket,
        ]

        # Should not crash!
        self.assertTrue(
            self.fs.exists(GcsfsBucketPath.from_absolute_path("gs://my-bucket"))
        )
        self.mock_storage_client.bucket.assert_called()

    def test_retry_with_fatal_error(self) -> None:
        mock_bucket = create_autospec(Bucket)
        mock_bucket.exists.return_value = True
        # Client first raises a Gateway timeout, then on retry will raise a ValueError
        self.mock_storage_client.bucket.side_effect = [
            exceptions.GatewayTimeout("Exception"),
            ValueError("This will crash"),
        ]

        with self.assertRaises(ValueError):
            self.fs.exists(GcsfsBucketPath.from_absolute_path("gs://my-bucket"))
        self.mock_storage_client.bucket.assert_called()

    def test_copy(self) -> None:
        bucket_path = GcsfsBucketPath.from_absolute_path("gs://my-bucket")
        src_path = GcsfsFilePath.from_directory_and_file_name(bucket_path, "src.txt")
        dst_path = GcsfsFilePath.from_directory_and_file_name(bucket_path, "dst.txt")

        mock_src_blob = create_autospec(Blob)

        mock_dst_blob = create_autospec(Blob)
        mock_dst_blob.rewrite.side_effect = [
            ("rewrite-token", 100, 200),
            (None, 200, 200),
        ]
        mock_dst_bucket = create_autospec(Bucket)

        def mock_get_blob(blob_name: str) -> Blob:
            if blob_name == src_path.file_name:
                return mock_src_blob
            if blob_name == dst_path.file_name:
                return mock_dst_blob
            raise ValueError(f"Unexpected blob: {blob_name}")

        mock_dst_bucket.get_blob.side_effect = mock_get_blob
        mock_dst_bucket.blob.return_value = mock_dst_blob

        self.mock_storage_client.bucket.return_value = mock_dst_bucket

        self.fs.copy(src_path=src_path, dst_path=dst_path)
        mock_dst_blob.rewrite.assert_has_calls(
            calls=[
                call(mock_src_blob, token=None),
                call(mock_src_blob, "rewrite-token"),
            ]
        )

    # Disable retries for "transient" errors to make test easier to mock / follow.
    @patch(
        "recidiviz.cloud_storage.gcs_file_system.retry.if_transient_error",
        no_retries_predicate,
    )
    def test_copy_failure(self) -> None:
        bucket_path = GcsfsBucketPath.from_absolute_path("gs://my-bucket")
        src_path = GcsfsFilePath.from_directory_and_file_name(bucket_path, "src.txt")
        dst_path = GcsfsFilePath.from_directory_and_file_name(bucket_path, "dst.txt")

        mock_src_blob = create_autospec(Blob)

        mock_dst_blob = create_autospec(Blob)
        mock_dst_blob.rewrite.side_effect = [
            ("rewrite-token", 100, 200),
            exceptions.TooManyRequests("Too many requests!"),  # type: ignore [attr-defined]
        ]
        mock_dst_bucket = create_autospec(Bucket)

        def mock_get_blob(blob_name: str) -> Blob:
            if blob_name == src_path.file_name:
                return mock_src_blob
            if blob_name == dst_path.file_name:
                return mock_dst_blob
            raise ValueError(f"Unexpected blob: {blob_name}")

        mock_dst_bucket.get_blob.side_effect = mock_get_blob
        mock_dst_bucket.blob.return_value = mock_dst_blob

        self.mock_storage_client.bucket.return_value = mock_dst_bucket

        with self.assertRaisesRegex(
            exceptions.TooManyRequests,  # type: ignore [attr-defined]
            "Too many requests!",
        ):
            self.fs.copy(src_path=src_path, dst_path=dst_path)
        mock_dst_blob.rewrite.assert_has_calls(
            calls=[
                call(mock_src_blob, token=None),
                call(mock_src_blob, "rewrite-token"),
            ]
        )
        # Assert we tried to clean up the destination file
        mock_dst_blob.delete.assert_called()
