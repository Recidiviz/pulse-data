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

from recidiviz.cloud_storage.gcs_file_system_impl import GCSFileSystemImpl, unzip
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest import fixtures


def no_retries_predicate(_exception: Exception) -> bool:
    return False


class FakeError(Exception):
    pass


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
        self.assertTrue(self.fs.exists(GcsfsBucketPath(bucket_name="my-bucket")))
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
            self.fs.exists(GcsfsBucketPath(bucket_name="my-bucket"))
        self.mock_storage_client.bucket.assert_called()

    def test_copy(self) -> None:
        bucket_path = GcsfsBucketPath(bucket_name="my-bucket")
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

    def test_copy_with_failure(self) -> None:
        bucket_path = GcsfsBucketPath(bucket_name="my-bucket")
        src_path = GcsfsFilePath.from_directory_and_file_name(bucket_path, "src.txt")
        dst_path = GcsfsFilePath.from_directory_and_file_name(bucket_path, "dst.txt")

        mock_src_blob = create_autospec(Blob)
        mock_dst_blob = create_autospec(Blob)

        mock_dst_blob.rewrite.side_effect = [
            # -- first try
            ("rewrite-token-1", 100, 200),
            (None, 200, 200),
            # -- second try
            ("rewrite-token-2", 100, 200),
            (None, 200, 200),
        ]
        mock_dst_bucket = create_autospec(Bucket)

        mock_dst_bucket.get_blob.side_effect = [
            # -- first try
            mock_src_blob,
            None,  # this is why we fail -- no new file!
            # -- second try
            mock_src_blob,
            mock_dst_blob,
        ]

        mock_dst_bucket.blob.return_value = mock_dst_blob

        self.mock_storage_client.bucket.return_value = mock_dst_bucket

        self.fs.copy(src_path=src_path, dst_path=dst_path)
        mock_dst_blob.rewrite.assert_has_calls(
            calls=[
                call(mock_src_blob, token=None),
                call(mock_src_blob, "rewrite-token-1"),
                call(mock_src_blob, token=None),
                call(mock_src_blob, "rewrite-token-2"),
            ]
        )

    # Disable retries for "transient" errors to make test easier to mock / follow.
    @patch(
        "recidiviz.cloud_storage.gcs_file_system_impl.retry.if_transient_error",
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
        mock_dst_bucket.name = bucket_path.bucket_name
        mock_dst_blob.name = dst_path.blob_name
        mock_dst_blob.bucket = mock_dst_bucket

        mock_dst_bucket.get_blob.side_effect = [
            mock_src_blob,
            mock_dst_blob,
            None,  # call for if the deleted path that we *tried* to make still exists
        ]
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

    def test_unzip_zip_with_single_file(self) -> None:
        # Arrange
        file_system = FakeGCSFileSystem()
        zip_file_name = "example_file_structure_commas.zip"
        local_zip_path = fixtures.as_filepath(zip_file_name)
        bucket_path = GcsfsBucketPath.from_absolute_path("gs://my-bucket")

        expected_contents_path = fixtures.as_filepath(
            "example_file_structure_commas.csv"
        )
        with open(expected_contents_path, mode="rb") as fixture_file:
            expected_contents = fixture_file.read()
        expected_destination_path = GcsfsFilePath.from_directory_and_file_name(
            bucket_path, "example_file_structure_commas.csv"
        )

        src_path = GcsfsFilePath.from_directory_and_file_name(
            bucket_path, zip_file_name
        )
        file_system.test_add_path(src_path, local_zip_path)

        # Act
        output_paths = unzip(
            file_system, zip_file_path=src_path, destination_dir=bucket_path
        )

        # Assert
        self.assertEqual([expected_destination_path], output_paths)

        unzipped_contents = file_system.download_as_bytes(expected_destination_path)
        self.assertEqual(expected_contents, unzipped_contents)

    def test_unzip_zip_with_multiple_files(self) -> None:
        # Arrange
        file_system = FakeGCSFileSystem()
        zip_file_name = "encoded_latin_1_and_encoded_utf_8.zip"
        local_zip_path = fixtures.as_filepath(zip_file_name)
        bucket_path = GcsfsBucketPath.from_absolute_path("gs://my-bucket")

        expected_contents_path_1 = fixtures.as_filepath("encoded_utf_8.csv")
        with open(expected_contents_path_1, mode="rb") as fixture_file:
            expected_contents_1 = fixture_file.read()

        expected_destination_path_1 = GcsfsFilePath.from_directory_and_file_name(
            bucket_path, "encoded_utf_8.csv"
        )

        expected_contents_path_2 = fixtures.as_filepath("encoded_latin_1.csv")
        with open(expected_contents_path_2, mode="rb") as fixture_file:
            expected_contents_2 = fixture_file.read()

        expected_destination_path_2 = GcsfsFilePath.from_directory_and_file_name(
            bucket_path, "encoded_latin_1.csv"
        )

        src_path = GcsfsFilePath.from_directory_and_file_name(
            bucket_path, zip_file_name
        )
        file_system.test_add_path(src_path, local_zip_path)

        # Act
        output_paths = unzip(
            file_system, zip_file_path=src_path, destination_dir=bucket_path
        )

        # Assert
        self.assertCountEqual(
            [expected_destination_path_1, expected_destination_path_2], output_paths
        )

        unzipped_contents_1 = file_system.download_as_bytes(expected_destination_path_1)
        self.assertEqual(expected_contents_1, unzipped_contents_1)

        unzipped_contents_2 = file_system.download_as_bytes(expected_destination_path_2)
        self.assertEqual(expected_contents_2, unzipped_contents_2)

    def test_mv_file_to_directory_safe(self) -> None:
        src_bucket = GcsfsBucketPath(bucket_name="my-source-bucket")
        dest_bucket = GcsfsBucketPath(bucket_name="my-destination-bucket")
        src_path = GcsfsFilePath.from_directory_and_file_name(
            src_bucket, "normalized_file.txt"
        )

        mock_blob = create_autospec(Blob)
        mock_blob.exists.return_value = True
        mock_dst_bucket = create_autospec(Bucket)
        mock_dst_bucket.get_blob.side_effect = [
            mock_blob,
            None,
            mock_blob,
            mock_blob,
            mock_blob,
            None,
        ]
        self.mock_storage_client.bucket.return_value = mock_dst_bucket

        with self.assertRaisesRegex(
            ValueError,
            r"Destination path \[.*normalized_file.txt\] already exists",
        ):
            self.fs.mv_file_to_directory_safe(src_path, dest_bucket)

        mock_dst_bucket.blob.return_value = mock_blob
        mock_blob.rewrite.return_value = (None, 200, 200)

        self.fs.mv_file_to_directory_safe(src_path, dest_bucket)

        mock_blob.delete.assert_called_once()
        mock_blob.rewrite.assert_called_once()
