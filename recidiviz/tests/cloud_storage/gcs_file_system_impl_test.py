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
from zipfile import ZIP_STORED, ZipFile

from google.api_core import exceptions
from google.cloud import storage
from google.cloud.storage import Blob, Bucket
from mock import create_autospec
from mock.mock import call, patch

from recidiviz.cloud_storage.gcs_file_system_impl import (
    GCSFileSystemImpl,
    generate_random_temp_path,
    gunzip,
    unzip,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle
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

    def test_upload_from_contents_handle_stream_sets_chunk_size(self) -> None:
        bucket_path = GcsfsBucketPath(bucket_name="my-bucket")
        dst_path = GcsfsFilePath.from_directory_and_file_name(bucket_path, "dst.csv")
        mock_bucket = create_autospec(Bucket)
        mock_blob = create_autospec(Blob)
        mock_bucket.blob.return_value = mock_blob
        self.mock_storage_client.bucket.return_value = mock_bucket

        self.fs.upload_from_contents_handle_stream(
            path=dst_path,
            contents_handle=LocalFileContentsHandle(
                fixtures.as_filepath("example_file_structure_commas.csv"),
                cleanup_file=False,
            ),
            content_type="text/csv",
            chunk_size=32 * 1024 * 1024,
        )

        mock_bucket.blob.assert_called_once_with("dst.csv", chunk_size=33554432)
        mock_blob.upload_from_file.assert_called_once()

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

    def test_gunzip(self) -> None:
        # Arrange
        file_system = FakeGCSFileSystem()
        gz_file_name = "example_file_structure_commas.csv.gz"
        bucket_path = GcsfsBucketPath.from_absolute_path("gs://my-bucket")

        expected_contents_path = fixtures.as_filepath(
            "example_file_structure_commas.csv"
        )
        with open(expected_contents_path, mode="rb") as fixture_file:
            expected_contents = fixture_file.read()
        # The .gz extension is stripped; everything else about the name is preserved.
        expected_destination_path = GcsfsFilePath.from_directory_and_file_name(
            bucket_path, "example_file_structure_commas.csv"
        )

        src_path = GcsfsFilePath.from_directory_and_file_name(bucket_path, gz_file_name)
        file_system.test_add_path(src_path, fixtures.as_filepath(gz_file_name))

        # Act
        output_paths = gunzip(
            file_system, gz_file_path=src_path, destination_dir=bucket_path
        )

        # Assert
        self.assertEqual([expected_destination_path], output_paths)
        self.assertEqual(
            expected_contents, file_system.download_as_bytes(expected_destination_path)
        )

    def test_gunzip_corrupt_gzip_raises_value_error(self) -> None:
        """A truncated member has valid magic bytes but fails mid-decompression. It must
        surface as invalid input (ValueError), not a server error the caller retries."""
        file_system = FakeGCSFileSystem()
        bucket_path = GcsfsBucketPath.from_absolute_path("gs://my-bucket")
        src_path = GcsfsFilePath.from_directory_and_file_name(
            bucket_path, "truncated.csv.gz"
        )
        with open(
            fixtures.as_filepath("example_file_structure_commas.csv.gz"), "rb"
        ) as f:
            valid_gz = f.read()
        truncated_path = generate_random_temp_path("truncated.csv.gz")
        with open(truncated_path, "wb") as f:
            f.write(valid_gz[: len(valid_gz) // 2])
        file_system.test_add_path(src_path, truncated_path)

        with self.assertRaisesRegex(ValueError, r"Could not decompress gzip file"):
            gunzip(file_system, gz_file_path=src_path, destination_dir=bucket_path)

        self.assertFalse(
            file_system.exists(
                GcsfsFilePath.from_directory_and_file_name(bucket_path, "truncated.csv")
            )
        )

    def test_gunzip_not_a_gzip_file(self) -> None:
        file_system = FakeGCSFileSystem()
        bucket_path = GcsfsBucketPath.from_absolute_path("gs://my-bucket")
        src_path = GcsfsFilePath.from_directory_and_file_name(
            bucket_path, "example_file_structure_commas.csv.gz"
        )
        # A plain CSV that only claims to be gzipped.
        file_system.test_add_path(
            src_path, fixtures.as_filepath("example_file_structure_commas.csv")
        )

        with self.assertRaisesRegex(ValueError, r"is not a gzip file\. Cannot"):
            gunzip(file_system, gz_file_path=src_path, destination_dir=bucket_path)

    def test_unzip_corrupt_member_reports_uploaded_members(self) -> None:
        """A member whose data fails its CRC check is only detected partway through the
        archive. Filename normalization may already have renamed the members uploaded
        before it, so they are not deleted; the error must list them for manual cleanup
        and surface as invalid input (ValueError)."""
        file_system = FakeGCSFileSystem()
        bucket_path = GcsfsBucketPath.from_absolute_path("gs://my-bucket")
        src_path = GcsfsFilePath.from_directory_and_file_name(
            bucket_path, "corrupt_second_member.zip"
        )
        second_member_contents = b"second,file\n" * 100

        zip_path = generate_random_temp_path("corrupt_second_member.zip")
        with ZipFile(zip_path, "w", compression=ZIP_STORED) as z:
            z.writestr("first.csv", b"first,file\n")
            z.writestr("second.csv", second_member_contents)
        with open(zip_path, "rb") as f:
            zip_bytes = bytearray(f.read())
        # Members are stored uncompressed, so flipping a byte inside the second
        # member's data leaves the archive structure intact but fails its CRC check.
        corrupt_offset = zip_bytes.index(second_member_contents) + 1
        zip_bytes[corrupt_offset] ^= 0xFF
        with open(zip_path, "wb") as f:
            f.write(zip_bytes)
        file_system.test_add_path(src_path, zip_path)

        with self.assertRaisesRegex(
            ValueError,
            r"^Could not unzip member \[second\.csv\] of zip file "
            r"\[gs://my-bucket/corrupt_second_member\.zip\]: "
            r"Bad CRC-32 for file 'second\.csv'\. Members already uploaded, "
            r"possibly since renamed to unprocessed_<timestamp>_raw_<name> by "
            r"filename normalization, must be deleted from the bucket manually: "
            r"\[gs://my-bucket/first\.csv\]$",
        ):
            unzip(file_system, zip_file_path=src_path, destination_dir=bucket_path)

        self.assertTrue(
            file_system.exists(
                GcsfsFilePath.from_directory_and_file_name(bucket_path, "first.csv")
            )
        )
        self.assertFalse(
            file_system.exists(
                GcsfsFilePath.from_directory_and_file_name(bucket_path, "second.csv")
            )
        )

    def test_unzip_not_a_zip_file(self) -> None:
        file_system = FakeGCSFileSystem()
        bucket_path = GcsfsBucketPath.from_absolute_path("gs://my-bucket")
        src_path = GcsfsFilePath.from_directory_and_file_name(
            bucket_path, "example_file_structure_commas.zip"
        )
        # A plain CSV that only claims to be zipped.
        file_system.test_add_path(
            src_path, fixtures.as_filepath("example_file_structure_commas.csv")
        )

        with self.assertRaisesRegex(ValueError, r"is not a zip file\. Cannot unzip"):
            unzip(file_system, zip_file_path=src_path, destination_dir=bucket_path)

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
