# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for un7z."""
from unittest import TestCase

import py7zr

from recidiviz.cloud_storage.gcs_file_system_impl import generate_random_temp_path
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.cloud_storage.gcsfs_seven_zip import un7z
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest import fixtures


class TestUn7z(TestCase):
    """Tests for un7z."""

    @staticmethod
    def _build_7z(members: dict[str, str], *, password: str | None) -> str:
        """Writes a 7z archive to a temp path from |members| (arcname -> local fixture
        path), AES256-encrypted when |password| is given, and returns the temp path.
        """
        archive_path = generate_random_temp_path("archive.7z")
        with py7zr.SevenZipFile(archive_path, mode="w", password=password) as archive:
            for arcname, local_path in members.items():
                archive.write(local_path, arcname)
        return archive_path

    def test_un7z_encrypted_single_file(self) -> None:
        # Arrange
        file_system = FakeGCSFileSystem()
        bucket_path = GcsfsBucketPath.from_absolute_path("gs://my-bucket")
        contents_path = fixtures.as_filepath("example_file_structure_commas.csv")
        with open(contents_path, mode="rb") as fixture_file:
            expected_contents = fixture_file.read()

        archive_path = self._build_7z(
            {"example_file_structure_commas.csv": contents_path}, password="hunter2"
        )
        src_path = GcsfsFilePath.from_directory_and_file_name(bucket_path, "archive.7z")
        file_system.test_add_path(src_path, archive_path)
        expected_destination_path = GcsfsFilePath.from_directory_and_file_name(
            bucket_path, "example_file_structure_commas.csv"
        )

        # Act
        output_paths = un7z(
            file_system,
            seven_zip_file_path=src_path,
            destination_dir=bucket_path,
            password="hunter2",
        )

        # Assert
        self.assertEqual([expected_destination_path], output_paths)
        self.assertEqual(
            expected_contents, file_system.download_as_bytes(expected_destination_path)
        )

    def test_un7z_unencrypted_extracts_with_no_password(self) -> None:
        """An unencrypted 7z archive extracts when no password is supplied."""
        file_system = FakeGCSFileSystem()
        bucket_path = GcsfsBucketPath.from_absolute_path("gs://my-bucket")
        contents_path = fixtures.as_filepath("example_file_structure_commas.csv")
        with open(contents_path, mode="rb") as fixture_file:
            expected_contents = fixture_file.read()

        archive_path = self._build_7z(
            {"example_file_structure_commas.csv": contents_path}, password=None
        )
        src_path = GcsfsFilePath.from_directory_and_file_name(bucket_path, "archive.7z")
        file_system.test_add_path(src_path, archive_path)
        expected_destination_path = GcsfsFilePath.from_directory_and_file_name(
            bucket_path, "example_file_structure_commas.csv"
        )

        output_paths = un7z(
            file_system,
            seven_zip_file_path=src_path,
            destination_dir=bucket_path,
            password=None,
        )

        self.assertEqual([expected_destination_path], output_paths)
        self.assertEqual(
            expected_contents, file_system.download_as_bytes(expected_destination_path)
        )

    def test_un7z_encrypted_with_no_password_raises_value_error(self) -> None:
        file_system = FakeGCSFileSystem()
        bucket_path = GcsfsBucketPath.from_absolute_path("gs://my-bucket")
        contents_path = fixtures.as_filepath("example_file_structure_commas.csv")
        archive_path = self._build_7z(
            {"example_file_structure_commas.csv": contents_path}, password="hunter2"
        )
        src_path = GcsfsFilePath.from_directory_and_file_name(bucket_path, "archive.7z")
        file_system.test_add_path(src_path, archive_path)

        with self.assertRaisesRegex(
            ValueError, r"is encrypted but no password was provided"
        ):
            un7z(
                file_system,
                seven_zip_file_path=src_path,
                destination_dir=bucket_path,
                password=None,
            )

    def test_un7z_multiple_files(self) -> None:
        file_system = FakeGCSFileSystem()
        bucket_path = GcsfsBucketPath.from_absolute_path("gs://my-bucket")
        contents_path_1 = fixtures.as_filepath("encoded_utf_8.csv")
        contents_path_2 = fixtures.as_filepath("encoded_latin_1.csv")
        with open(contents_path_1, mode="rb") as fixture_file:
            expected_contents_1 = fixture_file.read()
        with open(contents_path_2, mode="rb") as fixture_file:
            expected_contents_2 = fixture_file.read()

        archive_path = self._build_7z(
            {
                "encoded_utf_8.csv": contents_path_1,
                "encoded_latin_1.csv": contents_path_2,
            },
            password="hunter2",
        )
        src_path = GcsfsFilePath.from_directory_and_file_name(bucket_path, "archive.7z")
        file_system.test_add_path(src_path, archive_path)
        expected_destination_path_1 = GcsfsFilePath.from_directory_and_file_name(
            bucket_path, "encoded_utf_8.csv"
        )
        expected_destination_path_2 = GcsfsFilePath.from_directory_and_file_name(
            bucket_path, "encoded_latin_1.csv"
        )

        output_paths = un7z(
            file_system,
            seven_zip_file_path=src_path,
            destination_dir=bucket_path,
            password="hunter2",
        )

        self.assertCountEqual(
            [expected_destination_path_1, expected_destination_path_2], output_paths
        )
        self.assertEqual(
            expected_contents_1,
            file_system.download_as_bytes(expected_destination_path_1),
        )
        self.assertEqual(
            expected_contents_2,
            file_system.download_as_bytes(expected_destination_path_2),
        )

    def test_un7z_wrong_password_raises_value_error(self) -> None:
        file_system = FakeGCSFileSystem()
        bucket_path = GcsfsBucketPath.from_absolute_path("gs://my-bucket")
        contents_path = fixtures.as_filepath("example_file_structure_commas.csv")
        archive_path = self._build_7z(
            {"example_file_structure_commas.csv": contents_path}, password="hunter2"
        )
        src_path = GcsfsFilePath.from_directory_and_file_name(bucket_path, "archive.7z")
        file_system.test_add_path(src_path, archive_path)

        with self.assertRaisesRegex(
            ValueError, r"corrupt or the decryption password is incorrect"
        ):
            un7z(
                file_system,
                seven_zip_file_path=src_path,
                destination_dir=bucket_path,
                password="wrong-password",
            )

    def test_un7z_not_a_7z_file(self) -> None:
        file_system = FakeGCSFileSystem()
        bucket_path = GcsfsBucketPath.from_absolute_path("gs://my-bucket")
        src_path = GcsfsFilePath.from_directory_and_file_name(bucket_path, "archive.7z")
        # A plain CSV that only claims to be a 7z archive by its extension.
        file_system.test_add_path(
            src_path, fixtures.as_filepath("example_file_structure_commas.csv")
        )

        with self.assertRaisesRegex(ValueError, r"is not a 7z file\. Cannot extract"):
            un7z(
                file_system,
                seven_zip_file_path=src_path,
                destination_dir=bucket_path,
                password="hunter2",
            )
