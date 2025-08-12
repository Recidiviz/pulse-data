# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for our test-only impl of GCSFileSystem"""

from unittest import TestCase

from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


class TestFakeGCSFileSystem(TestCase):
    """Unit tests for FakeGCSFileSystem"""

    def test_list_dirs(self) -> None:
        bucket = "fake-bucket"
        test_subdir = GcsfsDirectoryPath.from_bucket_and_blob_name(
            bucket_name=bucket, blob_name="/dir/subdir/"
        )
        fs = FakeGCSFileSystem()

        # no directories, no problem
        assert not fs.list_directories(test_subdir)

        # directory is not a subdirectory of itself.
        fs.test_add_path(path=test_subdir, local_path=None)
        assert not fs.list_directories(test_subdir)

        # this is a file in the directory, so we don't need to worry about it
        fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                bucket_name=bucket, blob_name="dir/subdir/csv.py"
            ),
            local_path=None,
        )
        assert not fs.list_directories(test_subdir)

        # this is a valid subdirectory
        fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                bucket_name=bucket, blob_name="dir/subdir/a-valid-subdir/csv.py"
            ),
            local_path=None,
        )
        assert fs.list_directories(test_subdir) == [
            GcsfsDirectoryPath.from_bucket_and_blob_name(
                bucket_name=bucket, blob_name="dir/subdir/a-valid-subdir/"
            )
        ]

        # this is another valid subdirectory, but is the same as the above
        fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                bucket_name=bucket, blob_name="dir/subdir/a-valid-subdir/csv-2.py"
            ),
            local_path=None,
        )
        assert fs.list_directories(test_subdir) == [
            GcsfsDirectoryPath.from_bucket_and_blob_name(
                bucket_name=bucket, blob_name="dir/subdir/a-valid-subdir/"
            )
        ]

        # this is even another valid subdirectory, but is the same as the above
        fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                bucket_name=bucket,
                blob_name="dir/subdir/a-valid-subdir/another-subdir/csv.py",
            ),
            local_path=None,
        )
        assert fs.list_directories(test_subdir) == [
            GcsfsDirectoryPath.from_bucket_and_blob_name(
                bucket_name=bucket, blob_name="dir/subdir/a-valid-subdir/"
            )
        ]

        # this is a different subdir
        fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                bucket_name=bucket,
                blob_name="dir/subdir/another-valid-subdir/another-subdir/csv.py",
            ),
            local_path=None,
        )
        assert sorted(
            fs.list_directories(test_subdir), key=lambda x: x.relative_path
        ) == [
            GcsfsDirectoryPath.from_bucket_and_blob_name(
                bucket_name=bucket, blob_name="dir/subdir/a-valid-subdir/"
            ),
            GcsfsDirectoryPath.from_bucket_and_blob_name(
                bucket_name=bucket, blob_name="dir/subdir/another-valid-subdir/"
            ),
        ]

        # this is altogether a different dir
        fs.test_add_path(
            path=GcsfsFilePath.from_bucket_and_blob_name(
                bucket_name=bucket,
                blob_name="diff-dir/subdir/another-valid-subdir/another-subdir/csv.py",
            ),
            local_path=None,
        )
        assert sorted(
            fs.list_directories(test_subdir), key=lambda x: x.relative_path
        ) == [
            GcsfsDirectoryPath.from_bucket_and_blob_name(
                bucket_name=bucket, blob_name="dir/subdir/a-valid-subdir/"
            ),
            GcsfsDirectoryPath.from_bucket_and_blob_name(
                bucket_name=bucket, blob_name="dir/subdir/another-valid-subdir/"
            ),
        ]

    def test_ls_glob(self) -> None:
        bucket = "fake-bucket"
        fs = FakeGCSFileSystem()

        # no files, no problem
        assert not fs.ls(bucket_name=bucket)
        assert not fs.ls(bucket_name=bucket, blob_prefix="aaaa")
        assert not fs.ls(bucket_name=bucket, match_glob="aaaa")

        with self.assertRaisesRegex(
            ValueError, "Can only specify at most one of blob_prefix and match_glob"
        ):
            fs.ls(bucket_name=bucket, blob_prefix="aaaa", match_glob="aaaa")

        # let's add a file and see how we do
        simple_path = GcsfsFilePath.from_bucket_and_blob_name(
            bucket_name=bucket, blob_name="dir/subdir/csv.py"
        )
        fs.test_add_path(path=simple_path, local_path=None)

        # not matches
        assert not fs.ls(bucket_name=bucket, match_glob="another-dir/*")
        assert not fs.ls(bucket_name=bucket, match_glob="dir/subdir-2/*")
        assert not fs.ls(bucket_name=bucket, match_glob="dir/subdir/csv*python")
        # matches
        assert fs.ls(bucket_name=bucket, match_glob="dir/*") == [simple_path]
        assert fs.ls(bucket_name=bucket, match_glob="dir/subdir/*") == [simple_path]
        assert fs.ls(bucket_name=bucket, match_glob="dir/subdir/csv*") == [simple_path]
        assert fs.ls(bucket_name=bucket, match_glob="*.py") == [simple_path]
        assert fs.ls(bucket_name=bucket, match_glob="*csv[.]*") == [simple_path]

        # let's add another file and see how we do
        simple_path_2 = GcsfsFilePath.from_bucket_and_blob_name(
            bucket_name=bucket, blob_name="dir/subdir/json.py"
        )
        fs.test_add_path(path=simple_path_2, local_path=None)

        # not matches
        assert not fs.ls(bucket_name=bucket, match_glob="another-dir/*")
        assert not fs.ls(bucket_name=bucket, match_glob="dir/subdir-2/*")
        assert not fs.ls(bucket_name=bucket, match_glob="dir/subdir/csv*python")
        # matches
        assert fs.ls(bucket_name=bucket, match_glob="dir/*") == [
            simple_path,
            simple_path_2,
        ]
        assert fs.ls(bucket_name=bucket, match_glob="dir/subdir/*") == [
            simple_path,
            simple_path_2,
        ]
        assert fs.ls(bucket_name=bucket, match_glob="dir/subdir/csv*") == [simple_path]
        assert fs.ls(bucket_name=bucket, match_glob="dir/subdir/csv*") == [simple_path]
        assert fs.ls(bucket_name=bucket, match_glob="*.py") == [
            simple_path,
            simple_path_2,
        ]
        assert fs.ls(bucket_name=bucket, match_glob="*csv[.]*") == [simple_path]
        assert fs.ls(bucket_name=bucket, match_glob="*json[.]*") == [simple_path_2]
