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
"""Tests for cloud storage utils"""
import datetime
from itertools import chain
from unittest import TestCase

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tools.ingest.operations.helpers.cloud_storage_utils import (
    get_storage_directories_containing_raw_files,
)


class TestGetStorageDirectories(TestCase):
    """Unit tests for cloud storage directory utils"""

    fs: GCSFileSystem
    yy: GcsfsDirectoryPath
    zz: GcsfsDirectoryPath

    @classmethod
    def setUpClass(cls) -> None:
        fs = FakeGCSFileSystem()

        # for testing, we are stetting up a storage bucket for two states --
        cls.yy = GcsfsDirectoryPath(bucket_name="bucket", relative_path="us-yy/raw")
        cls.zz = GcsfsDirectoryPath(bucket_name="bucket", relative_path="us-zz/raw")

        # for yy we are laying out files like
        # -- 2024/
        #      |-- 01/
        #           |-- 01/
        #           |-- 02/
        #      |-- 02/
        #           |-- 01/
        # -- 2025/
        #       |-- 01/
        #           |-- 01/
        #           |-- 02/

        yy_files = [
            GcsfsFilePath.from_directory_and_file_name(cls.yy, "2024/01/01/tag.csv"),
            GcsfsFilePath.from_directory_and_file_name(cls.yy, "2024/01/01/tag-2.csv"),
            GcsfsFilePath.from_directory_and_file_name(cls.yy, "2024/01/02/tag.csv"),
            GcsfsFilePath.from_directory_and_file_name(cls.yy, "2024/02/01/tag-3.csv"),
            GcsfsFilePath.from_directory_and_file_name(cls.yy, "2025/01/01/tag.csv"),
            GcsfsFilePath.from_directory_and_file_name(cls.yy, "2025/01/02/tag.csv"),
        ]

        # for zz we are laying out files like
        # -- 2024/
        #       |-- 01/
        #           |-- 01/
        # -- 2025/
        #       |-- 01/
        #           |-- 01/

        zz_files = [
            GcsfsFilePath.from_directory_and_file_name(cls.zz, "2024/01/01/tag.csv"),
            GcsfsFilePath.from_directory_and_file_name(cls.zz, "2024/01/01/tag-2.csv"),
            GcsfsFilePath.from_directory_and_file_name(cls.zz, "2024/01/01/tag-3.csv"),
            GcsfsFilePath.from_directory_and_file_name(cls.zz, "2024/01/01/tag-4.csv"),
            GcsfsFilePath.from_directory_and_file_name(cls.zz, "2025/01/01/tag-5.csv"),
        ]

        for f in chain.from_iterable((yy_files, zz_files)):
            fs.test_add_path(f, local_path=None)

        cls.fs = fs

    def test_get_storage_directories_containing_raw_files_no_filters(self) -> None:
        # wrong bucket name
        assert not get_storage_directories_containing_raw_files(
            self.fs,
            storage_bucket_path=GcsfsDirectoryPath(
                bucket_name="bucket-2", relative_path="us-yy"
            ),
            upper_bound_date_inclusive=None,
            lower_bound_date_inclusive=None,
        )

        # wrong location
        assert not get_storage_directories_containing_raw_files(
            self.fs,
            storage_bucket_path=GcsfsDirectoryPath(
                bucket_name="bucket", relative_path="us-xx"
            ),
            upper_bound_date_inclusive=None,
            lower_bound_date_inclusive=None,
        )

        # all directories for xx
        assert sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-yy"
                ),
                upper_bound_date_inclusive=None,
                lower_bound_date_inclusive=None,
            ),
            key=lambda x: x.uri(),
        ) == [
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/02"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/02/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/02"),
        ]

        # all directories for zz
        assert sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-zz"
                ),
                upper_bound_date_inclusive=None,
                lower_bound_date_inclusive=None,
            ),
            key=lambda x: x.uri(),
        ) == [
            GcsfsDirectoryPath.from_dir_and_subdir(self.zz, "2024/01/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.zz, "2025/01/01"),
        ]

    def test_get_storage_directories_containing_raw_files_date_filters(self) -> None:
        jan_1_2024 = datetime.date(2024, 1, 1)
        jan_2_2024 = datetime.date(2024, 1, 2)
        feb_1_2024 = datetime.date(2024, 2, 1)
        jan_1_2025 = datetime.date(2025, 1, 1)
        jan_2_2025 = datetime.date(2025, 1, 2)

        # wrong bucket name
        assert not get_storage_directories_containing_raw_files(
            self.fs,
            storage_bucket_path=GcsfsDirectoryPath(
                bucket_name="bucket-2", relative_path="us-yy"
            ),
            upper_bound_date_inclusive=jan_1_2024,
            lower_bound_date_inclusive=None,
        )

        # wrong location
        assert not get_storage_directories_containing_raw_files(
            self.fs,
            storage_bucket_path=GcsfsDirectoryPath(
                bucket_name="bucket", relative_path="us-xx"
            ),
            upper_bound_date_inclusive=jan_1_2024,
            lower_bound_date_inclusive=None,
        )

        # inclusive means should include edges
        assert sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-yy"
                ),
                upper_bound_date_inclusive=None,
                lower_bound_date_inclusive=None,
            ),
            key=lambda x: x.uri(),
        ) == sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-yy"
                ),
                lower_bound_date_inclusive=jan_1_2024,
                upper_bound_date_inclusive=jan_2_2025,
            ),
            key=lambda x: x.uri(),
        )

        # apply single day filter on lower
        assert sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-yy"
                ),
                lower_bound_date_inclusive=jan_2_2024,
                upper_bound_date_inclusive=None,
            ),
            key=lambda x: x.uri(),
        ) == [
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/02"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/02/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/02"),
        ]

        # apply single day filter on upper
        assert sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-yy"
                ),
                lower_bound_date_inclusive=None,
                upper_bound_date_inclusive=jan_1_2025,
            ),
            key=lambda x: x.uri(),
        ) == [
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/02"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/02/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/01"),
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/02"),
        ]

        # apply single day filter on both sides
        assert sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-yy"
                ),
                lower_bound_date_inclusive=jan_2_2024,
                upper_bound_date_inclusive=jan_1_2025,
            ),
            key=lambda x: x.uri(),
        ) == [
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/02"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/02/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/01"),
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/02"),
        ]

        # apply month filter
        assert sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-yy"
                ),
                lower_bound_date_inclusive=feb_1_2024,
                upper_bound_date_inclusive=jan_1_2025,
            ),
            key=lambda x: x.uri(),
        ) == [
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/01"),
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/02"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/02/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/01"),
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/02"),
        ]

    def test_get_storage_directories_containing_raw_files_glob_filters(self) -> None:
        # wrong bucket name
        assert not get_storage_directories_containing_raw_files(
            self.fs,
            storage_bucket_path=GcsfsDirectoryPath(
                bucket_name="bucket-2", relative_path="us-yy"
            ),
            upper_bound_date_inclusive=None,
            lower_bound_date_inclusive=None,
            file_filter_globs=["*tag[.]*"],
        )

        # wrong location
        assert not get_storage_directories_containing_raw_files(
            self.fs,
            storage_bucket_path=GcsfsDirectoryPath(
                bucket_name="bucket", relative_path="us-xx"
            ),
            upper_bound_date_inclusive=None,
            lower_bound_date_inclusive=None,
            file_filter_globs=["*tag[.]*"],
        )

        # single star means should include everyone
        assert sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-yy"
                ),
                upper_bound_date_inclusive=None,
                lower_bound_date_inclusive=None,
            ),
            key=lambda x: x.uri(),
        ) == sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-yy"
                ),
                lower_bound_date_inclusive=None,
                upper_bound_date_inclusive=None,
                file_filter_globs=["*"],
            ),
            key=lambda x: x.uri(),
        )

        # apply single tag filter
        assert sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-yy"
                ),
                lower_bound_date_inclusive=None,
                upper_bound_date_inclusive=None,
                file_filter_globs=["*tag[.]*"],
            ),
            key=lambda x: x.uri(),
        ) == [
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/02"),
            # only has tag-3
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/02/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/02"),
        ]
        assert sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-yy"
                ),
                lower_bound_date_inclusive=None,
                upper_bound_date_inclusive=None,
                file_filter_globs=["*tag-3[.]*"],
            ),
            key=lambda x: x.uri(),
        ) == [
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/01"),
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/02"),
            # only dir with tag-3
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/02/01"),
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/01"),
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/02"),
        ]

        # apply two filters
        assert sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-yy"
                ),
                lower_bound_date_inclusive=None,
                upper_bound_date_inclusive=None,
                file_filter_globs=["*tag[.]*", "*tag-3[.]*"],  # whole team eats
            ),
            key=lambda x: x.uri(),
        ) == [
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/02"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/02/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/02"),
        ]

    def test_get_storage_directories_containing_raw_files_date_and_glob_filters(
        self,
    ) -> None:
        jan_1_2024 = datetime.date(2024, 1, 1)
        jan_2_2024 = datetime.date(2024, 1, 2)
        jan_1_2025 = datetime.date(2025, 1, 1)
        jan_2_2025 = datetime.date(2025, 1, 2)

        # wrong bucket name
        assert not get_storage_directories_containing_raw_files(
            self.fs,
            storage_bucket_path=GcsfsDirectoryPath(
                bucket_name="bucket-2", relative_path="us-yy"
            ),
            upper_bound_date_inclusive=jan_1_2024,
            lower_bound_date_inclusive=None,
            file_filter_globs=["*tag[.]*"],
        )

        # wrong location
        assert not get_storage_directories_containing_raw_files(
            self.fs,
            storage_bucket_path=GcsfsDirectoryPath(
                bucket_name="bucket", relative_path="us-xx"
            ),
            upper_bound_date_inclusive=jan_1_2024,
            lower_bound_date_inclusive=None,
            file_filter_globs=["*tag[.]*"],
        )

        # inclusive means should include edges
        assert sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-yy"
                ),
                upper_bound_date_inclusive=None,
                lower_bound_date_inclusive=None,
                file_filter_globs=["*"],
            ),
            key=lambda x: x.uri(),
        ) == sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-yy"
                ),
                lower_bound_date_inclusive=jan_1_2024,
                upper_bound_date_inclusive=jan_2_2025,
            ),
            key=lambda x: x.uri(),
        )

        # apply single day filter on lower with file filter
        assert sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-yy"
                ),
                lower_bound_date_inclusive=jan_2_2024,
                upper_bound_date_inclusive=None,
                file_filter_globs=["*tag[.]*"],
            ),
            key=lambda x: x.uri(),
        ) == [
            # filtered by date
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/02"),
            # filtered by file filter
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/02/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/01"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/02"),
        ]

        # apply single day filter on upper
        assert sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-yy"
                ),
                lower_bound_date_inclusive=None,
                upper_bound_date_inclusive=jan_1_2025,
                file_filter_globs=["*tag-3[.]*"],
            ),
            key=lambda x: x.uri(),
        ) == [
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/01"),
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/02"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/02/01"),
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/01"),
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/02"),
        ]

        # apply single day filter on both sides with file filter
        assert sorted(
            get_storage_directories_containing_raw_files(
                self.fs,
                storage_bucket_path=GcsfsDirectoryPath(
                    bucket_name="bucket", relative_path="us-yy"
                ),
                lower_bound_date_inclusive=jan_2_2024,
                upper_bound_date_inclusive=jan_1_2025,
                file_filter_globs=["*tag-3[.]*"],
            ),
            key=lambda x: x.uri(),
        ) == [
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/01"),
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/01/02"),
            GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2024/02/01"),
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/01"),
            # GcsfsDirectoryPath.from_dir_and_subdir(self.yy, "2025/01/02"),
        ]
