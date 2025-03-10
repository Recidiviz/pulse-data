# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for move_ingest_bucket_raw_files_to_deprecated_controller.py."""
import datetime
import unittest
from typing import Any
from unittest.mock import patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsDirectoryPath
from recidiviz.tools.ingest.operations.helpers.move_ingest_bucket_raw_files_to_deprecated_controller import (
    MoveIngestBucketRawFilesToDeprecatedController,
)


class MoveIngestBucketRawFilesToDeprecatedControllerTest(unittest.TestCase):
    """Tests for MoveIngestBucketRawFilesToDeprecatedController class"""

    def setUp(self) -> None:
        self.gsutil_ls_patcher = patch(
            "recidiviz.tools.ingest.operations.helpers.move_ingest_bucket_raw_files_to_deprecated_controller.gsutil_ls"
        )
        self.gsutil_ls_mock = self.gsutil_ls_patcher.start()
        self.files = [
            "gs://test_bucket/unprocessed_2024-11-12T12:00:00:000000_raw_file_tag.csv",
            "gs://test_bucket/unprocessed_2024-12-12T12:00:00:000000_raw_file_tag.csv",
            "gs://test_bucket/unprocessed_2024-12-24T12:00:00:000000_raw_file_tag.csv",
            "gs://test_bucket/unprocessed_2024-11-12T12:00:00:000000_raw_file_tag_2.csv",
            "gs://test_bucket/unprocessed_2024-12-12T12:00:00:000000_raw_file_tag_2.csv",
            "gs://test_bucket/unprocessed_2024-12-24T12:00:00:000000_raw_file_tag_2.csv",
        ]
        self.gsutil_ls_mock.return_value = self.files
        self.open_patcher = patch(
            "recidiviz.tools.ingest.operations.helpers.move_ingest_bucket_raw_files_to_deprecated_controller.open"
        )
        self.open_patcher.start()

        self.prompt_patcher = patch(
            "recidiviz.tools.ingest.operations.helpers.move_ingest_bucket_raw_files_to_deprecated_controller.prompt_for_confirmation"
        )
        self.prompt_patcher.start()

    def tearDown(self) -> None:
        self.gsutil_ls_patcher.stop()
        self.open_patcher.stop()
        self.prompt_patcher.stop()

    def test_move_ingest_bucket_raw_files_to_deprecated_controller(
        self,
    ) -> None:

        controller = MoveIngestBucketRawFilesToDeprecatedController(
            source_ingest_bucket=GcsfsBucketPath("test_bucket"),
            destination_region_deprecated_storage_dir_path=GcsfsDirectoryPath(
                bucket_name="test_bucket_storage", relative_path="us_xx/deprecated/"
            ),
            start_datetime_inclusive=None,
            end_datetime_exclusive=None,
            file_tag_filters=None,
            dry_run=True,
            log_output_path="log_output_path",
        )
        successful_files, failed_files = controller.run()

        self.assertEqual(6, len(successful_files))
        self.assertListEqual(
            sorted(self.files), sorted([file.uri() for file in successful_files])
        )
        self.assertEqual(0, len(failed_files))

    def test_move_ingest_bucket_raw_files_to_deprecated_controller_with_file_tag_filter(
        self,
    ) -> None:

        controller = MoveIngestBucketRawFilesToDeprecatedController(
            source_ingest_bucket=GcsfsBucketPath("test_bucket"),
            destination_region_deprecated_storage_dir_path=GcsfsDirectoryPath(
                bucket_name="test_bucket_storage", relative_path="us_xx/deprecated/"
            ),
            start_datetime_inclusive=None,
            end_datetime_exclusive=None,
            file_tag_filters=["file_tag"],
            dry_run=True,
            log_output_path="log_output_path",
        )
        successful_files, failed_files = controller.run()

        self.assertEqual(3, len(successful_files))
        self.assertListEqual(
            sorted(
                [
                    "gs://test_bucket/unprocessed_2024-11-12T12:00:00:000000_raw_file_tag.csv",
                    "gs://test_bucket/unprocessed_2024-12-12T12:00:00:000000_raw_file_tag.csv",
                    "gs://test_bucket/unprocessed_2024-12-24T12:00:00:000000_raw_file_tag.csv",
                ]
            ),
            sorted([file.uri() for file in successful_files]),
        )
        self.assertEqual(0, len(failed_files))

    def test_move_ingest_bucket_raw_files_to_deprecated_controller_at_bounds(
        self,
    ) -> None:

        controller = MoveIngestBucketRawFilesToDeprecatedController(
            source_ingest_bucket=GcsfsBucketPath("test_bucket"),
            destination_region_deprecated_storage_dir_path=GcsfsDirectoryPath(
                bucket_name="test_bucket_storage", relative_path="us_xx/deprecated/"
            ),
            start_datetime_inclusive=datetime.datetime(
                2024, 11, 12, 12, tzinfo=datetime.UTC
            ),
            end_datetime_exclusive=datetime.datetime(
                2024, 12, 12, 12, tzinfo=datetime.UTC
            ),
            file_tag_filters=["file_tag"],
            dry_run=True,
            log_output_path="log_output_path",
        )
        successful_files, failed_files = controller.run()

        self.assertEqual(1, len(successful_files))
        self.assertEqual(
            "gs://test_bucket/unprocessed_2024-11-12T12:00:00:000000_raw_file_tag.csv",
            successful_files[0].uri(),
        )
        self.assertEqual(0, len(failed_files))

        controller = MoveIngestBucketRawFilesToDeprecatedController(
            source_ingest_bucket=GcsfsBucketPath("test_bucket"),
            destination_region_deprecated_storage_dir_path=GcsfsDirectoryPath(
                bucket_name="test_bucket_storage", relative_path="us_xx/deprecated/"
            ),
            start_datetime_inclusive=datetime.datetime(
                2024, 11, 12, 12, tzinfo=datetime.UTC
            ),
            end_datetime_exclusive=datetime.datetime(
                2024, 12, 12, 12, 0, 0, 1, tzinfo=datetime.UTC
            ),
            file_tag_filters=["file_tag"],
            dry_run=True,
            log_output_path="log_output_path",
        )
        successful_files, failed_files = controller.run()

        self.assertEqual(2, len(successful_files))
        self.assertEqual(
            [
                "gs://test_bucket/unprocessed_2024-11-12T12:00:00:000000_raw_file_tag.csv",
                "gs://test_bucket/unprocessed_2024-12-12T12:00:00:000000_raw_file_tag.csv",
            ],
            sorted([file.uri() for file in successful_files]),
        )
        self.assertEqual(0, len(failed_files))

    def test_move_ingest_bucket_raw_files_to_deprecated_controller_with_date_filter(
        self,
    ) -> None:

        controller = MoveIngestBucketRawFilesToDeprecatedController(
            source_ingest_bucket=GcsfsBucketPath("test_bucket"),
            destination_region_deprecated_storage_dir_path=GcsfsDirectoryPath(
                bucket_name="test_bucket_storage", relative_path="us_xx/deprecated/"
            ),
            start_datetime_inclusive=datetime.datetime(
                2024, 12, 12, tzinfo=datetime.UTC
            ),
            end_datetime_exclusive=datetime.datetime(2024, 12, 13, tzinfo=datetime.UTC),
            file_tag_filters=["file_tag"],
            dry_run=True,
            log_output_path="log_output_path",
        )
        successful_files, failed_files = controller.run()

        self.assertEqual(1, len(successful_files))
        self.assertEqual(
            "gs://test_bucket/unprocessed_2024-12-12T12:00:00:000000_raw_file_tag.csv",
            successful_files[0].uri(),
        )
        self.assertEqual(0, len(failed_files))

    def test_invalid_bounds(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"start_datetime_inclusive and end_datetime_exclusive are both set to \[2024-12-12 00:00:00\+00:00\] which will exclude all files; please provide two distinct values",
        ):
            _ = MoveIngestBucketRawFilesToDeprecatedController(
                source_ingest_bucket=GcsfsBucketPath("test_bucket"),
                destination_region_deprecated_storage_dir_path=GcsfsDirectoryPath(
                    bucket_name="test_bucket_storage", relative_path="us_xx/deprecated/"
                ),
                start_datetime_inclusive=datetime.datetime(
                    2024, 12, 12, tzinfo=datetime.UTC
                ),
                end_datetime_exclusive=datetime.datetime(
                    2024, 12, 12, tzinfo=datetime.UTC
                ),
                file_tag_filters=None,
                dry_run=False,
                log_output_path="log_output_path",
            )

        with self.assertRaisesRegex(
            ValueError,
            r"start_datetime_inclusive \[2024-12-13 00:00:00\+00:00\] is after than end_datetime_exclusive \[2024-12-12 00:00:00\+00:00\]; please specify a start value that is before the provided end time",
        ):
            _ = MoveIngestBucketRawFilesToDeprecatedController(
                source_ingest_bucket=GcsfsBucketPath("test_bucket"),
                destination_region_deprecated_storage_dir_path=GcsfsDirectoryPath(
                    bucket_name="test_bucket_storage", relative_path="us_xx/deprecated/"
                ),
                start_datetime_inclusive=datetime.datetime(
                    2024, 12, 13, tzinfo=datetime.UTC
                ),
                end_datetime_exclusive=datetime.datetime(
                    2024, 12, 12, tzinfo=datetime.UTC
                ),
                file_tag_filters=None,
                dry_run=False,
                log_output_path="log_output_path",
            )

    def test_move_ingest_bucket_raw_files_to_deprecated_controller_error(
        self,
    ) -> None:
        def gsutil_mv(from_path: str, **_kwargs: Any) -> None:
            if (
                from_path
                == "gs://test_bucket/unprocessed_2024-11-12T12:00:00:000000_raw_file_tag.csv"
            ):
                raise ValueError("Error moving file")

        with patch(
            "recidiviz.tools.ingest.operations.helpers.move_ingest_bucket_raw_files_to_deprecated_controller.gsutil_mv",
            side_effect=gsutil_mv,
        ):
            controller = MoveIngestBucketRawFilesToDeprecatedController(
                source_ingest_bucket=GcsfsBucketPath("test_bucket"),
                destination_region_deprecated_storage_dir_path=GcsfsDirectoryPath(
                    bucket_name="test_bucket_storage", relative_path="us_xx/deprecated/"
                ),
                start_datetime_inclusive=None,
                end_datetime_exclusive=None,
                file_tag_filters=None,
                dry_run=False,
                log_output_path="log_output_path",
            )
            successful_files, failed_files = controller.run()

            self.assertEqual(5, len(successful_files))
            self.assertListEqual(
                sorted(
                    [
                        "gs://test_bucket/unprocessed_2024-12-12T12:00:00:000000_raw_file_tag.csv",
                        "gs://test_bucket/unprocessed_2024-12-24T12:00:00:000000_raw_file_tag.csv",
                        "gs://test_bucket/unprocessed_2024-11-12T12:00:00:000000_raw_file_tag_2.csv",
                        "gs://test_bucket/unprocessed_2024-12-12T12:00:00:000000_raw_file_tag_2.csv",
                        "gs://test_bucket/unprocessed_2024-12-24T12:00:00:000000_raw_file_tag_2.csv",
                    ]
                ),
                sorted([file.uri() for file in successful_files]),
            )
            self.assertEqual(1, len(failed_files))
            self.assertEqual(
                "gs://test_bucket/unprocessed_2024-11-12T12:00:00:000000_raw_file_tag.csv",
                failed_files[0].uri(),
            )

    def test_move_ingest_bucket_raw_files_to_deprecated_controller_no_matching_files(
        self,
    ) -> None:
        self.gsutil_ls_mock.return_value = []
        controller = MoveIngestBucketRawFilesToDeprecatedController(
            source_ingest_bucket=GcsfsBucketPath("test_bucket"),
            destination_region_deprecated_storage_dir_path=GcsfsDirectoryPath(
                bucket_name="test_bucket_storage", relative_path="us_xx/deprecated/"
            ),
            start_datetime_inclusive=None,
            end_datetime_exclusive=None,
            file_tag_filters=None,
            dry_run=True,
            log_output_path="log_output_path",
        )
        successful_files, failed_files = controller.run()

        self.assertEqual(0, len(successful_files))
        self.assertEqual(0, len(failed_files))
