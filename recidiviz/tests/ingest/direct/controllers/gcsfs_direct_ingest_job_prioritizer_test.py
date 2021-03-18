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
"""Tests for the GcsfsDirectIngestJobPrioritizer."""
import datetime
import os
import unittest
from typing import List

from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    to_normalized_unprocessed_file_path,
    DirectIngestGCSFileSystem,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_job_prioritizer import (
    GcsfsDirectIngestJobPrioritizer,
)
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    filename_parts_from_path,
    GcsfsDirectIngestFileType,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest.direct import fixture_util


class TestGcsfsDirectIngestJobPrioritizerIngestViewFilter(unittest.TestCase):
    """Tests for the GcsfsDirectIngestJobPrioritizer."""

    _DAY_1_TIME_1 = datetime.datetime(
        year=2019,
        month=1,
        day=2,
        hour=3,
        minute=4,
        second=5,
        microsecond=6789,
        tzinfo=datetime.timezone.utc,
    )

    _DAY_1_TIME_2 = datetime.datetime(
        year=2019,
        month=1,
        day=2,
        hour=3,
        minute=4,
        second=5,
        microsecond=7789,
        tzinfo=datetime.timezone.utc,
    )

    _DAY_1_TIME_3 = datetime.datetime(
        year=2019,
        month=1,
        day=2,
        hour=10,
        minute=4,
        second=5,
        microsecond=678,
        tzinfo=datetime.timezone.utc,
    )

    _DAY_2_TIME_1 = datetime.datetime(
        year=2019,
        month=1,
        day=3,
        hour=3,
        minute=4,
        second=5,
        microsecond=6789,
        tzinfo=datetime.timezone.utc,
    )

    _DAY_1 = _DAY_1_TIME_1.date()
    _DAY_2 = _DAY_2_TIME_1.date()

    _INGEST_BUCKET_PATH = GcsfsDirectoryPath.from_absolute_path(
        "direct/regions/us_nd/fixtures"
    )

    def setUp(self) -> None:
        self.fs = DirectIngestGCSFileSystem(FakeGCSFileSystem())
        self.prioritizer = GcsfsDirectIngestJobPrioritizer(
            self.fs,
            self._INGEST_BUCKET_PATH,
            ["tagA", "tagB"],
        )

    FIXTURE_PATH_PREFIX = "direct/regions/us_nd/fixtures"

    def _normalized_path_for_filename(
        self, filename: str, file_type: GcsfsDirectIngestFileType, dt: datetime.datetime
    ) -> GcsfsFilePath:
        normalized_path = to_normalized_unprocessed_file_path(
            original_file_path=os.path.join(
                self._INGEST_BUCKET_PATH.abs_path(), filename
            ),
            file_type=file_type,
            dt=dt,
        )
        return GcsfsFilePath.from_absolute_path(normalized_path)

    def _process_jobs_for_paths_with_no_gaps_in_expected_order(
        self, paths: List[GcsfsFilePath]
    ) -> None:
        for path in paths:
            date_str = filename_parts_from_path(path).date_str
            next_job_args = self.prioritizer.get_next_job_args()
            self.assertIsNotNone(next_job_args)
            if next_job_args is None:
                # Make mypy happy
                self.fail()
            self.assertEqual(next_job_args.file_path, path)
            self.assertTrue(self.prioritizer.are_next_args_expected(next_job_args))

            self.assertTrue(self.prioritizer.are_more_jobs_expected_for_day(date_str))

            # ... job runs ...

            self.fs.mv_path_to_processed_path(path)

    def test_empty_fs(self) -> None:
        self.assertTrue(
            self.prioritizer.are_more_jobs_expected_for_day(
                self._DAY_1_TIME_1.date().isoformat()
            )
        )
        self.assertIsNone(self.prioritizer.get_next_job_args())

    def test_single_expected_file(self) -> None:
        path = self._normalized_path_for_filename(
            "tagA.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_1_TIME_1
        )

        fixture_util.add_direct_ingest_path(
            self.fs.gcs_file_system, path, has_fixture=False
        )

        self._process_jobs_for_paths_with_no_gaps_in_expected_order([path])

        self.assertIsNone(self.prioritizer.get_next_job_args())

        # We still expect a file for tagB
        self.assertTrue(
            self.prioritizer.are_more_jobs_expected_for_day(self._DAY_1.isoformat())
        )

    def test_multiple_files(self) -> None:

        paths = [
            self._normalized_path_for_filename(
                "tagA.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_1_TIME_1
            ),
            self._normalized_path_for_filename(
                "tagB.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_1_TIME_2
            ),
            # This file shouldn't ge tpicked up
            self._normalized_path_for_filename(
                "tagC.csv", GcsfsDirectIngestFileType.RAW_DATA, self._DAY_1_TIME_3
            ),
        ]

        for path in paths:
            fixture_util.add_direct_ingest_path(
                self.fs.gcs_file_system, path, has_fixture=False
            )

        # Exclude last raw file
        expected_processed_paths = paths[0:-1]

        self._process_jobs_for_paths_with_no_gaps_in_expected_order(
            expected_processed_paths
        )

        self.assertIsNone(self.prioritizer.get_next_job_args())
        self.assertFalse(
            self.prioritizer.are_more_jobs_expected_for_day(self._DAY_1.isoformat())
        )

    def test_unexpected_file(self) -> None:
        # Only file is out of order
        path = self._normalized_path_for_filename(
            "tagB.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_1_TIME_1
        )

        fixture_util.add_direct_ingest_path(
            self.fs.gcs_file_system, path, has_fixture=False
        )

        self.assertTrue(
            self.prioritizer.are_more_jobs_expected_for_day(self._DAY_1.isoformat())
        )

        next_job_args = self.prioritizer.get_next_job_args()
        if next_job_args is None:
            self.fail("Next job args unexpectedly None")
        self.assertEqual(next_job_args.file_path, path)
        self.assertFalse(self.prioritizer.are_next_args_expected(next_job_args))

        # ... job runs eventually even though unexpected...

        self.fs.mv_path_to_processed_path(path)

        self.assertIsNone(self.prioritizer.get_next_job_args())

        # We still expect a file for tagA
        self.assertTrue(
            self.prioritizer.are_more_jobs_expected_for_day(self._DAY_1.isoformat())
        )

    def test_files_on_multiple_days(self) -> None:
        paths = [
            self._normalized_path_for_filename(
                "tagA.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_1_TIME_1
            ),
            self._normalized_path_for_filename(
                "tagB.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_1_TIME_2
            ),
            self._normalized_path_for_filename(
                "tagA.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_2_TIME_1
            ),
            # This file shouldn't ge tpicked up
            self._normalized_path_for_filename(
                "tagC.csv", GcsfsDirectIngestFileType.RAW_DATA, self._DAY_1_TIME_3
            ),
        ]
        for path in paths:
            fixture_util.add_direct_ingest_path(
                self.fs.gcs_file_system, path, has_fixture=False
            )

        # Exclude last raw file
        expected_processed_paths = paths[0:-1]

        self._process_jobs_for_paths_with_no_gaps_in_expected_order(
            expected_processed_paths
        )

        self.assertIsNone(self.prioritizer.get_next_job_args())
        self.assertFalse(
            self.prioritizer.are_more_jobs_expected_for_day(self._DAY_1.isoformat())
        )
        self.assertTrue(
            self.prioritizer.are_more_jobs_expected_for_day(self._DAY_2.isoformat())
        )

    def test_files_on_multiple_days_with_gap(self) -> None:
        """Runs a test where there are files on multiple days and there is a gap
        in the expected files for the first day.
        """
        paths = [
            self._normalized_path_for_filename(
                "tagB.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_1_TIME_2
            ),
            self._normalized_path_for_filename(
                "tagA.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_2_TIME_1
            ),
        ]
        for path in paths:
            fixture_util.add_direct_ingest_path(
                self.fs.gcs_file_system, path, has_fixture=False
            )

        for i, path in enumerate(paths):
            date_str = filename_parts_from_path(path).date_str
            next_job_args = self.prioritizer.get_next_job_args()
            if next_job_args is None:
                self.fail("Next job args unexpectedly None")
            self.assertEqual(next_job_args.file_path, path)

            are_args_expected = self.prioritizer.are_next_args_expected(next_job_args)
            if i == 0:
                self.assertFalse(are_args_expected)
            else:
                self.assertTrue(are_args_expected)

            self.assertTrue(self.prioritizer.are_more_jobs_expected_for_day(date_str))

            # ... job runs ...

            self.fs.mv_path_to_processed_path(path)

        self.assertIsNone(self.prioritizer.get_next_job_args())
        self.assertTrue(
            self.prioritizer.are_more_jobs_expected_for_day(self._DAY_1.isoformat())
        )
        self.assertTrue(
            self.prioritizer.are_more_jobs_expected_for_day(self._DAY_2.isoformat())
        )

    def test_multiple_files_same_tag(self) -> None:
        paths = [
            self._normalized_path_for_filename(
                "tagA.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_1_TIME_1
            ),
            self._normalized_path_for_filename(
                "tagA.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_1_TIME_2
            ),
            self._normalized_path_for_filename(
                "tagB.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_1_TIME_3
            ),
        ]
        for path in paths:
            fixture_util.add_direct_ingest_path(
                self.fs.gcs_file_system, path, has_fixture=False
            )

        self._process_jobs_for_paths_with_no_gaps_in_expected_order(paths)

        self.assertIsNone(self.prioritizer.get_next_job_args())
        self.assertFalse(
            self.prioritizer.are_more_jobs_expected_for_day(self._DAY_1.isoformat())
        )

    def test_multiple_files_times_out_of_order(self) -> None:
        """Runs a test where there are no gaps but the files have been added
        (i.e. have creation times) out of order.
        """
        paths = [
            self._normalized_path_for_filename(
                "tagA.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_1_TIME_2
            ),
            self._normalized_path_for_filename(
                "tagB.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_1_TIME_1
            ),
            self._normalized_path_for_filename(
                "tagB.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_1_TIME_3
            ),
        ]
        for path in paths:
            fixture_util.add_direct_ingest_path(
                self.fs.gcs_file_system, path, has_fixture=False
            )

        for i, path in enumerate(paths):
            date_str = filename_parts_from_path(path).date_str
            next_job_args = self.prioritizer.get_next_job_args()
            if next_job_args is None:
                self.fail("Next job args unexpectedly None")
            self.assertEqual(next_job_args.file_path, path)
            self.assertTrue(self.prioritizer.are_next_args_expected(next_job_args))

            are_more_jobs_expected = self.prioritizer.are_more_jobs_expected_for_day(
                date_str
            )
            if i == 2:
                self.assertFalse(are_more_jobs_expected)
            else:
                self.assertTrue(are_more_jobs_expected)

            # ... job runs ...

            self.fs.mv_path_to_processed_path(path)

        self.assertIsNone(self.prioritizer.get_next_job_args())
        self.assertFalse(
            self.prioritizer.are_more_jobs_expected_for_day(self._DAY_1.isoformat())
        )

    def test_run_multiple_copies_of_same_tag(self) -> None:
        paths = [
            self._normalized_path_for_filename(
                "tagA.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_1_TIME_2
            ),
            self._normalized_path_for_filename(
                "tagA_2.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_1_TIME_1
            ),
            self._normalized_path_for_filename(
                "tagB.csv", GcsfsDirectIngestFileType.INGEST_VIEW, self._DAY_1_TIME_3
            ),
        ]
        for path in paths:
            fixture_util.add_direct_ingest_path(
                self.fs.gcs_file_system, path, has_fixture=False
            )

        self._process_jobs_for_paths_with_no_gaps_in_expected_order(paths)

        self.assertIsNone(self.prioritizer.get_next_job_args())
        self.assertFalse(
            self.prioritizer.are_more_jobs_expected_for_day(self._DAY_1.isoformat())
        )
