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
"""Tests for the ExtractAndMergeJobPrioritizer."""
import datetime
import unittest
from typing import List
from unittest.mock import create_autospec

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.ingest.direct.controllers.extract_and_merge_job_prioritizer import (
    ExtractAndMergeJobPrioritizerImpl,
)
from recidiviz.ingest.direct.ingest_view_materialization.instance_ingest_view_contents import (
    ResultsBatchInfo,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tests.ingest.direct.fakes.fake_instance_ingest_view_contents import (
    FakeInstanceIngestViewContents,
)


class ExtractAndMergeJobPrioritizerTest(unittest.TestCase):
    """Tests for the ExtractAndMergeJobPrioritizer."""

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

    _DAY_2_TIME_2 = datetime.datetime(
        year=2019,
        month=1,
        day=3,
        hour=4,
        minute=5,
        second=6,
        microsecond=7890,
        tzinfo=datetime.timezone.utc,
    )

    _DAY_1 = _DAY_1_TIME_1.date()
    _DAY_2 = _DAY_2_TIME_1.date()

    def setUp(self) -> None:
        self.fake_ingest_view_contents = FakeInstanceIngestViewContents(
            big_query_client=create_autospec(BigQueryClient),
            region_code="us_xx",
            ingest_instance=DirectIngestInstance.PRIMARY,
            dataset_prefix=None,
        )
        self.prioritizer = ExtractAndMergeJobPrioritizerImpl(
            self.fake_ingest_view_contents,
            ["tagA", "tagB"],
        )

    def _process_jobs_in_order(
        self, expected_batch_order: List[ResultsBatchInfo]
    ) -> None:
        for batch_info in expected_batch_order:
            next_job_args = self.prioritizer.get_next_job_args()
            self.assertIsNotNone(next_job_args)
            if next_job_args is None:
                # Make mypy happy
                self.fail()
            self.assertEqual(
                next_job_args.ingest_view_name, batch_info.ingest_view_name
            )
            self.assertEqual(
                next_job_args.upper_bound_datetime_inclusive,
                batch_info.upper_bound_datetime_inclusive,
            )
            self.assertEqual(
                next_job_args.batch_number,
                batch_info.batch_number,
            )

            # ... job runs ...

            self.prioritizer.ingest_view_contents.mark_rows_as_processed(
                ingest_view_name=next_job_args.ingest_view_name,
                upper_bound_datetime_inclusive=next_job_args.upper_bound_datetime_inclusive,
                batch_number=next_job_args.batch_number,
            )

    def test_empty_results(self) -> None:
        self.assertIsNone(self.prioritizer.get_next_job_args())
        self._process_jobs_in_order([])
        self.assertIsNone(self.prioritizer.get_next_job_args())

    def test_single_expected_job(self) -> None:
        batch_info = ResultsBatchInfo(
            ingest_view_name="tagA",
            upper_bound_datetime_inclusive=self._DAY_1_TIME_1,
            batch_number=0,
        )
        self.fake_ingest_view_contents.test_add_batch(batch_info)

        self._process_jobs_in_order([batch_info])
        self.assertIsNone(self.prioritizer.get_next_job_args())

    def test_one_job_for_multiple_views(self) -> None:
        batches_to_add = [
            ResultsBatchInfo(
                ingest_view_name="tagA",
                upper_bound_datetime_inclusive=self._DAY_1_TIME_2,
                batch_number=0,
            ),
            ResultsBatchInfo(
                ingest_view_name="tagB",
                upper_bound_datetime_inclusive=self._DAY_1_TIME_1,
                batch_number=0,
            ),
            # This view shouldn't get picked up
            ResultsBatchInfo(
                ingest_view_name="tagC",
                upper_bound_datetime_inclusive=self._DAY_1_TIME_3,
                batch_number=0,
            ),
        ]
        for batch in reversed(batches_to_add):
            self.fake_ingest_view_contents.test_add_batch(batch)

        # Exclude last raw file
        expected_batch_order = batches_to_add[:-1]

        self._process_jobs_in_order(expected_batch_order)

        self.assertIsNone(self.prioritizer.get_next_job_args())

    def test_missing_job_for_one_tag(self) -> None:
        batches_to_add = [
            # No batch for tagA
            ResultsBatchInfo(
                ingest_view_name="tagB",
                upper_bound_datetime_inclusive=self._DAY_1_TIME_1,
                batch_number=0,
            ),
            # This view shouldn't get picked up
            ResultsBatchInfo(
                ingest_view_name="tagC",
                upper_bound_datetime_inclusive=self._DAY_1_TIME_3,
                batch_number=0,
            ),
        ]
        for batch in reversed(batches_to_add):
            self.fake_ingest_view_contents.test_add_batch(batch)

        next_job_args = self.prioritizer.get_next_job_args()
        if next_job_args is None:
            self.fail("Next job args unexpectedly None")
        self.assertEqual(next_job_args.ingest_view_name, "tagB")

        # Exclude last raw file
        expected_batch_order = batches_to_add[:-1]

        self._process_jobs_in_order(expected_batch_order)

        self.assertIsNone(self.prioritizer.get_next_job_args())

    def test_jobs_on_multiple_days(self) -> None:
        batches_to_add = [
            ResultsBatchInfo(
                ingest_view_name="tagA",
                upper_bound_datetime_inclusive=self._DAY_1_TIME_2,
                batch_number=0,
            ),
            ResultsBatchInfo(
                ingest_view_name="tagB",
                upper_bound_datetime_inclusive=self._DAY_1_TIME_1,
                batch_number=0,
            ),
            ResultsBatchInfo(
                ingest_view_name="tagA",
                upper_bound_datetime_inclusive=self._DAY_2_TIME_1,
                batch_number=0,
            ),
            ResultsBatchInfo(
                ingest_view_name="tagB",
                upper_bound_datetime_inclusive=self._DAY_2_TIME_2,
                batch_number=0,
            ),
        ]
        for batch in reversed(batches_to_add):
            self.fake_ingest_view_contents.test_add_batch(batch)

        expected_batch_order = batches_to_add

        self._process_jobs_in_order(expected_batch_order)

        self.assertIsNone(self.prioritizer.get_next_job_args())

    def test_jobs_multiple_days_different_views_each_day(self) -> None:
        batches_to_add = [
            ResultsBatchInfo(
                ingest_view_name="tagB",
                upper_bound_datetime_inclusive=self._DAY_1_TIME_1,
                batch_number=0,
            ),
            ResultsBatchInfo(
                ingest_view_name="tagA",
                upper_bound_datetime_inclusive=self._DAY_2_TIME_1,
                batch_number=0,
            ),
        ]
        for batch in reversed(batches_to_add):
            self.fake_ingest_view_contents.test_add_batch(batch)

        expected_batch_order = batches_to_add

        self._process_jobs_in_order(expected_batch_order)

        self.assertIsNone(self.prioritizer.get_next_job_args())

    def test_multiple_datetimes_on_same_day_for_tag(self) -> None:
        batches_to_add = [
            ResultsBatchInfo(
                ingest_view_name="tagA",
                upper_bound_datetime_inclusive=self._DAY_1_TIME_1,
                batch_number=0,
            ),
            ResultsBatchInfo(
                ingest_view_name="tagA",
                upper_bound_datetime_inclusive=self._DAY_1_TIME_2,
                batch_number=0,
            ),
            ResultsBatchInfo(
                ingest_view_name="tagB",
                upper_bound_datetime_inclusive=self._DAY_1_TIME_3,
                batch_number=0,
            ),
            ResultsBatchInfo(
                ingest_view_name="tagA",
                upper_bound_datetime_inclusive=self._DAY_2_TIME_1,
                batch_number=0,
            ),
        ]

        for batch in reversed(batches_to_add):
            self.fake_ingest_view_contents.test_add_batch(batch)

        self._process_jobs_in_order(expected_batch_order=batches_to_add)

        self.assertIsNone(self.prioritizer.get_next_job_args())

    def test_multiple_datetimes_on_same_day_for_tag_times_out_of_order(self) -> None:
        batches_to_add = [
            ResultsBatchInfo(
                ingest_view_name="tagA",
                upper_bound_datetime_inclusive=self._DAY_1_TIME_2,
                batch_number=0,
            ),
            ResultsBatchInfo(
                ingest_view_name="tagB",
                upper_bound_datetime_inclusive=self._DAY_1_TIME_1,
                batch_number=0,
            ),
            ResultsBatchInfo(
                ingest_view_name="tagB",
                upper_bound_datetime_inclusive=self._DAY_1_TIME_3,
                batch_number=0,
            ),
            ResultsBatchInfo(
                ingest_view_name="tagA",
                upper_bound_datetime_inclusive=self._DAY_2_TIME_1,
                batch_number=0,
            ),
        ]

        for batch in reversed(batches_to_add):
            self.fake_ingest_view_contents.test_add_batch(batch)

        self._process_jobs_in_order(expected_batch_order=batches_to_add)

        self.assertIsNone(self.prioritizer.get_next_job_args())

    def test_view_has_multiple_batches(self) -> None:
        batches_to_add = [
            ResultsBatchInfo(
                ingest_view_name="tagA",
                upper_bound_datetime_inclusive=self._DAY_1_TIME_2,
                batch_number=0,
            ),
            ResultsBatchInfo(
                ingest_view_name="tagA",
                upper_bound_datetime_inclusive=self._DAY_1_TIME_2,
                batch_number=1,
            ),
            ResultsBatchInfo(
                ingest_view_name="tagB",
                upper_bound_datetime_inclusive=self._DAY_1_TIME_1,
                batch_number=0,
            ),
            ResultsBatchInfo(
                ingest_view_name="tagA",
                upper_bound_datetime_inclusive=self._DAY_2_TIME_1,
                batch_number=0,
            ),
        ]

        for batch in reversed(batches_to_add):
            self.fake_ingest_view_contents.test_add_batch(batch)

        self._process_jobs_in_order(expected_batch_order=batches_to_add)

        self.assertIsNone(self.prioritizer.get_next_job_args())
