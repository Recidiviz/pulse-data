# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Implements tests for the generate_demo_data script."""
import csv
import io
from datetime import date
from unittest import TestCase, mock
from unittest.mock import MagicMock, patch

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tools.pathways.generate_demo_data import (
    generate_row,
    generate_rows,
    main,
)


class TestGenerateDemoData(TestCase):
    """Implements tests for the generate_demo_data script."""

    def setUp(self) -> None:
        self.gcs_factory_patcher = mock.patch(
            "recidiviz.tools.pathways.generate_demo_data.GcsfsFactory.build"
        )
        self.fake_gcs = FakeGCSFileSystem()
        self.gcs_factory_patcher.start().return_value = self.fake_gcs

    def tearDown(self) -> None:
        self.gcs_factory_patcher.stop()

    def test_generate_row(self) -> None:
        row = generate_row(
            year=2020,
            month=1,
            state_code="US_XX",
            dimensions={"gender": "FEMALE"},
            db_columns=["supervision_start_date", "person_id"],
        )

        self.assertCountEqual(
            row.keys(),
            [
                "year",
                "month",
                "transition_date",
                "state_code",
                "gender",
                "supervision_start_date",
                "person_id",
            ],
        )

        # Assert on the contents for the non-random values
        self.assertEqual(
            row,
            row
            | {
                "year": 2020,
                "month": 1,
                "state_code": "US_XX",
                "gender": "FEMALE",
            },
        )

    @patch("recidiviz.tools.pathways.generate_demo_data.random.randint")
    def test_generate_rows(self, mock_randint: MagicMock) -> None:
        first_month_day = 2
        second_month_day = 3
        person_id = 4
        expected_random_values = [
            1,  # create one row for MALE
            first_month_day,  # day of month
            person_id,  # person ID
            1,  # create one row for FEMALE
            first_month_day,  # day of month
            person_id,  # person ID
            # Now we've hit a duplicate and need to try again
            second_month_day,  # try a different day of the month
            person_id,  # person ID
        ]
        mock_randint.side_effect = expected_random_values

        rows = generate_rows(
            year=2020,
            month=1,
            state_code="US_XX",
            columns=["gender", "person_id"],
        )

        self.assertEqual(mock_randint.call_count, len(expected_random_values))
        self.assertCountEqual(
            rows,
            [
                {
                    "transition_date": date(2020, 1, first_month_day),
                    "year": 2020,
                    "month": 1,
                    "person_id": str(person_id).zfill(10),
                    "gender": "MALE",
                    "state_code": "US_XX",
                },
                {
                    "transition_date": date(2020, 1, second_month_day),
                    "year": 2020,
                    "month": 1,
                    "person_id": str(person_id).zfill(10),
                    "gender": "FEMALE",
                    "state_code": "US_XX",
                },
            ],
        )

    @patch("recidiviz.tools.pathways.generate_demo_data.generate_demo_data")
    def test_upload_to_gcs(
        self,
        mock_generate_demo_data: MagicMock,
    ) -> None:
        expected_rows = [
            {
                "transition_date": "2020-01-01",
                "year": "2020",
                "month": "1",
                "time_period": "months_0_6",
                "person_id": "0001",
                "age_group": "<25",
                "gender": "FEMALE",
                "race": "WHITE",
                "judicial_district": "JUDICIAL_DISTRICT_1",
                "prior_length_of_incarceration": "6",
                "state_code": "US_XX",
            },
            {
                "transition_date": "2021-02-02",
                "year": "2021",
                "month": "2",
                "time_period": "months_7_12",
                "person_id": "0002",
                "age_group": "25-29",
                "gender": "MALE",
                "race": "BLACK",
                "judicial_district": "JUDICIAL_DISTRICT_2",
                "prior_length_of_incarceration": "12",
                "state_code": "US_XX",
            },
        ]
        mock_generate_demo_data.return_value = expected_rows

        main(
            state_codes=["US_XX"],
            views=["liberty_to_prison_transitions"],
            bucket="fake-bucket",
        )

        expected_file_path = GcsfsFilePath.from_absolute_path(
            "gs://fake-bucket/US_XX/liberty_to_prison_transitions.csv"
        )
        self.assertTrue(self.fake_gcs.exists(expected_file_path))

        file_contents = self.fake_gcs.download_as_string(expected_file_path)
        rows = list(
            csv.DictReader(
                io.StringIO(file_contents),
                fieldnames=[
                    "transition_date",
                    "year",
                    "month",
                    "time_period",
                    "person_id",
                    "age_group",
                    "gender",
                    "race",
                    "judicial_district",
                    "prior_length_of_incarceration",
                    "state_code",
                ],
            )
        )

        self.assertCountEqual(expected_rows, rows)
