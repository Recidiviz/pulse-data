#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests raw data diff query functionality using the BQ emulator.

To run, you must first run:
  pipenv run start-bq-emulator
"""
import datetime

import mock
from mock import patch

from recidiviz.tests.ingest.direct.views.raw_data_diff_query_test_case import (
    RawDataDiffEmulatorQueryTestCase,
)


@patch(
    "recidiviz.ingest.direct.views.raw_table_query_builder.raw_data_pruning_enabled_in_state_and_instance",
    mock.MagicMock(return_value=True),
)
@patch(
    "recidiviz.ingest.direct.views.raw_data_diff_query_builder.raw_data_pruning_enabled_in_state_and_instance",
    mock.MagicMock(return_value=True),
)
class RawDataDiffQueryTest(RawDataDiffEmulatorQueryTestCase):
    """Tests the raw data diff query functionality."""

    def setUp(self) -> None:
        self.raw_data_dag_patch = patch(
            "recidiviz.ingest.direct.views.raw_data_diff_query_builder.raw_data_pruning_enabled_in_state_and_instance",
        )
        self.raw_data_dag_mock = self.raw_data_dag_patch.start()
        self.raw_data_dag_mock.return_value = False
        super().setUp()

    def tearDown(self) -> None:
        self.raw_data_dag_patch.stop()
        return super().tearDown()

    def test_raw_data_diff_query_simple(self) -> None:
        self.run_diff_query_and_validate_output(
            file_tag="singlePrimaryKey",
            fixture_directory_name="singlePrimaryKey",
            new_file_id=2,
            new_update_datetime=datetime.datetime(2023, 5, 5),
        )

    def test_raw_data_diff_query_multiple_col_primary_key_historical(self) -> None:
        self.run_diff_query_and_validate_output(
            file_tag="multipleColPrimaryKeyHistorical",
            fixture_directory_name="multipleColPrimaryKeyHistoricalManyIsDeleted",
            new_file_id=5,
            new_update_datetime=datetime.datetime(2023, 5, 5),
        )

    # TODO(#28239) remove once raw data import is rolled out
    def test_raw_data_diff_query_multiple_col_primary_key_historical_select_except(
        self,
    ) -> None:
        self.raw_data_dag_mock.return_value = True
        self.run_diff_query_and_validate_output(
            file_tag="multipleColPrimaryKeyHistorical",
            fixture_directory_name="multipleColPrimaryKeyHistoricalManyIsDeletedSelectExcept",
            new_file_id=5,
            new_update_datetime=datetime.datetime(2023, 5, 5),
        )
