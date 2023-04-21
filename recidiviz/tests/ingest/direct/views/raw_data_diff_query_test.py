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
        super().setUp()

    def test_raw_data_diff_query_simple(self) -> None:
        self.run_diff_query_and_validate_output(
            file_tag="singlePrimaryKey", fixture_directory_name="singlePrimaryKey"
        )

    def test_raw_data_diff_query_multiple_col_primary_key_historical(self) -> None:
        self.run_diff_query_and_validate_output(
            file_tag="multipleColPrimaryKeyHistorical",
            fixture_directory_name="multipleColPrimaryKeyHistoricalManyIsDeleted",
        )
