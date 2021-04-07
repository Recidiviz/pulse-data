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
"""Tests for aggregate_ingest_utils."""
import datetime
from unittest import TestCase

import pandas as pd
from pandas.testing import assert_frame_equal

from recidiviz.ingest.aggregate import aggregate_ingest_utils


class TestAggregateIngestUtils(TestCase):
    """Tests for aggregate_ingest_utils."""

    def testRenameWithoutRegex(self) -> None:
        # Arrange
        subject = pd.DataFrame(
            {
                "County": ["Anderson", "Andrews", "Angelina"],
                "PRETRIAL": [90, 20, 105],
                "CON. Felons": [2, 11, 26],
            }
        )

        rename_dict = {
            "County": "facility_name",
            "PRETRIAL": "pretrial_adp",
            "CON. Felons": "convicted_adp",
        }

        # Act
        result = aggregate_ingest_utils.rename_columns_and_select(subject, rename_dict)

        # Assert
        expected_result = pd.DataFrame(
            {
                "facility_name": ["Anderson", "Andrews", "Angelina"],
                "pretrial_adp": [90, 20, 105],
                "convicted_adp": [2, 11, 26],
            }
        )

        assert_frame_equal(result, expected_result)

    def testRenameWithRegex(self) -> None:
        # Arrange
        subject = pd.DataFrame(
            {
                "County": ["Anderson", "Andrews", "Angelina"],
                "PRETRIAL": [90, 20, 105],
                "CON. Felons": [2, 11, 26],
            }
        )

        rename_dict = {
            r".*Cou.*": "facility_name",
            r"PRETRIAL": "pretrial_adp",
            r"CON\. Felons": "convicted_adp",
        }

        # Act
        result = aggregate_ingest_utils.rename_columns_and_select(
            subject, rename_dict, use_regex=True
        )

        # Assert
        expected_result = pd.DataFrame(
            {
                "facility_name": ["Anderson", "Andrews", "Angelina"],
                "pretrial_adp": [90, 20, 105],
                "convicted_adp": [2, 11, 26],
            }
        )

        assert_frame_equal(result, expected_result)

    def test_subtractMonth(self) -> None:
        result = aggregate_ingest_utils.subtract_month(
            datetime.date(year=2019, month=3, day=15)
        )
        assert result == datetime.date(year=2019, month=2, day=15)

    def test_subtractMonth_truncatesDay(self) -> None:
        result = aggregate_ingest_utils.subtract_month(
            datetime.date(year=2019, month=3, day=29)
        )
        assert result == datetime.date(year=2019, month=2, day=28)

    def test_subtractMonth_priorYear(self) -> None:
        result = aggregate_ingest_utils.subtract_month(
            datetime.date(year=2019, month=1, day=29)
        )
        assert result == datetime.date(year=2018, month=12, day=29)

    def test_subtractMonth_firstDay(self) -> None:
        result = aggregate_ingest_utils.subtract_month(
            datetime.date(year=2019, month=3, day=1)
        )
        assert result == datetime.date(year=2019, month=2, day=1)
