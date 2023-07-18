# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for simulation power calculation methods"""

import datetime
import unittest
from unittest.mock import MagicMock

import pandas as pd

from recidiviz.tools.analyst.power_calculations.simulation_power_calculations import (
    detect_date_granularity,
    get_coefficient_and_ci,
    get_date_sequence,
)


class TestSimulationPowerCalc(unittest.TestCase):
    "Tests for simulation-based power calc methods"

    def test_detect_date_granularity(self) -> None:
        "Verify that detect_date_granularity() works as expected"

        # weeks
        df = pd.DataFrame(
            {
                "unit_of_analysis": ["a"] * 2 + ["b"] * 2,
                "start_date": [
                    datetime.date(2023, 1, 1),
                    datetime.date(2023, 1, 8),
                ]
                * 2,
            }
        )
        granularity, days_between_periods = detect_date_granularity(
            df, "unit_of_analysis", "start_date"
        )

        self.assertEqual(granularity, "week")
        self.assertEqual(days_between_periods, 7)

        # months
        df["start_date"] = [
            datetime.date(2023, 1, 1),
            datetime.date(2023, 2, 1),
        ] * 2
        granularity, days_between_periods = detect_date_granularity(
            df, "unit_of_analysis", "start_date"
        )

        self.assertEqual(granularity, "month")
        self.assertEqual(days_between_periods, 31)

        # quarters
        df["start_date"] = [
            datetime.date(2023, 1, 1),
            datetime.date(2023, 4, 1),
        ] * 2
        granularity, days_between_periods = detect_date_granularity(
            df, "unit_of_analysis", "start_date"
        )

        self.assertEqual(granularity, "quarter")
        self.assertEqual(days_between_periods, 90)

        # years
        df["start_date"] = [
            datetime.date(2023, 1, 1),
            datetime.date(2024, 1, 1),
        ] * 2
        granularity, days_between_periods = detect_date_granularity(
            df, "unit_of_analysis", "start_date"
        )

        self.assertEqual(granularity, "year")
        self.assertEqual(days_between_periods, 365)

        # days
        df["start_date"] = [
            datetime.date(2023, 1, 1),
            datetime.date(2023, 1, 3),
        ] * 2
        granularity, days_between_periods = detect_date_granularity(
            df, "unit_of_analysis", "start_date"
        )

        self.assertEqual(granularity, "2 day period")
        self.assertEqual(days_between_periods, 2)

        # mixed - take average number of days within groups
        df["start_date"] = [
            datetime.date(2023, 1, 1),
            datetime.date(2023, 1, 4),
            datetime.date(2023, 1, 8),
            datetime.date(2023, 1, 9),
        ]
        granularity, days_between_periods = detect_date_granularity(
            df, "unit_of_analysis", "start_date"
        )

        self.assertEqual(granularity, "2 day period")
        self.assertEqual(days_between_periods, 2)

    def test_get_date_sequence(self) -> None:
        "Verify that get_date_sequence() works as expected"

        # weeks
        date_sequence = get_date_sequence(
            datetime.datetime(2023, 1, 1), "week", periods=3
        )
        self.assertEqual(
            date_sequence,
            [
                datetime.datetime(2023, 1, 8),
                datetime.datetime(2023, 1, 15),
                datetime.datetime(2023, 1, 22),
            ],
        )

        # months
        date_sequence = get_date_sequence(
            datetime.datetime(2023, 1, 1), "month", periods=3
        )
        self.assertEqual(
            date_sequence,
            [
                datetime.datetime(2023, 2, 1),
                datetime.datetime(2023, 3, 1),
                datetime.datetime(2023, 4, 1),
            ],
        )

        # quarters
        date_sequence = get_date_sequence(
            datetime.datetime(2023, 1, 1), "quarter", periods=3
        )
        self.assertEqual(
            date_sequence,
            [
                datetime.datetime(2023, 4, 1),
                datetime.datetime(2023, 7, 1),
                datetime.datetime(2023, 10, 1),
            ],
        )

        # years
        date_sequence = get_date_sequence(
            datetime.datetime(2023, 1, 1), "year", periods=3
        )
        self.assertEqual(
            date_sequence,
            [
                datetime.datetime(2024, 1, 1),
                datetime.datetime(2025, 1, 1),
                datetime.datetime(2026, 1, 1),
            ],
        )

        # days
        date_sequence = get_date_sequence(
            datetime.datetime(2023, 1, 1),
            "2 day period",
            days_between_periods=2,
            periods=3,
        )
        self.assertEqual(
            date_sequence,
            [
                datetime.datetime(2023, 1, 3),
                datetime.datetime(2023, 1, 5),
                datetime.datetime(2023, 1, 7),
            ],
        )

        # error thrown if days_between_periods not provided and granularity not known
        with self.assertRaises(ValueError):
            get_date_sequence(datetime.datetime(2023, 1, 1), "2 day period", periods=3)

    # for now just test that get_coefficient_and_ci() returns the correct values
    def test_get_coefficient_and_ci(self) -> None:
        "Verify that get_coefficient_and_ci() works as expected"

        # Mock a PanelEffectsResults object
        res = MagicMock()
        # res.params[variable] returns a coefficient
        # res.conf_int(level=confidence_level).loc[variable].lower returns lower bound
        # res.conf_int(level=confidence_level).loc[variable].upper returns upper bound
        res.params = {"var1": 1, "var2": 2}
        res.conf_int = MagicMock()
        res.conf_int.return_value = pd.DataFrame(
            {"lower": [0, 1], "upper": [2, 3]}, index=["var1", "var2"]
        )

        coefficient, lower, upper = get_coefficient_and_ci(res, "var1")

        self.assertEqual(coefficient, 1)
        self.assertEqual(lower, 0)
        self.assertEqual(upper, 2)
