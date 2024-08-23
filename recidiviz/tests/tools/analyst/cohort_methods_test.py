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
"""Tests for cohort methods"""

import datetime
import unittest
import warnings

import pandas as pd

from recidiviz.tools.analyst.cohort_methods import (
    gen_aggregated_cohort_event_df,
    gen_cohort_status_df,
)


class TestGenCohortStatusDf(unittest.TestCase):
    "Tests for gen_cohort_status_df"

    def test_full_observability_flag(self) -> None:
        """
        If there are cohort starts that do not have full observability as of
        `last_day_of_data`, and if `full_observability` is false, the function will
        throw an error.
        """
        cohort_df = pd.DataFrame(
            {
                "person_id": [1, 2],
                "cohort_date": pd.to_datetime(["2020-01-01", "2020-01-01"]),
            }
        )
        status_df = pd.DataFrame(
            {
                "person_id": [1, 2],
                "start_date": pd.to_datetime(["2020-02-01", "2020-03-01"]),
                "end_date_exclusive": pd.to_datetime(["2020-02-02", "2020-03-02"]),
                "compartment_level_0": ["INCARCERATION", "INCARCERATION"],
            }
        )
        # individuals start on 2020-01-01, full observability occurs on 2020-03-01
        with self.assertWarns(UserWarning):
            gen_cohort_status_df(
                cohort_df,
                status_df,
                status_start_date_field="start_date",
                status_end_date_field="end_date_exclusive",
                cohort_date_field="cohort_date",
                time_index=[0, 1, 2],
                last_day_of_data=datetime.datetime(2020, 2, 28),
                full_observability=False,
            )

        # setting suppress_full_observability_warning suppresses the warning
        with warnings.catch_warnings(record=True) as warnings_log:
            gen_cohort_status_df(
                cohort_df,
                status_df,
                status_start_date_field="start_date",
                status_end_date_field="end_date_exclusive",
                cohort_date_field="cohort_date",
                time_index=[0, 1, 2],
                last_day_of_data=datetime.datetime(2020, 2, 28),
                full_observability=False,
                suppress_full_observability_warning=True,
            )
            # no warnings
            self.assertEqual(len(warnings_log), 0)

        # no warning raised on 3/1 with full_observability = True
        with warnings.catch_warnings(record=True) as warnings_log:
            gen_cohort_status_df(
                cohort_df,
                status_df,
                status_start_date_field="start_date",
                status_end_date_field="end_date_exclusive",
                cohort_date_field="cohort_date",
                time_index=[0, 1, 2],
                last_day_of_data=datetime.datetime(2020, 3, 1),
                full_observability=True,
            )
            # no warnings
            self.assertEqual(len(warnings_log), 0)

        # no warning raised on 3/1 with full_observability = False
        with warnings.catch_warnings(record=True) as warnings_log:
            gen_cohort_status_df(
                cohort_df,
                status_df,
                status_start_date_field="start_date",
                status_end_date_field="end_date_exclusive",
                cohort_date_field="cohort_date",
                time_index=[0, 1, 2],
                last_day_of_data=datetime.datetime(2020, 3, 1),
                full_observability=False,
            )
            # no warnings
            self.assertEqual(len(warnings_log), 0)

        # error raised on 2/28 if full_observability = True (because drops all rows)
        with self.assertRaises(ValueError):
            with self.assertWarns(UserWarning):
                gen_cohort_status_df(
                    cohort_df,
                    status_df,
                    status_start_date_field="start_date",
                    status_end_date_field="end_date_exclusive",
                    cohort_date_field="cohort_date",
                    time_index=[0, 1, 2],
                    last_day_of_data=datetime.datetime(2020, 2, 28),
                    full_observability=True,
                )

        # case when one row has full observability and the other doesn't, with full_observability = True
        cohort_df = pd.DataFrame(
            {
                "person_id": [1, 2],
                "cohort_date": pd.to_datetime(["2020-01-01", "2020-02-01"]),
            }
        )
        with self.assertWarns(UserWarning):
            output_df = gen_cohort_status_df(
                cohort_df,
                status_df,
                status_start_date_field="start_date",
                status_end_date_field="end_date_exclusive",
                cohort_date_field="cohort_date",
                time_index=[0, 1, 2],
                last_day_of_data=datetime.datetime(2020, 3, 1),
                full_observability=True,
            )

        # first cohort start has full observability
        expected_df = pd.Series(
            index=pd.MultiIndex.from_arrays(
                [
                    [1, 1, 1],
                    pd.to_datetime(["2020-01-01", "2020-01-01", "2020-01-01"]).date,
                    [0, 1, 2],
                    pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01"]).date,
                ],
                names=["person_id", "cohort_date", "cohort_months", "cohort_eval_date"],
            ),
            data=[0, 0, 1],
            name="days_at_status",
        )
        pd.testing.assert_series_equal(output_df, expected_df)


class TestGenAggregatedCohortEventDf(unittest.TestCase):
    "Tests for gen_aggregated_cohort_event_df"

    def test_full_observability_flag(self) -> None:
        """
        If there are cohort starts that do not have full observability as of
        `last_day_of_data`, the function will issue a warning.
        """
        df = pd.DataFrame(
            {
                "person_id": [1, 2],
                "cohort_date": pd.to_datetime(["2020-01-01", "2020-01-01"]),
                "event_date": pd.to_datetime(["2020-02-01", "2020-03-01"]),
            }
        )

        # individuals start on 2020-01-01, full observability occurs on 2020-03-01
        with self.assertWarns(UserWarning):
            gen_aggregated_cohort_event_df(
                df,
                event_date_field="event_date",
                cohort_date_field="cohort_date",
                time_index=[0, 1, 2],
                last_day_of_data=datetime.datetime(2020, 2, 28),
                full_observability=False,
            )

        # setting suppress_full_observability_warning suppresses the warning
        with warnings.catch_warnings(record=True) as warnings_log:
            gen_aggregated_cohort_event_df(
                df,
                event_date_field="event_date",
                cohort_date_field="cohort_date",
                time_index=[0, 1, 2],
                last_day_of_data=datetime.datetime(2020, 2, 28),
                full_observability=False,
                suppress_full_observability_warning=True,
            )
            # no warnings
            self.assertEqual(len(warnings_log), 0)

        # no warning raised on 3/1 with full_observability = True
        with warnings.catch_warnings(record=True) as warnings_log:
            gen_aggregated_cohort_event_df(
                df,
                event_date_field="event_date",
                cohort_date_field="cohort_date",
                time_index=[0, 1, 2],
                last_day_of_data=datetime.datetime(2020, 3, 1),
                full_observability=True,
            )
            # no warnings
            self.assertEqual(len(warnings_log), 0)

        # no warning raised on 3/1 with full_observability = False
        with warnings.catch_warnings(record=True) as warnings_log:
            gen_aggregated_cohort_event_df(
                df,
                event_date_field="event_date",
                cohort_date_field="cohort_date",
                time_index=[0, 1, 2],
                last_day_of_data=datetime.datetime(2020, 3, 1),
                full_observability=False,
            )
            # no warnings
            self.assertEqual(len(warnings_log), 0)

        # error raised on 2/28 if full_observability = True (because drops all rows)
        with self.assertRaises(ValueError):
            with self.assertWarns(UserWarning):
                gen_aggregated_cohort_event_df(
                    df,
                    event_date_field="event_date",
                    cohort_date_field="cohort_date",
                    time_index=[0, 1, 2],
                    last_day_of_data=datetime.datetime(2020, 2, 28),
                    full_observability=True,
                )

        # case when one row has full observability and the other doesn't, with full_observability = True
        df = pd.DataFrame(
            {
                "person_id": [1, 2],
                "cohort_date": pd.to_datetime(["2020-01-01", "2020-02-01"]),
                "event_date": pd.to_datetime(["2020-02-01", "2020-03-01"]),
            }
        )
        with self.assertWarns(UserWarning):
            output_df = gen_aggregated_cohort_event_df(
                df,
                event_date_field="event_date",
                cohort_date_field="cohort_date",
                time_index=[0, 1, 2],
                last_day_of_data=datetime.datetime(2020, 3, 1),
                full_observability=True,
            )

        # first cohort start has full observability
        expected_df = pd.DataFrame(
            index=pd.Index([0, 1, 2], name="cohort_months"),
            data={
                "event_count": [0, 0, 1],
                "cohort_size": [1] * 3,
                "event_rate": [0.0, 0.0, 1.0],
            },
        )
        pd.testing.assert_frame_equal(output_df, expected_df)
