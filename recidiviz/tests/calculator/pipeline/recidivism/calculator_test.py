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

# pylint: disable=unused-import,wrong-import-order

"""Tests for recidivism/calculator.py."""
import unittest
from collections import defaultdict
from datetime import date
from typing import Dict, List

from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

from recidiviz.calculator.pipeline.recidivism import calculator
from recidiviz.calculator.pipeline.recidivism.calculator import FOLLOW_UP_PERIODS
from recidiviz.calculator.pipeline.recidivism.release_event import (
    ReleaseEvent,
    RecidivismReleaseEvent,
    NonRecidivismReleaseEvent,
)
from recidiviz.calculator.pipeline.recidivism.release_event import (
    ReincarcerationReturnType,
)
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    Gender,
    StatePersonRace,
    Race,
    StatePersonEthnicity,
    Ethnicity,
)
from recidiviz.calculator.pipeline.recidivism.metrics import (
    ReincarcerationRecidivismMetricType as MetricType,
)

_COUNTY_OF_RESIDENCE = "county"


class TestReincarcerations(unittest.TestCase):
    """Tests the reincarcerations() function in the calculator."""

    def test_reincarcerations(self):
        release_date = date(2018, 1, 1)
        original_admission_date = release_date - relativedelta(years=4)
        reincarceration_date = release_date + relativedelta(years=3)
        second_release_date = reincarceration_date + relativedelta(years=1)

        first_event = RecidivismReleaseEvent(
            "CA",
            original_admission_date,
            release_date,
            "Sing Sing",
            _COUNTY_OF_RESIDENCE,
            reincarceration_date,
            "Sing Sing",
            ReincarcerationReturnType.NEW_ADMISSION,
        )
        second_event = NonRecidivismReleaseEvent(
            "CA",
            reincarceration_date,
            second_release_date,
            "Sing Sing",
            _COUNTY_OF_RESIDENCE,
        )
        release_events = {2018: [first_event], 2022: [second_event]}

        expected_reincarcerations = {reincarceration_date: first_event}

        reincarcerations = calculator.reincarcerations(release_events)
        self.assertEqual(expected_reincarcerations, reincarcerations)

    def test_reincarcerations_two_releases_same_reincarceration(self):
        release_date = date(2018, 1, 1)
        original_admission_date = release_date - relativedelta(years=4)
        second_release_date = release_date + relativedelta(years=1)
        reincarceration_date = second_release_date + relativedelta(years=1)

        first_event = RecidivismReleaseEvent(
            "CA",
            original_admission_date,
            release_date,
            "Sing Sing",
            _COUNTY_OF_RESIDENCE,
            reincarceration_date,
            "Sing Sing",
            ReincarcerationReturnType.NEW_ADMISSION,
        )
        second_event = RecidivismReleaseEvent(
            "CA",
            release_date,
            second_release_date,
            "Sing Sing",
            _COUNTY_OF_RESIDENCE,
            reincarceration_date,
            "Sing Sing",
            ReincarcerationReturnType.NEW_ADMISSION,
        )
        release_events = {2018: [first_event], 2022: [second_event]}

        # Both release events have identified the same admission that can be counted as a valid reincarceration
        # The second event is prioritized because it has fewer days between release and reincarceration
        expected_reincarcerations = {reincarceration_date: second_event}

        reincarcerations = calculator.reincarcerations(release_events)
        self.assertEqual(expected_reincarcerations, reincarcerations)

    def test_reincarcerations_empty(self):
        reincarcerations = calculator.reincarcerations({})
        self.assertEqual({}, reincarcerations)


class TestReincarcerationsInWindow(unittest.TestCase):
    """Tests the reincarcerations_in_window() function in the calculator."""

    def test_reincarcerations_in_window(self):
        return_dates = [
            # Too early
            date(2012, 4, 30),
            # Just right
            date(2016, 5, 13),
            date(2020, 11, 20),
            date(2021, 5, 13),
            # Too late
            date(2022, 5, 13),
        ]

        all_reincarcerations = {
            return_date: RecidivismReleaseEvent(
                state_code="US_XX",
                original_admission_date=date(2000, 1, 1),
                release_date=date(2001, 1, 1),
                reincarceration_date=return_date,
                return_type=ReincarcerationReturnType.NEW_ADMISSION,
                from_supervision_type=None,
            )
            for return_date in return_dates
        }

        start_date = date(2016, 5, 13)

        reincarcerations = calculator.reincarcerations_in_window(
            start_date, start_date + relativedelta(years=6), all_reincarcerations
        )
        self.assertEqual(3, len(reincarcerations))

    def test_reincarcerations_in_window_all_early(self):
        return_dates = [
            # Too early
            date(2012, 4, 30),
            date(2016, 5, 13),
            date(2020, 11, 20),
            date(2021, 5, 13),
            date(2022, 5, 13),
        ]

        all_reincarcerations = {
            return_date: RecidivismReleaseEvent(
                state_code="US_XX",
                original_admission_date=date(2000, 1, 1),
                release_date=date(2001, 1, 1),
                reincarceration_date=return_date,
                return_type=ReincarcerationReturnType.NEW_ADMISSION,
                from_supervision_type=None,
            )
            for return_date in return_dates
        }

        start_date = date(2026, 5, 13)

        reincarcerations = calculator.reincarcerations_in_window(
            start_date, start_date + relativedelta(years=6), all_reincarcerations
        )

        self.assertEqual([], reincarcerations)

    def test_reincarcerations_in_window_all_late(self):
        return_dates = [
            # Too early
            date(2012, 4, 30),
            date(2016, 5, 13),
            date(2020, 11, 20),
            date(2021, 5, 13),
            date(2022, 5, 13),
        ]

        all_reincarcerations = {
            return_date: RecidivismReleaseEvent(
                state_code="US_XX",
                original_admission_date=date(2000, 1, 1),
                release_date=date(2001, 1, 1),
                reincarceration_date=return_date,
                return_type=ReincarcerationReturnType.NEW_ADMISSION,
                from_supervision_type=None,
            )
            for return_date in return_dates
        }

        start_date = date(2004, 5, 13)

        reincarcerations = calculator.reincarcerations_in_window(
            start_date, start_date + relativedelta(years=5), all_reincarcerations
        )

        self.assertEqual([], reincarcerations)


class TestStayLengthFromEvent(unittest.TestCase):
    """Tests the built-in stay_length function on the ReleaseEvent class."""

    def test_stay_length_from_event_earlier_month_and_date(self):
        original_admission_date = date(2013, 6, 17)
        release_date = date(2014, 4, 15)
        event = ReleaseEvent("CA", original_admission_date, release_date, "Sing Sing")

        self.assertEqual(9, event.stay_length)

    def test_stay_length_from_event_same_month_earlier_date(self):
        original_admission_date = date(2013, 6, 17)
        release_date = date(2014, 6, 16)
        event = ReleaseEvent("NH", original_admission_date, release_date, "Sing Sing")

        self.assertEqual(11, event.stay_length)

    def test_stay_length_from_event_same_month_same_date(self):
        original_admission_date = date(2013, 6, 17)
        release_date = date(2014, 6, 17)
        event = ReleaseEvent("TX", original_admission_date, release_date, "Sing Sing")

        self.assertEqual(12, event.stay_length)

    def test_stay_length_from_event_same_month_later_date(self):
        original_admission_date = date(2013, 6, 17)
        release_date = date(2014, 6, 18)
        event = ReleaseEvent("UT", original_admission_date, release_date, "Sing Sing")

        self.assertEqual(12, event.stay_length)

    def test_stay_length_from_event_later_month(self):
        original_admission_date = date(2013, 6, 17)
        release_date = date(2014, 8, 11)
        event = ReleaseEvent("HI", original_admission_date, release_date, "Sing Sing")

        self.assertEqual(13, event.stay_length)

    def test_stay_length_from_event_original_admission_date_unknown(self):
        release_date = date(2014, 7, 11)
        event = ReleaseEvent("MT", None, release_date, "Sing Sing")

        self.assertIsNone(event.stay_length)

    def test_stay_length_from_event_release_date_unknown(self):
        original_admission_date = date(2014, 7, 11)
        event = ReleaseEvent("UT", original_admission_date, None, "Sing Sing")

        self.assertIsNone(event.stay_length)

    def test_stay_length_from_event_both_dates_unknown(self):
        event = ReleaseEvent("NH", None, None, "Sing Sing")

        self.assertIsNone(event.stay_length)


class TestStayLengthBucket(unittest.TestCase):
    """Tests the built-in stay_length_bucket attribute on the ReleaseEvent class."""

    def setUp(self) -> None:
        self.months_to_bucket_map = {
            11: "<12",
            12: "12-24",
            20: "12-24",
            24: "24-36",
            30: "24-36",
            36: "36-48",
            40: "36-48",
            48: "48-60",
            50: "48-60",
            60: "60-72",
            70: "60-72",
            72: "72-84",
            80: "72-84",
            84: "84-96",
            96: "96-108",
            100: "96-108",
            108: "108-120",
            110: "108-120",
            120: "120<",
            130: "120<",
        }

    def test_stay_length_bucket(self):
        original_admission_date = date(1903, 6, 17)

        for months, bucket in self.months_to_bucket_map.items():
            release_date = original_admission_date + relativedelta(months=months)
            event = ReleaseEvent(
                "CA", original_admission_date, release_date, "Sing Sing"
            )
            self.assertEqual(bucket, event.stay_length_bucket)


_ALL_METRIC_INCLUSIONS_DICT = {
    MetricType.REINCARCERATION_COUNT: True,
    MetricType.REINCARCERATION_RATE: True,
}

_DEFAULT_PERSON_METADATA = PersonMetadata(prioritized_race_or_ethnicity="BLACK")


class TestMapRecidivismCombinations(unittest.TestCase):
    """Tests the map_recidivism_combinations function."""

    @staticmethod
    def expected_metric_combos_count(
        release_events_by_cohort: Dict[int, List[ReleaseEvent]]
    ) -> int:
        """Calculates the expected number of characteristic combinations given the release events."""
        all_release_events = [
            re for re_list in release_events_by_cohort.values() for re in re_list
        ]

        recidivism_release_events = [
            re for re in all_release_events if isinstance(re, RecidivismReleaseEvent)
        ]

        expected_rate_metrics = len(FOLLOW_UP_PERIODS) * len(all_release_events)
        expected_count_metrics = len(recidivism_release_events)

        return expected_rate_metrics + expected_count_metrics

    @freeze_time("2100-01-01")
    def test_map_recidivism_combinations(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism."""
        person = StatePerson.new_with_defaults(
            state_code="CA",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="CA", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="CA", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            2008: [
                RecidivismReleaseEvent(
                    "CA",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(2014, 5, 12),
                    "Upstate",
                    ReincarcerationReturnType.NEW_ADMISSION,
                )
            ]
        }

        days_at_liberty = (date(2014, 5, 12) - date(2008, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )

        expected_combos_count = self.expected_metric_combos_count(
            release_events_by_cohort
        )

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(
            len(
                set(
                    id(combination_dict)
                    for combination_dict, _ in recidivism_combinations
                )
            ),
            len(recidivism_combinations),
        )

        for combination, value in recidivism_combinations:
            if (
                combination.get("metric_type") == MetricType.REINCARCERATION_RATE
                and combination.get("follow_up_period") <= 5
            ):
                self.assertEqual(0, value)
            else:
                self.assertEqual(1, value)
                if combination.get("metric_type") == MetricType.REINCARCERATION_COUNT:
                    assert combination.get("days_at_liberty") == days_at_liberty

    def test_map_recidivism_combinations_multiple_in_period(self):
        """Tests the map_recidivism_combinations function where there are multiple instances of recidivism within a
        follow-up period."""
        person = StatePerson.new_with_defaults(
            state_code="CA",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="CA", race=Race.BLACK)
        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="CA", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            1908: [
                RecidivismReleaseEvent(
                    "CA",
                    date(1905, 7, 19),
                    date(1908, 9, 19),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(1910, 8, 12),
                    "Upstate",
                    ReincarcerationReturnType.NEW_ADMISSION,
                )
            ],
            1912: [
                RecidivismReleaseEvent(
                    "CA",
                    date(1910, 8, 12),
                    date(1912, 8, 19),
                    "Upstate",
                    _COUNTY_OF_RESIDENCE,
                    date(1914, 7, 15),
                    "Sing Sing",
                    ReincarcerationReturnType.NEW_ADMISSION,
                )
            ],
        }

        days_at_liberty_1 = (date(1910, 8, 12) - date(1908, 9, 19)).days
        days_at_liberty_2 = (date(1914, 7, 15) - date(1912, 8, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )

        # For the first event:
        #   For the first 5 periods:
        #       5 periods = 5 metrics
        #   For the second 5 periods, there is an additional return:
        #       5 periods * 2 returns = 10 metrics
        #
        #   Count metrics: 1
        #
        # For the second event:
        #   10 periods = 10 metrics
        #
        # Count metrics: 1

        expected_count = 5 + 10 + 1 + 10 + 1

        # Multiplied by 2 to include the county of residence field
        assert len(recidivism_combinations) == expected_count

        for combination, value in recidivism_combinations:
            if (
                combination.get("metric_type") == MetricType.REINCARCERATION_RATE
                and combination.get("follow_up_period") < 2
            ):
                self.assertEqual(0, value)
            else:
                self.assertEqual(1, value)

                if combination.get("metric_type") == MetricType.REINCARCERATION_COUNT:
                    if combination.get("year") == 1910:
                        self.assertEqual(
                            days_at_liberty_1, combination.get("days_at_liberty")
                        )
                    else:
                        self.assertEqual(
                            days_at_liberty_2, combination.get("days_at_liberty")
                        )

    def test_map_recidivism_combinations_multiple_in_period_different_types(self):
        """Tests the map_recidivism_combinations function where there are multiple instances of recidivism within a
        follow-up period with different return type information"""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.BLACK)
        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            1910: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(1905, 7, 19),
                    date(1910, 1, 1),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(1910, 1, 12),
                    "Upstate",
                    return_type=ReincarcerationReturnType.NEW_ADMISSION,
                ),
                RecidivismReleaseEvent(
                    "US_XX",
                    date(1910, 1, 12),
                    date(1910, 8, 19),
                    "Upstate",
                    _COUNTY_OF_RESIDENCE,
                    date(1910, 10, 15),
                    "Sing Sing",
                    return_type=ReincarcerationReturnType.REVOCATION,
                    from_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                    source_violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )

        # For the first event:
        #   For all periods there is an additional return:
        #       10 periods * 2 event-based = 20 rate metrics
        #
        #   Count metrics: 1
        #
        # For the second event:
        #   10 periods = 10 rate metrics
        #
        # Count metrics: 1

        expected_count = 20 + 1 + 10 + 1

        expected_return_type_counts = {
            ReincarcerationReturnType.NEW_ADMISSION: 11,
            ReincarcerationReturnType.REVOCATION: 21,
        }

        expected_from_supervision_type_counts = {
            StateSupervisionPeriodSupervisionType.PAROLE: 21,
            None: 11,
        }

        expected_source_violation_type_counts = {
            StateSupervisionViolationType.FELONY: 21,
            None: 11,
        }

        # Multiplied by 2 to include the county of residence field
        assert len(recidivism_combinations) == expected_count

        return_type_counts: Dict[ReincarcerationReturnType, int] = defaultdict(int)
        from_supervision_type_counts: Dict[
            StateSupervisionPeriodSupervisionType, int
        ] = defaultdict(int)
        source_violation_type_counts: Dict[
            StateSupervisionViolationType, int
        ] = defaultdict(int)

        for combination, value in recidivism_combinations:
            self.assertEqual(1, value)

            return_type_counts[combination.get("return_type")] += 1
            from_supervision_type_counts[combination.get("from_supervision_type")] += 1
            source_violation_type_counts[combination.get("source_violation_type")] += 1

        self.assertEqual(expected_return_type_counts, return_type_counts)
        self.assertEqual(
            expected_from_supervision_type_counts, from_supervision_type_counts
        )
        self.assertEqual(
            expected_source_violation_type_counts, source_violation_type_counts
        )

    def test_map_recidivism_combinations_multiple_releases_in_year(self):
        """Tests the map_recidivism_combinations function where there are multiple releases in the same year."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.BLACK)
        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            1908: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(1905, 7, 19),
                    date(1908, 1, 19),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(1908, 5, 12),
                    "Upstate",
                    ReincarcerationReturnType.NEW_ADMISSION,
                ),
                NonRecidivismReleaseEvent(
                    "US_XX",
                    date(1908, 5, 12),
                    date(1908, 8, 19),
                    "Upstate",
                    _COUNTY_OF_RESIDENCE,
                ),
            ],
        }

        days_at_liberty_1 = (date(1908, 5, 12) - date(1908, 1, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )

        expected_combos_count = self.expected_metric_combos_count(
            release_events_by_cohort
        )

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(
            len(
                set(
                    id(combination_dict)
                    for combination_dict, _ in recidivism_combinations
                )
            ),
            len(recidivism_combinations),
        )

        for combination, value in recidivism_combinations:
            if combination.get("metric_type") == MetricType.REINCARCERATION_COUNT:
                self.assertEqual(days_at_liberty_1, combination.get("days_at_liberty"))
                self.assertEqual(1, value)
            else:
                if combination.get("release_date") == date(1908, 8, 19):
                    self.assertEqual(0, value)
                else:
                    self.assertEqual(1, value)

    @freeze_time("2100-01-01")
    def test_map_recidivism_combinations_multiple_releases_same_reincarceration(self):
        """Tests the map_recidivism_combinations function where there are multiple releases that have identified the
        same admission as the next valid reincarceration."""
        person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="US_XX", race=Race.BLACK)
        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="US_XX", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            1908: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(1905, 7, 19),
                    date(1908, 9, 19),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(1913, 8, 12),
                    "Upstate",
                    ReincarcerationReturnType.NEW_ADMISSION,
                )
            ],
            1912: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(1908, 9, 19),
                    date(1912, 9, 19),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(1913, 8, 12),
                    "Upstate",
                    ReincarcerationReturnType.NEW_ADMISSION,
                )
            ],
        }

        days_at_liberty_2 = (date(1913, 8, 12) - date(1912, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )

        # For the first event:
        #  10 periods = 10 rate metrics
        #
        # For the second event:
        #   10 periods = 10 rate metrics
        #
        # Count metrics: 1

        expected_count = 10 + 10 + 1

        assert len(recidivism_combinations) == expected_count

        for combination, value in recidivism_combinations:
            if (
                combination.get("metric_type") == MetricType.REINCARCERATION_RATE
                and combination.get("release_cohort") == 1908
                and combination.get("follow_up_period") < 5
            ):
                self.assertEqual(0, value)
            else:
                self.assertEqual(1, value)

            if combination.get("metric_type") == MetricType.REINCARCERATION_COUNT:
                self.assertEqual(days_at_liberty_2, combination.get("days_at_liberty"))

    @freeze_time("2100-01-01")
    def test_map_recidivism_combinations_return_one_year_later(self):
        """Tests the map_recidivism_combinations function where the person returned to prison exactly one year after
        they were released."""
        person = StatePerson.new_with_defaults(
            state_code="CA",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="CA", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="CA", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            2008: [
                RecidivismReleaseEvent(
                    "CA",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(2009, 9, 19),
                    "Upstate",
                    ReincarcerationReturnType.NEW_ADMISSION,
                )
            ]
        }

        days_at_liberty = (date(2009, 9, 19) - date(2008, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )

        expected_combos_count = self.expected_metric_combos_count(
            release_events_by_cohort
        )

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(
            len(
                set(
                    id(combination_dict)
                    for combination_dict, _ in recidivism_combinations
                )
            ),
            len(recidivism_combinations),
        )

        for combination, value in recidivism_combinations:
            if (
                combination.get("metric_type") == MetricType.REINCARCERATION_RATE
                and combination.get("follow_up_period") < 2
            ):
                self.assertEqual(0, value)
            else:
                self.assertEqual(1, value)
                if combination.get("metric_type") == MetricType.REINCARCERATION_COUNT:
                    assert combination.get("days_at_liberty") == days_at_liberty

    def test_map_recidivism_combinations_no_recidivism(self):
        """Tests the map_recidivism_combinations function where there is no
        recidivism."""
        person = StatePerson.new_with_defaults(
            state_code="CA",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="CA", race=Race.BLACK)
        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="CA", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            2008: [
                NonRecidivismReleaseEvent(
                    "CA",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                )
            ]
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )

        expected_combos_count = self.expected_metric_combos_count(
            release_events_by_cohort
        )

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(
            len(
                set(
                    id(combination_dict)
                    for combination_dict, _ in recidivism_combinations
                )
            ),
            len(recidivism_combinations),
        )
        self.assertTrue(
            all(value == 0 for _combination, value in recidivism_combinations)
        )

    def test_map_recidivism_combinations_recidivated_after_last_period(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism but it occurred after the last follow-up period we track."""
        person = StatePerson.new_with_defaults(
            state_code="CA",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="CA", race=Race.BLACK)
        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="CA", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            1998: [
                RecidivismReleaseEvent(
                    "CA",
                    date(1995, 7, 19),
                    date(1998, 9, 19),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(2008, 10, 12),
                    "Upstate",
                    ReincarcerationReturnType.NEW_ADMISSION,
                )
            ]
        }

        days_at_liberty = (date(2008, 10, 12) - date(1998, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )

        expected_combos_count = self.expected_metric_combos_count(
            release_events_by_cohort
        )

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(
            len(
                set(
                    id(combination_dict)
                    for combination_dict, _ in recidivism_combinations
                )
            ),
            len(recidivism_combinations),
        )

        assert all(
            value == 0
            for _combination, value in recidivism_combinations
            if _combination["metric_type"] == MetricType.REINCARCERATION_RATE
        )
        assert all(
            value == 1
            for _combination, value in recidivism_combinations
            if _combination["metric_type"] == MetricType.REINCARCERATION_COUNT
            and _combination.get("return_type") != ReincarcerationReturnType.REVOCATION
        )
        assert all(
            _combination.get("days_at_liberty") == days_at_liberty
            for _combination, _ in recidivism_combinations
            if _combination["metric_type"] == MetricType.REINCARCERATION_COUNT
        )

    def test_map_recidivism_combinations_multiple_races(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism, and the person has more than one race."""
        person = StatePerson.new_with_defaults(
            state_code="CA",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race_white = StatePersonRace.new_with_defaults(state_code="CA", race=Race.WHITE)

        race_black = StatePersonRace.new_with_defaults(state_code="MT", race=Race.BLACK)

        person.races = [race_white, race_black]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="CA", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            2008: [
                RecidivismReleaseEvent(
                    "CA",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(2014, 5, 12),
                    "Upstate",
                    ReincarcerationReturnType.NEW_ADMISSION,
                )
            ]
        }

        days_at_liberty = (date(2014, 5, 12) - date(2008, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )

        expected_combos_count = self.expected_metric_combos_count(
            release_events_by_cohort
        )

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(
            len(
                set(
                    id(combination_dict)
                    for combination_dict, _ in recidivism_combinations
                )
            ),
            len(recidivism_combinations),
        )

        for combination, value in recidivism_combinations:
            if (
                combination.get("metric_type") == MetricType.REINCARCERATION_RATE
                and combination.get("follow_up_period") <= 5
            ):
                self.assertEqual(0, value)
            elif combination.get("metric_type") == MetricType.REINCARCERATION_COUNT:
                self.assertEqual(1, value)
                assert combination.get("days_at_liberty") == days_at_liberty
            else:
                self.assertEqual(1, value)

    def test_map_recidivism_combinations_multiple_ethnicities(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism, and the person has more than one ethnicity."""
        person = StatePerson.new_with_defaults(
            state_code="CA",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="CA", race=Race.BLACK)
        person.races = [race]

        ethnicity_hispanic = StatePersonEthnicity.new_with_defaults(
            state_code="CA", ethnicity=Ethnicity.HISPANIC
        )

        ethnicity_not_hispanic = StatePersonEthnicity.new_with_defaults(
            state_code="MT", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity_hispanic, ethnicity_not_hispanic]

        release_events_by_cohort = {
            2008: [
                RecidivismReleaseEvent(
                    "CA",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(2014, 5, 12),
                    "Upstate",
                    ReincarcerationReturnType.NEW_ADMISSION,
                )
            ]
        }

        days_at_liberty = (date(2014, 5, 12) - date(2008, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )

        expected_combos_count = self.expected_metric_combos_count(
            release_events_by_cohort
        )

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(
            len(
                set(
                    id(combination_dict)
                    for combination_dict, _ in recidivism_combinations
                )
            ),
            len(recidivism_combinations),
        )

        for combination, value in recidivism_combinations:
            if (
                combination.get("metric_type") == MetricType.REINCARCERATION_RATE
                and combination.get("follow_up_period") <= 5
            ):
                self.assertEqual(0, value)
            else:
                self.assertEqual(1, value)

                if combination.get("metric_type") == MetricType.REINCARCERATION_COUNT:
                    assert combination.get("days_at_liberty") == days_at_liberty

    def test_map_recidivism_combinations_multiple_races_ethnicities(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism, and the person has multiple races and multiple
        ethnicities."""
        person = StatePerson.new_with_defaults(
            state_code="CA",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race_white = StatePersonRace.new_with_defaults(state_code="CA", race=Race.WHITE)

        race_black = StatePersonRace.new_with_defaults(state_code="MT", race=Race.BLACK)

        person.races = [race_white, race_black]

        ethnicity_hispanic = StatePersonEthnicity.new_with_defaults(
            state_code="CA", ethnicity=Ethnicity.HISPANIC
        )

        ethnicity_not_hispanic = StatePersonEthnicity.new_with_defaults(
            state_code="MT", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity_hispanic, ethnicity_not_hispanic]

        release_events_by_cohort = {
            2008: [
                RecidivismReleaseEvent(
                    "CA",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(2014, 5, 12),
                    "Upstate",
                    ReincarcerationReturnType.NEW_ADMISSION,
                )
            ]
        }

        days_at_liberty = (date(2014, 5, 12) - date(2008, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )

        expected_combos_count = self.expected_metric_combos_count(
            release_events_by_cohort
        )

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(
            len(
                set(
                    id(combination_dict)
                    for combination_dict, _ in recidivism_combinations
                )
            ),
            len(recidivism_combinations),
        )

        for combination, value in recidivism_combinations:
            if (
                combination.get("metric_type") == MetricType.REINCARCERATION_RATE
                and combination.get("follow_up_period") <= 5
            ):
                self.assertEqual(0, value)
            else:
                self.assertEqual(1, value)

                if combination.get("metric_type") == MetricType.REINCARCERATION_COUNT:
                    assert combination.get("days_at_liberty") == days_at_liberty

    def test_map_recidivism_combinations_revocation_parole(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism, and they returned from a revocation of parole."""
        person = StatePerson.new_with_defaults(
            state_code="CA",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="CA", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="CA", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            2008: [
                RecidivismReleaseEvent(
                    "CA",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(2014, 5, 12),
                    "Upstate",
                    ReincarcerationReturnType.REVOCATION,
                    from_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                )
            ]
        }

        days_at_liberty = (date(2014, 5, 12) - date(2008, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )
        expected_combos_count = self.expected_metric_combos_count(
            release_events_by_cohort
        )

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(
            len(
                set(
                    id(combination_dict)
                    for combination_dict, _ in recidivism_combinations
                )
            ),
            len(recidivism_combinations),
        )

        for combination, value in recidivism_combinations:
            if (
                combination.get("metric_type") == MetricType.REINCARCERATION_RATE
                and combination.get("follow_up_period") <= 5
            ):
                self.assertEqual(0, value)
            else:
                self.assertEqual(1, value)

                if combination.get("metric_type") == MetricType.REINCARCERATION_COUNT:
                    assert combination.get("days_at_liberty") == days_at_liberty

    def test_map_recidivism_combinations_revocation_probation(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism, and they returned from a revocation of parole."""
        person = StatePerson.new_with_defaults(
            state_code="CA",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="CA", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="CA", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            2008: [
                RecidivismReleaseEvent(
                    "CA",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(2014, 5, 12),
                    "Upstate",
                    ReincarcerationReturnType.REVOCATION,
                    from_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                )
            ]
        }

        days_at_liberty = (date(2014, 5, 12) - date(2008, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )

        expected_combos_count = self.expected_metric_combos_count(
            release_events_by_cohort
        )

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(
            len(
                set(
                    id(combination_dict)
                    for combination_dict, _ in recidivism_combinations
                )
            ),
            len(recidivism_combinations),
        )

        for combination, value in recidivism_combinations:
            if (
                combination.get("metric_type") == MetricType.REINCARCERATION_RATE
                and combination.get("follow_up_period") <= 5
            ):
                self.assertEqual(0, value)
            else:
                self.assertEqual(1, value)

                if combination.get("metric_type") == MetricType.REINCARCERATION_COUNT:
                    assert combination.get("days_at_liberty") == days_at_liberty

    def test_map_recidivism_combinations_technical_revocation_parole(self):
        """Tests the map_recidivism_combinations function where there is
        recidivism, and they returned from a technical violation that resulted
        in the revocation of parole."""
        person = StatePerson.new_with_defaults(
            state_code="CA",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="CA", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="CA", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            2008: [
                RecidivismReleaseEvent(
                    "CA",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(2014, 5, 12),
                    "Upstate",
                    ReincarcerationReturnType.REVOCATION,
                    from_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                    source_violation_type=StateSupervisionViolationType.TECHNICAL,
                )
            ]
        }

        days_at_liberty = (date(2014, 5, 12) - date(2008, 9, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )

        expected_combos_count = self.expected_metric_combos_count(
            release_events_by_cohort
        )

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(
            len(
                set(
                    id(combination_dict)
                    for combination_dict, _ in recidivism_combinations
                )
            ),
            len(recidivism_combinations),
        )

        for combination, value in recidivism_combinations:
            if (
                combination.get("metric_type") == MetricType.REINCARCERATION_RATE
                and combination.get("follow_up_period") <= 5
            ):
                self.assertEqual(0, value)
            else:
                self.assertEqual(1, value)

                if combination.get("metric_type") == MetricType.REINCARCERATION_COUNT:
                    assert combination.get("days_at_liberty") == days_at_liberty

    def test_map_recidivism_combinations_count_metric_buckets(self):
        person = StatePerson.new_with_defaults(
            state_code="CA",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="CA", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="CA", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            2008: [
                RecidivismReleaseEvent(
                    "CA",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(2014, 5, 12),
                    "Upstate",
                    ReincarcerationReturnType.NEW_ADMISSION,
                )
            ]
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )

        expected_combos_count = self.expected_metric_combos_count(
            release_events_by_cohort
        )

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(
            len(
                set(
                    id(combination_dict)
                    for combination_dict, _ in recidivism_combinations
                )
            ),
            len(recidivism_combinations),
        )

        for combo, value in recidivism_combinations:
            if combo["metric_type"] == MetricType.REINCARCERATION_COUNT:
                assert combo["year"] == 2014
                assert combo["month"] == 5

                self.assertEqual(1, value)

    def test_map_recidivism_combinations_count_metric_no_recidivism(self):
        person = StatePerson.new_with_defaults(
            state_code="CA",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="CA", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="CA", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            2008: [
                NonRecidivismReleaseEvent(
                    "CA", date(2005, 7, 19), date(2008, 9, 19), "Hudson"
                )
            ]
        }

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )

        assert all(value == 0 for _combination, value in recidivism_combinations)
        assert all(
            _combination["metric_type"] == MetricType.REINCARCERATION_RATE
            for _combination, value in recidivism_combinations
        )

    @freeze_time("1914-09-30")
    def test_map_recidivism_combinations_count_relevant_periods(self):
        person = StatePerson.new_with_defaults(
            state_code="CA",
            person_id=12345,
            birthdate=date(1884, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="CA", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="CA", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            1908: [
                RecidivismReleaseEvent(
                    "TX",
                    date(1905, 7, 19),
                    date(1908, 9, 19),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(1914, 3, 12),
                    "Upstate",
                    ReincarcerationReturnType.NEW_ADMISSION,
                )
            ],
            1914: [
                RecidivismReleaseEvent(
                    "TX",
                    date(1914, 3, 12),
                    date(1914, 7, 3),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(1914, 9, 1),
                    "Upstate",
                    ReincarcerationReturnType.NEW_ADMISSION,
                )
            ],
        }

        days_at_liberty_1 = (date(1914, 3, 12) - date(1908, 9, 19)).days
        days_at_liberty_2 = (date(1914, 9, 1) - date(1914, 7, 3)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )

        # For the first event:
        #   For the first 5 periods:
        #       5 periods = 5 metrics
        #   For the next 2 periods:
        #       2 periods * 2 returns = 4 metrics
        #
        #   Count metrics: 1 count window = 1
        #
        # For the second event:
        #   1 relevant period = 1
        #   1 count metric = 1

        expected_combos_count = 5 + 4 + 1 + 1 + 1

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(
            len(
                set(
                    id(combination_dict)
                    for combination_dict, _ in recidivism_combinations
                )
            ),
            len(recidivism_combinations),
        )

        for combo, value in recidivism_combinations:
            if combo["metric_type"] == MetricType.REINCARCERATION_COUNT:

                assert combo["year"] == 1914
                assert combo["month"] in (3, 9)

                self.assertEqual(1, value)

                if combo.get("metric_type") == MetricType.REINCARCERATION_COUNT:
                    if combo["month"] == 3:
                        assert combo.get("days_at_liberty") == days_at_liberty_1
                    else:
                        assert combo.get("days_at_liberty") == days_at_liberty_2

    def test_map_recidivism_combinations_count_twice_in_month(self):
        person = StatePerson.new_with_defaults(
            state_code="CA",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=Gender.FEMALE,
        )

        race = StatePersonRace.new_with_defaults(state_code="CA", race=Race.WHITE)

        person.races = [race]

        ethnicity = StatePersonEthnicity.new_with_defaults(
            state_code="CA", ethnicity=Ethnicity.NOT_HISPANIC
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort = {
            1908: [
                RecidivismReleaseEvent(
                    "CA",
                    date(1905, 7, 19),
                    date(1908, 9, 19),
                    "Hudson",
                    _COUNTY_OF_RESIDENCE,
                    date(1914, 3, 12),
                    "Upstate",
                    ReincarcerationReturnType.NEW_ADMISSION,
                )
            ],
            1914: [
                RecidivismReleaseEvent(
                    "CA",
                    date(1914, 3, 12),
                    date(1914, 3, 19),
                    "Upstate",
                    _COUNTY_OF_RESIDENCE,
                    date(1914, 3, 30),
                    "Upstate",
                    ReincarcerationReturnType.NEW_ADMISSION,
                )
            ],
        }

        days_at_liberty_1 = (date(1914, 3, 12) - date(1908, 9, 19)).days
        days_at_liberty_2 = (date(1914, 3, 30) - date(1914, 3, 19)).days

        recidivism_combinations = calculator.map_recidivism_combinations(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS_DICT,
            _DEFAULT_PERSON_METADATA,
        )

        # For the first event:
        #   For the first 5 periods:
        #       5 periods = 5 metrics
        #   For the second 5 periods
        #       5 periods * 2 returns = 10 metrics
        #
        #   Count: 1 return
        #
        # For the second event:
        #   10 periods = 10 metrics
        #
        #   Count: 1 return

        expected_combos_count = 5 + 10 + 1 + 10 + 1

        self.assertEqual(expected_combos_count, len(recidivism_combinations))
        self.assertEqual(
            len(
                set(
                    id(combination_dict)
                    for combination_dict, _ in recidivism_combinations
                )
            ),
            len(recidivism_combinations),
        )

        for combo, value in recidivism_combinations:
            if combo["metric_type"] == MetricType.REINCARCERATION_COUNT:
                assert combo["year"] == 1914
                assert combo["month"] == 3

                self.assertEqual(1, value)

            if combo.get("metric_type") == MetricType.REINCARCERATION_COUNT:
                if combo["release_facility"] == "Hudson":
                    assert combo.get("days_at_liberty") == days_at_liberty_1
                else:
                    assert combo.get("days_at_liberty") == days_at_liberty_2


class TestReincarcerationsByPeriod(unittest.TestCase):
    """Tests the reincarcerations_by_period function."""

    def test_reincarcerations_by_period(self):
        first_return = RecidivismReleaseEvent(
            state_code="US_XX",
            original_admission_date=date(1900, 1, 1),
            release_date=date(1908, 9, 19),
            reincarceration_date=date(1910, 8, 12),
        )

        second_return = RecidivismReleaseEvent(
            state_code="US_XX",
            original_admission_date=date(1910, 8, 12),
            release_date=date(1912, 4, 1),
            reincarceration_date=date(1914, 7, 15),
        )

        all_reincarcerations = {
            date(1910, 8, 12): first_return,
            date(1914, 7, 15): second_return,
        }

        reincarcerations_by_period = calculator.reincarcerations_by_period(
            date(1908, 9, 19), all_reincarcerations
        )

        expected_output = {
            1: [],
            2: [first_return],
            3: [first_return],
            4: [first_return],
            5: [first_return],
            6: [first_return, second_return],
            7: [first_return, second_return],
            8: [first_return, second_return],
            9: [first_return, second_return],
            10: [first_return, second_return],
        }

        self.assertEqual(expected_output, reincarcerations_by_period)

    def test_reincarcerations_by_period_no_recidivism(self):
        all_reincarcerations = {}

        reincarcerations_by_period = calculator.reincarcerations_by_period(
            date(1908, 9, 19), all_reincarcerations
        )

        expected_output = {year: [] for year in range(1, 11)}

        self.assertEqual(expected_output, reincarcerations_by_period)

    @freeze_time("2000-01-01")
    def test_reincarcerations_by_period_only_two_periods(self):
        first_return = RecidivismReleaseEvent(
            state_code="US_XX",
            original_admission_date=date(1990, 1, 1),
            release_date=date(1998, 9, 19),
            reincarceration_date=date(1998, 12, 12),
        )

        second_return = RecidivismReleaseEvent(
            state_code="US_XX",
            original_admission_date=date(1998, 12, 12),
            release_date=date(1999, 4, 1),
            reincarceration_date=date(1999, 7, 15),
        )

        all_reincarcerations = {
            date(1998, 12, 12): first_return,
            date(1999, 7, 15): second_return,
        }

        reincarcerations_by_period = calculator.reincarcerations_by_period(
            date(1998, 9, 19), all_reincarcerations
        )

        expected_output = {year: [first_return, second_return] for year in range(1, 3)}

        self.assertEqual(expected_output, reincarcerations_by_period)


def test_relevant_follow_up_periods():
    today = date(2018, 1, 26)

    assert calculator.relevant_follow_up_periods(
        date(2015, 1, 5), today, calculator.FOLLOW_UP_PERIODS
    ) == [1, 2, 3, 4]
    assert calculator.relevant_follow_up_periods(
        date(2015, 1, 26), today, calculator.FOLLOW_UP_PERIODS
    ) == [1, 2, 3, 4]
    assert calculator.relevant_follow_up_periods(
        date(2015, 1, 27), today, calculator.FOLLOW_UP_PERIODS
    ) == [1, 2, 3]
    assert calculator.relevant_follow_up_periods(
        date(2016, 1, 5), today, calculator.FOLLOW_UP_PERIODS
    ) == [1, 2, 3]
    assert calculator.relevant_follow_up_periods(
        date(2017, 4, 10), today, calculator.FOLLOW_UP_PERIODS
    ) == [1]
    assert calculator.relevant_follow_up_periods(
        date(2018, 1, 5), today, calculator.FOLLOW_UP_PERIODS
    ) == [1]
    assert (
        calculator.relevant_follow_up_periods(
            date(2018, 2, 5), today, calculator.FOLLOW_UP_PERIODS
        )
        == []
    )
