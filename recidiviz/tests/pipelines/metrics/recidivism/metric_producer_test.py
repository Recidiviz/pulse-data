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

"""Tests for recidivism/metric_producer.py."""
import datetime
import unittest
from datetime import date
from typing import Dict, List

from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

from recidiviz.common.constants.state.external_id_types import US_ND_ELITE
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePerson,
    NormalizedStatePersonEthnicity,
    NormalizedStatePersonExternalId,
    NormalizedStatePersonRace,
)
from recidiviz.pipelines.metrics.recidivism import metric_producer, pipeline
from recidiviz.pipelines.metrics.recidivism.events import (
    NonRecidivismReleaseEvent,
    RecidivismReleaseEvent,
    ReleaseEvent,
)
from recidiviz.pipelines.metrics.recidivism.metric_producer import FOLLOW_UP_PERIODS
from recidiviz.pipelines.metrics.recidivism.metrics import (
    ReincarcerationRecidivismMetricType as MetricType,
)
from recidiviz.pipelines.metrics.recidivism.metrics import (
    ReincarcerationRecidivismRateMetric,
)
from recidiviz.pipelines.utils.state_utils.state_specific_metrics_producer_delegate import (
    StateSpecificMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.state_specific_recidivism_metrics_producer_delegate import (
    StateSpecificRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_recidivism_metrics_producer_delegate import (
    UsXxRecidivismMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_nd.us_nd_recidivism_metrics_producer_delegate import (
    UsNdRecidivismMetricsProducerDelegate,
)

_PIPELINE_JOB_ID = "TEST_JOB_ID"


class TestReincarcerations(unittest.TestCase):
    """Tests the reincarcerations() function in the metric_producer."""

    def setUp(self) -> None:
        self.metric_producer = metric_producer.RecidivismMetricProducer()

    def test_reincarcerations(self) -> None:
        release_date = date(2018, 1, 1)
        original_admission_date = release_date - relativedelta(years=4)
        reincarceration_date = release_date + relativedelta(years=3)
        second_release_date = reincarceration_date + relativedelta(years=1)

        first_event = RecidivismReleaseEvent(
            "US_XX",
            original_admission_date,
            release_date,
            "Sing Sing",
            reincarceration_date,
            "Sing Sing",
        )
        second_event = NonRecidivismReleaseEvent(
            "US_XX",
            reincarceration_date,
            second_release_date,
            "Sing Sing",
        )
        release_events: Dict[int, List[ReleaseEvent]] = {
            2018: [first_event],
            2022: [second_event],
        }

        expected_reincarcerations = {reincarceration_date: first_event}

        reincarcerations = self.metric_producer.reincarcerations(release_events)
        self.assertEqual(expected_reincarcerations, reincarcerations)

    def test_reincarcerations_two_releases_same_reincarceration(self) -> None:
        release_date = date(2018, 1, 1)
        original_admission_date = release_date - relativedelta(years=4)
        second_release_date = release_date + relativedelta(years=1)
        reincarceration_date = second_release_date + relativedelta(years=1)

        first_event = RecidivismReleaseEvent(
            "US_XX",
            original_admission_date,
            release_date,
            "Sing Sing",
            reincarceration_date,
            "Sing Sing",
        )
        second_event = RecidivismReleaseEvent(
            "US_XX",
            release_date,
            second_release_date,
            "Sing Sing",
            reincarceration_date,
            "Sing Sing",
        )
        release_events: Dict[int, List[ReleaseEvent]] = {
            2018: [first_event],
            2022: [second_event],
        }

        # Both release events have identified the same admission that can be counted as a valid reincarceration
        # The second event is prioritized because it has fewer days between release and reincarceration
        expected_reincarcerations = {reincarceration_date: second_event}

        reincarcerations = self.metric_producer.reincarcerations(release_events)
        self.assertEqual(expected_reincarcerations, reincarcerations)

    def test_reincarcerations_empty(self) -> None:
        reincarcerations = self.metric_producer.reincarcerations({})
        self.assertEqual({}, reincarcerations)


class TestReincarcerationsInWindow(unittest.TestCase):
    """Tests the reincarcerations_in_window() function in the metric_producer."""

    def setUp(self) -> None:
        self.metric_producer = metric_producer.RecidivismMetricProducer()

    def test_reincarcerations_in_window(self) -> None:
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
            )
            for return_date in return_dates
        }

        start_date = date(2016, 5, 13)

        reincarcerations = self.metric_producer.reincarcerations_in_window(
            start_date, start_date + relativedelta(years=6), all_reincarcerations
        )
        self.assertEqual(3, len(reincarcerations))

    def test_reincarcerations_in_window_all_early(self) -> None:
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
            )
            for return_date in return_dates
        }

        start_date = date(2026, 5, 13)

        reincarcerations = self.metric_producer.reincarcerations_in_window(
            start_date, start_date + relativedelta(years=6), all_reincarcerations
        )

        self.assertEqual([], reincarcerations)

    def test_reincarcerations_in_window_all_late(self) -> None:
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
            )
            for return_date in return_dates
        }

        start_date = date(2004, 5, 13)

        reincarcerations = self.metric_producer.reincarcerations_in_window(
            start_date, start_date + relativedelta(years=5), all_reincarcerations
        )

        self.assertEqual([], reincarcerations)


class TestStayLengthFromEvent(unittest.TestCase):
    """Tests the built-in stay_length function on the ReleaseEvent class."""

    def test_stay_length_from_event_earlier_month_and_date(self) -> None:
        original_admission_date = date(2013, 6, 17)
        release_date = date(2014, 4, 15)
        event = ReleaseEvent(
            "US_XX", original_admission_date, release_date, "Sing Sing"
        )

        self.assertEqual(9, event.stay_length)

    def test_stay_length_from_event_same_month_earlier_date(self) -> None:
        original_admission_date = date(2013, 6, 17)
        release_date = date(2014, 6, 16)
        event = ReleaseEvent("NH", original_admission_date, release_date, "Sing Sing")

        self.assertEqual(11, event.stay_length)

    def test_stay_length_from_event_same_month_same_date(self) -> None:
        original_admission_date = date(2013, 6, 17)
        release_date = date(2014, 6, 17)
        event = ReleaseEvent("TX", original_admission_date, release_date, "Sing Sing")

        self.assertEqual(12, event.stay_length)

    def test_stay_length_from_event_same_month_later_date(self) -> None:
        original_admission_date = date(2013, 6, 17)
        release_date = date(2014, 6, 18)
        event = ReleaseEvent("UT", original_admission_date, release_date, "Sing Sing")

        self.assertEqual(12, event.stay_length)

    def test_stay_length_from_event_later_month(self) -> None:
        original_admission_date = date(2013, 6, 17)
        release_date = date(2014, 8, 11)
        event = ReleaseEvent("HI", original_admission_date, release_date, "Sing Sing")

        self.assertEqual(13, event.stay_length)


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

    def test_stay_length_bucket(self) -> None:
        original_admission_date = date(1903, 6, 17)

        for months, bucket in self.months_to_bucket_map.items():
            release_date = original_admission_date + relativedelta(months=months)
            event = ReleaseEvent(
                "US_XX", original_admission_date, release_date, "Sing Sing"
            )
            self.assertEqual(bucket, event.stay_length_bucket)


_ALL_METRIC_INCLUSIONS = {MetricType.REINCARCERATION_RATE}


_DEFAULT_METRICS_PRODUCER_CLASS: Dict[str, StateSpecificMetricsProducerDelegate] = {
    StateSpecificRecidivismMetricsProducerDelegate.__name__: UsXxRecidivismMetricsProducerDelegate()
}


class TestProduceMetrics(unittest.TestCase):
    """Tests the produce_metrics function."""

    def setUp(self) -> None:
        self.metric_producer = metric_producer.RecidivismMetricProducer()
        self.pipeline_class = pipeline.RecidivismMetricsPipeline

    @staticmethod
    def expected_metric_counts(
        release_events_by_cohort: Dict[int, List[ReleaseEvent]]
    ) -> int:
        """Calculates the expected number of metrics given the release events."""
        all_release_events = [
            re for re_list in release_events_by_cohort.values() for re in re_list
        ]

        expected_rate_metrics = len(FOLLOW_UP_PERIODS) * len(all_release_events)

        return expected_rate_metrics

    @freeze_time("2100-01-01 00:00:00-05:00")
    def test_produce_recidivism_metrics(self) -> None:
        """Tests the produce_recidivism_metrics function where there is
        recidivism."""
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort: Dict[int, List[ReleaseEvent]] = {
            2008: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    date(2014, 5, 12),
                    "Upstate",
                )
            ]
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS,
            _PIPELINE_JOB_ID,
            _DEFAULT_METRICS_PRODUCER_CLASS,
        )

        expected_count = self.expected_metric_counts(release_events_by_cohort)

        self.assertEqual(expected_count, len(metrics))
        self.assertEqual(
            len(set(id(metric) for metric in metrics)),
            len(metrics),
        )

        for metric in metrics:
            if isinstance(metric, ReincarcerationRecidivismRateMetric):
                if metric.follow_up_period <= 5:
                    self.assertFalse(metric.did_recidivate)
                else:
                    self.assertTrue(metric.did_recidivate)

    def test_produce_recidivism_metrics_multiple_in_period(self) -> None:
        """Tests the produce_recidivism_metrics function where there are multiple instances of recidivism within a
        follow-up period."""
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.BLACK
        )
        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort: Dict[int, List[ReleaseEvent]] = {
            1908: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(1905, 7, 19),
                    date(1908, 9, 19),
                    "Hudson",
                    date(1910, 8, 12),
                    "Upstate",
                )
            ],
            1912: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(1910, 8, 12),
                    date(1912, 8, 19),
                    "Upstate",
                    date(1914, 7, 15),
                    "Sing Sing",
                )
            ],
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS,
            _PIPELINE_JOB_ID,
            _DEFAULT_METRICS_PRODUCER_CLASS,
        )

        # For the first event:
        #   For the first 5 periods:
        #       5 periods = 5 metrics
        #   For the second 5 periods, there is an additional return:
        #       5 periods * 2 returns = 10 metrics
        #
        # For the second event:
        #   10 periods = 10 metrics

        expected_count = 5 + 10 + 10

        # Multiplied by 2 to include the county of residence field
        assert len(metrics) == expected_count

        for metric in metrics:
            if isinstance(metric, ReincarcerationRecidivismRateMetric):
                if metric.follow_up_period < 2:
                    self.assertFalse(metric.did_recidivate)
                else:
                    self.assertTrue(metric.did_recidivate)

    def test_produce_recidivism_metrics_multiple_in_period_different_types(
        self,
    ) -> None:
        """Tests the produce_recidivism_metrics function where there are multiple instances of recidivism within a
        follow-up period with different return type information"""
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.BLACK
        )
        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort: Dict[int, List[ReleaseEvent]] = {
            1910: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(1905, 7, 19),
                    date(1910, 1, 1),
                    "Hudson",
                    date(1910, 1, 12),
                    "Upstate",
                ),
                RecidivismReleaseEvent(
                    "US_XX",
                    date(1910, 1, 12),
                    date(1910, 8, 19),
                    "Upstate",
                    date(1910, 10, 15),
                    "Sing Sing",
                ),
            ],
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS,
            _PIPELINE_JOB_ID,
            _DEFAULT_METRICS_PRODUCER_CLASS,
        )

        # For the first event:
        #   For all periods there is an additional return:
        #       10 periods * 2 event-based = 20 rate metrics
        #
        #
        # For the second event:
        #   10 periods = 10 rate metrics
        #

        expected_count = 20 + 10

        # Multiplied by 2 to include the county of residence field
        assert len(metrics) == expected_count

        for metric in metrics:
            if isinstance(metric, ReincarcerationRecidivismRateMetric):
                self.assertTrue(metric.did_recidivate)

    def test_produce_recidivism_metrics_multiple_releases_in_year(self) -> None:
        """Tests the produce_recidivism_metrics function where there are multiple releases in the same year."""
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.BLACK
        )
        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort: Dict[int, List[ReleaseEvent]] = {
            1908: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(1905, 7, 19),
                    date(1908, 1, 19),
                    "Hudson",
                    date(1908, 5, 12),
                    "Upstate",
                ),
                NonRecidivismReleaseEvent(
                    "US_XX",
                    date(1908, 5, 12),
                    date(1908, 8, 19),
                    "Upstate",
                ),
            ],
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS,
            _PIPELINE_JOB_ID,
            _DEFAULT_METRICS_PRODUCER_CLASS,
        )

        expected_count = self.expected_metric_counts(release_events_by_cohort)

        self.assertEqual(expected_count, len(metrics))
        self.assertEqual(
            len(set(id(metric) for metric in metrics)),
            len(metrics),
        )

        for metric in metrics:
            if isinstance(metric, ReincarcerationRecidivismRateMetric):
                if metric.release_date == date(1908, 8, 19):
                    self.assertFalse(metric.did_recidivate)
                else:
                    self.assertTrue(metric.did_recidivate)

    @freeze_time("2100-01-01 00:00:00-05:00")
    def test_produce_recidivism_metrics_multiple_releases_same_reincarceration(
        self,
    ) -> None:
        """Tests the produce_recidivism_metrics function where there are multiple releases that have identified the
        same admission as the next valid reincarceration."""
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.BLACK
        )
        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort: Dict[int, List[ReleaseEvent]] = {
            1908: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(1905, 7, 19),
                    date(1908, 9, 19),
                    "Hudson",
                    date(1913, 8, 12),
                    "Upstate",
                )
            ],
            1912: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(1908, 9, 19),
                    date(1912, 9, 19),
                    "Hudson",
                    date(1913, 8, 12),
                    "Upstate",
                )
            ],
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS,
            _PIPELINE_JOB_ID,
            _DEFAULT_METRICS_PRODUCER_CLASS,
        )

        # For the first event:
        #  10 periods = 10 rate metrics
        #
        # For the second event:
        #   10 periods = 10 rate metrics

        expected_count = 10 + 10

        assert len(metrics) == expected_count

        for metric in metrics:
            if isinstance(metric, ReincarcerationRecidivismRateMetric):
                if metric.release_cohort == 1908 and metric.follow_up_period < 5:
                    self.assertFalse(metric.did_recidivate)
                else:
                    self.assertTrue(metric.did_recidivate)

    @freeze_time("2100-01-01 00:00:00-05:00")
    def test_produce_recidivism_metrics_return_one_year_later(self) -> None:
        """Tests the produce_recidivism_metrics function where the person returned to prison exactly one year after
        they were released."""
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort: Dict[int, List[ReleaseEvent]] = {
            2008: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    date(2009, 9, 19),
                    "Upstate",
                )
            ]
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS,
            _PIPELINE_JOB_ID,
            _DEFAULT_METRICS_PRODUCER_CLASS,
        )

        expected_count = self.expected_metric_counts(release_events_by_cohort)

        self.assertEqual(expected_count, len(metrics))
        self.assertEqual(
            len(set(id(metric) for metric in metrics)),
            len(metrics),
        )

        for metric in metrics:
            if isinstance(metric, ReincarcerationRecidivismRateMetric):
                if metric.follow_up_period < 2:
                    self.assertFalse(metric.did_recidivate)
                else:
                    self.assertTrue(metric.did_recidivate)

    def test_produce_recidivism_metrics_no_recidivism(self) -> None:
        """Tests the produce_recidivism_metrics function where there is no
        recidivism."""
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.BLACK
        )
        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort: Dict[int, List[ReleaseEvent]] = {
            2008: [
                NonRecidivismReleaseEvent(
                    "US_XX",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                )
            ]
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS,
            _PIPELINE_JOB_ID,
            _DEFAULT_METRICS_PRODUCER_CLASS,
        )

        expected_count = self.expected_metric_counts(release_events_by_cohort)

        self.assertEqual(expected_count, len(metrics))
        self.assertEqual(
            len(set(id(metric) for metric in metrics)),
            len(metrics),
        )
        assert all(
            isinstance(metric, ReincarcerationRecidivismRateMetric)
            and not metric.did_recidivate
            for metric in metrics
        )

    def test_produce_recidivism_metrics_recidivated_after_last_period(self) -> None:
        """Tests the produce_recidivism_metrics function where there is
        recidivism but it occurred after the last follow-up period we track."""
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.BLACK
        )
        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort: Dict[int, List[ReleaseEvent]] = {
            1998: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(1995, 7, 19),
                    date(1998, 9, 19),
                    "Hudson",
                    date(2008, 10, 12),
                    "Upstate",
                )
            ]
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS,
            _PIPELINE_JOB_ID,
            _DEFAULT_METRICS_PRODUCER_CLASS,
        )

        expected_count = self.expected_metric_counts(release_events_by_cohort)

        self.assertEqual(expected_count, len(metrics))
        self.assertEqual(
            len(set(id(metric) for metric in metrics)),
            len(metrics),
        )

        assert all(
            not metric.did_recidivate
            for metric in metrics
            if isinstance(metric, ReincarcerationRecidivismRateMetric)
        )

    def test_produce_recidivism_metrics_multiple_races(self) -> None:
        """Tests the produce_recidivism_metrics function where there is
        recidivism, and the person has more than one race."""
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race_white = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
        )

        race_black = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=123345, race=StateRace.BLACK
        )

        person.races = [race_white, race_black]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort: Dict[int, List[ReleaseEvent]] = {
            2008: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    date(2014, 5, 12),
                    "Upstate",
                )
            ]
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS,
            _PIPELINE_JOB_ID,
            _DEFAULT_METRICS_PRODUCER_CLASS,
        )

        expected_count = self.expected_metric_counts(release_events_by_cohort)

        self.assertEqual(expected_count, len(metrics))
        self.assertEqual(
            len(set(id(metric) for metric in metrics)),
            len(metrics),
        )

        for metric in metrics:
            if isinstance(metric, ReincarcerationRecidivismRateMetric):
                if metric.follow_up_period <= 5:
                    self.assertFalse(metric.did_recidivate)
                else:
                    self.assertTrue(metric.did_recidivate)

    def test_produce_recidivism_metrics_multiple_ethnicities(self) -> None:
        """Tests the produce_recidivism_metrics function where there is
        recidivism, and the person has more than one ethnicity."""
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.BLACK
        )
        person.races = [race]

        ethnicity_hispanic = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.HISPANIC,
        )

        ethnicity_not_hispanic = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity_hispanic, ethnicity_not_hispanic]

        release_events_by_cohort: Dict[int, List[ReleaseEvent]] = {
            2008: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    date(2014, 5, 12),
                    "Upstate",
                )
            ]
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS,
            _PIPELINE_JOB_ID,
            _DEFAULT_METRICS_PRODUCER_CLASS,
        )

        expected_count = self.expected_metric_counts(release_events_by_cohort)

        self.assertEqual(expected_count, len(metrics))
        self.assertEqual(
            len(set(id(metric) for metric in metrics)),
            len(metrics),
        )

        for metric in metrics:
            if isinstance(metric, ReincarcerationRecidivismRateMetric):
                if metric.follow_up_period <= 5:
                    self.assertFalse(metric.did_recidivate)
                else:
                    self.assertTrue(metric.did_recidivate)

    def test_produce_recidivism_metrics_multiple_races_ethnicities(self) -> None:
        """Tests the produce_recidivism_metrics function where there is
        recidivism, and the person has multiple races and multiple
        ethnicities."""
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race_white = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
        )

        race_black = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=123345, race=StateRace.BLACK
        )

        person.races = [race_white, race_black]

        ethnicity_hispanic = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.HISPANIC,
        )

        ethnicity_not_hispanic = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity_hispanic, ethnicity_not_hispanic]

        release_events_by_cohort: Dict[int, List[ReleaseEvent]] = {
            2008: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    date(2014, 5, 12),
                    "Upstate",
                )
            ]
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS,
            _PIPELINE_JOB_ID,
            _DEFAULT_METRICS_PRODUCER_CLASS,
        )

        expected_count = self.expected_metric_counts(release_events_by_cohort)

        self.assertEqual(expected_count, len(metrics))
        self.assertEqual(
            len(set(id(metric) for metric in metrics)),
            len(metrics),
        )

        for metric in metrics:
            if isinstance(metric, ReincarcerationRecidivismRateMetric):
                if metric.follow_up_period <= 5:
                    self.assertFalse(metric.did_recidivate)
                else:
                    self.assertTrue(metric.did_recidivate)

    def test_produce_recidivism_metrics_revocation_parole(self) -> None:
        """Tests the produce_recidivism_metrics function where there is
        recidivism, and they returned from a revocation of parole."""
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort: Dict[int, List[ReleaseEvent]] = {
            2008: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    date(2014, 5, 12),
                    "Upstate",
                )
            ]
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS,
            _PIPELINE_JOB_ID,
            _DEFAULT_METRICS_PRODUCER_CLASS,
        )
        expected_count = self.expected_metric_counts(release_events_by_cohort)

        self.assertEqual(expected_count, len(metrics))
        self.assertEqual(
            len(set(id(metric) for metric in metrics)),
            len(metrics),
        )

        for metric in metrics:
            if isinstance(metric, ReincarcerationRecidivismRateMetric):
                if metric.follow_up_period <= 5:
                    self.assertFalse(metric.did_recidivate)
                else:
                    self.assertTrue(metric.did_recidivate)

    def test_produce_recidivism_metrics_revocation_probation(self) -> None:
        """Tests the produce_recidivism_metrics function where there is
        recidivism, and they returned from a revocation of parole."""
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort: Dict[int, List[ReleaseEvent]] = {
            2008: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    date(2014, 5, 12),
                    "Upstate",
                )
            ]
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS,
            _PIPELINE_JOB_ID,
            _DEFAULT_METRICS_PRODUCER_CLASS,
        )

        expected_count = self.expected_metric_counts(release_events_by_cohort)

        self.assertEqual(expected_count, len(metrics))
        self.assertEqual(
            len(set(id(metric) for metric in metrics)),
            len(metrics),
        )

        for metric in metrics:
            if isinstance(metric, ReincarcerationRecidivismRateMetric):
                if metric.follow_up_period <= 5:
                    self.assertFalse(metric.did_recidivate)
                else:
                    self.assertTrue(metric.did_recidivate)

    def test_produce_recidivism_metrics_technical_revocation_parole(self) -> None:
        """Tests the produce_recidivism_metrics function where there is
        recidivism, and they returned from a technical violation that resulted
        in the revocation of parole."""
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort: Dict[int, List[ReleaseEvent]] = {
            2008: [
                RecidivismReleaseEvent(
                    "US_XX",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    date(2014, 5, 12),
                    "Upstate",
                )
            ]
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS,
            _PIPELINE_JOB_ID,
            _DEFAULT_METRICS_PRODUCER_CLASS,
        )

        expected_count = self.expected_metric_counts(release_events_by_cohort)

        self.assertEqual(expected_count, len(metrics))
        self.assertEqual(
            len(set(id(metric) for metric in metrics)),
            len(metrics),
        )

        for metric in metrics:
            if isinstance(metric, ReincarcerationRecidivismRateMetric):
                if metric.follow_up_period <= 5:
                    self.assertFalse(metric.did_recidivate)
                else:
                    self.assertTrue(metric.did_recidivate)

    def test_produce_recidivism_metrics_count_metric_no_recidivism(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
        )

        race = NormalizedStatePersonRace(
            state_code="US_XX", person_race_id=12345, race=StateRace.WHITE
        )

        person.races = [race]

        ethnicity = NormalizedStatePersonEthnicity(
            state_code="US_XX",
            person_ethnicity_id=12345,
            ethnicity=StateEthnicity.NOT_HISPANIC,
        )

        person.ethnicities = [ethnicity]

        release_events_by_cohort: Dict[int, List[ReleaseEvent]] = {
            2008: [
                NonRecidivismReleaseEvent(
                    "US_XX",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                )
            ]
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS,
            _PIPELINE_JOB_ID,
            _DEFAULT_METRICS_PRODUCER_CLASS,
        )

        assert all(
            isinstance(metric, ReincarcerationRecidivismRateMetric)
            and not metric.did_recidivate
            for metric in metrics
        )

    @freeze_time("2100-01-01 00:00:00-05:00")
    def test_produce_recidivism_metrics_external_id(self) -> None:
        """Tests the produce_recidivism_metrics function where there is
        recidivism."""
        person = NormalizedStatePerson(
            state_code="US_ND",
            person_id=12345,
            birthdate=date(1984, 8, 31),
            gender=StateGender.FEMALE,
            races=[
                NormalizedStatePersonRace(
                    state_code="US_ND", person_race_id=12345, race=StateRace.WHITE
                )
            ],
            ethnicities=[
                NormalizedStatePersonEthnicity(
                    state_code="US_ND",
                    person_ethnicity_id=12345,
                    ethnicity=StateEthnicity.NOT_HISPANIC,
                )
            ],
            external_ids=[
                NormalizedStatePersonExternalId(
                    state_code="US_ND",
                    person_external_id_id=12345,
                    external_id="ABC",
                    id_type=US_ND_ELITE,
                    is_current_display_id_for_type=True,
                    id_active_from_datetime=datetime.datetime(2020, 1, 1),
                    id_active_to_datetime=None,
                )
            ],
        )

        release_events_by_cohort: Dict[int, List[ReleaseEvent]] = {
            2008: [
                RecidivismReleaseEvent(
                    "US_ND",
                    date(2005, 7, 19),
                    date(2008, 9, 19),
                    "Hudson",
                    date(2014, 5, 12),
                    "Upstate",
                )
            ]
        }

        metrics = self.metric_producer.produce_metrics(
            person,
            release_events_by_cohort,
            _ALL_METRIC_INCLUSIONS,
            _PIPELINE_JOB_ID,
            metrics_producer_delegates={
                StateSpecificRecidivismMetricsProducerDelegate.__name__: UsNdRecidivismMetricsProducerDelegate()
            },
        )

        expected_count = self.expected_metric_counts(release_events_by_cohort)

        self.assertEqual(expected_count, len(metrics))
        self.assertEqual(
            len(set(id(metric) for metric in metrics)),
            len(metrics),
        )

        for metric in metrics:
            self.assertEqual("ABC", metric.person_external_id)


class TestReincarcerationsByPeriod(unittest.TestCase):
    """Tests the reincarcerations_by_period function."""

    def setUp(self) -> None:
        self.metric_producer = metric_producer.RecidivismMetricProducer()

    def test_reincarcerations_by_period(self) -> None:
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

        reincarcerations_by_period = self.metric_producer.reincarcerations_by_period(
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

    def test_reincarcerations_by_period_no_recidivism(self) -> None:
        reincarcerations_by_period = self.metric_producer.reincarcerations_by_period(
            date(1908, 9, 19), {}
        )

        expected_output: Dict[int, List] = {year: [] for year in range(1, 11)}

        self.assertEqual(expected_output, reincarcerations_by_period)

    @freeze_time("2000-01-01 00:00:00-05:00")
    def test_reincarcerations_by_period_only_two_periods(self) -> None:
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

        reincarcerations_by_period = self.metric_producer.reincarcerations_by_period(
            date(1998, 9, 19), all_reincarcerations
        )

        expected_output = {year: [first_return, second_return] for year in range(1, 3)}

        self.assertEqual(expected_output, reincarcerations_by_period)

    def test_relevant_follow_up_periods(self) -> None:
        today = date(2018, 1, 26)

        assert self.metric_producer.relevant_follow_up_periods(
            date(2015, 1, 5), today, metric_producer.FOLLOW_UP_PERIODS
        ) == [1, 2, 3, 4]
        assert self.metric_producer.relevant_follow_up_periods(
            date(2015, 1, 26), today, metric_producer.FOLLOW_UP_PERIODS
        ) == [1, 2, 3, 4]
        assert self.metric_producer.relevant_follow_up_periods(
            date(2015, 1, 27), today, metric_producer.FOLLOW_UP_PERIODS
        ) == [1, 2, 3]
        assert self.metric_producer.relevant_follow_up_periods(
            date(2016, 1, 5), today, metric_producer.FOLLOW_UP_PERIODS
        ) == [1, 2, 3]
        assert self.metric_producer.relevant_follow_up_periods(
            date(2017, 4, 10), today, metric_producer.FOLLOW_UP_PERIODS
        ) == [1]
        assert self.metric_producer.relevant_follow_up_periods(
            date(2018, 1, 5), today, metric_producer.FOLLOW_UP_PERIODS
        ) == [1]
        assert (
            self.metric_producer.relevant_follow_up_periods(
                date(2018, 2, 5), today, metric_producer.FOLLOW_UP_PERIODS
            )
            == []
        )
