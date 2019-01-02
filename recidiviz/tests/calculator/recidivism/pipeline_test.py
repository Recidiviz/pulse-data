# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

"""Tests for recidivism/pipeline.py."""


from datetime import date
from datetime import datetime

import pytest

from dateutil.relativedelta import relativedelta

from recidiviz.tests.context import calculator
from recidiviz.calculator.recidivism import (calculator, metrics, pipeline,
                                             recidivism_event)


class TestMapReduceMethods:
    """Tests for the MapReduce methods in the class."""

    def test_map_person(self):
        """Tests the map_person function happy path."""
        person = FakePerson(key="test-person", birthdate=date(1987, 2, 24),
                            race="black", sex="male")

        initial_incarceration = FakeRecord(
            key="initial", is_released=True, custody_date=date(2008, 11, 20),
            latest_release_date=date(2010, 12, 4))
        initial_incarceration_first_snapshot = FakeSnapshot(
            initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")
        initial_incarceration_second_snapshot = FakeSnapshot(
            initial_incarceration.key, datetime(2010, 10, 17), "Adirondack")

        first_reincarceration = FakeRecord(
            key="first_re", is_released=True, custody_date=date(2011, 4, 5),
            latest_release_date=date(2014, 4, 14))
        first_reincarceration_first_snapshot = FakeSnapshot(
            first_reincarceration.key, datetime(2012, 10, 15), "Adirondack")
        first_reincarceration_second_snapshot = FakeSnapshot(
            first_reincarceration.key, datetime(2013, 10, 15), "Upstate")

        subsequent_reincarceration = FakeRecord(
            "subsequent_re", is_released=False, custody_date=date(2017, 1, 4))
        subsequent_reincarceration_snapshot = FakeSnapshot(
            subsequent_reincarceration.key, datetime(2017, 10, 15), "Downstate")

        result_generator = pipeline.map_person(
            person, records=[
                initial_incarceration,
                first_reincarceration,
                subsequent_reincarceration,
            ], snapshots=[
                subsequent_reincarceration_snapshot,
                first_reincarceration_second_snapshot,
                first_reincarceration_first_snapshot,
                initial_incarceration_second_snapshot,
                initial_incarceration_first_snapshot,
            ]
        )
        total_combinations_2010 = 0
        total_combinations_2014 = 0

        # The method yields up counters that we increment,
        # and yields up (combination, value) tuples for reduction.
        for result in result_generator:
            combination, value = result

            # The value is always 1, because recidivism occurred
            # within the first year of release.
            if combination['release_cohort'] == 2010:
                total_combinations_2010 += 1
                assert value == 1

            # This is the last instance of recidivism
            # and it occurs at the 3 year mark.
            elif combination['release_cohort'] == 2014:
                total_combinations_2014 += 1
                if combination['follow_up_period'] < 3:
                    assert value == 0
                else:
                    assert value == 1

        # Get the number of combinations of person-event characteristics.
        original_entry_date = date(2013, 6, 17)
        event = recidivism_event.RecidivismEvent(False, original_entry_date,
                                                 None, "Sing Sing")
        num_combinations = len(calculator.characteristic_combinations(
            person, event))
        assert num_combinations > 0

        # num_combinations * 2 methodologies * 10 periods = 320 combinations
        # for the 2010 release cohort over all periods into the future. But we
        # do not track metrics for periods that start after today, so we need to
        # subtract for some number of periods that go beyond whatever today is.
        periods = relativedelta(date.today(), date(2010, 12, 4)).years + 1
        periods_with_single = 6
        periods_with_double = periods - periods_with_single
        assert (total_combinations_2010 ==
                (num_combinations * 2 * periods_with_single) +
                (num_combinations * 3 * periods_with_double))

        # num_combinations * 2 methodologies * 5 periods = 160 combinations
        # for the 2014 release cohort over all periods into the future. Same
        # deal here as previous.
        periods = relativedelta(date.today(), date(2014, 4, 14)).years + 1
        assert total_combinations_2014 == num_combinations * 2 * periods

    def test_map_persons_no_results(self):
        """Tests the map_person function when the person has no records."""
        person = FakePerson(key="test-person", birthdate=date(1987, 2, 24),
                            race="black", sex="male")

        result_generator = pipeline.map_person(person, records=[], snapshots=[])

        total_results = len(list(result_generator))

        assert total_results == 0

    def test_reduce_recidivism_events(self):
        """Tests the reduce_recidivism_events function happy path."""
        metric_key_offender = "{'follow_up_period': 4, " \
                              "'age': '<25', " \
                              "'stay_length': '12-24', " \
                              "'sex': u'male', " \
                              "'methodology': 'OFFENDER', " \
                              "'race': u'black', " \
                              "'release_facility': u'Adirondack', " \
                              "'release_cohort': 2010}"

        offender_result_generator = pipeline.reduce_recidivism_events(
            metric_key_offender, [0.5, 0.0, 0.5, 1.0])

        for result in offender_result_generator:
            expected = metrics.RecidivismMetric(
                release_cohort=2010, follow_up_period=4, age_bucket='<25',
                stay_length_bucket='12-24', sex='male', race='black',
                release_facility='Adirondack', methodology='OFFENDER',
                execution_id=pipeline.EXECUTION_ID, total_records=4,
                total_recidivism=2.0, recidivism_rate=0.5)
            assert result.__dict__ == expected.__dict__

        metric_key_event = "{'follow_up_period': 4, " \
                           "'age': '<25', " \
                           "'stay_length': '12-24', " \
                           "'sex': u'male', " \
                           "'methodology': 'EVENT', " \
                           "'race': u'black', " \
                           "'release_facility': u'Adirondack', " \
                           "'release_cohort': 2010}"

        event_result_generator = pipeline.reduce_recidivism_events(
            metric_key_event, [1, 1, 0, 0, 0, 1, 1, 0, 0, 0])

        for result in event_result_generator:
            expected = metrics.RecidivismMetric(
                release_cohort=2010, follow_up_period=4, age_bucket='<25',
                stay_length_bucket='12-24', sex='male', race='black',
                release_facility='Adirondack', methodology='EVENT',
                execution_id=pipeline.EXECUTION_ID, total_records=10,
                total_recidivism=4, recidivism_rate=0.4)
            assert result.__dict__ == expected.__dict__

    def test_reduce_recidivism_events_no_values(self):
        """Tests the reduce_recidivism_events function when there are no
        values provided alongside the metric key.

        This should not happen in
        the GAE MapReduce framework, but we test against it anyway.
        """
        metric_key_offender = "{'follow_up_period': 4, " \
                              "'age': '<25', " \
                              "'stay_length': '12-24', " \
                              "'sex': u'male', " \
                              "'methodology': 'OFFENDER', " \
                              "'race': u'black', " \
                              "'release_facility': u'Adirondack', " \
                              "'release_cohort': 2010}"

        offender_result_generator = pipeline.reduce_recidivism_events(
            metric_key_offender, [])

        total_results = 0
        for _ in offender_result_generator:
            total_results += 1
        assert not total_results


class FakePerson:
    def __init__(self, key=None, birthdate=None, race=None, sex=None):
        self.key = key
        self.birthdate = birthdate
        self.race = race
        self.sex = sex


class FakeRecord:
    def __init__(self, key=None, is_released=None, custody_date=None,
                 last_custody_date=None, latest_release_date=None,
                 latest_facility=None, created_on=None):
        self.key = key
        self.is_released = is_released
        self.custody_date = custody_date
        self.last_custody_date = last_custody_date
        self.latest_release_date = latest_release_date
        self.latest_facility = latest_facility
        self.created_on = created_on


class FakeSnapshot:
    def __init__(self, parent_key=None, created_on=None, latest_facility=None):
        self.parent = parent_key
        self.created_on = created_on
        self.latest_facility = latest_facility
