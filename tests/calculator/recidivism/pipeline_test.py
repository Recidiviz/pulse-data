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
from google.appengine.ext import ndb
from google.appengine.ext import testbed
from mapreduce import context
from mapreduce import model as mapreduce_model
from mapreduce import operation as op

from tests.context import calculator
from calculator.recidivism import (calculator, metrics, pipeline,
                                   recidivism_event)
from ingest.us_ny.us_ny_record import UsNyRecord
from models.inmate import Inmate
from models.snapshot import Snapshot


class TestMapReduceMethods(object):
    """Tests for the MapReduce methods in the class."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_map_inmate(self):
        """Tests the map_inmate function happy path."""
        set_pipeline_context({})

        inmate = Inmate(id="test-inmate", birthday=date(1987, 2, 24),
                        race="black", sex="male")
        inmate.put()

        initial_incarceration = record(inmate.key, True, date(2008, 11, 20),
                                       date(2010, 12, 4))
        snapshot(initial_incarceration.key, datetime(2009, 6, 17), "Sing Sing")
        snapshot(initial_incarceration.key, datetime(2010, 10, 17),
                 "Adirondack")

        first_reincarceration = record(inmate.key, True, date(2011, 4, 5),
                                       date(2014, 4, 14))
        snapshot(first_reincarceration.key, datetime(2012, 10, 15),
                 "Adirondack")
        snapshot(first_reincarceration.key, datetime(2013, 10, 15), "Upstate")

        subsequent_reincarceration = record(inmate.key, False, date(2017, 1, 4))
        snapshot(subsequent_reincarceration.key, datetime(2017, 10, 15),
                 "Downstate")

        result_generator = pipeline.map_inmate(inmate)
        total_combinations_2010 = 0
        total_combinations_2014 = 0

        # The method yields up counters that we increment,
        # and yields up (combination, value) tuples for reduction.
        for result in result_generator:
            if isinstance(result, op.counters.Increment):
                assert result.counter_name in [
                    'total_metric_combinations_mapped', 'total_inmates_mapped']
            else:
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

        # Get the number of combinations of inmate-event characteristics.
        original_entry_date = date(2013, 6, 17)
        event = recidivism_event.RecidivismEvent(False, original_entry_date,
                                                 None, "Sing Sing")
        num_combinations = len(calculator.characteristic_combinations(
            inmate, event))
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

    def test_map_inmates_no_results(self):
        """Tests the map_inmate function when the inmate has no records."""
        set_pipeline_context({})

        inmate = Inmate(id="test-inmate", birthday=date(1987, 2, 24),
                        race="black", sex="male")
        inmate.put()

        result_generator = pipeline.map_inmate(inmate)

        total_results = 0
        for result in result_generator:
            total_results += 1
            assert isinstance(result, op.counters.Increment)
            assert result.counter_name == 'total_inmates_mapped'

        assert total_results == 1

    def test_reduce_recidivism_events(self):
        """Tests the reduce_recidivism_events function happy path."""
        set_pipeline_context({})

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
            if isinstance(result, op.counters.Increment):
                assert result.counter_name in ['unique_metric_keys_reduced',
                                               'total_records_reduced',
                                               'total_recidivisms_reduced']
            else:
                expected = metrics.RecidivismMetric(
                    release_cohort=2010, follow_up_period=4, age_bucket='<25',
                    stay_length_bucket='12-24', sex='male', race='black',
                    release_facility='Adirondack', methodology='OFFENDER',
                    execution_id='some-id', total_records=4,
                    total_recidivism=2.0, recidivism_rate=0.5)

                assert result.entity == expected

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
            if isinstance(result, op.counters.Increment):
                assert result.counter_name in ['unique_metric_keys_reduced',
                                               'total_records_reduced',
                                               'total_recidivisms_reduced']
            else:
                expected = metrics.RecidivismMetric(
                    release_cohort=2010, follow_up_period=4, age_bucket='<25',
                    stay_length_bucket='12-24', sex='male', race='black',
                    release_facility='Adirondack', methodology='EVENT',
                    execution_id='some-id', total_records=10,
                    total_recidivism=4, recidivism_rate=0.4)
                assert result.entity == expected

    def test_reduce_recidivism_events_no_values(self):
        """Tests the reduce_recidivism_events function when there are no
        values provided alongside the metric key.

        This should not happen in
        the GAE MapReduce framework, but we test against it anyway.
        """
        set_pipeline_context({})

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


def set_pipeline_context(params):
    mapper_spec_json = mapreduce_model.MapperSpec(
        "handler", "input", params, 8).to_json()
    mapreduce_spec = mapreduce_model.MapreduceSpec(
        "test-mr-pipeline", "some-id", mapper_spec_json)

    pipeline_context = context.Context(mapreduce_spec, None)
    pipeline_context._set(pipeline_context)  # pylint: disable=protected-access


def record(parent_key, is_released, custody_date, latest_release_date=None):
    new_record = UsNyRecord(parent=parent_key,
                            is_released=is_released,
                            custody_date=custody_date,
                            latest_release_date=latest_release_date)
    new_record.put()
    return new_record


def snapshot(parent_key, snapshot_date, facility):
    new_snapshot = Snapshot(parent=parent_key,
                            created_on=snapshot_date,
                            latest_facility=facility)
    new_snapshot.put()
    return new_snapshot
