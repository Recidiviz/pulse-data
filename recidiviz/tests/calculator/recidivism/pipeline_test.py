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

"""Tests for recidivism/pipeline.py."""

import unittest

import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to, BeamAssertException
from apache_beam.testing.test_pipeline import TestPipeline

from datetime import date
from dateutil.relativedelta import relativedelta

from recidiviz.calculator.recidivism import pipeline
from recidiviz.calculator.recidivism import calculator, \
    RecidivismEvent, RecidivismMetric
from recidiviz.calculator.recidivism.metrics import Methodology
from recidiviz.calculator.recidivism.recidivism_event import \
    IncarcerationReturnType
from recidiviz.common.constants.entity_enum import EntityEnum
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, \
    StateIncarcerationFacilitySecurityLevel
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.persistence.entity.state.entities import StatePerson, \
    StateIncarcerationPeriod, Gender, ResidencyStatus


class TestRecidivismPipeline(unittest.TestCase):
    """Tests the entire recidivism pipeline."""

    def testRecidivismPipeline(self):
        """Tests the entire recidivism pipeline with one person and three
        incarceration periods."""

        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        persons_data = [NormalizeEntityDict.normalize(fake_person.__dict__)]

        initial_incarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_person_id=fake_person_id,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='CA',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.
            MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.
            CONDITIONAL_RELEASE,
            admission_date=date(2008, 11, 20),
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        first_reincarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            state_person_id=fake_person_id,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='CA',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.
            MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.
            CONDITIONAL_RELEASE,
            admission_date=date(2011, 4, 5),
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        subsequent_reincarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            state_person_id=fake_person_id,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='CA',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.
            MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.
            CONDITIONAL_RELEASE,
            admission_date=date(2017, 1, 4))

        incarceration_periods_data = [
            NormalizeEntityDict.normalize(initial_incarceration.__dict__),
            NormalizeEntityDict.normalize(first_reincarceration.__dict__),
            NormalizeEntityDict.normalize(subsequent_reincarceration.__dict__)
        ]

        dimensions_to_filter = ['age_bucket', 'race', 'release_facility',
                                'stay_length_bucket']

        test_pipeline = TestPipeline()

        # Read persons from input and load them into StatePerson entities
        # Read persons from input and load them into StatePerson entities
        persons = (test_pipeline
                   | 'Read Persons' >> beam.Create(persons_data)
                   | 'Load StatePerson entities' >> beam.ParDo(
                       pipeline.ExtractPerson()))

        # Read incarceration periods from input and load them into
        # StateIncarcerationPeriod entities
        incarceration_periods = (test_pipeline
                                 | 'Read Incarceration Periods' >>
                                 beam.Create(incarceration_periods_data)
                                 | 'Load StateIncarcerationPeriod entities' >>
                                 beam.ParDo(
                                     pipeline.ExtractIncarcerationPeriod()))

        # Group each StatePerson with their StateIncarcerationPeriods
        person_and_incarceration_periods = \
            ({'person': persons,
              'incarceration_periods': incarceration_periods}
             | 'Group StatePerson to StateIncarcerationPeriods' >>
             beam.CoGroupByKey()
             )

        find_recidivism_kwargs = {'include_revocation_returns': True}

        # Find recidivism events from the StatePerson's
        # StateIncarcerationPeriods
        recidivism_events = (person_and_incarceration_periods
                             | 'Find Recidivism Events' >>
                             beam.ParDo(pipeline.FindRecidivism(),
                                        **find_recidivism_kwargs))

        # Group each StatePerson with their RecidivismEvents
        person_events = ({'person': persons,
                          'recidivism_events': recidivism_events}
                         | 'Group StatePerson to RecidivismEvents' >>
                         beam.CoGroupByKey())

        # Map each person and their events to the combinations of metrics
        # Note: We are preventing fusion here by having a GroupByKey in between
        # this high fan-out ParDo and the metric ParDo
        mapped_metric_combinations = (person_events
                                      | 'Map to metric combinations' >>
                                      beam.ParDo(
                                          pipeline.MapMetricCombinations())
                                      )

        # Group metrics that have the same metric_key
        grouped_metric_combinations = (mapped_metric_combinations
                                       | 'Group metrics by metric key' >>
                                       beam.GroupByKey())

        # Build RecidivismMetric objects
        metrics = (grouped_metric_combinations
                   | 'Produce recidivism metrics' >>
                   beam.ParDo(pipeline.ProduceRecidivismMetric()))

        filter_metrics_kwargs = {
            'dimensions_to_filter_out': dimensions_to_filter,
            'methodology': Methodology.BOTH,
            'release_count_min': 0}

        # Filter out unneeded metrics
        filtered_metrics = (metrics
                            | 'Filter out unwanted metrics' >>
                            beam.ParDo(pipeline.FilterMetrics(),
                                       **filter_metrics_kwargs))

        assert_that(filtered_metrics, AssertMatchers.validate_pipeline_test())

        test_pipeline.run()

    def testRecidivismPipeline_WithConditionalReturns(self):
        """Tests the entire RecidivismPipeline with two person and three
        incarceration periods each. One StatePerson has a return from a
        supervision violation.
        """

        fake_person_id_1 = 12345

        fake_person_1 = StatePerson.new_with_defaults(
            person_id=fake_person_id_1, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        fake_person_id_2 = 6789

        fake_person_2 = StatePerson.new_with_defaults(
            person_id=fake_person_id_2, gender=Gender.FEMALE,
            birthdate=date(1990, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        persons_data = [NormalizeEntityDict.normalize(fake_person_1.__dict__),
                        NormalizeEntityDict.normalize(fake_person_2.__dict__)]

        initial_incarceration_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_person_id=fake_person_id_1,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='CA',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.
            MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.
            CONDITIONAL_RELEASE,
            admission_date=date(2008, 11, 20),
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        first_reincarceration_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            state_person_id=fake_person_id_1,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='CA',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.
            MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.
            CONDITIONAL_RELEASE,
            admission_date=date(2011, 4, 5),
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        subsequent_reincarceration_1 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=3333,
                state_person_id=fake_person_id_1,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='CA',
                county_code='124',
                facility='San Quentin',
                facility_security_level=StateIncarcerationFacilitySecurityLevel.
                MAXIMUM,
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                projected_release_reason=StateIncarcerationPeriodReleaseReason.
                CONDITIONAL_RELEASE,
                admission_date=date(2017, 1, 4))

        initial_incarceration_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=4444,
            state_person_id=fake_person_id_2,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='CA',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.
            MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.
            CONDITIONAL_RELEASE,
            admission_date=date(2004, 12, 20),
            release_date=date(2010, 6, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.
            CONDITIONAL_RELEASE)

        first_reincarceration_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=5555,
            state_person_id=fake_person_id_2,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='CA',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.
            MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            PAROLE_REVOCATION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.
            CONDITIONAL_RELEASE,
            admission_date=date(2011, 4, 5),
            release_date=date(2014, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        subsequent_reincarceration_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=6666,
                state_person_id=fake_person_id_2,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='CA',
                county_code='124',
                facility='San Quentin',
                facility_security_level=StateIncarcerationFacilitySecurityLevel.
                MAXIMUM,
                admission_reason=StateIncarcerationPeriodAdmissionReason.
                NEW_ADMISSION,
                projected_release_reason=StateIncarcerationPeriodReleaseReason.
                CONDITIONAL_RELEASE,
                admission_date=date(2018, 3, 9))

        incarceration_periods_data = [
            NormalizeEntityDict.normalize(initial_incarceration_1.__dict__),
            NormalizeEntityDict.normalize(first_reincarceration_1.__dict__),
            NormalizeEntityDict.normalize(
                subsequent_reincarceration_1.__dict__),
            NormalizeEntityDict.normalize(initial_incarceration_2.__dict__),
            NormalizeEntityDict.normalize(first_reincarceration_2.__dict__),
            NormalizeEntityDict.normalize(
                subsequent_reincarceration_2.__dict__)
        ]

        dimensions_to_filter = ['age_bucket', 'race', 'release_facility',
                                'stay_length_bucket']

        test_pipeline = TestPipeline()

        # Read persons from input and load them into StatePerson entities
        # Read persons from input and load them into StatePerson entities
        persons = (test_pipeline
                   | 'Read Persons' >> beam.Create(persons_data)
                   | 'Load StatePerson entities' >> beam.ParDo(
                       pipeline.ExtractPerson()))

        # Read incarceration periods from input and load them into
        # StateIncarcerationPeriod entities
        incarceration_periods = (test_pipeline
                                 | 'Read Incarceration Periods' >>
                                 beam.Create(incarceration_periods_data)
                                 | 'Load StateIncarcerationPeriod entities' >>
                                 beam.ParDo(
                                     pipeline.ExtractIncarcerationPeriod()))

        # Group each StatePerson with their StateIncarcerationPeriods
        person_and_incarceration_periods = \
            ({'person': persons,
              'incarceration_periods': incarceration_periods}
             | 'Group StatePerson to StateIncarcerationPeriods' >>
             beam.CoGroupByKey()
             )

        find_recidivism_kwargs = {'include_revocation_returns': True}

        # Find recidivism events from the StatePerson's
        # StateIncarcerationPeriods
        recidivism_events = (person_and_incarceration_periods
                             | 'Find Recidivism Events' >>
                             beam.ParDo(pipeline.FindRecidivism(),
                                        **find_recidivism_kwargs))

        # Group each StatePerson with their RecidivismEvents
        person_events = ({'person': persons,
                          'recidivism_events': recidivism_events}
                         | 'Group StatePerson to RecidivismEvents' >>
                         beam.CoGroupByKey())

        # Map each person and their events to the combinations of metrics
        # Note: We are preventing fusion here by having a GroupByKey in between
        # this high fan-out ParDo and the metric ParDo
        mapped_metric_combinations = (person_events
                                      | 'Map to metric combinations' >>
                                      beam.ParDo(
                                          pipeline.MapMetricCombinations())
                                      )

        # Group metrics that have the same metric_key
        grouped_metric_combinations = (mapped_metric_combinations
                                       | 'Group metrics by metric key' >>
                                       beam.GroupByKey())

        # Build RecidivismMetric objects
        metrics = (grouped_metric_combinations
                   | 'Produce recidivism metrics' >>
                   beam.ParDo(pipeline.ProduceRecidivismMetric()))

        filter_metrics_kwargs = {
            'dimensions_to_filter_out': dimensions_to_filter,
            'methodology': Methodology.BOTH,
            'release_count_min': 0}

        # Filter out unneeded metrics
        filtered_metrics = (metrics
                            | 'Filter out unwanted metrics' >>
                            beam.ParDo(pipeline.FilterMetrics(),
                                       **filter_metrics_kwargs))

        assert_that(filtered_metrics, AssertMatchers.validate_pipeline_test())

        test_pipeline.run()


class TestExtractPerson(unittest.TestCase):
    """Tests for the ExtractPerson DoFn."""

    def testExtractPerson(self):
        """Tests the extraction of a valid StatePerson entity."""

        fake_person = StatePerson.new_with_defaults(
            person_id=12345, current_address='123 Street',
            full_name='Jack Smith', birthdate=date(1970, 1, 1),
            gender=Gender.MALE,
            residency_status=ResidencyStatus.PERMANENT, aliases=[])

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([
                      NormalizeEntityDict.normalize(fake_person.__dict__)])
                  | 'Load StatePerson entity' >>
                  beam.ParDo(pipeline.ExtractPerson())
                  )

        assert_that(output, equal_to([(12345, fake_person)]))

        test_pipeline.run()

    def testExtractPerson_EmptyDict(self):
        """Tests the extraction of a StatePerson entity when the dictionary is
        empty."""

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([{}])
                  | 'Load StatePerson entity' >>
                  beam.ParDo(pipeline.ExtractPerson())
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testExtractPerson_NoPersonID(self):
        """Tests the extraction of a StatePerson entity when there is no
        person_id given."""

        fake_person = StatePerson.new_with_defaults(
            current_address='123 Street',
            full_name='Jack Smith', birthdate=date(1970, 1, 1),
            gender=Gender.MALE,
            residency_status=ResidencyStatus.PERMANENT, aliases=[])

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([
                      NormalizeEntityDict.normalize(fake_person.__dict__)])
                  | 'Load StatePerson entity' >>
                  beam.ParDo(pipeline.ExtractPerson())
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class TestExtractIncarcerationPeriod(unittest.TestCase):
    """Tests for the ExtractIncarcerationPeriod DoFn."""

    def testExtractIncarcerationPeriod(self):
        """Tests the extraction of a valid StateIncarcerationPeriod entity."""

        fake_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=12345,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2001, 1, 1),
            state_code='CA',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.
            MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.
            CONDITIONAL_RELEASE,
            state_person_id=6789)

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([
                      NormalizeEntityDict.normalize(
                          fake_incarceration_period.__dict__)
                  ])
                  | 'Load StateIncarcerationPeriod entity' >>
                  beam.ParDo(pipeline.ExtractIncarcerationPeriod())
                  )

        assert_that(output,
                    equal_to([(6789, fake_incarceration_period)]))

        test_pipeline.run()

    def testExtractIncarcerationPeriod_NoStatePersonID(self):
        """Tests the extraction of a StateIncarcerationPeriod entity when
        there is no state_person_id present."""

        fake_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=12345,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2001, 1, 1),
            state_code='CA',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.
            MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.
            CONDITIONAL_RELEASE)

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([
                      NormalizeEntityDict.normalize(
                          fake_incarceration_period.__dict__)
                  ])
                  | 'Load StateIncarcerationPeriod entity' >>
                  beam.ParDo(pipeline.ExtractIncarcerationPeriod())
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testExtractIncarcerationPeriod_EmptyDict(self):
        """Tests the extraction of a StateIncarcerationPeriod entity when the
        dictionary is empty."""

        test_pipeline = TestPipeline()
        output = (test_pipeline
                  | beam.Create([{}])
                  | 'Load StateIncarcerationPeriod entity' >>
                  beam.ParDo(pipeline.ExtractIncarcerationPeriod())
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class TestFindRecidivism(unittest.TestCase):
    """Tests the FindRecidivism DoFn in the pipeline."""

    def testFindRecidivism(self):
        """Tests the FindRecidivism DoFn when there are two instances of
        recidivism."""

        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        initial_incarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_person_id=fake_person_id,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            admission_date=date(2008, 11, 20),
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        first_reincarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            state_person_id=fake_person_id,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            admission_date=date(2011, 4, 5),
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        subsequent_reincarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            state_person_id=fake_person_id,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='TX',
            admission_date=date(2017, 1, 4))

        person_incarceration_periods = {'person': fake_person,
                                        'incarceration_periods': [
                                            initial_incarceration,
                                            first_reincarceration,
                                            subsequent_reincarceration]}

        first_recidivism_event = RecidivismEvent(
            recidivated=True,
            original_admission_date=initial_incarceration.admission_date,
            release_date=initial_incarceration.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration.admission_date,
            reincarceration_facility=None,
            return_type=IncarcerationReturnType.RECONVICTION)

        second_recidivism_event = RecidivismEvent(
            recidivated=True,
            original_admission_date=first_reincarceration.admission_date,
            release_date=first_reincarceration.release_date,
            release_facility=None,
            reincarceration_date=subsequent_reincarceration.admission_date,
            reincarceration_facility=None,
            return_type=IncarcerationReturnType.RECONVICTION)

        correct_output = [
            (fake_person_id, {initial_incarceration.release_date.year:
                                  [first_recidivism_event],
                              first_reincarceration.release_date.year:
                                  [second_recidivism_event]})]

        find_recidivism_kwargs = {'include_revocation_returns': True}

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(fake_person_id,
                                  person_incarceration_periods)])
                  | 'Find Recidivism' >>
                  beam.ParDo(pipeline.FindRecidivism(),
                             **find_recidivism_kwargs)
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testFindRecidivism_NoRecidivism(self):
        """Tests the FindRecidivism DoFn in the pipeline when there is no
        instance of recidivism."""

        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        only_incarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111, state_person_id=fake_person_id,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX', admission_date=date(2008, 11, 20),
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        person_incarceration_periods = {'person': fake_person,
                                        'incarceration_periods':
                                            [only_incarceration]}

        non_recidivism_event = RecidivismEvent.non_recidivism_event(
            only_incarceration.admission_date, only_incarceration.release_date,
            only_incarceration.facility)

        correct_output = [(fake_person_id,
                           {only_incarceration.release_date.year:
                            [non_recidivism_event]})]

        find_recidivism_kwargs = {'include_revocation_returns': True}

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(fake_person_id,
                                  person_incarceration_periods)])
                  | 'Find Recidivism' >>
                  beam.ParDo(pipeline.FindRecidivism(),
                             **find_recidivism_kwargs))

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testFindRecidivism_NoIncarcerationPeriods(self):
        """Tests the FindRecidivism DoFn in the pipeline when there are no
        incarceration periods."""

        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        person_incarceration_periods = {'person': fake_person,
                                        'incarceration_periods': []}

        correct_output = [(fake_person_id, {})]

        find_recidivism_kwargs = {'include_revocation_returns': True}

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(fake_person_id,
                                  person_incarceration_periods)])
                  | 'Find Recidivism' >>
                  beam.ParDo(pipeline.FindRecidivism(),
                             **find_recidivism_kwargs)
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testFindRecidivism_TwoReleasesSameYear(self):
        """Tests the FindRecidivism DoFn in the pipeline when a person is
        released twice in the same calendar year."""

        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        initial_incarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            state_person_id=fake_person_id,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            admission_date=date(2008, 11, 20),
            release_date=date(2010, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        first_reincarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            state_person_id=fake_person_id,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            admission_date=date(2010, 4, 5),
            release_date=date(2010, 10, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        subsequent_reincarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            state_person_id=fake_person_id,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='TX',
            admission_date=date(2017, 1, 4))

        person_incarceration_periods = {'person': fake_person,
                                        'incarceration_periods': [
                                            initial_incarceration,
                                            first_reincarceration,
                                            subsequent_reincarceration]}

        first_recidivism_event = RecidivismEvent(
            recidivated=True,
            original_admission_date=initial_incarceration.admission_date,
            release_date=initial_incarceration.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration.admission_date,
            reincarceration_facility=None,
            return_type=IncarcerationReturnType.RECONVICTION)

        second_recidivism_event = RecidivismEvent(
            recidivated=True,
            original_admission_date=first_reincarceration.admission_date,
            release_date=first_reincarceration.release_date,
            release_facility=None,
            reincarceration_date=subsequent_reincarceration.admission_date,
            reincarceration_facility=None,
            return_type=IncarcerationReturnType.RECONVICTION)

        correct_output = [
            (fake_person_id, {initial_incarceration.release_date.year:
                              [first_recidivism_event,
                               second_recidivism_event]})]

        find_recidivism_kwargs = {'include_revocation_returns': True}

        test_pipeline = TestPipeline()
        output = (test_pipeline
                  | beam.Create([(fake_person_id,
                                  person_incarceration_periods)])
                  | 'Find Recidivism' >>
                  beam.ParDo(pipeline.FindRecidivism(),
                             **find_recidivism_kwargs)
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()


class TestMapMetricCombination(unittest.TestCase):
    """Tests for the MapMetricCombinations DoFn in the pipeline."""

    def testMapMetricCombinations(self):
        """Tests the MapMetricCombinations DoFn in the pipeline."""

        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            residency_status=ResidencyStatus.PERMANENT)

        first_recidivism_event = RecidivismEvent(
            recidivated=True, original_admission_date=date(2008, 11, 20),
            release_date=date(2010, 12, 4), release_facility=None,
            reincarceration_date=date(2011, 4, 5),
            reincarceration_facility=None, return_type=IncarcerationReturnType.
            RECONVICTION)

        second_recidivism_event = RecidivismEvent(
            recidivated=True, original_admission_date=date(2011, 4, 5),
            release_date=date(2014, 4, 14), release_facility=None,
            reincarceration_date=date(2017, 1, 4),
            reincarceration_facility=None, return_type=IncarcerationReturnType.
            RECONVICTION)

        person_events = [
            (fake_person_id,
             {'person': [fake_person], 'recidivism_events': [
                 {first_recidivism_event.release_date.year:
                  [first_recidivism_event],
                  second_recidivism_event.release_date.year:
                      [second_recidivism_event]}]})]

        # Get the number of combinations of person-event characteristics.
        num_combinations = len(calculator.characteristic_combinations(
            fake_person, first_recidivism_event))
        assert num_combinations > 0

        # We do not track metrics for periods that start after today, so we
        # need to subtract for some number of periods that go beyond whatever
        # today is.
        periods = relativedelta(date.today(), date(2010, 12, 4)).years + 1
        periods_with_single = 6
        periods_with_double = periods - periods_with_single

        expected_combinations_count_2010 = \
            ((num_combinations * 2 * periods_with_single) +
             (num_combinations * 3 * periods_with_double))

        periods = relativedelta(date.today(), date(2014, 4, 14)).years + 1

        expected_combinations_count_2014 = num_combinations * 2 * periods

        expected_combination_counts = {2010: expected_combinations_count_2010,
                                       2014: expected_combinations_count_2014}

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create(person_events)
                  | 'Map Metric Combinations' >>
                  beam.ParDo(pipeline.MapMetricCombinations())
                  )

        assert_that(output, AssertMatchers.
                    count_combinations(expected_combination_counts))

        test_pipeline.run()

    def testMapMetricCombinations_NoResults(self):
        """Tests the MapMetricCombinations DoFn in the pipeline when there
        are no RecidivismEvents associated with the StatePerson."""

        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            residency_status=ResidencyStatus.PERMANENT)

        person_events = [(fake_person_id,
                          {'person': [fake_person], 'recidivism_events':[]})]

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create(person_events)
                  | 'Map Metric Combinations' >>
                  beam.ParDo(pipeline.MapMetricCombinations())
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testMapMetricCombinations_NoPersonEvents(self):
        """Tests the MapMetricCombinations DoFn in the pipeline when there
        are no RecidivismEvents associated with the StatePerson."""

        fake_person_id = 12345

        person_events = [(fake_person_id,
                          {'person': [], 'recidivism_events':[]})]

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create(person_events)
                  | 'Map Metric Combinations' >>
                  beam.ParDo(pipeline.MapMetricCombinations())
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class TestProduceRecidivismMetric(unittest.TestCase):
    """Tests for the ProduceRecidivismMetric DoFn in the pipeline."""

    def testProduceRecidivismMetric(self):
        """Tests the ProduceRecidivismMetric DoFn in the pipeline."""

        metric_key = {'stay_length': '36-48', 'gender': Gender.MALE,
                      'release_cohort': 2014, 'methodology': Methodology.PERSON,
                      'follow_up_period': 1}

        group = [1, 0, 1, 0, 0, 0, 1, 1, 1, 1]
        expected_recidivism_rate = (sum(group) + 0.0) / len(group)

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(metric_key, group)])
                  | 'Produce Recidivism Metric' >>
                  beam.ParDo(pipeline.ProduceRecidivismMetric())
                  )

        assert_that(output, AssertMatchers.
                    validate_recidivism_metric(expected_recidivism_rate))

        test_pipeline.run()

    def testProduceRecidivismMetric_NoRecidivism(self):
        """Tests the ProduceRecivismMetric DoFn in the pipeline when the
        recidivism rate for the metric is 0.0."""

        metric_key = {'stay_length': '36-48', 'gender': Gender.MALE,
                      'release_cohort': 2014, 'methodology': Methodology.PERSON,
                      'follow_up_period': 1}

        group = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        expected_recidivism_rate = (sum(group) + 0.0) / len(group)

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(metric_key, group)])
                  | 'Produce Recidivism Metric' >>
                  beam.ParDo(pipeline.ProduceRecidivismMetric())
                  )

        assert_that(output, AssertMatchers.
                    validate_recidivism_metric(expected_recidivism_rate))

        test_pipeline.run()

    def testProduceRecidivismMetric_EmptyGroup(self):
        """Tests the ProduceRecivismMetric DoFn in the pipeline when there is
        no group associated with the metric.

        This should not happen in the pipeline, but we test against it
        anyways."""

        metric_key = {'stay_length': '36-48', 'gender': Gender.MALE,
                      'release_cohort': 2014, 'methodology': Methodology.PERSON,
                      'follow_up_period': 1}

        group = []

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(metric_key, group)])
                  | 'Produce Recidivism Metric' >>
                  beam.ParDo(pipeline.ProduceRecidivismMetric())
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testProduceRecidivismMetric_EmptyMetric(self):
        """Tests the ProduceRecivismMetric DoFn in the pipeline when there is
        no group associated with the metric.

        This should not happen in the pipeline, but we test against it
        anyways."""

        metric_key = {}

        group = [0, 0, 0, 1, 0, 0, 0, 0, 1, 0]

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(metric_key, group)])
                  | 'Produce Recidivism Metric' >>
                  beam.ParDo(pipeline.ProduceRecidivismMetric())
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class TestFilterMetrics(unittest.TestCase):
    """Tests for the FilterMetrics DoFn in the pipeline."""

    def testFilterMetrics_ExcludeAllDimensions(self):
        """Tests the FilterMetrics DoFn when all metrics with specific
        dimensions should be filtered from the output."""

        dimensions_to_filter = ['age_bucket', 'race', 'release_facility',
                                'gender', 'stay_length_bucket']
        methodology = Methodology.BOTH
        release_count_min = 0

        filter_metrics_kwargs = {
            'dimensions_to_filter_out': dimensions_to_filter,
            'methodology': methodology,
            'release_count_min': release_count_min}

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create(MetricGroup.get_list())
                  | 'Produce Recidivism Metric' >>
                  beam.ParDo(pipeline.FilterMetrics(),
                             **filter_metrics_kwargs))

        assert_that(output,
                    equal_to(
                        [MetricGroup.recidivism_metric_without_dimensions]))

        test_pipeline.run()

    def testFilterMetrics_IncludeAge(self):
        """Tests the FilterMetrics DoFn when metrics including an age
         dimension should be included in the output. All other dimensions
         should be filtered from the output."""

        dimensions_to_filter = ['gender', 'race', 'release_facility',
                                'stay_length_bucket']
        methodology = Methodology.BOTH
        release_count_min = 0

        filter_metrics_kwargs = {
            'dimensions_to_filter_out': dimensions_to_filter,
            'methodology': methodology,
            'release_count_min': release_count_min}

        test_pipeline = TestPipeline()
        output = (test_pipeline
                  | beam.Create(MetricGroup.get_list())
                  | 'Produce Recidivism Metric' >>
                  beam.ParDo(pipeline.FilterMetrics(),
                             **filter_metrics_kwargs))

        assert_that(output,
                    equal_to(
                        [MetricGroup.recidivism_metric_without_dimensions,
                         MetricGroup.recidivism_metric_with_age]))

        test_pipeline.run()

    def testFilterMetrics_IncludeGender(self):
        """Tests the FilterMetrics DoFn when metrics including a gender
         dimension should be included in the output. All other dimensions
         should be filtered from the output."""

        dimensions_to_filter = ['age_bucket', 'race', 'release_facility',
                                'stay_length_bucket']
        methodology = Methodology.BOTH
        release_count_min = 0

        filter_metrics_kwargs = {
            'dimensions_to_filter_out': dimensions_to_filter,
            'methodology': methodology,
            'release_count_min': release_count_min}

        test_pipeline = TestPipeline()
        output = (test_pipeline
                  | beam.Create(MetricGroup.get_list())
                  | 'Produce Recidivism Metric' >>
                  beam.ParDo(pipeline.FilterMetrics(),
                             **filter_metrics_kwargs))

        assert_that(output,
                    equal_to(
                        [MetricGroup.recidivism_metric_without_dimensions,
                         MetricGroup.recidivism_metric_with_gender]))

        test_pipeline.run()

    # TODO(1781): Test filtering races and ethnicities

    def testFilterMetrics_IncludeReleaseFacility(self):
        """Tests the FilterMetrics DoFn when metrics including a release
         facility dimension should be included in the output. All other
         dimensions should be filtered from the output."""

        dimensions_to_filter = ['age_bucket', 'gender', 'race',
                                'stay_length_bucket']
        methodology = Methodology.BOTH
        release_count_min = 0

        filter_metrics_kwargs = {
            'dimensions_to_filter_out': dimensions_to_filter,
            'methodology': methodology,
            'release_count_min': release_count_min}

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create(MetricGroup.get_list())
                  | 'Produce Recidivism Metric' >>
                  beam.ParDo(pipeline.FilterMetrics(),
                             **filter_metrics_kwargs))

        assert_that(output, equal_to([
            MetricGroup.recidivism_metric_without_dimensions,
            MetricGroup.recidivism_metric_with_release_facility]))

        test_pipeline.run()

    def testFilterMetrics_IncludeStayLength(self):
        """Tests the FilterMetrics DoFn when metrics including a stay
        length dimension should be included in the output. All other
        dimensions should be filtered from the output."""

        dimensions_to_filter = ['age_bucket', 'gender', 'race',
                                'release_facility']
        methodology = Methodology.BOTH
        release_count_min = 0

        filter_metrics_kwargs = {
            'dimensions_to_filter_out': dimensions_to_filter,
            'methodology': methodology,
            'release_count_min': release_count_min}

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create(MetricGroup.get_list())
                  | 'Produce Recidivism Metric' >>
                  beam.ParDo(pipeline.FilterMetrics(),
                             **filter_metrics_kwargs))

        assert_that(output,
                    equal_to(
                        [MetricGroup.recidivism_metric_without_dimensions,
                         MetricGroup.recidivism_metric_with_stay_length]))

        test_pipeline.run()

    def testFilterMetrics_IncludeTwoDimensions(self):
        """Tests the FilterMetrics DoFn when metrics including a gender
         or a stay length dimension should be included in the output. All other
        dimensions should be filtered from the output."""

        dimensions_to_filter = ['age_bucket', 'race',
                                'release_facility']
        methodology = Methodology.BOTH
        release_count_min = 0

        filter_metrics_kwargs = {
            'dimensions_to_filter_out': dimensions_to_filter,
            'methodology': methodology,
            'release_count_min': release_count_min}

        test_pipeline = TestPipeline()
        output = (test_pipeline
                  | beam.Create(MetricGroup.get_list())
                  | 'Produce Recidivism Metric' >>
                  beam.ParDo(pipeline.FilterMetrics(),
                             **filter_metrics_kwargs))

        assert_that(output,
                    equal_to(
                        [MetricGroup.recidivism_metric_without_dimensions,
                         MetricGroup.recidivism_metric_with_gender,
                         MetricGroup.recidivism_metric_with_stay_length]))

        test_pipeline.run()


class NormalizeEntityDict:
    """Converts entity __dict__ dictionaries into dictionaries interpretable
    by the pipeline extract functions."""

    @staticmethod
    def normalize(person_dict: dict) -> dict:

        def _normalize(v):
            return v.value if isinstance(v, EntityEnum) else v

        return {k: _normalize(v) for k, v in person_dict.items()}


class MetricGroup:
    """Stores a set of metrics where every dimension is included for testing
    dimension filtering."""
    recidivism_metric_with_age = RecidivismMetric(
        execution_id=12345, release_cohort=2015, follow_up_period=1,
        methodology=Methodology.PERSON, age_bucket='25-29', total_releases=1000,
        total_returns=900, recidivism_rate=0.9)

    # TODO(1781): Add metricS with races and ethnicities

    recidivism_metric_with_gender = RecidivismMetric(
        execution_id=12345, release_cohort=2015, follow_up_period=1,
        methodology=Methodology.PERSON, gender=Gender.MALE, total_releases=1000,
        total_returns=875, recidivism_rate=0.875)

    recidivism_metric_with_release_facility = RecidivismMetric(
        execution_id=12345, release_cohort=2015, follow_up_period=1,
        methodology=Methodology.PERSON, release_facility='Red',
        total_releases=1000, total_returns=300, recidivism_rate=0.30)

    recidivism_metric_with_stay_length = RecidivismMetric(
        execution_id=12345, release_cohort=2015, follow_up_period=1,
        methodology=Methodology.PERSON, stay_length_bucket='12-24',
        total_releases=1000, total_returns=300, recidivism_rate=0.30)

    recidivism_metric_without_dimensions = RecidivismMetric(
        execution_id=12345, release_cohort=2015,
        follow_up_period=1, methodology=Methodology.PERSON, total_releases=1500,
        total_returns=1200, recidivism_rate=0.80)

    @staticmethod
    def get_list():
        return [MetricGroup.recidivism_metric_with_age,
                MetricGroup.recidivism_metric_with_gender,
                MetricGroup.recidivism_metric_with_release_facility,
                MetricGroup.recidivism_metric_with_stay_length,
                MetricGroup.recidivism_metric_without_dimensions]


class AssertMatchers:
    """Functions to be used by Apache Beam testing `assert_that` functions to
    validate pipeline outputs."""

    @staticmethod
    def validate_pipeline_test():

        def _validate_pipeline_test(output):

            for metric in output:
                if not isinstance(metric, RecidivismMetric):
                    raise BeamAssertException('Failed assert. Output is not'
                                              'of type RecidivismMetric.')

        return _validate_pipeline_test

    @staticmethod
    def count_combinations(expected_combination_counts):
        """Asserts that the number of metric combinations matches the expected
        counts for each release cohort year."""
        def _count_combinations(output):
            actual_combination_counts = {}

            for year in expected_combination_counts.keys():
                actual_combination_counts[year] = 0

            for result in output:
                combination, _ = result

                release_cohort_year = combination['release_cohort']
                actual_combination_counts[release_cohort_year] = \
                    actual_combination_counts[release_cohort_year] + 1

            for year in expected_combination_counts:
                if expected_combination_counts[year] != \
                        actual_combination_counts[year]:
                    raise BeamAssertException('Failed assert. Count does not'
                                              'match expected value.')

        return _count_combinations

    @staticmethod
    def validate_recidivism_metric(expected_recidivism_rate):
        """Asserts that the recidivism rate on the RecidivismMetric produced
        by the pipeline matches the expected recidivism rate."""
        def _validate_recidivism_metric(output):
            if len(output) != 1:
                raise BeamAssertException('Failed assert. Should be only one '
                                          'RecidivismMetric returned.')

            recidivism_metric = output[0]

            if recidivism_metric.recidivism_rate != expected_recidivism_rate:
                raise BeamAssertException('Failed assert. Recidivism rate does'
                                          'not match expected value.')

        return _validate_recidivism_metric
