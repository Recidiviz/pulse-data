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
import json
import unittest

import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to, BeamAssertException
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.pipeline_options import PipelineOptions

import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
from enum import Enum

from recidiviz.calculator.pipeline.recidivism import calculator, pipeline
from recidiviz.calculator.pipeline.recidivism.metrics import \
    ReincarcerationRecidivismMetric,\
    ReincarcerationRecidivismRateMetric
from recidiviz.calculator.pipeline.recidivism.metrics import \
    ReincarcerationRecidivismMetricType as MetricType
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.calculator.pipeline.utils import extractor_utils
from recidiviz.calculator.pipeline.recidivism.pipeline import \
    json_serializable_metric_key
from recidiviz.calculator.pipeline.recidivism.release_event import \
    ReincarcerationReturnType, RecidivismReleaseEvent, \
    NonRecidivismReleaseEvent
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, \
    StateIncarcerationFacilitySecurityLevel
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state.entities import \
    StateIncarcerationPeriod, Gender, Race, ResidencyStatus, Ethnicity, \
    StateSupervisionViolationResponse, StateSupervisionViolation
from recidiviz.persistence.entity.state.entities import StatePerson
from recidiviz.tests.calculator.calculator_test_utils import \
    normalized_database_base_dict, normalized_database_base_dict_list
from recidiviz.tests.persistence.database import database_test_utils


class TestRecidivismPipeline(unittest.TestCase):
    """Tests the entire recidivism pipeline."""

    def testRecidivismPipeline(self):
        """Tests the entire recidivism pipeline with one person and three
        incarceration periods."""

        fake_person_id = 12345

        fake_person = schema.StatePerson(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        persons_data = [normalized_database_base_dict(fake_person)]

        race_1 = schema.StatePersonRace(
            person_race_id=111,
            state_code='CA',
            race=Race.BLACK,
            person_id=fake_person_id
        )

        race_2 = schema.StatePersonRace(
            person_race_id=111,
            state_code='ND',
            race=Race.WHITE,
            person_id=fake_person_id
        )

        races_data = normalized_database_base_dict_list([race_1, race_2])

        ethnicity = schema.StatePersonEthnicity(
            person_ethnicity_id=111,
            state_code='CA',
            ethnicity=Ethnicity.HISPANIC,
            person_id=fake_person_id)

        ethnicity_data = normalized_database_base_dict_list([ethnicity])

        initial_incarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
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
            SENTENCE_SERVED,
            person_id=fake_person_id
        )

        first_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=2222,
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
            SENTENCE_SERVED,
            person_id=fake_person_id)

        subsequent_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=3333,
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
            admission_date=date(2017, 1, 4),
            person_id=fake_person_id)

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration),
            normalized_database_base_dict(first_reincarceration),
            normalized_database_base_dict(subsequent_reincarceration)
        ]

        supervision_violation_response = \
            database_test_utils.generate_test_supervision_violation_response(
                fake_person_id)

        state_agent = database_test_utils.generate_test_assessment_agent()

        supervision_violation_response.decision_agents = [state_agent]

        supervision_violation = \
            database_test_utils.generate_test_supervision_violation(
                fake_person_id, [supervision_violation_response])

        supervision_violation_response.supervision_violation_id = \
            supervision_violation.supervision_violation_id

        supervision_violation_response_data = [
            normalized_database_base_dict(supervision_violation_response)
        ]

        supervision_violation_data = [
            normalized_database_base_dict(supervision_violation)
        ]

        inclusions = {
            'age_bucket': False,
            'gender': False,
            'race': False,
            'ethnicity': False,
            'release_facility': False,
            'stay_length_bucket': False
        }

        methodologies = [MetricMethodologyType.EVENT,
                         MetricMethodologyType.PERSON]

        data_dict = {schema.StatePerson.__tablename__: persons_data,
                     schema.StatePersonRace.__tablename__: races_data,
                     schema.StatePersonEthnicity.__tablename__: ethnicity_data,
                     schema.StateIncarcerationPeriod.__tablename__:
                         incarceration_periods_data,
                     schema.StateSupervisionViolationResponse.__tablename__:
                         supervision_violation_response_data,
                     schema.StateSupervisionViolation.__tablename__:
                         supervision_violation_data}

        test_pipeline = TestPipeline()

        # Get StatePersons
        persons = (
            test_pipeline
            | 'Load Persons' >>
            extractor_utils.BuildRootEntity(
                dataset=None,
                data_dict=data_dict,
                root_schema_class=schema.StatePerson,
                root_entity_class=StatePerson,
                unifying_id_field='person_id',
                build_related_entities=True))

        # Get StateIncarcerationPeriods
        incarceration_periods = (
            test_pipeline
            | 'Load IncarcerationPeriods' >>
            extractor_utils.BuildRootEntity(
                dataset=None,
                data_dict=data_dict,
                root_schema_class=schema.StateIncarcerationPeriod,
                root_entity_class=StateIncarcerationPeriod,
                unifying_id_field='person_id',
                build_related_entities=True))

        # Get StateSupervisionViolationResponses
        supervision_violation_responses = \
            (test_pipeline
             | 'Load SupervisionViolationResponses' >>
             extractor_utils.BuildRootEntity(
                 dataset=None,
                 data_dict=data_dict,
                 root_schema_class=schema.StateSupervisionViolationResponse,
                 root_entity_class=StateSupervisionViolationResponse,
                 unifying_id_field='person_id',
                 build_related_entities=True
             ))

        incarceration_periods_and_violation_responses = (
            {'incarceration_periods': incarceration_periods,
             'violation_responses': supervision_violation_responses}
            | 'Group StateIncarcerationPeriods to '
            'StateSupervisionViolationResponses' >>
            beam.CoGroupByKey()
        )

        # Set the fully hydrated StateSupervisionViolationResponse entities on
        # the corresponding StateIncarcerationPeriods
        incarceration_periods_with_source_violations = (
            incarceration_periods_and_violation_responses
            | 'Set hydrated StateSupervisionViolationResponses on'
            'the StateIncarcerationPeriods' >>
            beam.ParDo(
                pipeline.SetViolationResponseOnIncarcerationPeriod()))

        # Group each StatePerson with their StateIncarcerationPeriods
        person_and_incarceration_periods = (
            {'person': persons,
             'incarceration_periods':
                 incarceration_periods_with_source_violations}
            | 'Group StatePerson to StateIncarcerationPeriods' >>
            beam.CoGroupByKey()
        )

        # Identify ReleaseEvents events from the StatePerson's
        # StateIncarcerationPeriods
        person_events = (
            person_and_incarceration_periods |
            'Get Recidivism Events' >>
            pipeline.GetReleaseEvents())

        # Get pipeline job details for accessing job_id
        all_pipeline_options = PipelineOptions().get_all_options()

        # Add timestamp for local jobs
        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        # Get recidivism metrics
        recidivism_metrics = (person_events
                              | 'Get Recidivism Metrics' >>
                              pipeline.GetRecidivismMetrics(
                                  all_pipeline_options, inclusions))

        filter_metrics_kwargs = {'methodologies': methodologies}

        # Filter out unneeded metrics
        final_recidivism_metrics = (
            recidivism_metrics
            | 'Filter out unwanted metrics' >>
            beam.ParDo(pipeline.FilterMetrics(), **filter_metrics_kwargs))

        assert_that(final_recidivism_metrics,
                    AssertMatchers.validate_pipeline_test())

        test_pipeline.run()

    def testRecidivismPipeline_WithConditionalReturns(self):
        """Tests the entire RecidivismPipeline with two person and three
        incarceration periods each. One StatePerson has a return from a
        technical supervision violation.
        """

        fake_person_id_1 = 12345

        fake_person_1 = schema.StatePerson(
            person_id=fake_person_id_1, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        fake_person_id_2 = 6789

        fake_person_2 = schema.StatePerson(
            person_id=fake_person_id_2, gender=Gender.FEMALE,
            birthdate=date(1990, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        persons_data = [normalized_database_base_dict(fake_person_1),
                        normalized_database_base_dict(fake_person_2)]

        initial_incarceration_1 = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
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
            SENTENCE_SERVED,
            person_id=fake_person_id_1)

        first_reincarceration_1 = schema.StateIncarcerationPeriod(
            incarceration_period_id=2222,
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
            SENTENCE_SERVED,
            person_id=fake_person_id_1)

        subsequent_reincarceration_1 = \
            schema.StateIncarcerationPeriod(
                incarceration_period_id=3333,
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
                admission_date=date(2017, 1, 4),
                person_id=fake_person_id_1)

        initial_incarceration_2 = schema.StateIncarcerationPeriod(
            incarceration_period_id=4444,
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
            CONDITIONAL_RELEASE,
            person_id=fake_person_id_2)

        supervision_violation_response = \
            database_test_utils.generate_test_supervision_violation_response(
                fake_person_id_2)

        supervision_violation = \
            database_test_utils.generate_test_supervision_violation(
                fake_person_id_2, [supervision_violation_response])

        supervision_violation_response.supervision_violation_id = \
            supervision_violation.supervision_violation_id

        supervision_violation_response_data = [
            normalized_database_base_dict(supervision_violation_response)
        ]

        supervision_violation_data = [
            normalized_database_base_dict(supervision_violation)
        ]

        first_reincarceration_2 = schema.StateIncarcerationPeriod(
            incarceration_period_id=5555,
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
            SENTENCE_SERVED,
            source_supervision_violation_response_id=
            supervision_violation_response.supervision_violation_response_id,
            person_id=fake_person_id_2)

        subsequent_reincarceration_2 = \
            schema.StateIncarcerationPeriod(
                incarceration_period_id=6666,
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
                admission_date=date(2018, 3, 9),
                person_id=fake_person_id_2)

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration_1),
            normalized_database_base_dict(first_reincarceration_1),
            normalized_database_base_dict(subsequent_reincarceration_1),
            normalized_database_base_dict(initial_incarceration_2),
            normalized_database_base_dict(first_reincarceration_2),
            normalized_database_base_dict(subsequent_reincarceration_2)
        ]

        inclusions = {
            'age_bucket': False,
            'gender': False,
            'race': False,
            'ethnicity': False,
            'release_facility': False,
            'stay_length_bucket': False
        }

        methodologies = [MetricMethodologyType.EVENT,
                         MetricMethodologyType.PERSON]

        data_dict = {schema.StatePerson.__tablename__: persons_data,
                     schema.StateIncarcerationPeriod.__tablename__:
                         incarceration_periods_data,
                     schema.StateSupervisionViolationResponse.__tablename__:
                         supervision_violation_response_data,
                     schema.StateSupervisionViolation.__tablename__:
                         supervision_violation_data}

        test_pipeline = TestPipeline()

        # Get StatePersons
        persons = (
            test_pipeline
            | 'Load Persons' >>
            extractor_utils.BuildRootEntity(
                dataset=None,
                data_dict=data_dict,
                root_schema_class=schema.StatePerson,
                root_entity_class=StatePerson,
                unifying_id_field='person_id',
                build_related_entities=True))

        # Get StateIncarcerationPeriods
        incarceration_periods = (
            test_pipeline
            | 'Load IncarcerationPeriods' >>
            extractor_utils.BuildRootEntity(
                dataset=None,
                data_dict=data_dict,
                root_schema_class=schema.StateIncarcerationPeriod,
                root_entity_class=StateIncarcerationPeriod,
                unifying_id_field='person_id',
                build_related_entities=True))

        # Get StateSupervisionViolations
        supervision_violations = \
            (test_pipeline
             | 'Load SupervisionViolations' >>
             extractor_utils.BuildRootEntity(
                 dataset=None,
                 data_dict=data_dict,
                 root_schema_class=schema.StateSupervisionViolation,
                 root_entity_class=StateSupervisionViolation,
                 unifying_id_field='person_id',
                 build_related_entities=True
             ))

        # Get StateSupervisionViolationResponses
        supervision_violation_responses = \
            (test_pipeline
             | 'Load SupervisionViolationResponses' >>
             extractor_utils.BuildRootEntity(
                 dataset=None,
                 data_dict=data_dict,
                 root_schema_class=schema.StateSupervisionViolationResponse,
                 root_entity_class=StateSupervisionViolationResponse,
                 unifying_id_field='person_id',
                 build_related_entities=True
             ))

        # Group StateSupervisionViolationResponses and
        # StateSupervisionViolations by person_id
        supervision_violations_and_responses = (
            {'violations': supervision_violations,
             'violation_responses': supervision_violation_responses
             } | 'Group StateSupervisionViolationResponses to '
                 'StateSupervisionViolations' >>
            beam.CoGroupByKey()
        )

        # Set the fully hydrated StateSupervisionViolation entities on
        # the corresponding StateSupervisionViolationResponses
        violation_responses_with_hydrated_violations = (
            supervision_violations_and_responses
            | 'Set hydrated StateSupervisionViolations on '
              'the StateSupervisionViolationResponses' >>
            beam.ParDo(pipeline.SetViolationOnViolationsResponse()))

        # Group StateIncarcerationPeriods and StateSupervisionViolationResponses
        # by person_id
        incarceration_periods_and_violation_responses = (
            {'incarceration_periods': incarceration_periods,
             'violation_responses':
                 violation_responses_with_hydrated_violations}
            | 'Group StateIncarcerationPeriods to '
              'StateSupervisionViolationResponses' >>
            beam.CoGroupByKey()
        )

        # Set the fully hydrated StateSupervisionViolationResponse entities on
        # the corresponding StateIncarcerationPeriods
        incarceration_periods_with_source_violations = (
            incarceration_periods_and_violation_responses
            | 'Set hydrated StateSupervisionViolationResponses on'
            'the StateIncarcerationPeriods' >>
            beam.ParDo(
                pipeline.SetViolationResponseOnIncarcerationPeriod()))

        # Group each StatePerson with their StateIncarcerationPeriods
        person_and_incarceration_periods = (
            {'person': persons,
             'incarceration_periods':
                 incarceration_periods_with_source_violations}
            | 'Group StatePerson to StateIncarcerationPeriods' >>
            beam.CoGroupByKey()
        )

        # Identify ReleaseEvents events from the StatePerson's
        # StateIncarcerationPeriods
        person_events = (
            person_and_incarceration_periods |
            'Get Recidivism Events' >>
            pipeline.GetReleaseEvents())

        # Get pipeline job details for accessing job_id
        all_pipeline_options = PipelineOptions().get_all_options()

        # Add timestamp for local jobs
        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        # Get recidivism metrics
        recidivism_metrics = (person_events
                              | 'Get Recidivism Metrics' >>
                              pipeline.GetRecidivismMetrics(
                                  all_pipeline_options,
                                  inclusions))

        filter_metrics_kwargs = {'methodologies': methodologies}

        # Filter out unneeded metrics
        final_recidivism_metrics = (
            recidivism_metrics
            | 'Filter out unwanted metrics' >>
            beam.ParDo(pipeline.FilterMetrics(), **filter_metrics_kwargs))

        assert_that(final_recidivism_metrics,
                    AssertMatchers.validate_pipeline_test())

        test_pipeline.run()


class TestClassifyReleaseEvents(unittest.TestCase):
    """Tests the ClassifyReleaseEvents DoFn in the pipeline."""

    def testClassifyReleaseEvents(self):
        """Tests the ClassifyReleaseEvents DoFn when there are two instances
        of recidivism."""

        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        initial_incarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        first_reincarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            admission_date=date(2011, 4, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        subsequent_reincarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='TX',
            admission_date=date(2017, 1, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION)

        person_incarceration_periods = {'person': [fake_person],
                                        'incarceration_periods': [
                                            initial_incarceration,
                                            first_reincarceration,
                                            subsequent_reincarceration]}

        first_recidivism_release_event = RecidivismReleaseEvent(
            state_code='TX',
            original_admission_date=initial_incarceration.admission_date,
            release_date=initial_incarceration.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration.admission_date,
            reincarceration_facility=None,
            return_type=ReincarcerationReturnType.NEW_ADMISSION)

        second_recidivism_release_event = RecidivismReleaseEvent(
            state_code='TX',
            original_admission_date=first_reincarceration.admission_date,
            release_date=first_reincarceration.release_date,
            release_facility=None,
            reincarceration_date=subsequent_reincarceration.admission_date,
            reincarceration_facility=None,
            return_type=ReincarcerationReturnType.NEW_ADMISSION)

        correct_output = [
            (fake_person, {initial_incarceration.release_date.year:
                           [first_recidivism_release_event],
                           first_reincarceration.release_date.year:
                           [second_recidivism_release_event]})]

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(fake_person_id,
                                  person_incarceration_periods)])
                  | 'Identify Recidivism Events' >>
                  beam.ParDo(pipeline.ClassifyReleaseEvents())
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyReleaseEvents_NoRecidivism(self):
        """Tests the ClassifyReleaseEvents DoFn in the pipeline when there
        is no instance of recidivism."""

        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        only_incarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX', admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        person_incarceration_periods = {'person': [fake_person],
                                        'incarceration_periods':
                                            [only_incarceration]}

        non_recidivism_release_event = NonRecidivismReleaseEvent(
            'TX', only_incarceration.admission_date,
            only_incarceration.release_date, only_incarceration.facility)

        correct_output = [(fake_person,
                           {only_incarceration.release_date.year:
                            [non_recidivism_release_event]})]

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(fake_person_id,
                                  person_incarceration_periods)])
                  | 'Identify Recidivism Events' >>
                  beam.ParDo(pipeline.ClassifyReleaseEvents()))

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyReleaseEvents_NoIncarcerationPeriods(self):
        """Tests the ClassifyReleaseEvents DoFn in the pipeline when there
        are no incarceration periods. The person in this case should be
        excluded from the calculations."""

        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        person_incarceration_periods = {'person': [fake_person],
                                        'incarceration_periods': []}

        correct_output = []

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(fake_person_id,
                                  person_incarceration_periods)])
                  | 'Identify Recidivism Events' >>
                  beam.ParDo(pipeline.ClassifyReleaseEvents())
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyReleaseEvents_TwoReleasesSameYear(self):
        """Tests the ClassifyReleaseEvents DoFn in the pipeline when a person
        is released twice in the same calendar year."""

        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        initial_incarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            release_date=date(2010, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        first_reincarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            admission_date=date(2010, 4, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            release_date=date(2010, 10, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        subsequent_reincarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='TX',
            admission_date=date(2017, 1, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
        )

        person_incarceration_periods = {'person': [fake_person],
                                        'incarceration_periods': [
                                            initial_incarceration,
                                            first_reincarceration,
                                            subsequent_reincarceration]}

        first_recidivism_release_event = RecidivismReleaseEvent(
            state_code='TX',
            original_admission_date=initial_incarceration.admission_date,
            release_date=initial_incarceration.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration.admission_date,
            reincarceration_facility=None,
            return_type=ReincarcerationReturnType.NEW_ADMISSION)

        second_recidivism_release_event = RecidivismReleaseEvent(
            state_code='TX',
            original_admission_date=first_reincarceration.admission_date,
            release_date=first_reincarceration.release_date,
            release_facility=None,
            reincarceration_date=subsequent_reincarceration.admission_date,
            reincarceration_facility=None,
            return_type=ReincarcerationReturnType.NEW_ADMISSION)

        correct_output = [
            (fake_person, {initial_incarceration.release_date.year:
                           [first_recidivism_release_event,
                            second_recidivism_release_event]})]

        test_pipeline = TestPipeline()
        output = (test_pipeline
                  | beam.Create([(fake_person_id,
                                  person_incarceration_periods)])
                  | 'Identify Recidivism Events' >>
                  beam.ParDo(pipeline.ClassifyReleaseEvents())
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyReleaseEvents_WrongOrder(self):
        """Tests the ClassifyReleaseEvents DoFn when there are two instances
        of recidivism."""

        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        initial_incarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        first_reincarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            admission_date=date(2011, 4, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        subsequent_reincarceration = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='TX',
            admission_date=date(2017, 1, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION)

        person_incarceration_periods = {'person': [fake_person],
                                        'incarceration_periods': [
                                            subsequent_reincarceration,
                                            initial_incarceration,
                                            first_reincarceration]}

        first_recidivism_release_event = RecidivismReleaseEvent(
            state_code='TX',
            original_admission_date=initial_incarceration.admission_date,
            release_date=initial_incarceration.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration.admission_date,
            reincarceration_facility=None,
            return_type=ReincarcerationReturnType.NEW_ADMISSION)

        second_recidivism_release_event = RecidivismReleaseEvent(
            state_code='TX',
            original_admission_date=first_reincarceration.admission_date,
            release_date=first_reincarceration.release_date,
            release_facility=None,
            reincarceration_date=subsequent_reincarceration.admission_date,
            reincarceration_facility=None,
            return_type=ReincarcerationReturnType.NEW_ADMISSION)

        correct_output = [
            (fake_person, {initial_incarceration.release_date.year:
                           [first_recidivism_release_event],
                           first_reincarceration.release_date.year:
                           [second_recidivism_release_event]})]

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(fake_person_id,
                                  person_incarceration_periods)])
                  | 'Identify Recidivism Events' >>
                  beam.ParDo(pipeline.ClassifyReleaseEvents())
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()


class TestCalculateRecidivismMetricCombinations(unittest.TestCase):
    """Tests for the CalculateRecidivismMetricCombinations DoFn in the
    pipeline."""

    def testCalculateRecidivismMetricCombinations(self):
        """Tests the CalculateRecidivismMetricCombinations DoFn in the
        pipeline."""

        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            residency_status=ResidencyStatus.PERMANENT)

        first_recidivism_release_event = RecidivismReleaseEvent(
            state_code='CA',
            original_admission_date=date(2008, 11, 20),
            release_date=date(2010, 12, 4), release_facility=None,
            reincarceration_date=date(2011, 4, 5),
            reincarceration_facility=None,
            return_type=ReincarcerationReturnType.NEW_ADMISSION)

        second_recidivism_release_event = RecidivismReleaseEvent(
            state_code='CA',
            original_admission_date=date(2011, 4, 5),
            release_date=date(2014, 4, 14), release_facility=None,
            reincarceration_date=date(2017, 1, 4),
            reincarceration_facility=None,
            return_type=ReincarcerationReturnType.NEW_ADMISSION)

        person_events = [
            (fake_person, {first_recidivism_release_event.release_date.year:
                           [first_recidivism_release_event],
                           second_recidivism_release_event.release_date.year:
                               [second_recidivism_release_event]})]
        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
            'release_facility': True,
            'stay_length_bucket': True
        }

        # Get the number of combinations of person-event characteristics.
        num_combinations = len(calculator.characteristic_combinations(
            fake_person, first_recidivism_release_event, inclusions))
        assert num_combinations > 0

        # We do not track metrics for periods that start after today, so we
        # need to subtract for some number of periods that go beyond whatever
        # today is.
        periods = relativedelta(date.today(), date(2010, 12, 4)).years + 1
        periods_with_single = 6
        periods_with_double = periods - periods_with_single

        expected_combinations_count_2010 = \
            ((num_combinations * 46 * periods_with_single) +
             (num_combinations * 69 * periods_with_double))

        periods = relativedelta(date.today(), date(2014, 4, 14)).years + 1

        expected_combinations_count_2014 = (num_combinations * 46 * periods)

        expected_count_metric_combinations = (num_combinations * 2 * 46 +
                                              num_combinations * 2 * 46)

        expected_combination_counts_rates = \
            {2010: expected_combinations_count_2010,
             2014: expected_combinations_count_2014}

        expected_combination_counts_counts = \
            {'counts': expected_count_metric_combinations}

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create(person_events)
                  | 'Calculate Metric Combinations' >>
                  beam.ParDo(pipeline.CalculateRecidivismMetricCombinations(),
                             **inclusions)
                  .with_outputs('rates', 'counts')
                  )

        assert_that(output.rates, AssertMatchers.
                    count_combinations(expected_combination_counts_rates),
                    'Assert number of rates metrics is expected value')

        assert_that(output.counts, AssertMatchers.
                    count_combinations(expected_combination_counts_counts),
                    'Assert number of counts metrics is expected value')

        test_pipeline.run()

    def testCalculateRecidivismMetricCombinations_NoResults(self):
        """Tests the CalculateRecidivismMetricCombinations DoFn in the pipeline
        when there are no ReleaseEvents associated with the StatePerson."""

        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            residency_status=ResidencyStatus.PERMANENT)

        person_events = [(fake_person, {})]

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create(person_events)
                  | 'Calculate Metric Combinations' >>
                  beam.ParDo(pipeline.CalculateRecidivismMetricCombinations())
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testCalculateRecidivismMetricCombinations_NoPersonEvents(self):
        """Tests the CalculateRecidivismMetricCombinations DoFn in the pipeline
        when there is no StatePerson and no ReleaseEvents."""

        person_events = []

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create(person_events)
                  | 'Calculate Metric Combinations' >>
                  beam.ParDo(pipeline.CalculateRecidivismMetricCombinations())
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class TestProduceReincarcerationRecidivismRateMetric(unittest.TestCase):
    """Tests for the ProduceReincarcerationRecidivismRateMetric DoFn in the
     pipeline."""

    def testProduceReincarcerationRecidivismRateMetric(self):
        """Tests the ProduceReincarcerationRecidivismMetric DoFn in the
         pipeline."""
        metric_key_dict = {'stay_length_bucket': '36-48', 'gender': Gender.MALE,
                           'release_cohort': 2014,
                           'methodology': MetricMethodologyType.PERSON,
                           'follow_up_period': 1,
                           'metric_type': MetricType.RATE, 'state_code': 'CA'}

        metric_key = json.dumps(json_serializable_metric_key(metric_key_dict),
                                sort_keys=True)

        value = {'total_releases': 10,
                 'recidivated_releases': 7,
                 'recidivism_rate': .7}

        test_pipeline = TestPipeline()

        all_pipeline_options = PipelineOptions().get_all_options()

        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        output = (test_pipeline
                  | beam.Create([(metric_key, value)])
                  | 'Produce Recidivism Rate Metric' >>
                  beam.ParDo(pipeline.
                             ProduceReincarcerationRecidivismMetric(),
                             **all_pipeline_options)
                  )

        assert_that(output, AssertMatchers.
                    validate_recidivism_rate_metric(0.7))

        test_pipeline.run()

    def testProduceReincarcerationRecidivismRateMetric_NoReleases(self):
        """Tests the ProduceRecivismMetric DoFn in the pipeline when the
        recidivism rate is NaN because there are no releases.

        This should not happen in the pipeline, but we test against it
        anyways."""

        metric_key_dict = {'stay_length_bucket': '36-48', 'gender': Gender.MALE,
                           'release_cohort': 2014,
                           'methodology': MetricMethodologyType.PERSON,
                           'follow_up_period': 1,
                           'metric_type': MetricType.RATE, 'state_code': 'CA'}

        metric_key = json.dumps(json_serializable_metric_key(metric_key_dict),
                                sort_keys=True)

        value = {'total_releases': 0,
                 'recidivated_releases': 0,
                 'recidivism_rate': float('NaN')}

        test_pipeline = TestPipeline()

        all_pipeline_options = PipelineOptions().get_all_options()

        job_timestamp = datetime.datetime.now().strftime(
            '%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        output = (test_pipeline
                  | beam.Create([(metric_key, value)])
                  | 'Produce Recidivism Metric' >>
                  beam.ParDo(
                      pipeline.ProduceReincarcerationRecidivismMetric(),
                      **all_pipeline_options))

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testProduceReincarcerationRecidivismLibertyMetric(self):
        """Tests the ProduceReincarcerationRecidivismLibertyMetric DoFn in the
         pipeline."""

        metric_key_dict = {'stay_length_bucket': '36-48', 'gender': Gender.MALE,
                           'start_date': date(2010, 1, 1),
                           'end_date': date(2010, 12, 31),
                           'methodology': MetricMethodologyType.PERSON,
                           'metric_type': MetricType.LIBERTY,
                           'state_code': 'CA'}

        metric_key = json.dumps(json_serializable_metric_key(metric_key_dict),
                                sort_keys=True)

        value = {'returns': 10,
                 'avg_liberty': 300}

        test_pipeline = TestPipeline()

        all_pipeline_options = PipelineOptions().get_all_options()

        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        output = (test_pipeline
                  | beam.Create([(metric_key, value)])
                  | 'Produce Recidivism Liberty Metric' >>
                  beam.ParDo(pipeline.
                             ProduceReincarcerationRecidivismMetric(),
                             **all_pipeline_options)
                  )

        assert_that(output, AssertMatchers.
                    validate_recidivism_liberty_metric(300))

        test_pipeline.run()

    def testProduceReincarcerationRecidivismLibertyMetric_NoReturns(self):
        """Tests the ProduceReincarcerationRecidivismLibertyMetric DoFn in the
         pipeline when the value for avg_liberty is NaN because the number of
        returns is 0."""

        metric_key_dict = {'stay_length_bucket': '36-48', 'gender': Gender.MALE,
                           'start_date': date(2010, 1, 1),
                           'end_date': date(2010, 12, 31),
                           'methodology': MetricMethodologyType.PERSON,
                           'metric_type': MetricType.LIBERTY,
                           'state_code': 'CA'}

        metric_key = json.dumps(json_serializable_metric_key(metric_key_dict),
                                sort_keys=True)

        value = {'returns': 0,
                 'avg_liberty': float('NaN')}

        test_pipeline = TestPipeline()

        all_pipeline_options = PipelineOptions().get_all_options()

        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        output = (test_pipeline
                  | beam.Create([(metric_key, value)])
                  | 'Produce Recidivism Liberty Metric' >>
                  beam.ParDo(pipeline.
                             ProduceReincarcerationRecidivismMetric(),
                             **all_pipeline_options)
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testProduceReincarcerationRecidivismLibertyMetric_EmptyMetric(self):
        """Tests the ProduceReincarcerationRecidivismLibertyMetric DoFn in the
        pipeline when the metric dictionary is empty.

        This should not happen in the pipeline, but we test against it
        anyways.
        """

        metric_key = json.dumps({})

        value = {'total_releases': [10],
                 'recidivated_releases': [7],
                 'recidivism_rate': [0.7]}

        test_pipeline = TestPipeline()

        all_pipeline_options = PipelineOptions().get_all_options()

        job_timestamp = datetime.datetime.now().strftime(
            '%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        output = (test_pipeline
                  | beam.Create([(metric_key, value)])
                  | 'Produce Recidivism Liberty Metric' >>
                  beam.ParDo(
                      pipeline.ProduceReincarcerationRecidivismMetric(),
                      **all_pipeline_options))

        assert_that(output, equal_to([]))

        test_pipeline.run()


class TestProduceReincarcerationRecidivismCountMetric(unittest.TestCase):
    """Tests for the ProduceReincarcerationRecidivismCountMetric DoFn in the
     pipeline."""

    def testProduceReincarcerationRecidivismCountMetric(self):
        """Tests the ProduceReincarcerationRecidivismCountMetric DoFn in the
        pipeline."""

        metric_key_dict = {'stay_length_bucket': '36-48', 'gender': Gender.MALE,
                           'methodology': MetricMethodologyType.PERSON,
                           'start_date': date(2010, 1, 1),
                           'end_date': date(2010, 12, 31),
                           'metric_type': MetricType.COUNT, 'state_code': 'CA'}

        metric_key = json.dumps(json_serializable_metric_key(metric_key_dict),
                                sort_keys=True)

        value = 10

        test_pipeline = TestPipeline()

        all_pipeline_options = PipelineOptions().get_all_options()

        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        output = (test_pipeline
                  | beam.Create([(metric_key, value)])
                  | 'Produce Recidivism Metric' >>
                  beam.ParDo(
                      pipeline.ProduceReincarcerationRecidivismCountMetric(),
                      **all_pipeline_options)
                  )

        assert_that(output, AssertMatchers.
                    validate_recidivism_count_metric(10))

        test_pipeline.run()

    def testProduceReincarcerationRecidivismCountMetric_NoRecidivism(self):
        """Tests the ProduceReincarcerationRecidivismCountMetric DoFn in the
        pipeline when the number of returns is 0."""

        metric_key_dict = {'stay_length_bucket': '36-48', 'gender': Gender.MALE,
                           'methodology': MetricMethodologyType.PERSON,
                           'start_date': date(2010, 1, 1),
                           'end_date': date(2010, 12, 31),
                           'metric_type': MetricType.COUNT, 'state_code': 'CA'}

        metric_key = json.dumps(json_serializable_metric_key(metric_key_dict),
                                sort_keys=True)

        value = 0

        test_pipeline = TestPipeline()

        all_pipeline_options = PipelineOptions().get_all_options()

        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        output = (test_pipeline
                  | beam.Create([(metric_key, value)])
                  | 'Produce Recidivism Metric' >>
                  beam.ParDo(
                      pipeline.ProduceReincarcerationRecidivismCountMetric(),
                      **all_pipeline_options)
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testProduceReincarcerationRecidivismCountMetric_EmptyMetric(self):
        """Tests the ProduceReincarcerationRecidivismCountMetric DoFn in the
        pipeline when the metric dictionary is empty.

        This should not happen in the pipeline, but we test against it
        anyways.
        """

        metric_key = json.dumps({})

        value = 100

        test_pipeline = TestPipeline()

        all_pipeline_options = PipelineOptions().get_all_options()

        job_timestamp = datetime.datetime.now().strftime(
            '%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        output = (test_pipeline
                  | beam.Create([(metric_key, value)])
                  | 'Produce Recidivism Metric' >>
                  beam.ParDo(
                      pipeline.ProduceReincarcerationRecidivismCountMetric(),
                      **all_pipeline_options))

        assert_that(output, equal_to([]))

        test_pipeline.run()


class TestFilterMetrics(unittest.TestCase):
    """Tests for the FilterMetrics DoFn in the pipeline."""

    def testFilterMetrics_ExcludePerson(self):
        """Tests the FilterMetrics DoFn when the Person-based metrics should
         be excluded from the output."""
        methodologies = [MetricMethodologyType.EVENT]

        filter_metrics_kwargs = {'methodologies': methodologies}

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create(MetricGroup.get_list())
                  | 'Produce Recidivism Metric' >>
                  beam.ParDo(pipeline.FilterMetrics(),
                             **filter_metrics_kwargs))

        assert_that(output,
                    equal_to(
                        [MetricGroup.recidivism_metric_event_based]))

        test_pipeline.run()

    def testFilterMetrics_ExcludeEvent(self):
        """Tests the FilterMetrics DoFn when the Event-based metrics should
         be excluded from the output."""

        methodologies = [MetricMethodologyType.PERSON]

        filter_metrics_kwargs = {'methodologies': methodologies}

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create(MetricGroup.get_list())
                  | 'Produce Recidivism Metric' >>
                  beam.ParDo(pipeline.FilterMetrics(),
                             **filter_metrics_kwargs))

        expected_output = MetricGroup.get_list()
        expected_output.remove(MetricGroup.recidivism_metric_event_based)

        assert_that(output, equal_to(expected_output))

        test_pipeline.run()


class TestRecidivismMetricWritableDict(unittest.TestCase):
    """Tests the RecidivismMetricWritableDict DoFn in the pipeline."""

    def testRecidivismMetricWritableDict(self):
        """Tests converting the ReincarcerationRecidivismMetric to a dictionary
        that can be written to BigQuery."""
        metric = MetricGroup.recidivism_metric_with_race

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([metric])
                  | 'Convert to writable dict' >>
                  beam.ParDo(pipeline.RecidivismMetricWritableDict()))

        assert_that(output, AssertMatchers.
                    validate_metric_writable_dict())

        test_pipeline.run()

    def testRecidivismMetricWritableDict_WithDateField(self):
        """Tests converting the ReincarcerationRecidivismMetric to a dictionary
        that can be written to BigQuery, where the metric contains a date
        attribute."""
        metric = MetricGroup.recidivism_metric_without_dimensions
        metric.created_on = date.today()

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([metric])
                  | 'Convert to writable dict' >>
                  beam.ParDo(pipeline.RecidivismMetricWritableDict()))

        assert_that(output, AssertMatchers.
                    validate_metric_writable_dict())

        test_pipeline.run()

    def testRecidivismMetricWritableDict_WithEmptyDateField(self):
        """Tests converting the ReincarcerationRecidivismMetric to a dictionary
        that can be written to BigQuery, where the metric contains None on a
        date attribute."""
        metric = MetricGroup.recidivism_metric_without_dimensions
        metric.created_on = None

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([metric])
                  | 'Convert to writable dict' >>
                  beam.ParDo(pipeline.RecidivismMetricWritableDict()))

        assert_that(output, AssertMatchers.
                    validate_metric_writable_dict())

        test_pipeline.run()


class MetricGroup:
    """Stores a set of metrics where every dimension is included for testing
    dimension filtering."""
    recidivism_metric_with_age = ReincarcerationRecidivismRateMetric(
        job_id='12345', state_code='CA', release_cohort=2015,
        follow_up_period=1, methodology=MetricMethodologyType.PERSON,
        age_bucket='25-29', total_releases=1000, recidivated_releases=900,
        recidivism_rate=0.9)

    recidivism_metric_with_gender = ReincarcerationRecidivismRateMetric(
        job_id='12345', state_code='CA', release_cohort=2015,
        follow_up_period=1, methodology=MetricMethodologyType.PERSON,
        gender=Gender.MALE, total_releases=1000, recidivated_releases=875,
        recidivism_rate=0.875)

    recidivism_metric_with_race = ReincarcerationRecidivismRateMetric(
        job_id='12345', state_code='CA', release_cohort=2015,
        follow_up_period=1, methodology=MetricMethodologyType.PERSON,
        race=Race.BLACK, total_releases=1000, recidivated_releases=875,
        recidivism_rate=0.875)

    recidivism_metric_with_ethnicity = ReincarcerationRecidivismRateMetric(
        job_id='12345', state_code='CA', release_cohort=2015,
        follow_up_period=1, methodology=MetricMethodologyType.PERSON,
        ethnicity=Ethnicity.HISPANIC, total_releases=1000,
        recidivated_releases=875, recidivism_rate=0.875)

    recidivism_metric_with_release_facility = \
        ReincarcerationRecidivismRateMetric(
            job_id='12345', state_code='CA', release_cohort=2015,
            follow_up_period=1, methodology=MetricMethodologyType.PERSON,
            release_facility='Red',
            total_releases=1000, recidivated_releases=300, recidivism_rate=0.30)

    recidivism_metric_with_stay_length = ReincarcerationRecidivismRateMetric(
        job_id='12345', state_code='CA', release_cohort=2015,
        follow_up_period=1, methodology=MetricMethodologyType.PERSON,
        stay_length_bucket='12-24', total_releases=1000,
        recidivated_releases=300, recidivism_rate=0.30)

    recidivism_metric_without_dimensions = ReincarcerationRecidivismRateMetric(
        job_id='12345', state_code='CA', release_cohort=2015,
        follow_up_period=1, methodology=MetricMethodologyType.PERSON,
        total_releases=1500, recidivated_releases=1200, recidivism_rate=0.80)

    recidivism_metric_event_based = ReincarcerationRecidivismRateMetric(
        job_id='12345', state_code='CA', release_cohort=2015,
        follow_up_period=1, methodology=MetricMethodologyType.EVENT,
        total_releases=1500, recidivated_releases=1200, recidivism_rate=0.80)

    @staticmethod
    def get_list():
        return [MetricGroup.recidivism_metric_with_age,
                MetricGroup.recidivism_metric_with_gender,
                MetricGroup.recidivism_metric_with_race,
                MetricGroup.recidivism_metric_with_ethnicity,
                MetricGroup.recidivism_metric_with_release_facility,
                MetricGroup.recidivism_metric_with_stay_length,
                MetricGroup.recidivism_metric_without_dimensions,
                MetricGroup.recidivism_metric_event_based]


class AssertMatchers:
    """Functions to be used by Apache Beam testing `assert_that` functions to
    validate pipeline outputs."""

    @staticmethod
    def validate_pipeline_test():

        def _validate_pipeline_test(output):

            for metric in output:
                if not isinstance(metric, ReincarcerationRecidivismMetric):
                    raise BeamAssertException(
                        'Failed assert. Output is not'
                        'of type'
                        ' ReincarcerationRecidivismMetric.')

        return _validate_pipeline_test

    @staticmethod
    def count_combinations(expected_combination_counts):
        """Asserts that the number of metric combinations matches the expected
        counts for each release cohort year."""
        def _count_combinations(output):
            actual_combination_counts = {}

            for key in expected_combination_counts.keys():
                actual_combination_counts[key] = 0

            for result in output:
                combination, _ = result

                combination_dict = json.loads(combination)

                if combination_dict.get('metric_type') == MetricType.RATE.value:
                    release_cohort_year = combination_dict['release_cohort']
                    actual_combination_counts[release_cohort_year] = \
                        actual_combination_counts[release_cohort_year] + 1
                elif combination_dict.get('metric_type') == \
                        MetricType.COUNT.value:
                    actual_combination_counts['counts'] = \
                        actual_combination_counts['counts'] + 1

            for key in expected_combination_counts:
                if expected_combination_counts[key] != \
                        actual_combination_counts[key]:
                    raise BeamAssertException('Failed assert. Count does not'
                                              'match expected value.')

        return _count_combinations

    @staticmethod
    def validate_recidivism_rate_metric(expected_recidivism_rate):
        """Asserts that the recidivism rate on the
        ReincarcerationRecidivismRateMetric produced by the pipeline matches
        the expected recidivism rate."""
        def _validate_recidivism_rate_metric(output):
            if len(output) != 1:
                raise BeamAssertException('Failed assert. Should be only one '
                                          'ReincarcerationRecidivismMetric'
                                          ' returned.')

            recidivism_metric = output[0]

            if recidivism_metric.recidivism_rate != expected_recidivism_rate:
                raise BeamAssertException('Failed assert. Recidivism rate does'
                                          'not match expected value.')

        return _validate_recidivism_rate_metric

    @staticmethod
    def validate_recidivism_count_metric(expected_recidivism_count):
        """Asserts that the recidivism count on the
        ReincarcerationRecidivismCountMetric produced by the pipeline matches
        the expected recidivism count."""
        def _validate_recidivism_count_metric(output):
            if len(output) != 1:
                raise BeamAssertException('Failed assert. Should be only one '
                                          'ReincarcerationRecidivismMetric'
                                          ' returned.')

            recidivism_metric = output[0]

            if recidivism_metric.returns != expected_recidivism_count:
                raise BeamAssertException('Failed assert. Recidivism count does'
                                          'not match expected value.')

        return _validate_recidivism_count_metric

    @staticmethod
    def validate_recidivism_liberty_metric(expected_days_at_liberty):
        """Asserts that the average days of liberty on the
        ReincarcerationRecidivismLibertyMetric produced by the pipeline matches
        the expected count."""

        def _validate_recidivism_liberty_metric(output):
            if len(output) != 1:
                raise BeamAssertException('Failed assert. Should be only one '
                                          'ReincarcerationRecidivismMetric'
                                          ' returned.')

            recidivism_metric = output[0]

            if recidivism_metric.avg_liberty != expected_days_at_liberty:
                raise BeamAssertException('Failed assert. Avg_liberty does'
                                          'not match expected value.')

        return _validate_recidivism_liberty_metric

    @staticmethod
    def validate_job_id_on_metric(expected_job_id):

        def _validate_job_id_on_metric(output):

            for metric in output:

                if metric.job_id != expected_job_id:
                    raise BeamAssertException('Failed assert. Output job_id: '
                                              f"{metric.job_id} does not match"
                                              "expected value: "
                                              f"{expected_job_id}")

        return _validate_job_id_on_metric

    @staticmethod
    def validate_metric_writable_dict():

        def _validate_metric_writable_dict(output):
            for metric_dict in output:
                for _, value in metric_dict.items():
                    if isinstance(value, Enum):
                        raise BeamAssertException('Failed assert. Dictionary'
                                                  'contains invalid Enum '
                                                  f"value: {value}")
                    if isinstance(value, datetime.date):
                        raise BeamAssertException('Failed assert. Dictionary'
                                                  'contains invalid date '
                                                  f"value: {value}")

        return _validate_metric_writable_dict
