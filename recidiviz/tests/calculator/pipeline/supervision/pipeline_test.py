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

"""Tests for supervision/pipeline.py"""
import json
import unittest

import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to, BeamAssertException
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.pipeline_options import PipelineOptions

import datetime
from datetime import date

from recidiviz.calculator.pipeline.supervision import pipeline, calculator
from recidiviz.calculator.pipeline.supervision.metrics import \
    SupervisionMetric, SupervisionMetricType
from recidiviz.calculator.pipeline.supervision.supervision_month import \
    NonRevocationReturnSupervisionMonth, RevocationReturnSupervisionMonth
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType
from recidiviz.calculator.pipeline.utils import extractor_utils
from recidiviz.calculator.pipeline.recidivism.pipeline import \
    json_serializable_metric_key
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, \
    StateIncarcerationFacilitySecurityLevel
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import \
    StateIncarcerationPeriod, Gender, Race, ResidencyStatus, Ethnicity
from recidiviz.persistence.entity.state.entities import StatePerson
from recidiviz.tests.calculator.calculator_test_utils import \
    normalized_database_base_dict, normalized_database_base_dict_list
from recidiviz.tests.persistence.database import database_test_utils


class TestSupervisionPipeline(unittest.TestCase):
    """Tests the entire supervision pipeline."""

    def testSupervisionPipeline(self):
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

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=1111,
            state_code='CA',
            county_code='124',
            start_date=date(2015, 3, 14),
            termination_date=date(2016, 12, 29),
            supervision_type=StateSupervisionType.PROBATION,
            person_id=fake_person_id
        )

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration),
            normalized_database_base_dict(first_reincarceration),
            normalized_database_base_dict(subsequent_reincarceration)
        ]

        supervision_periods_data = [
            normalized_database_base_dict(supervision_period)
        ]

        supervision_violation_response = \
            database_test_utils.generate_test_supervision_violation_response(
                fake_person_id)

        supervision_violation_response_data = [
            normalized_database_base_dict(supervision_violation_response)
        ]

        inclusions = {
            'age_bucket': False,
            'gender': False,
            'race': False,
            'ethnicity': False,
        }

        data_dict = {schema.StatePerson.__tablename__: persons_data,
                     schema.StatePersonRace.__tablename__: races_data,
                     schema.StatePersonEthnicity.__tablename__: ethnicity_data,
                     schema.StateIncarcerationPeriod.__tablename__:
                         incarceration_periods_data,
                     schema.StateSupervisionViolationResponse.__tablename__:
                         supervision_violation_response_data,
                     schema.StateSupervisionPeriod.__tablename__:
                         supervision_periods_data}

        test_pipeline = TestPipeline()

        # Get StatePersons
        persons = (test_pipeline
                   | 'Load Persons' >>
                   extractor_utils.BuildRootEntity(
                       dataset=None,
                       data_dict=data_dict,
                       root_schema_class=schema.StatePerson,
                       root_entity_class=entities.StatePerson,
                       unifying_id_field='person_id',
                       build_related_entities=True))

        # Get StateIncarcerationPeriods
        incarceration_periods = (test_pipeline
                                 | 'Load IncarcerationPeriods' >>
                                 extractor_utils.BuildRootEntity(
                                     dataset=None,
                                     data_dict=data_dict,
                                     root_schema_class=
                                     schema.StateIncarcerationPeriod,
                                     root_entity_class=
                                     entities.StateIncarcerationPeriod,
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
                 root_entity_class=entities.StateSupervisionViolationResponse,
                 unifying_id_field='person_id',
                 build_related_entities=True
             ))

        # Get StateSupervisionPeriods
        supervision_periods = (test_pipeline
                               | 'Load SupervisionPeriods' >>
                               extractor_utils.BuildRootEntity(
                                   dataset=None,
                                   data_dict=data_dict,
                                   root_schema_class=
                                   schema.StateSupervisionPeriod,
                                   root_entity_class=
                                   entities.StateSupervisionPeriod,
                                   unifying_id_field='person_id',
                                   build_related_entities=False))

        # Group StateIncarcerationPeriods and StateSupervisionViolationResponses
        # by person_id
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
            | 'Set hydrated StateSupervisionViolationResponses on '
            'the StateIncarcerationPeriods' >>
            beam.ParDo(pipeline.SetViolationResponseOnIncarcerationPeriod()))

        # Group each StatePerson with their StateIncarcerationPeriods and
        # StateSupervisionPeriods
        person_and_periods = ({'person': persons,
                               'incarceration_periods':
                                   incarceration_periods_with_source_violations,
                               'supervision_periods':
                                   supervision_periods
                               }
                              | 'Group StatePerson to StateIncarcerationPeriods'
                                ' and StateSupervisionPeriods' >>
                              beam.CoGroupByKey()
                              )

        # Identify SupervisionMonths from the StatePerson's
        # StateSupervisionPeriods and StateIncarcerationPeriods
        person_months = (
            person_and_periods |
            'Get Supervision Months' >>
            pipeline.GetSupervisionMonths())

        # Get pipeline job details for accessing job_id
        all_pipeline_options = PipelineOptions().get_all_options()

        # Add timestamp for local jobs
        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        # Get supervision metrics
        supervision_metrics = (person_months
                               | 'Get Supervision Metrics' >>
                               pipeline.GetSupervisionMetrics(
                                   pipeline_options=all_pipeline_options,
                                   inclusions=inclusions))

        assert_that(supervision_metrics,
                    AssertMatchers.validate_pipeline_test())

        test_pipeline.run()

    def testSupervisionPipelineNoSupervision(self):
        """Tests the supervision pipeline when a person doesn't have any
        supervision periods."""
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

        supervision_period__1 = schema.StateSupervisionPeriod(
            supervision_period_id=1111,
            state_code='CA',
            county_code='124',
            start_date=date(2015, 3, 14),
            termination_date=date(2016, 12, 29),
            supervision_type=StateSupervisionType.PROBATION,
            person_id=fake_person_id_1
        )

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

        supervision_periods_data = [
            normalized_database_base_dict(supervision_period__1)
        ]

        inclusions = {
            'age_bucket': False,
            'gender': False,
            'race': False,
            'ethnicity': False,
        }

        data_dict = {schema.StatePerson.__tablename__: persons_data,
                     schema.StateIncarcerationPeriod.__tablename__:
                         incarceration_periods_data,
                     schema.StateSupervisionViolationResponse.__tablename__:
                         supervision_violation_response_data,
                     schema.StateSupervisionViolation.__tablename__:
                         supervision_violation_data,
                     schema.StateSupervisionPeriod.__tablename__:
                         supervision_periods_data}

        test_pipeline = TestPipeline()

        # Get StatePersons
        persons = (test_pipeline
                   | 'Load Persons' >>
                   extractor_utils.BuildRootEntity(
                       dataset=None,
                       data_dict=data_dict,
                       root_schema_class=schema.StatePerson,
                       root_entity_class=entities.StatePerson,
                       unifying_id_field='person_id',
                       build_related_entities=True))

        # Get StateIncarcerationPeriods
        incarceration_periods = (test_pipeline
                                 | 'Load IncarcerationPeriods' >>
                                 extractor_utils.BuildRootEntity(
                                     dataset=None,
                                     data_dict=data_dict,
                                     root_schema_class=
                                     schema.StateIncarcerationPeriod,
                                     root_entity_class=
                                     entities.StateIncarcerationPeriod,
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
                 root_entity_class=entities.StateSupervisionViolationResponse,
                 unifying_id_field='person_id',
                 build_related_entities=True
             ))

        # Get StateSupervisionPeriods
        supervision_periods = (test_pipeline
                               | 'Load SupervisionPeriods' >>
                               extractor_utils.BuildRootEntity(
                                   dataset=None,
                                   data_dict=data_dict,
                                   root_schema_class=
                                   schema.StateSupervisionPeriod,
                                   root_entity_class=
                                   entities.StateSupervisionPeriod,
                                   unifying_id_field='person_id',
                                   build_related_entities=False))

        # Group StateIncarcerationPeriods and StateSupervisionViolationResponses
        # by person_id
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
            | 'Set hydrated StateSupervisionViolationResponses on '
            'the StateIncarcerationPeriods' >>
            beam.ParDo(pipeline.SetViolationResponseOnIncarcerationPeriod()))

        # Group each StatePerson with their StateIncarcerationPeriods and
        # StateSupervisionPeriods
        person_and_periods = ({'person': persons,
                               'incarceration_periods':
                                   incarceration_periods_with_source_violations,
                               'supervision_periods':
                                   supervision_periods
                               }
                              | 'Group StatePerson to StateIncarcerationPeriods'
                                ' and StateSupervisionPeriods' >>
                              beam.CoGroupByKey()
                              )

        # Identify SupervisionMonths from the StatePerson's
        # StateSupervisionPeriods and StateIncarcerationPeriods
        person_months = (
            person_and_periods |
            'Get Supervision Months' >>
            pipeline.GetSupervisionMonths())

        # Get pipeline job details for accessing job_id
        all_pipeline_options = PipelineOptions().get_all_options()

        # Add timestamp for local jobs
        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        # Get supervision metrics
        supervision_metrics = (person_months
                               | 'Get Supervision Metrics' >>
                               pipeline.GetSupervisionMetrics(
                                   pipeline_options=all_pipeline_options,
                                   inclusions=inclusions))

        assert_that(supervision_metrics,
                    AssertMatchers.validate_pipeline_test())

        test_pipeline.run()


class TestClassifySupervisionMonths(unittest.TestCase):
    """Tests the ClassifySupervisionMonths DoFn in the pipeline."""

    def testClassifySupervisionMonths(self):
        """Tests the ClassifySupervisionMonths DoFn."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=1111,
            state_code='CA',
            county_code='124',
            start_date=date(2015, 3, 14),
            termination_date=date(2015, 5, 29),
            supervision_type=StateSupervisionType.PROBATION,
            person_id=fake_person_id
        )

        person_periods = {'person': [fake_person],
                          'incarceration_periods': [
                              incarceration_period
                          ],
                          'supervision_periods': [
                              supervision_period
                          ]}

        supervision_months = [
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2015, 3,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2015, 4,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2015, 5,
                supervision_period.supervision_type)
        ]

        correct_output = [
            (fake_person, supervision_months)]

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(fake_person_id,
                                  person_periods)])
                  | 'Identify Supervision Months' >>
                  beam.ParDo(pipeline.ClassifySupervisionMonths())
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifySupervisionMonthsRevocation(self):
        """Tests the ClassifySupervisionMonths DoFn when there is an instance
        of revocation."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=1111,
            state_code='CA',
            county_code='124',
            start_date=date(2015, 3, 14),
            termination_date=date(2015, 5, 29),
            supervision_type=StateSupervisionType.PROBATION,
            person_id=fake_person_id
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='CA',
            admission_date=date(2015, 5, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            PROBATION_REVOCATION,
            release_date=date(2018, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        person_periods = {'person': [fake_person],
                          'incarceration_periods': [
                              incarceration_period
                          ],
                          'supervision_periods': [
                              supervision_period
                          ]}

        supervision_months = [
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2015, 3,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2015, 4,
                supervision_period.supervision_type),
            RevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2015, 5,
                supervision_period.supervision_type)
        ]

        correct_output = [
            (fake_person, supervision_months)]

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(fake_person_id,
                                  person_periods)])
                  | 'Identify Supervision Months' >>
                  beam.ParDo(pipeline.ClassifySupervisionMonths())
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifySupervisionMonths_NoIncarcerationPeriods(self):
        """Tests the ClassifySupervisionMonths DoFn when the person has no
        incarceration periods."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=1111,
            state_code='CA',
            county_code='124',
            start_date=date(2015, 3, 14),
            termination_date=date(2015, 5, 29),
            supervision_type=StateSupervisionType.PROBATION,
            person_id=fake_person_id
        )

        person_periods = {'person': [fake_person],
                          'incarceration_periods': [],
                          'supervision_periods': [
                              supervision_period
                          ]}

        supervision_months = [
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2015, 3,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2015, 4,
                supervision_period.supervision_type),
            NonRevocationReturnSupervisionMonth(
                supervision_period.state_code,
                2015, 5,
                supervision_period.supervision_type)
        ]

        correct_output = [
            (fake_person, supervision_months)]

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(fake_person_id,
                                  person_periods)])
                  | 'Identify Supervision Months' >>
                  beam.ParDo(pipeline.ClassifySupervisionMonths())
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifySupervisionMonths_NoSupervisionPeriods(self):
        """Tests the ClassifySupervisionMonths DoFn when the person
        has no supervision periods."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='TX',
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        person_periods = {'person': [fake_person],
                          'incarceration_periods': [
                              incarceration_period
                          ],
                          'supervision_periods': []}

        correct_output = []

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(fake_person_id,
                                  person_periods)])
                  | 'Identify Supervision Months' >>
                  beam.ParDo(pipeline.ClassifySupervisionMonths())
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()


class TestCalculateSupervisionMetricCombinations(unittest.TestCase):
    """Tests the CalculateSupervisionMetricCombinations DoFn in the pipeline."""

    def testCalculateSupervisionMetricCombinations(self):
        """Tests the CalculateSupervisionMetricCombinations DoFn."""
        fake_person = StatePerson.new_with_defaults(
            person_id=123, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        supervision_months = [
            NonRevocationReturnSupervisionMonth('CA',
                                                2015, 3,
                                                StateSupervisionType.PROBATION),
        ]

        inclusions = {
            'age_bucket': True,
            'gender': True,
            'race': True,
            'ethnicity': True,
        }

        # Get the number of combinations of person-event characteristics.
        num_combinations = len(calculator.characteristic_combinations(
            fake_person, supervision_months[0], inclusions))
        assert num_combinations > 0

        # Each characteristic combination will be tracked for each of the
        # months and the two methodology types
        expected_population_metric_count = \
            num_combinations * len(supervision_months) * 2

        expected_combination_counts = \
            {'population': expected_population_metric_count}

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(fake_person, supervision_months)])
                  | 'Calculate Supervision Metrics' >>
                  beam.ParDo(pipeline.CalculateSupervisionMetricCombinations(),
                             **inclusions).with_outputs('populations')
                  )

        assert_that(output.populations, AssertMatchers.
                    count_combinations(expected_combination_counts),
                    'Assert number of population metrics is expected value')

        test_pipeline.run()

    def testCalculateSupervisionMetricCombinations_NoSupervision(self):
        """Tests the CalculateSupervisionMetricCombinations when there are
        no supervision months. This should never happen because any person
        without supervision time is dropped entirely from the pipeline."""
        fake_person = StatePerson.new_with_defaults(
            person_id=123, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([(fake_person, [])])
                  | 'Calculate Supervision Metrics' >>
                  beam.ParDo(pipeline.CalculateSupervisionMetricCombinations())
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testCalculateSupervisionMetricCombinations_NoInput(self):
        """Tests the CalculateSupervisionMetricCombinations when there is
        no input to the function."""

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([])
                  | 'Calculate Supervision Metrics' >>
                  beam.ParDo(pipeline.CalculateSupervisionMetricCombinations())
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class TestProduceSupervisionPopulationMetric(unittest.TestCase):
    """Tests the ProduceSupervisionPopulationMetric DoFn in the pipeline."""

    def testProduceSupervisionPopulationMetric(self):
        metric_key_dict = {'gender': Gender.MALE,
                           'methodology': MetricMethodologyType.PERSON,
                           'year': 1999,
                           'month': 3,
                           'metric_type':
                               SupervisionMetricType.POPULATION.value,
                           'state_code': 'CA'}

        metric_key = json.dumps(json_serializable_metric_key(metric_key_dict),
                                sort_keys=True)

        value = 10

        test_pipeline = TestPipeline()

        all_pipeline_options = PipelineOptions().get_all_options()

        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        output = (test_pipeline
                  | beam.Create([(metric_key, value)])
                  | 'Produce Supervision Population Metric' >>
                  beam.ParDo(pipeline.
                             ProduceSupervisionPopulationMetric(),
                             **all_pipeline_options)
                  )

        assert_that(output, AssertMatchers.
                    validate_supervision_population_metric(value))

        test_pipeline.run()

    def testProduceSupervisionPopulationMetric_EmptyMetric(self):
        metric_key_dict = {}

        metric_key = json.dumps(json_serializable_metric_key(metric_key_dict),
                                sort_keys=True)

        value = 1131

        test_pipeline = TestPipeline()

        all_pipeline_options = PipelineOptions().get_all_options()

        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        output = (test_pipeline
                  | beam.Create([(metric_key, value)])
                  | 'Produce Supervision Population Metric' >>
                  beam.ParDo(pipeline.
                             ProduceSupervisionPopulationMetric(),
                             **all_pipeline_options)
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class AssertMatchers:
    """Functions to be used by Apache Beam testing `assert_that` functions to
    validate pipeline outputs."""

    @staticmethod
    def validate_pipeline_test():

        def _validate_pipeline_test(output):

            for metric in output:
                if not isinstance(metric, SupervisionMetric):
                    raise BeamAssertException(
                        'Failed assert. Output is not'
                        'of type'
                        ' SupervisionMetric.')

        return _validate_pipeline_test

    @staticmethod
    def count_combinations(expected_combination_counts):
        """Asserts that the number of metric combinations matches the expected
        counts."""
        def _count_combinations(output):
            actual_combination_counts = {}

            for key in expected_combination_counts.keys():
                actual_combination_counts[key] = 0

            for result in output:
                combination, _ = result

                combination_dict = json.loads(combination)

                if combination_dict.get('metric_type') == \
                        SupervisionMetricType.POPULATION.value:
                    actual_combination_counts['population'] = \
                        actual_combination_counts['population'] + 1

            for key in expected_combination_counts:
                if expected_combination_counts[key] != \
                        actual_combination_counts[key]:
                    raise BeamAssertException('Failed assert. Count does not'
                                              'match expected value.')

        return _count_combinations

    @staticmethod
    def validate_supervision_population_metric(expected_population_count):
        """Asserts that the count on the
        SupervisionPopulationMetric produced by the pipeline matches
        the expected population count."""
        def _validate_supervision_population_metric(output):
            if len(output) != 1:
                raise BeamAssertException('Failed assert. Should be only one '
                                          'SupervisionPopulationMetric'
                                          ' returned.')

            population_metric = output[0]

            if population_metric.count != expected_population_count:
                raise BeamAssertException('Failed assert. Population count does'
                                          'not match expected value.')

        return _validate_supervision_population_metric
