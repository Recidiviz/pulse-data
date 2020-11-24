# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
# pylint: disable=wrong-import-order

"""Tests for program/pipeline.py"""
import unittest
from typing import Optional, Set
from unittest import mock

import apache_beam as beam
import pytest
from apache_beam.pvalue import AsDict, AsList
from apache_beam.testing.util import assert_that, equal_to, BeamAssertException
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.pipeline_options import PipelineOptions

import datetime
from datetime import date

from freezegun import freeze_time
from mock import patch

from recidiviz.calculator.pipeline.program import pipeline
from recidiviz.calculator.pipeline.program.metrics import ProgramMetric, \
    ProgramMetricType
from recidiviz.calculator.pipeline.program.program_event import \
    ProgramReferralEvent, ProgramParticipationEvent
from recidiviz.calculator.pipeline.utils import extractor_utils
from recidiviz.calculator.pipeline.utils.beam_utils import ConvertDictToKVTuple
from recidiviz.calculator.pipeline.utils.metric_utils import MetricMethodologyType
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata, BuildPersonMetadata, \
    ExtractPersonEventsMetadata
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType
from recidiviz.common.constants.state.state_program_assignment import StateProgramAssignmentParticipationStatus
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus, StateSupervisionPeriodTerminationReason
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import \
    Gender, Race, ResidencyStatus, Ethnicity, StatePerson

from recidiviz.tests.calculator.calculator_test_utils import \
    normalized_database_base_dict, normalized_database_base_dict_list
from recidiviz.tests.calculator.pipeline.fake_bigquery import FakeReadFromBigQueryFactory
from recidiviz.tests.persistence.database import database_test_utils

ALL_METRIC_INCLUSIONS_DICT = {
    metric_type: True for metric_type in ProgramMetricType
}


class TestProgramPipeline(unittest.TestCase):
    """Tests the entire program pipeline."""
    def setUp(self) -> None:
        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()
        self.assessment_types_patcher = mock.patch(
            'recidiviz.calculator.pipeline.program.identifier.assessment_utils.'
            '_assessment_types_of_class_for_state')
        self.mock_assessment_types = self.assessment_types_patcher.start()
        self.mock_assessment_types.return_value = [StateAssessmentType.ORAS]

    def tearDown(self) -> None:
        self.assessment_types_patcher.stop()

    @staticmethod
    def build_data_dict(fake_person_id: int,
                        fake_supervision_period_id: int):
        """Builds a data_dict for a basic run of the pipeline."""
        fake_person = schema.StatePerson(
            state_code='US_XX',
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        persons_data = [normalized_database_base_dict(fake_person)]

        race_1 = schema.StatePersonRace(
            person_race_id=111,
            state_code='US_XX',
            race=Race.BLACK,
            person_id=fake_person_id
        )

        race_2 = schema.StatePersonRace(
            person_race_id=111,
            state_code='US_XX',
            race=Race.WHITE,
            person_id=fake_person_id
        )

        races_data = normalized_database_base_dict_list([race_1, race_2])

        ethnicity = schema.StatePersonEthnicity(
            person_ethnicity_id=111,
            state_code='US_XX',
            ethnicity=Ethnicity.HISPANIC,
            person_id=fake_person_id)

        ethnicity_data = normalized_database_base_dict_list([ethnicity])

        program_assignment = schema.StateProgramAssignment(
            state_code='US_XX',
            program_assignment_id=123,
            referral_date=date(2015, 5, 10),
            person_id=fake_person_id,
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS
        )

        assessment = schema.StateAssessment(
            assessment_id=298374,
            assessment_date=date(2015, 3, 19),
            assessment_type='LSIR',
            person_id=fake_person_id
        )

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=fake_supervision_period_id,
            state_code='US_XX',
            county_code='124',
            start_date=date(2015, 3, 14),
            termination_date=date(2016, 12, 29),
            supervision_type=StateSupervisionType.PROBATION,
            person_id=fake_person_id
        )

        program_assignment_data = [
            normalized_database_base_dict(program_assignment)
        ]

        assessment_data = [
            normalized_database_base_dict(assessment)
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

        data_dict = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StatePersonRace.__tablename__: races_data,
            schema.StatePersonEthnicity.__tablename__: ethnicity_data,
            schema.StateSupervisionViolationResponse.__tablename__:
                supervision_violation_response_data,
            schema.StateSupervisionPeriod.__tablename__:
                supervision_periods_data,
            schema.StateProgramAssignment.__tablename__:
                program_assignment_data,
            schema.StateAssessment.__tablename__:
                assessment_data,
            schema.StatePersonExternalId.__tablename__: [],
            schema.StatePersonAlias.__tablename__: [],
            schema.StateSentenceGroup.__tablename__: [],
        }

        return data_dict

    def testProgramPipeline(self):
        """Tests the program pipeline."""
        fake_person_id = 12345
        fake_supervision_period_id = 12345

        data_dict = self.build_data_dict(fake_person_id, fake_supervision_period_id)

        dataset = 'recidiviz-123.state'

        with patch('recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery',
                   self.fake_bq_source_factory.create_fake_bq_source_constructor(dataset, data_dict)):
            self.run_test_pipeline(dataset, fake_supervision_period_id)

    def testProgramPipelineWithFilterSet(self):
        """Tests the program pipeline."""
        fake_person_id = 12345
        fake_supervision_period_id = 12345

        data_dict = self.build_data_dict(fake_person_id, fake_supervision_period_id)

        dataset = 'recidiviz-123.state'

        with patch('recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery',
                   self.fake_bq_source_factory.create_fake_bq_source_constructor(dataset, data_dict)):
            self.run_test_pipeline(
                dataset, fake_supervision_period_id, unifying_id_field_filter_set={fake_person_id})

    # TODO(#4375): Update tests to run actual pipeline code and only mock BQ I/O
    def run_test_pipeline(self,
                          dataset: str,
                          fake_supervision_period_id: int,
                          unifying_id_field_filter_set: Optional[Set[int]] = None,
                          metric_types_filter: Optional[Set[str]] = None):
        """Runs a test version of the program pipeline."""
        test_pipeline = TestPipeline()

        # Get StatePersons
        persons = (test_pipeline
                   | 'Load Persons' >>  # type: ignore
                   extractor_utils.BuildRootEntity(
                       dataset=dataset,
                       root_entity_class=entities.StatePerson,
                       unifying_id_field=entities.StatePerson.get_class_id_name(),
                       build_related_entities=True))

        # Get StateProgramAssignments
        program_assignments = (test_pipeline
                               | 'Load Program Assignments' >>  # type: ignore
                               extractor_utils.BuildRootEntity(
                                   dataset=dataset,
                                   root_entity_class=entities.
                                   StateProgramAssignment,
                                   unifying_id_field=entities.StatePerson.get_class_id_name(),
                                   build_related_entities=True,
                                   unifying_id_field_filter_set=unifying_id_field_filter_set))

        # Get StateAssessments
        assessments = (test_pipeline
                       | 'Load Assessments' >>  # type: ignore
                       extractor_utils.BuildRootEntity(
                           dataset=dataset,
                           root_entity_class=entities.
                           StateAssessment,
                           unifying_id_field=entities.StatePerson.get_class_id_name(),
                           build_related_entities=False,
                           unifying_id_field_filter_set=unifying_id_field_filter_set))

        # Get StateSupervisionPeriods
        supervision_periods = (test_pipeline
                               | 'Load SupervisionPeriods' >>  # type: ignore
                               extractor_utils.BuildRootEntity(
                                   dataset=dataset,
                                   root_entity_class=
                                   entities.StateSupervisionPeriod,
                                   unifying_id_field=entities.StatePerson.get_class_id_name(),
                                   build_related_entities=False,
                                   unifying_id_field_filter_set=unifying_id_field_filter_set))

        supervision_period_to_agent_map = {
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_period_id': fake_supervision_period_id
        }

        supervision_period_to_agent_associations = (
            test_pipeline
            | 'Create SupervisionPeriod to Agent table' >>
            beam.Create([supervision_period_to_agent_map])
        )

        supervision_period_to_agent_associations_as_kv = (
            supervision_period_to_agent_associations |
            'Convert SupervisionPeriod to Agent table to KV tuples' >>
            beam.ParDo(ConvertDictToKVTuple(),
                       'supervision_period_id')
        )

        state_race_ethnicity_population_count = {
            'state_code': 'US_XX',
            'race_or_ethnicity': 'BLACK',
            'population_count': 1,
            'representation_priority': 1
        }

        state_race_ethnicity_population_counts = (
            test_pipeline | 'Create state_race_ethnicity_population_count table' >> beam.Create(
                [state_race_ethnicity_population_count])
        )

        # Group each StatePerson with their other entities
        persons_entities = (
            {'person': persons,
             'program_assignments': program_assignments,
             'assessments': assessments,
             'supervision_periods': supervision_periods
             }
            | 'Group StatePerson to StateProgramAssignments and' >>
            beam.CoGroupByKey()
        )

        # Identify ProgramEvents from the StatePerson's
        # StateProgramAssignments
        person_program_events = (
            persons_entities
            | beam.ParDo(pipeline.ClassifyProgramAssignments(),
                         AsDict(
                             supervision_period_to_agent_associations_as_kv
                         ))
        )

        person_metadata = (persons
                           | "Build the person_metadata dictionary" >>
                           beam.ParDo(BuildPersonMetadata(),
                                      AsList(state_race_ethnicity_population_counts)))

        person_program_events_with_metadata = (
            {
                'person_events': person_program_events,
                'person_metadata': person_metadata
            }
            | 'Group ProgramEvents with person-level metadata' >> beam.CoGroupByKey()
            | 'Organize StatePerson, PersonMetadata and ProgramEvents for calculations' >>
            beam.ParDo(ExtractPersonEventsMetadata())
        )

        # Get pipeline job details for accessing job_id
        all_pipeline_options = PipelineOptions().get_all_options()

        # Add timestamp for local jobs
        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        metric_types = metric_types_filter if metric_types_filter else {'ALL'}

        # Get program metrics
        program_metrics = (person_program_events_with_metadata
                           | 'Get Program Metrics' >>  # type: ignore
                           pipeline.GetProgramMetrics(
                               pipeline_options=all_pipeline_options,
                               metric_types=metric_types,
                               calculation_end_month=None,
                               calculation_month_count=-1))

        assert_that(program_metrics, AssertMatchers.validate_pipeline_test())

        test_pipeline.run()

    def testProgramPipelineNoReferrals(self):
        """Tests the program pipeline where one person does not have any
        program assignment entities."""
        fake_person_id = 12345
        fake_person_id_2 = 9876

        fake_person = schema.StatePerson(
            state_code='US_XX',
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        fake_person_2 = schema.StatePerson(
            state_code='US_XX',
            person_id=fake_person_id_2, gender=Gender.MALE,
            birthdate=date(1974, 3, 12),
            residency_status=ResidencyStatus.PERMANENT)

        persons_data = normalized_database_base_dict_list([fake_person,
                                                           fake_person_2])

        race_1 = schema.StatePersonRace(
            person_race_id=111,
            state_code='US_XX',
            race=Race.BLACK,
            person_id=fake_person_id
        )

        race_2 = schema.StatePersonRace(
            person_race_id=111,
            state_code='US_XX',
            race=Race.WHITE,
            person_id=fake_person_id
        )

        races_data = normalized_database_base_dict_list([race_1, race_2])

        ethnicity = schema.StatePersonEthnicity(
            person_ethnicity_id=111,
            state_code='US_XX',
            ethnicity=Ethnicity.HISPANIC,
            person_id=fake_person_id)

        ethnicity_data = normalized_database_base_dict_list([ethnicity])

        # Program assignment for a different person
        program_assignment = schema.StateProgramAssignment(
            state_code='US_XX',
            program_assignment_id=123,
            referral_date=date(2015, 5, 10),
            person_id=fake_person_id_2,
            participation_status=StateProgramAssignmentParticipationStatus.DENIED
        )

        assessment = schema.StateAssessment(
            assessment_id=298374,
            assessment_date=date(2015, 3, 19),
            assessment_type='LSIR',
            person_id=fake_person_id
        )

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=1111,
            state_code='US_XX',
            county_code='124',
            start_date=date(2015, 3, 14),
            termination_date=date(2016, 12, 29),
            supervision_type=StateSupervisionType.PROBATION,
            person_id=fake_person_id
        )

        program_assignment_data = [
            normalized_database_base_dict(program_assignment)
        ]

        assessment_data = [
            normalized_database_base_dict(assessment)
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

        data_dict = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StatePersonRace.__tablename__: races_data,
            schema.StatePersonEthnicity.__tablename__: ethnicity_data,
            schema.StateSupervisionViolationResponse.__tablename__:
                supervision_violation_response_data,
            schema.StateSupervisionPeriod.__tablename__:
                supervision_periods_data,
            schema.StateProgramAssignment.__tablename__:
                program_assignment_data,
            schema.StateAssessment.__tablename__:
                assessment_data,
            schema.StatePersonExternalId.__tablename__: [],
            schema.StatePersonAlias.__tablename__: [],
            schema.StateSentenceGroup.__tablename__: [],
        }

        dataset = 'recidiviz-123.state'

        with patch('recidiviz.calculator.pipeline.utils.extractor_utils.ReadFromBigQuery',
                   self.fake_bq_source_factory.create_fake_bq_source_constructor(dataset, data_dict)):
            self.run_test_pipeline(dataset, supervision_period.supervision_period_id)


class TestClassifyProgramAssignments(unittest.TestCase):
    """Tests the ClassifyProgramAssignments DoFn."""

    def setUp(self) -> None:
        self.assessment_types_patcher = mock.patch(
            'recidiviz.calculator.pipeline.program.identifier.assessment_utils.'
            '_assessment_types_of_class_for_state')
        self.mock_assessment_types = self.assessment_types_patcher.start()
        self.mock_assessment_types.return_value = [StateAssessmentType.ORAS]

    def tearDown(self) -> None:
        self.assessment_types_patcher.stop()

    @freeze_time('2009-10-19')
    def testClassifyProgramAssignments(self):
        """Tests the ClassifyProgramAssignments DoFn."""
        fake_person_id = 12345

        fake_person = entities.StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT
        )

        program_assignment = entities.StateProgramAssignment.new_with_defaults(
            state_code='US_XX',
            program_id='PG3',
            program_location_id='XYZ',
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            start_date=date(2009, 10, 19)
        )

        assessment = entities.StateAssessment.new_with_defaults(
            state_code='US_XX',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2009, 7, 10)
        )

        supervision_period = \
            entities.StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2008, 3, 5),
                supervision_type=StateSupervisionType.PAROLE
            )

        person_periods = {'person': [fake_person],
                          'program_assignments': [program_assignment],
                          'assessments': [assessment],
                          'supervision_periods': [supervision_period]
                          }

        program_events = [ProgramReferralEvent(
            state_code=program_assignment.state_code,
            program_id=program_assignment.program_id,
            event_date=program_assignment.referral_date,
            participation_status=program_assignment.participation_status,
            assessment_score=33,
            assessment_type=StateAssessmentType.ORAS,
            supervision_type=supervision_period.supervision_type,
            supervising_officer_external_id='OFFICER0009',
            supervising_district_external_id='10'
        ), ProgramParticipationEvent(
            state_code=program_assignment.state_code,
            program_id=program_assignment.program_id,
            program_location_id=program_assignment.program_location_id,
            event_date=date.today(),
            is_first_day_in_program=True,
            supervision_type=supervision_period.supervision_type
        )]

        correct_output = [(fake_person.person_id, (fake_person, program_events))]

        test_pipeline = TestPipeline()

        supervision_period_to_agent_map = {
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_period_id':
                supervision_period.supervision_period_id
        }

        supervision_period_to_agent_associations = (
            test_pipeline
            | 'Create SupervisionPeriod to Agent table' >>
            beam.Create([supervision_period_to_agent_map])
        )

        supervision_periods_to_agent_associations_as_kv = (
            supervision_period_to_agent_associations |
            'Convert SupervisionPeriod to Agent table to KV tuples' >>
            beam.ParDo(ConvertDictToKVTuple(),
                       'supervision_period_id')
        )

        output = (test_pipeline
                  | beam.Create([(fake_person_id,
                                  person_periods)])
                  | 'Identify Program Events' >>
                  beam.ParDo(
                      pipeline.ClassifyProgramAssignments(),
                      AsDict(supervision_periods_to_agent_associations_as_kv))
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyProgramAssignments_NoReferrals(self):
        """Tests the ClassifyProgramAssignments DoFn."""
        fake_person_id = 12345

        fake_person = entities.StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT
        )

        assessment = entities.StateAssessment.new_with_defaults(
            state_code='US_XX',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2009, 7, 10)
        )

        supervision_period = \
            entities.StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2008, 3, 5),
                termination_date=date(2010, 5, 19),
                termination_reason=
                StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE
            )

        person_periods = {'person': [fake_person],
                          'program_assignments': [],
                          'assessments': [assessment],
                          'supervision_periods': [supervision_period]
                          }

        correct_output = []

        test_pipeline = TestPipeline()

        supervision_period_to_agent_map = {
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_period_id':
                supervision_period.supervision_period_id
        }

        supervision_period_to_agent_associations = (
            test_pipeline
            | 'Create SupervisionPeriod to Agent table' >>
            beam.Create([supervision_period_to_agent_map])
        )

        supervision_periods_to_agent_associations_as_kv = (
            supervision_period_to_agent_associations |
            'Convert SupervisionPeriod to Agent table to KV tuples' >>
            beam.ParDo(ConvertDictToKVTuple(),
                       'supervision_period_id')
        )

        output = (test_pipeline
                  | beam.Create([(fake_person_id,
                                  person_periods)])
                  | 'Identify Program Events' >>
                  beam.ParDo(
                      pipeline.ClassifyProgramAssignments(),
                      AsDict(supervision_periods_to_agent_associations_as_kv))
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyProgramAssignments_NoAssessments(self):
        """Tests the ClassifyProgramAssignments DoFn."""
        fake_person_id = 12345

        fake_person = entities.StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT
        )

        program_assignment = entities.StateProgramAssignment.new_with_defaults(
            state_code='US_XX',
            program_id='PG3',
            referral_date=date(2009, 10, 3)
        )

        supervision_period = \
            entities.StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2008, 3, 5),
                termination_date=date(2010, 5, 19),
                termination_reason=
                StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE
            )

        person_periods = {'person': [fake_person],
                          'program_assignments': [program_assignment],
                          'assessments': [],
                          'supervision_periods': [supervision_period]
                          }

        program_event = ProgramReferralEvent(
            state_code=program_assignment.state_code,
            program_id=program_assignment.program_id,
            event_date=program_assignment.referral_date,
            supervision_type=supervision_period.supervision_type,
            supervising_officer_external_id='OFFICER0009',
            supervising_district_external_id='10'
        )

        correct_output = [(fake_person.person_id, (fake_person, [program_event]))]

        test_pipeline = TestPipeline()

        supervision_period_to_agent_map = {
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_period_id':
                supervision_period.supervision_period_id
        }

        supervision_period_to_agent_associations = (
            test_pipeline
            | 'Create SupervisionPeriod to Agent table' >>
            beam.Create([supervision_period_to_agent_map])
        )

        supervision_periods_to_agent_associations_as_kv = (
            supervision_period_to_agent_associations |
            'Convert SupervisionPeriod to Agent table to KV tuples' >>
            beam.ParDo(ConvertDictToKVTuple(),
                       'supervision_period_id')
        )

        output = (test_pipeline
                  | beam.Create([(fake_person_id,
                                  person_periods)])
                  | 'Identify Program Events' >>
                  beam.ParDo(
                      pipeline.ClassifyProgramAssignments(),
                      AsDict(supervision_periods_to_agent_associations_as_kv))
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyProgramAssignments_NoSupervision(self):
        """Tests the ClassifyProgramAssignments DoFn."""
        fake_person_id = 12345

        fake_person = entities.StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT
        )

        program_assignment = entities.StateProgramAssignment.new_with_defaults(
            state_code='US_XX',
            program_id='PG3',
            referral_date=date(2009, 10, 3)
        )

        assessment = entities.StateAssessment.new_with_defaults(
            state_code='US_XX',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2009, 7, 10)
        )

        person_periods = {'person': [fake_person],
                          'program_assignments': [program_assignment],
                          'assessments': [assessment],
                          'supervision_periods': []
                          }

        program_event = ProgramReferralEvent(
            state_code=program_assignment.state_code,
            program_id=program_assignment.program_id,
            event_date=program_assignment.referral_date,
            assessment_score=33,
            assessment_type=StateAssessmentType.ORAS,
        )

        correct_output = [(fake_person.person_id, (fake_person, [program_event]))]

        test_pipeline = TestPipeline()

        supervision_period_to_agent_map = {
            'supervision_period_id': 'fake_map'
        }

        supervision_period_to_agent_associations = (
            test_pipeline
            | 'Create SupervisionPeriod to Agent table' >>
            beam.Create([supervision_period_to_agent_map])
        )

        supervision_periods_to_agent_associations_as_kv = (
            supervision_period_to_agent_associations |
            'Convert SupervisionPeriod to Agent table to KV tuples' >>
            beam.ParDo(ConvertDictToKVTuple(),
                       'supervision_period_id')
        )

        output = (test_pipeline
                  | beam.Create([(fake_person_id,
                                  person_periods)])
                  | 'Identify Program Events' >>
                  beam.ParDo(
                      pipeline.ClassifyProgramAssignments(),
                      AsDict(supervision_periods_to_agent_associations_as_kv))
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()


class TestCalculateProgramMetricCombinations(unittest.TestCase):
    """Tests the CalculateProgramMetricCombinations DoFn in the pipeline."""
    def setUp(self) -> None:
        self.fake_person_id = 12345

        self.person_metadata = PersonMetadata(prioritized_race_or_ethnicity='BLACK')

    def testCalculateProgramMetricCombinations(self):
        """Tests the CalculateProgramMetricCombinations DoFn."""

        fake_person = StatePerson.new_with_defaults(
            state_code='US_XX',
            person_id=123, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        program_events = [
            ProgramReferralEvent(
                state_code='US_XX',
                event_date=date(2011, 4, 3),
                program_id='program',
                participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS
            ),
            ProgramParticipationEvent(
                state_code='US_XX',
                event_date=date(2011, 6, 3),
                program_id='program'
            )]

        # Each event will be have an output for each methodology type
        expected_metric_count = 2

        expected_combination_counts = \
            {'referrals': expected_metric_count,
             'participation': expected_metric_count}

        test_pipeline = TestPipeline()

        inputs = [(self.fake_person_id, {
            'person_events': [(fake_person, program_events)],
            'person_metadata': [self.person_metadata]
        })]

        output = (test_pipeline
                  | beam.Create(inputs)
                  | beam.ParDo(ExtractPersonEventsMetadata())
                  | 'Calculate Program Metrics' >>
                  beam.ParDo(pipeline.CalculateProgramMetricCombinations(),
                             None, -1, ALL_METRIC_INCLUSIONS_DICT)
                  )

        assert_that(output, AssertMatchers.count_combinations(expected_combination_counts),
                    'Assert number of metrics is expected value')

        test_pipeline.run()

    def testCalculateProgramMetricCombinations_NoReferrals(self):
        """Tests the CalculateProgramMetricCombinations when there are
        no supervision months. This should never happen because any person
        without program events is dropped entirely from the pipeline."""
        fake_person = StatePerson.new_with_defaults(
            person_id=123, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        test_pipeline = TestPipeline()

        inputs = [(self.fake_person_id, {
            'person_events': [(fake_person, [])],
            'person_metadata': [self.person_metadata]
        })]

        output = (test_pipeline
                  | beam.Create(inputs)
                  | beam.ParDo(ExtractPersonEventsMetadata())
                  | 'Calculate Program Metrics' >>
                  beam.ParDo(pipeline.CalculateProgramMetricCombinations(),
                             None, -1, ALL_METRIC_INCLUSIONS_DICT)
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testCalculateProgramMetricCombinations_NoInput(self):
        """Tests the CalculateProgramMetricCombinations when there is
        no input to the function."""

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([])
                  | beam.ParDo(ExtractPersonEventsMetadata())
                  | 'Calculate Program Metrics' >>
                  beam.ParDo(pipeline.CalculateProgramMetricCombinations(),
                             None, -1, ALL_METRIC_INCLUSIONS_DICT)
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class TestProduceProgramMetric(unittest.TestCase):
    """Tests the ProduceProgramMetric DoFn in the pipeline."""

    def testProduceProgramMetric(self):
        metric_key = {'gender': Gender.MALE,
                      'methodology': MetricMethodologyType.PERSON,
                      'year': 1999,
                      'month': 3,
                      'metric_type':
                          ProgramMetricType.PROGRAM_REFERRAL,
                      'state_code': 'US_XX'}

        value = 10

        test_pipeline = TestPipeline()

        all_pipeline_options = PipelineOptions().get_all_options()

        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        output = (test_pipeline
                  | beam.Create([(metric_key, value)])
                  | 'Produce Program Metric' >>
                  beam.ParDo(pipeline.
                             ProduceProgramMetrics(),
                             **all_pipeline_options)
                  )

        assert_that(output, AssertMatchers.
                    validate_program_referral_metric(value))

        test_pipeline.run()

    def testProduceProgramMetric_EmptyMetric(self):
        metric_key = {}

        value = 102

        test_pipeline = TestPipeline()

        all_pipeline_options = PipelineOptions().get_all_options()

        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        # This should never happen, and we want the pipeline to fail loudly if it does.
        with pytest.raises(ValueError):
            _ = (test_pipeline
                 | beam.Create([(metric_key, value)])
                 | 'Produce Program Metric' >>
                 beam.ParDo(pipeline.
                            ProduceProgramMetrics(),
                            **all_pipeline_options)
                 )

            test_pipeline.run()


class AssertMatchers:
    """Functions to be used by Apache Beam testing `assert_that` functions to
    validate pipeline outputs."""

    @staticmethod
    def validate_pipeline_test():

        def _validate_pipeline_test(output, allow_empty=False):
            if not allow_empty and not output:
                raise BeamAssertException('Output metrics unexpectedly empty')

            for metric in output:
                if not isinstance(metric, ProgramMetric):
                    raise BeamAssertException('Failed assert. Output is not of type ProgramMetric.')

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

                metric_type = combination.get('metric_type')

                if metric_type == ProgramMetricType.PROGRAM_REFERRAL:
                    actual_combination_counts['referrals'] = actual_combination_counts['referrals'] + 1
                elif metric_type == ProgramMetricType.PROGRAM_PARTICIPATION:
                    actual_combination_counts['participation'] = actual_combination_counts['participation'] + 1

            for key in expected_combination_counts:
                if expected_combination_counts[key] != actual_combination_counts[key]:
                    raise BeamAssertException('Failed assert. Count does not match expected value.')

        return _count_combinations

    @staticmethod
    def validate_program_referral_metric(expected_referral_count):
        """Asserts that the count on the ProgramReferral produced by the
        pipeline matches the expected referral count."""
        def _validate_program_referral_metric(output):
            if len(output) != 1:
                raise BeamAssertException('Failed assert. Should be only one '
                                          'ProgramReferral returned.')

            program_referral_metric = output[0]

            if program_referral_metric.count != expected_referral_count:
                raise BeamAssertException('Failed assert. Referral count '
                                          'does not match expected value.')

        return _validate_program_referral_metric
