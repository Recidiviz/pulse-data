# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
from datetime import date
from typing import Any, Callable, Dict, List, Optional, Set
from unittest import mock

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException, assert_that, equal_to
from freezegun import freeze_time

from recidiviz.calculator.pipeline.metrics.base_metric_pipeline import (
    ClassifyEvents,
    MetricPipelineJobArgs,
    ProduceMetrics,
)
from recidiviz.calculator.pipeline.metrics.program import identifier, pipeline
from recidiviz.calculator.pipeline.metrics.program.events import (
    ProgramParticipationEvent,
    ProgramReferralEvent,
)
from recidiviz.calculator.pipeline.metrics.program.metrics import (
    ProgramMetric,
    ProgramMetricType,
    ProgramParticipationMetric,
    ProgramReferralMetric,
)
from recidiviz.calculator.pipeline.metrics.utils.metric_utils import PersonMetadata
from recidiviz.calculator.pipeline.normalization.utils import normalized_entities
from recidiviz.calculator.pipeline.utils.assessment_utils import (
    DEFAULT_ASSESSMENT_SCORE_BUCKET,
)
from recidiviz.calculator.pipeline.utils.beam_utils.person_utils import (
    PERSON_EVENTS_KEY,
    PERSON_METADATA_KEY,
    ExtractPersonEventsMetadata,
)
from recidiviz.calculator.pipeline.utils.beam_utils.pipeline_args_utils import (
    derive_apache_beam_pipeline_args,
)
from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
    StateResidencyStatus,
)
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import StatePerson
from recidiviz.tests.calculator.calculator_test_utils import (
    normalized_database_base_dict,
    normalized_database_base_dict_list,
)
from recidiviz.tests.calculator.pipeline.fake_bigquery import (
    DataTablesDict,
    FakeReadFromBigQueryFactory,
    FakeWriteMetricsToBigQuery,
    FakeWriteToBigQueryFactory,
)
from recidiviz.tests.calculator.pipeline.utils.run_pipeline_test_utils import (
    default_data_dict_for_run_delegate,
    run_test_pipeline,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.state_calculation_config_manager_test import (
    STATE_DELEGATES_FOR_TESTS,
)
from recidiviz.tests.persistence.database import database_test_utils

ALL_METRIC_INCLUSIONS_DICT = {metric_type: True for metric_type in ProgramMetricType}


class TestProgramPipeline(unittest.TestCase):
    """Tests the entire program pipeline."""

    def setUp(self) -> None:
        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()
        self.fake_bq_sink_factory = FakeWriteToBigQueryFactory(
            FakeWriteMetricsToBigQuery
        )

        self.state_specific_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.metrics.base_metric_pipeline.get_required_state_specific_delegates",
            return_value=STATE_DELEGATES_FOR_TESTS,
        )
        self.mock_get_required_state_delegates = (
            self.state_specific_delegate_patcher.start()
        )
        self.run_delegate_class = pipeline.ProgramMetricsPipelineRunDelegate

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        self.state_specific_delegate_patcher.stop()

    def build_data_dict(
        self, fake_person_id: int, fake_supervision_period_id: int
    ) -> Dict[str, List]:
        """Builds a data_dict for a basic run of the pipeline."""
        fake_person = schema.StatePerson(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        persons_data = [normalized_database_base_dict(fake_person)]

        race_1 = schema.StatePersonRace(
            person_race_id=111,
            state_code="US_XX",
            race=StateRace.BLACK,
            person_id=fake_person_id,
        )

        race_2 = schema.StatePersonRace(
            person_race_id=111,
            state_code="US_XX",
            race=StateRace.WHITE,
            person_id=fake_person_id,
        )

        races_data = normalized_database_base_dict_list([race_1, race_2])

        ethnicity = schema.StatePersonEthnicity(
            person_ethnicity_id=111,
            state_code="US_XX",
            ethnicity=StateEthnicity.HISPANIC,
            person_id=fake_person_id,
        )

        ethnicity_data = normalized_database_base_dict_list([ethnicity])

        program_assignment = schema.StateProgramAssignment(
            state_code="US_XX",
            program_assignment_id=123,
            referral_date=date(2015, 5, 10),
            person_id=fake_person_id,
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
        )

        assessment = schema.StateAssessment(
            assessment_id=298374,
            state_code="US_XX",
            assessment_date=date(2015, 3, 19),
            assessment_type="LSIR",
            person_id=fake_person_id,
        )

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=fake_supervision_period_id,
            state_code="US_XX",
            county_code="124",
            start_date=date(2015, 3, 14),
            termination_date=date(2016, 12, 29),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            person_id=fake_person_id,
        )

        program_assignment_data = [
            normalized_database_base_dict(program_assignment, {"sequence_num": 0})
        ]

        assessment_data = [normalized_database_base_dict(assessment)]

        supervision_periods_data = [
            normalized_database_base_dict(supervision_period, {"sequence_num": 0})
        ]

        supervision_violation_response = (
            database_test_utils.generate_test_supervision_violation_response(
                fake_person_id
            )
        )

        supervision_violation_response_data = [
            normalized_database_base_dict(supervision_violation_response)
        ]

        supervision_period_to_agent_data = [
            {
                "agent_id": 1010,
                "person_id": fake_person_id,
                "state_code": "US_XX",
                "agent_external_id": "OFFICER0009",
                "supervision_period_id": fake_supervision_period_id,
            }
        ]

        state_race_ethnicity_population_count_data = [
            {
                "state_code": "US_XX",
                "race_or_ethnicity": "BLACK",
                "population_count": 1,
                "representation_priority": 1,
            }
        ]

        data_dict = default_data_dict_for_run_delegate(self.run_delegate_class)

        data_dict_overrides: Dict[str, List[Any]] = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StatePersonRace.__tablename__: races_data,
            schema.StatePersonEthnicity.__tablename__: ethnicity_data,
            schema.StateSupervisionViolationResponse.__tablename__: supervision_violation_response_data,
            schema.StateSupervisionPeriod.__tablename__: supervision_periods_data,
            schema.StateProgramAssignment.__tablename__: program_assignment_data,
            schema.StateAssessment.__tablename__: assessment_data,
            "supervision_period_to_agent_association": supervision_period_to_agent_data,
            "state_race_ethnicity_population_counts": state_race_ethnicity_population_count_data,
        }
        data_dict.update(data_dict_overrides)

        return data_dict

    def testProgramPipeline(self) -> None:
        """Tests the program pipeline."""
        fake_person_id = 12345
        fake_supervision_period_id = 12345

        data_dict = self.build_data_dict(fake_person_id, fake_supervision_period_id)

        self.run_test_pipeline(data_dict)

    def testProgramPipelineWithFilterSet(self) -> None:
        """Tests the program pipeline."""
        fake_person_id = 12345
        fake_supervision_period_id = 12345

        data_dict = self.build_data_dict(fake_person_id, fake_supervision_period_id)

        self.run_test_pipeline(data_dict, unifying_id_field_filter_set={fake_person_id})

    def run_test_pipeline(
        self,
        data_dict: DataTablesDict,
        unifying_id_field_filter_set: Optional[Set[int]] = None,
        metric_types_filter: Optional[Set[str]] = None,
    ) -> None:
        """Runs a test version of the program pipeline."""
        project = "project"
        dataset = "dataset"
        normalized_dataset = "us_xx_normalized_state"

        expected_metric_types = {
            ProgramMetricType.PROGRAM_REFERRAL,
        }

        read_from_bq_constructor = (
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict, expected_normalized_dataset=normalized_dataset
            )
        )
        write_to_bq_constructor = (
            self.fake_bq_sink_factory.create_fake_bq_sink_constructor(
                dataset,
                expected_output_tags=[
                    metric_type.value for metric_type in expected_metric_types
                ],
            )
        )
        run_test_pipeline(
            run_delegate=self.run_delegate_class,
            state_code="US_XX",
            project_id=project,
            dataset_id=dataset,
            read_from_bq_constructor=read_from_bq_constructor,
            write_to_bq_constructor=write_to_bq_constructor,
            unifying_id_field_filter_set=unifying_id_field_filter_set,
            metric_types_filter=metric_types_filter,
        )

    def testProgramPipelineNoReferrals(self) -> None:
        """Tests the program pipeline where one person does not have any
        program assignment entities."""
        fake_person_id = 12345
        fake_person_id_2 = 9876

        fake_person = schema.StatePerson(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        fake_person_2 = schema.StatePerson(
            state_code="US_XX",
            person_id=fake_person_id_2,
            gender=StateGender.MALE,
            birthdate=date(1974, 3, 12),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        persons_data = normalized_database_base_dict_list([fake_person, fake_person_2])

        race_1 = schema.StatePersonRace(
            person_race_id=111,
            state_code="US_XX",
            race=StateRace.BLACK,
            person_id=fake_person_id,
        )

        race_2 = schema.StatePersonRace(
            person_race_id=111,
            state_code="US_XX",
            race=StateRace.WHITE,
            person_id=fake_person_id,
        )

        races_data = normalized_database_base_dict_list([race_1, race_2])

        ethnicity = schema.StatePersonEthnicity(
            person_ethnicity_id=111,
            state_code="US_XX",
            ethnicity=StateEthnicity.HISPANIC,
            person_id=fake_person_id,
        )

        ethnicity_data = normalized_database_base_dict_list([ethnicity])

        # Program assignment for a different person
        program_assignment = schema.StateProgramAssignment(
            state_code="US_XX",
            program_assignment_id=123,
            referral_date=date(2015, 5, 10),
            person_id=fake_person_id_2,
            participation_status=StateProgramAssignmentParticipationStatus.DENIED,
        )

        assessment = schema.StateAssessment(
            assessment_id=298374,
            state_code="US_XX",
            assessment_date=date(2015, 3, 19),
            assessment_type="LSIR",
            person_id=fake_person_id,
        )

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=1111,
            state_code="US_XX",
            county_code="124",
            start_date=date(2015, 3, 14),
            termination_date=date(2016, 12, 29),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            person_id=fake_person_id,
        )

        program_assignment_data = [
            normalized_database_base_dict(program_assignment, {"sequence_num": 0})
        ]

        assessment_data = [normalized_database_base_dict(assessment)]

        supervision_periods_data = [
            normalized_database_base_dict(supervision_period, {"sequence_num": 0})
        ]

        supervision_violation_response = (
            database_test_utils.generate_test_supervision_violation_response(
                fake_person_id
            )
        )

        supervision_violation_response_data = [
            normalized_database_base_dict(supervision_violation_response)
        ]

        supervision_period_to_agent_data = [
            {
                "agent_id": 1010,
                "person_id": fake_person_id,
                "state_code": "US_XX",
                "agent_external_id": "OFFICER0009",
                "supervision_period_id": supervision_period.supervision_period_id,
            }
        ]

        state_race_ethnicity_population_count_data = [
            {
                "state_code": "US_XX",
                "race_or_ethnicity": "BLACK",
                "population_count": 1,
                "representation_priority": 1,
            }
        ]

        data_dict = default_data_dict_for_run_delegate(self.run_delegate_class)

        data_dict_overrides: Dict[str, List[Dict[str, Any]]] = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StatePersonRace.__tablename__: races_data,
            schema.StatePersonEthnicity.__tablename__: ethnicity_data,
            schema.StateSupervisionViolationResponse.__tablename__: supervision_violation_response_data,
            schema.StateSupervisionPeriod.__tablename__: supervision_periods_data,
            schema.StateProgramAssignment.__tablename__: program_assignment_data,
            schema.StateAssessment.__tablename__: assessment_data,
            "supervision_period_to_agent_association": supervision_period_to_agent_data,
            "state_race_ethnicity_population_counts": state_race_ethnicity_population_count_data,
        }

        data_dict.update(data_dict_overrides)

        self.run_test_pipeline(data_dict)


class TestClassifyProgramAssignments(unittest.TestCase):
    """Tests the ClassifyProgramAssignments DoFn."""

    def setUp(self) -> None:
        self.state_code = "US_XX"
        self.identifier = identifier.ProgramIdentifier()
        self.run_delegate_class = pipeline.ProgramMetricsPipelineRunDelegate
        self.state_specific_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.metrics.base_metric_pipeline.get_required_state_specific_delegates",
            return_value=STATE_DELEGATES_FOR_TESTS,
        )
        self.mock_get_required_state_delegates = (
            self.state_specific_delegate_patcher.start()
        )

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        self.state_specific_delegate_patcher.stop()

    @freeze_time("2009-10-19")
    def testClassifyProgramAssignments(self) -> None:
        """Tests the ClassifyProgramAssignments DoFn."""
        fake_person_id = 12345

        fake_person = entities.StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        program_assignment = normalized_entities.NormalizedStateProgramAssignment.new_with_defaults(
            sequence_num=0,
            state_code="US_XX",
            program_id="PG3",
            program_location_id="XYZ",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            start_date=date(2009, 10, 19),
        )

        assessment = entities.StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2009, 7, 10),
        )

        supervision_period = (
            normalized_entities.NormalizedStateSupervisionPeriod.new_with_defaults(
                sequence_num=0,
                supervision_period_id=111,
                state_code="US_XX",
                start_date=date(2008, 3, 5),
                termination_date=date(2010, 2, 19),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                supervision_site="10",
            )
        )

        supervision_period_to_agent_map = {
            "agent_id": 1010,
            "person_id": fake_person_id,
            "agent_external_id": "OFFICER0009",
            "supervision_period_id": supervision_period.supervision_period_id,
        }

        person_periods = {
            entities.StatePerson.__name__: [fake_person],
            entities.StateProgramAssignment.__name__: [program_assignment],
            entities.StateAssessment.__name__: [assessment],
            entities.StateSupervisionPeriod.__name__: [supervision_period],
            "supervision_period_to_agent_association": [
                supervision_period_to_agent_map
            ],
        }

        assert program_assignment.program_id is not None
        assert program_assignment.referral_date is not None
        program_events = [
            ProgramReferralEvent(
                state_code=program_assignment.state_code,
                program_id=program_assignment.program_id,
                event_date=program_assignment.referral_date,
                participation_status=program_assignment.participation_status,
                assessment_score=33,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
                supervision_type=supervision_period.supervision_type,
                supervising_officer_external_id="OFFICER0009",
                supervising_district_external_id="10",
                level_1_supervision_location_external_id="10",
            ),
            ProgramParticipationEvent(
                state_code=program_assignment.state_code,
                program_id=program_assignment.program_id,
                program_location_id=program_assignment.program_location_id,
                event_date=date.today(),
                is_first_day_in_program=True,
                supervision_type=supervision_period.supervision_type,
            ),
        ]

        correct_output = [(fake_person.person_id, (fake_person, program_events))]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_periods)])
            | "Identify Program Events"
            >> beam.ParDo(
                ClassifyEvents(),
                self.state_code,
                self.identifier,
                pipeline_config=self.run_delegate_class.pipeline_config(),
            )
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    @freeze_time("2009-10-19")
    def testClassifyProgramAssignments_us_nd(self) -> None:
        """Tests the ClassifyProgramAssignments DoFn."""
        fake_person_id = 12345
        state_code = "US_ND"

        fake_person = entities.StatePerson.new_with_defaults(
            state_code="US_ND",
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        program_assignment = normalized_entities.NormalizedStateProgramAssignment.new_with_defaults(
            sequence_num=0,
            state_code="US_ND",
            program_id="PG3",
            program_location_id="XYZ",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            start_date=date(2009, 10, 19),
        )

        assessment = entities.StateAssessment.new_with_defaults(
            state_code="US_ND",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2009, 7, 10),
        )

        supervision_period = (
            normalized_entities.NormalizedStateSupervisionPeriod.new_with_defaults(
                sequence_num=0,
                supervision_period_id=111,
                state_code="US_ND",
                start_date=date(2008, 3, 5),
                termination_date=date(2010, 3, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                supervision_site="10",
            )
        )

        supervision_period_to_agent_map = {
            "agent_id": 1010,
            "person_id": fake_person_id,
            "agent_external_id": "OFFICER0009",
            "supervision_period_id": supervision_period.supervision_period_id,
        }

        person_periods = {
            entities.StatePerson.__name__: [fake_person],
            entities.StateProgramAssignment.__name__: [program_assignment],
            entities.StateAssessment.__name__: [assessment],
            entities.StateSupervisionPeriod.__name__: [supervision_period],
            "supervision_period_to_agent_association": [
                supervision_period_to_agent_map
            ],
        }

        assert program_assignment.program_id is not None
        assert program_assignment.referral_date is not None
        program_events = [
            ProgramReferralEvent(
                state_code=program_assignment.state_code,
                program_id=program_assignment.program_id,
                event_date=program_assignment.referral_date,
                participation_status=program_assignment.participation_status,
                assessment_score=33,
                assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
                supervision_type=supervision_period.supervision_type,
                supervising_officer_external_id="OFFICER0009",
                supervising_district_external_id="10",
                level_1_supervision_location_external_id="10",
            ),
            ProgramParticipationEvent(
                state_code=program_assignment.state_code,
                program_id=program_assignment.program_id,
                program_location_id=program_assignment.program_location_id,
                event_date=date.today(),
                is_first_day_in_program=True,
                supervision_type=supervision_period.supervision_type,
            ),
        ]

        correct_output = [(fake_person.person_id, (fake_person, program_events))]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_periods)])
            | "Identify Program Events"
            >> beam.ParDo(
                ClassifyEvents(),
                state_code,
                self.identifier,
                pipeline_config=self.run_delegate_class.pipeline_config(),
            )
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyProgramAssignments_NoReferrals(self) -> None:
        """Tests the ClassifyProgramAssignments DoFn."""
        fake_person_id = 12345

        fake_person = entities.StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        assessment = entities.StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2009, 7, 10),
        )

        supervision_period = (
            normalized_entities.NormalizedStateSupervisionPeriod.new_with_defaults(
                sequence_num=0,
                supervision_period_id=111,
                state_code="US_XX",
                start_date=date(2008, 3, 5),
                termination_date=date(2010, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            )
        )

        supervision_period_to_agent_map = {
            "agent_id": 1010,
            "person_id": fake_person_id,
            "agent_external_id": "OFFICER0009",
            "supervision_period_id": supervision_period.supervision_period_id,
        }

        person_periods = {
            entities.StatePerson.__name__: [fake_person],
            entities.StateProgramAssignment.__name__: [],
            entities.StateAssessment.__name__: [assessment],
            entities.StateSupervisionPeriod.__name__: [supervision_period],
            "supervision_period_to_agent_association": [
                supervision_period_to_agent_map
            ],
        }

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_periods)])
            | "Identify Program Events"
            >> beam.ParDo(
                ClassifyEvents(),
                self.state_code,
                self.identifier,
                pipeline_config=self.run_delegate_class.pipeline_config(),
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testClassifyProgramAssignments_NoAssessments(self) -> None:
        """Tests the ClassifyProgramAssignments DoFn."""
        fake_person_id = 12345

        fake_person = entities.StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        program_assignment = normalized_entities.NormalizedStateProgramAssignment.new_with_defaults(
            sequence_num=0,
            state_code="US_XX",
            program_id="PG3",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period = (
            normalized_entities.NormalizedStateSupervisionPeriod.new_with_defaults(
                sequence_num=0,
                supervision_period_id=111,
                state_code="US_XX",
                start_date=date(2008, 3, 5),
                termination_date=date(2010, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                supervision_site="10",
            )
        )

        supervision_period_to_agent_map = {
            "agent_id": 1010,
            "person_id": fake_person_id,
            "agent_external_id": "OFFICER0009",
            "supervision_period_id": supervision_period.supervision_period_id,
        }

        person_periods = {
            entities.StatePerson.__name__: [fake_person],
            entities.StateProgramAssignment.__name__: [program_assignment],
            entities.StateAssessment.__name__: [],
            entities.StateSupervisionPeriod.__name__: [supervision_period],
            "supervision_period_to_agent_association": [
                supervision_period_to_agent_map
            ],
        }

        assert program_assignment.program_id is not None
        assert program_assignment.referral_date is not None
        program_event = ProgramReferralEvent(
            state_code=program_assignment.state_code,
            program_id=program_assignment.program_id,
            event_date=program_assignment.referral_date,
            participation_status=program_assignment.participation_status,
            supervision_type=supervision_period.supervision_type,
            supervising_officer_external_id="OFFICER0009",
            supervising_district_external_id="10",
            level_1_supervision_location_external_id="10",
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
        )

        correct_output = [(fake_person.person_id, (fake_person, [program_event]))]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_periods)])
            | "Identify Program Events"
            >> beam.ParDo(
                ClassifyEvents(),
                self.state_code,
                self.identifier,
                pipeline_config=self.run_delegate_class.pipeline_config(),
            )
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyProgramAssignments_NoSupervision(self) -> None:
        """Tests the ClassifyProgramAssignments DoFn."""
        fake_person_id = 12345

        fake_person = entities.StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        program_assignment = normalized_entities.NormalizedStateProgramAssignment.new_with_defaults(
            sequence_num=0,
            state_code="US_XX",
            program_id="PG3",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = entities.StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2009, 7, 10),
        )

        supervision_period_to_agent_map = {"supervision_period_id": "fake_map"}

        person_periods = {
            entities.StatePerson.__name__: [fake_person],
            entities.StateProgramAssignment.__name__: [program_assignment],
            entities.StateAssessment.__name__: [assessment],
            entities.StateSupervisionPeriod.__name__: [],
            "supervision_period_to_agent_association": [
                supervision_period_to_agent_map
            ],
        }

        assert program_assignment.program_id is not None
        assert program_assignment.referral_date is not None
        program_event = ProgramReferralEvent(
            state_code=program_assignment.state_code,
            program_id=program_assignment.program_id,
            event_date=program_assignment.referral_date,
            assessment_score=33,
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            participation_status=program_assignment.participation_status,
        )

        correct_output = [(fake_person.person_id, (fake_person, [program_event]))]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_periods)])
            | "Identify Program Events"
            >> beam.ParDo(
                ClassifyEvents(),
                self.state_code,
                self.identifier,
                pipeline_config=self.run_delegate_class.pipeline_config(),
            )
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()


class TestProduceProgramMetrics(unittest.TestCase):
    """Tests the ProduceProgramMetrics DoFn in the pipeline."""

    def setUp(self) -> None:
        self.fake_person_id = 12345

        self.person_metadata = PersonMetadata(prioritized_race_or_ethnicity="BLACK")
        self.job_id_patcher = mock.patch(
            "recidiviz.calculator.pipeline.metrics.base_metric_pipeline.job_id"
        )
        self.mock_job_id = self.job_id_patcher.start()
        self.mock_job_id.return_value = "job_id"

        self.metric_producer = pipeline.metric_producer.ProgramMetricProducer()
        self.pipeline_name = (
            pipeline.ProgramMetricsPipelineRunDelegate.pipeline_config().pipeline_name
        )

        default_beam_args: List[str] = [
            "--project",
            "project",
            "--job_name",
            "test",
        ]

        beam_pipeline_options = PipelineOptions(
            derive_apache_beam_pipeline_args(default_beam_args)
        )

        self.pipeline_job_args = MetricPipelineJobArgs(
            state_code="US_XX",
            project_id="project",
            input_dataset="dataset_id",
            normalized_input_dataset="dataset_id",
            reference_dataset="dataset_id",
            static_reference_dataset="dataset_id",
            output_dataset="dataset_id",
            metric_inclusions=ALL_METRIC_INCLUSIONS_DICT,
            region="region",
            job_name="job",
            person_id_filter_set=None,
            calculation_end_month=None,
            calculation_month_count=-1,
            apache_beam_pipeline_options=beam_pipeline_options,
        )

    def tearDown(self) -> None:
        self.job_id_patcher.stop()

    def testProduceProgramMetrics(self) -> None:
        """Tests the ProduceProgramMetrics DoFn."""

        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=123,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        program_events = [
            ProgramReferralEvent(
                state_code="US_XX",
                event_date=date(2011, 4, 3),
                program_id="program",
                participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            ProgramParticipationEvent(
                state_code="US_XX", event_date=date(2011, 6, 3), program_id="program"
            ),
        ]

        expected_metric_count = 1

        expected_combination_counts = {
            "referrals": expected_metric_count,
            "participation": expected_metric_count,
        }

        test_pipeline = TestPipeline()

        inputs = [
            (
                self.fake_person_id,
                {
                    PERSON_EVENTS_KEY: [(fake_person, program_events)],
                    PERSON_METADATA_KEY: [self.person_metadata],
                },
            )
        ]

        output = (
            test_pipeline
            | beam.Create(inputs)
            | beam.ParDo(ExtractPersonEventsMetadata())
            | "Produce Program Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_job_args,
                self.metric_producer,
                self.pipeline_name,
            )
        )

        assert_that(
            output,
            AssertMatchers.count_metrics(expected_combination_counts),
            "Assert number of metrics is expected value",
        )

        test_pipeline.run()

    def testProduceProgramMetrics_NoReferrals(self) -> None:
        """Tests the ProduceProgramMetrics when there are
        no supervision months. This should never happen because any person
        without program events is dropped entirely from the pipeline."""
        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=123,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        test_pipeline = TestPipeline()

        inputs = [
            (
                self.fake_person_id,
                {
                    PERSON_EVENTS_KEY: [(fake_person, [])],
                    PERSON_METADATA_KEY: [self.person_metadata],
                },
            )
        ]

        output = (
            test_pipeline
            | beam.Create(inputs)
            | beam.ParDo(ExtractPersonEventsMetadata())
            | "Produce Program Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_job_args,
                self.metric_producer,
                self.pipeline_name,
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testProduceProgramMetrics_NoInput(self) -> None:
        """Tests the ProduceProgramMetrics when there is
        no input to the function."""

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([])
            | beam.ParDo(ExtractPersonEventsMetadata())
            | "Produce Program Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_job_args,
                self.metric_producer,
                self.pipeline_name,
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class AssertMatchers:
    """Functions to be used by Apache Beam testing `assert_that` functions to
    validate pipeline outputs."""

    @staticmethod
    def validate_pipeline_test() -> Callable:
        def _validate_pipeline_test(output: List, allow_empty: bool = False) -> None:
            if not allow_empty and not output:
                raise BeamAssertException("Output metrics unexpectedly empty")

            for metric in output:
                if not isinstance(metric, ProgramMetric):
                    raise BeamAssertException(
                        "Failed assert. Output is not of type ProgramMetric."
                    )

        return _validate_pipeline_test

    @staticmethod
    def count_metrics(expected_metric_counts: Dict[Any, Any]) -> Callable:
        """Asserts that the number of ProgramMetrics matches the expected counts."""

        def _count_metrics(output: List) -> None:
            actual_metric_counts = {}

            for key in expected_metric_counts.keys():
                actual_metric_counts[key] = 0

            for metric in output:
                if isinstance(metric, ProgramReferralMetric):
                    actual_metric_counts["referrals"] = (
                        actual_metric_counts["referrals"] + 1
                    )
                elif isinstance(metric, ProgramParticipationMetric):
                    actual_metric_counts["participation"] = (
                        actual_metric_counts["participation"] + 1
                    )

            for key in expected_metric_counts:
                if expected_metric_counts[key] != actual_metric_counts[key]:
                    raise BeamAssertException(
                        "Failed assert. Count does not match expected value."
                    )

        return _count_metrics
