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
from typing import Any, Callable, Dict, Iterable, Optional, Set
from unittest import mock
from unittest.mock import patch

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException, assert_that, equal_to
from freezegun import freeze_time

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
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
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateAssessment,
    NormalizedStatePerson,
    NormalizedStateProgramAssignment,
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.ingest.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.assessment_normalization_manager import (
    DEFAULT_ASSESSMENT_SCORE_BUCKET,
)
from recidiviz.pipelines.metrics.base_metric_pipeline import (
    ClassifyResults,
    ProduceMetrics,
)
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.pipelines.metrics.program import identifier, pipeline
from recidiviz.pipelines.metrics.program.events import ProgramParticipationEvent
from recidiviz.pipelines.metrics.program.metrics import (
    ProgramMetric,
    ProgramMetricType,
    ProgramParticipationMetric,
)
from recidiviz.pipelines.utils.execution_utils import RootEntityId
from recidiviz.tests.persistence.database import database_test_utils
from recidiviz.tests.pipelines.calculator_test_utils import (
    normalized_database_base_dict,
    normalized_database_base_dict_list,
)
from recidiviz.tests.pipelines.fake_bigquery import (
    DataTablesDict,
    FakeReadFromBigQueryFactory,
    FakeWriteMetricsToBigQuery,
    FakeWriteToBigQueryFactory,
)
from recidiviz.tests.pipelines.utils.run_pipeline_test_utils import (
    DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX,
    default_data_dict_for_pipeline_class,
    run_test_pipeline,
)

ALL_METRIC_INCLUSIONS = set(ProgramMetricType)


class TestProgramPipeline(unittest.TestCase):
    """Tests the entire program pipeline."""

    def setUp(self) -> None:
        self.project_id = "test-project"
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id

        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()
        self.fake_bq_sink_factory = FakeWriteToBigQueryFactory(
            FakeWriteMetricsToBigQuery
        )

        self.pipeline_class = pipeline.ProgramMetricsPipeline

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    def build_data_dict(
        self, fake_person_id: int, fake_supervision_period_id: int
    ) -> Dict[str, Iterable]:
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
            external_id="pa1",
            program_assignment_id=123,
            referral_date=date(2015, 5, 10),
            person_id=fake_person_id,
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            start_date=date(2015, 5, 15),
        )

        assessment = schema.StateAssessment(
            assessment_id=298374,
            external_id="a1",
            state_code="US_XX",
            assessment_date=date(2015, 3, 19),
            assessment_type="LSIR",
            person_id=fake_person_id,
        )

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=fake_supervision_period_id,
            external_id="sp1",
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

        assessment_data = [
            normalized_database_base_dict(
                assessment,
                {"assessment_score_bucket": "NOT_ASSESSED", "sequence_num": 0},
            )
        ]

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

        data_dict = default_data_dict_for_pipeline_class(self.pipeline_class)

        data_dict_overrides: Dict[str, Iterable[Any]] = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StatePersonRace.__tablename__: races_data,
            schema.StatePersonEthnicity.__tablename__: ethnicity_data,
            schema.StateSupervisionViolationResponse.__tablename__: supervision_violation_response_data,
            schema.StateSupervisionPeriod.__tablename__: supervision_periods_data,
            schema.StateProgramAssignment.__tablename__: program_assignment_data,
            schema.StateAssessment.__tablename__: assessment_data,
        }
        data_dict.update(data_dict_overrides)

        return data_dict

    def testProgramPipeline(self) -> None:
        """Tests the program pipeline."""
        fake_person_id = 12345
        fake_supervision_period_id = 12345

        data_dict = self.build_data_dict(fake_person_id, fake_supervision_period_id)

        expected_metric_types = {
            ProgramMetricType.PROGRAM_PARTICIPATION,
        }

        self.run_test_pipeline(data_dict, expected_metric_types)

    def testProgramPipelineWithFilterSet(self) -> None:
        """Tests the program pipeline."""
        fake_person_id = 12345
        fake_supervision_period_id = 12345

        data_dict = self.build_data_dict(fake_person_id, fake_supervision_period_id)

        expected_metric_types = {
            ProgramMetricType.PROGRAM_PARTICIPATION,
        }

        self.run_test_pipeline(
            data_dict,
            expected_metric_types,
            root_entity_id_filter_set={fake_person_id},
        )

    def run_test_pipeline(
        self,
        data_dict: DataTablesDict,
        expected_metric_types: Set[ProgramMetricType],
        root_entity_id_filter_set: Optional[Set[RootEntityId]] = None,
        metric_types_filter: Optional[Set[str]] = None,
    ) -> None:
        """Runs a test version of the program pipeline."""
        read_from_bq_constructor = (
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                expected_entities_dataset=normalized_state_dataset_for_state_code(
                    StateCode.US_XX
                ),
                data_dict=data_dict,
            )
        )
        write_to_bq_constructor = (
            self.fake_bq_sink_factory.create_fake_bq_sink_constructor(
                expected_dataset=BigQueryAddressOverrides.format_sandbox_dataset(
                    DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX,
                    DATAFLOW_METRICS_DATASET,
                ),
                expected_output_tags=[
                    metric_type.value for metric_type in expected_metric_types
                ],
            )
        )
        run_test_pipeline(
            pipeline_cls=self.pipeline_class,
            state_code="US_XX",
            project_id=self.project_id,
            read_from_bq_constructor=read_from_bq_constructor,
            write_to_bq_constructor=write_to_bq_constructor,
            root_entity_id_filter_set=root_entity_id_filter_set,
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
            external_id="pa1",
            program_assignment_id=123,
            referral_date=date(2015, 5, 10),
            person_id=fake_person_id_2,
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            start_date=date(2015, 5, 15),
        )

        assessment = schema.StateAssessment(
            assessment_id=298374,
            external_id="a1",
            state_code="US_XX",
            assessment_date=date(2015, 3, 19),
            assessment_type="LSIR",
            person_id=fake_person_id,
        )

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=1111,
            external_id="sp1",
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

        assessment_data = [
            normalized_database_base_dict(
                assessment,
                {"sequence_num": 0, "assessment_score_bucket": "NOT_ASSESSED"},
            )
        ]

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

        data_dict = default_data_dict_for_pipeline_class(self.pipeline_class)

        data_dict_overrides: Dict[str, Iterable[Dict[str, Any]]] = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StatePersonRace.__tablename__: races_data,
            schema.StatePersonEthnicity.__tablename__: ethnicity_data,
            schema.StateSupervisionViolationResponse.__tablename__: supervision_violation_response_data,
            schema.StateSupervisionPeriod.__tablename__: supervision_periods_data,
            schema.StateProgramAssignment.__tablename__: program_assignment_data,
            schema.StateAssessment.__tablename__: assessment_data,
        }

        data_dict.update(data_dict_overrides)

        expected_metric_types = {
            ProgramMetricType.PROGRAM_PARTICIPATION,
        }

        self.run_test_pipeline(data_dict, expected_metric_types)


class TestClassifyProgramAssignments(unittest.TestCase):
    """Tests the ClassifyProgramAssignments DoFn."""

    def setUp(self) -> None:
        self.state_code = "US_XX"
        self.identifier = identifier.ProgramIdentifier()
        self.pipeline_class = pipeline.ProgramMetricsPipeline

    @freeze_time("2009-10-19 00:00:00-05:00")
    def testClassifyProgramAssignments(self) -> None:
        """Tests the ClassifyProgramAssignments DoFn."""
        fake_person_id = 12345

        fake_person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        program_assignment = normalized_entities.NormalizedStateProgramAssignment(
            program_assignment_id=1,
            sequence_num=0,
            external_id="pa1",
            state_code="US_XX",
            program_id="PG3",
            program_location_id="XYZ",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            start_date=date(2009, 10, 19),
        )

        assessment = normalized_entities.NormalizedStateAssessment(
            assessment_id=1,
            state_code="US_XX",
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2009, 7, 10),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        supervision_period = normalized_entities.NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 2, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_site="10",
        )

        person_periods = {
            NormalizedStatePerson.__name__: [fake_person],
            NormalizedStateProgramAssignment.__name__: [program_assignment],
            NormalizedStateAssessment.__name__: [assessment],
            NormalizedStateSupervisionPeriod.__name__: [supervision_period],
        }

        assert program_assignment.program_id is not None
        assert program_assignment.referral_date is not None
        program_events = [
            ProgramParticipationEvent(
                state_code=program_assignment.state_code,
                program_id=program_assignment.program_id,
                program_location_id=program_assignment.program_location_id,
                event_date=date.today(),
                is_first_day_in_program=True,
                supervision_type=supervision_period.supervision_type,
            ),
        ]

        correct_output = [(fake_person, program_events)]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_periods)])
            | "Identify Program Events"
            >> beam.ParDo(
                ClassifyResults(),
                self.identifier,
                included_result_classes={ProgramParticipationEvent},
            )
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    @freeze_time("2009-10-19 00:00:00-05:00")
    def testClassifyProgramAssignments_us_nd(self) -> None:
        """Tests the ClassifyProgramAssignments DoFn."""
        fake_person_id = 12345

        fake_person = NormalizedStatePerson(
            state_code="US_ND",
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        program_assignment = normalized_entities.NormalizedStateProgramAssignment(
            program_assignment_id=1,
            sequence_num=0,
            external_id="pa1",
            state_code="US_ND",
            program_id="PG3",
            program_location_id="XYZ",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            start_date=date(2009, 10, 19),
        )

        assessment = normalized_entities.NormalizedStateAssessment(
            assessment_id=1,
            state_code="US_ND",
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2009, 7, 10),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        supervision_period = normalized_entities.NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 3, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_site="10",
        )

        person_periods = {
            NormalizedStatePerson.__name__: [fake_person],
            NormalizedStateProgramAssignment.__name__: [program_assignment],
            NormalizedStateAssessment.__name__: [assessment],
            NormalizedStateSupervisionPeriod.__name__: [supervision_period],
        }

        assert program_assignment.program_id is not None
        assert program_assignment.referral_date is not None
        program_events = [
            ProgramParticipationEvent(
                state_code=program_assignment.state_code,
                program_id=program_assignment.program_id,
                program_location_id=program_assignment.program_location_id,
                event_date=date.today(),
                is_first_day_in_program=True,
                supervision_type=supervision_period.supervision_type,
            ),
        ]

        correct_output = [(fake_person, program_events)]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_periods)])
            | "Identify Program Events"
            >> beam.ParDo(
                ClassifyResults(),
                self.identifier,
                included_result_classes={ProgramParticipationEvent},
            )
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyProgramAssignments_NoReferrals(self) -> None:
        """Tests the ClassifyProgramAssignments DoFn."""
        fake_person_id = 12345

        fake_person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        assessment = normalized_entities.NormalizedStateAssessment(
            assessment_id=1,
            state_code="US_XX",
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2009, 7, 10),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        supervision_period = normalized_entities.NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        person_periods = {
            NormalizedStatePerson.__name__: [fake_person],
            NormalizedStateProgramAssignment.__name__: [],
            NormalizedStateAssessment.__name__: [assessment],
            NormalizedStateSupervisionPeriod.__name__: [supervision_period],
        }

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_periods)])
            | "Identify Program Events"
            >> beam.ParDo(
                ClassifyResults(),
                self.identifier,
                included_result_classes={ProgramParticipationEvent},
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testClassifyProgramAssignments_NoSupervision(self) -> None:
        """Tests the ClassifyProgramAssignments DoFn."""
        fake_person_id = 12345

        fake_person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        program_assignment = normalized_entities.NormalizedStateProgramAssignment(
            program_assignment_id=1,
            sequence_num=0,
            external_id="pa1",
            state_code="US_XX",
            program_id="PG3",
            referral_date=date(2009, 10, 3),
            participation_status=StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = normalized_entities.NormalizedStateAssessment(
            assessment_id=1,
            state_code="US_XX",
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2009, 7, 10),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        person_periods = {
            NormalizedStatePerson.__name__: [fake_person],
            NormalizedStateProgramAssignment.__name__: [program_assignment],
            NormalizedStateAssessment.__name__: [assessment],
            NormalizedStateSupervisionPeriod.__name__: [],
        }

        assert program_assignment.program_id is not None
        assert program_assignment.referral_date is not None

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_periods)])
            | "Identify Program Events"
            >> beam.ParDo(
                ClassifyResults(),
                self.identifier,
                included_result_classes={ProgramParticipationEvent},
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class TestProduceProgramMetrics(unittest.TestCase):
    """Tests the ProduceProgramMetrics DoFn in the pipeline."""

    def setUp(self) -> None:
        self.fake_person_id = 12345

        self.job_id_patcher = mock.patch(
            "recidiviz.pipelines.metrics.base_metric_pipeline.job_id"
        )
        self.mock_job_id = self.job_id_patcher.start()
        self.mock_job_id.return_value = "job_id"

        self.metric_producer = pipeline.metric_producer.ProgramMetricProducer()

        self.pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-staging",
            state_code="US_XX",
            pipeline="program_metrics",
            metric_types="ALL",
            region="region",
            worker_zone="zone",
            person_filter_ids=None,
            calculation_month_count=-1,
        )

    def tearDown(self) -> None:
        self.job_id_patcher.stop()

    def testProduceProgramMetrics(self) -> None:
        """Tests the ProduceProgramMetrics DoFn."""

        fake_person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=123,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        program_events = [
            ProgramParticipationEvent(
                state_code="US_XX", event_date=date(2011, 6, 3), program_id="program"
            ),
        ]

        expected_metric_count = 1

        expected_combination_counts = {
            "participation": expected_metric_count,
        }

        test_pipeline = TestPipeline()

        inputs = [(fake_person, program_events)]

        output = (
            test_pipeline
            | beam.Create(inputs)
            | "Produce Program Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_parameters.project,
                self.pipeline_parameters.region,
                self.pipeline_parameters.job_name,
                {ProgramMetricType.PROGRAM_PARTICIPATION},
                self.pipeline_parameters.calculation_month_count,
                self.metric_producer,
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
        fake_person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=123,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        test_pipeline = TestPipeline()

        inputs: list[tuple[NormalizedStatePerson, list[ProgramParticipationMetric]]] = [
            (fake_person, [])
        ]

        output = (
            test_pipeline
            | beam.Create(inputs)
            | "Produce Program Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_parameters.project,
                self.pipeline_parameters.region,
                self.pipeline_parameters.job_name,
                {ProgramMetricType.PROGRAM_PARTICIPATION},
                self.pipeline_parameters.calculation_month_count,
                self.metric_producer,
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
            | "Produce Program Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_parameters.project,
                self.pipeline_parameters.region,
                self.pipeline_parameters.job_name,
                {ProgramMetricType.PROGRAM_PARTICIPATION},
                self.pipeline_parameters.calculation_month_count,
                self.metric_producer,
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class AssertMatchers:
    """Functions to be used by Apache Beam testing `assert_that` functions to
    validate pipeline outputs."""

    @staticmethod
    def validate_pipeline_test() -> Callable:
        def _validate_pipeline_test(
            output: Iterable, allow_empty: bool = False
        ) -> None:
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

        def _count_metrics(output: Iterable) -> None:
            actual_metric_counts = {}

            for key in expected_metric_counts.keys():
                actual_metric_counts[key] = 0

            for metric in output:
                if isinstance(metric, ProgramParticipationMetric):
                    actual_metric_counts["participation"] = (
                        actual_metric_counts["participation"] + 1
                    )

            for key in expected_metric_counts:
                if expected_metric_counts[key] != actual_metric_counts[key]:
                    raise BeamAssertException(
                        "Failed assert. Count does not match expected value."
                    )

        return _count_metrics
