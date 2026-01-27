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
"""Tests for violation/pipeline.py"""
import unittest
from datetime import date
from typing import Any, Callable, Dict, Iterable, Optional, Set, Tuple
from unittest import mock
from unittest.mock import patch

import apache_beam as beam
from apache_beam.testing.util import BeamAssertException, assert_that, equal_to

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePerson,
    NormalizedStateSupervisionViolation,
)
from recidiviz.pipelines.ingest.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.pipelines.metrics.base_metric_pipeline import (
    ClassifyResults,
    ProduceMetrics,
)
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.pipelines.metrics.utils.metric_utils import RecidivizMetric
from recidiviz.pipelines.metrics.violation import identifier as violation_identifier
from recidiviz.pipelines.metrics.violation import pipeline
from recidiviz.pipelines.metrics.violation.events import ViolationWithResponseEvent
from recidiviz.pipelines.metrics.violation.identifier import ViolationIdentifier
from recidiviz.pipelines.metrics.violation.metric_producer import (
    ViolationMetricProducer,
)
from recidiviz.pipelines.metrics.violation.metrics import (
    ViolationMetric,
    ViolationMetricType,
)
from recidiviz.pipelines.utils.execution_utils import RootEntityId
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline
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
from recidiviz.tests.pipelines.fake_state_calculation_config_manager import (
    start_pipeline_delegate_getter_patchers,
)
from recidiviz.tests.pipelines.utils.run_pipeline_test_utils import (
    DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX,
    default_data_dict_for_pipeline_class,
    run_test_pipeline,
)

ALL_METRIC_INCLUSIONS_DICT = {metric_type: True for metric_type in ViolationMetricType}


class TestViolationPipeline(unittest.TestCase):
    """Tests the entire violation pipeline."""

    def setUp(self) -> None:
        self.project_id = "test-project"
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id

        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            violation_identifier
        )

        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()
        self.fake_bq_sink_factory = FakeWriteToBigQueryFactory(
            FakeWriteMetricsToBigQuery
        )
        self.pipeline_class = pipeline.ViolationMetricsPipeline

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()
        self.project_id_patcher.stop()

    def build_data_dict(
        self, fake_person_id: int, fake_supervision_violation_id: int
    ) -> Dict[str, Iterable[Any]]:
        """Builds a data_dict for a basic run of the pipeline."""
        fake_person = schema.StatePerson(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=StateGender.FEMALE,
            birthdate=date(1985, 2, 1),
        )
        persons_data = [
            normalized_database_base_dict(
                fake_person, {"ethnicity": "PRESENT_WITHOUT_INFO"}
            )
        ]

        race_1 = schema.StatePersonRace(
            person_race_id=111,
            state_code="US_XX",
            race=StateRace.ASIAN,
            person_id=fake_person_id,
        )
        race_2 = schema.StatePersonRace(
            person_race_id=111,
            state_code="US_XX",
            race=StateRace.AMERICAN_INDIAN_ALASKAN_NATIVE,
            person_id=fake_person_id,
        )

        races_data = normalized_database_base_dict_list([race_1, race_2])

        violation_decision = schema.StateSupervisionViolationResponseDecisionEntry(
            state_code="US_XX",
            decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            person_id=fake_person_id,
            supervision_violation_response_decision_entry_id=234,
            supervision_violation_response_id=1234,
        )
        violation_response = schema.StateSupervisionViolationResponse(
            state_code="US_XX",
            external_id="svr1",
            supervision_violation_response_id=1234,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2021, 1, 4),
            is_draft=False,
            supervision_violation_response_decisions=[violation_decision],
            person_id=fake_person_id,
        )

        violation_type = schema.StateSupervisionViolationTypeEntry(
            supervision_violation_type_entry_id=123,
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.FELONY,
            person_id=fake_person_id,
        )
        violation = schema.StateSupervisionViolation(
            state_code="US_XX",
            external_id="sv1",
            supervision_violation_id=fake_supervision_violation_id,
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[violation_type],
            supervision_violation_responses=[violation_response],
            person_id=fake_person_id,
        )
        violation_type.supervision_violation_id = fake_supervision_violation_id
        violation_response.supervision_violation_id = fake_supervision_violation_id

        violations_data = [normalized_database_base_dict(violation)]
        violation_responses_data = [
            normalized_database_base_dict(violation_response, {"sequence_num": 0})
        ]
        violation_types_data = [normalized_database_base_dict(violation_type)]
        violation_decisions_data = [normalized_database_base_dict(violation_decision)]
        data_dict = default_data_dict_for_pipeline_class(self.pipeline_class)

        data_dict_overrides: Dict[str, Iterable[Any]] = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StatePersonRace.__tablename__: races_data,
            schema.StateSupervisionViolation.__tablename__: violations_data,
            schema.StateSupervisionViolationResponse.__tablename__: violation_responses_data,
            schema.StateSupervisionViolationTypeEntry.__tablename__: violation_types_data,
            schema.StateSupervisionViolationResponseDecisionEntry.__tablename__: violation_decisions_data,
        }
        data_dict.update(data_dict_overrides)

        return data_dict

    def run_test_pipeline(
        self,
        data_dict: DataTablesDict,
        expected_metric_types: Set[ViolationMetricType],
        root_entity_id_filter_set: Optional[Set[RootEntityId]] = None,
        metric_types_filter: Optional[Set[str]] = None,
    ) -> None:
        """Runs a test version of the violation pipeline."""
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

    def testViolationPipeline(self) -> None:
        """Tests the violation pipeline."""
        data_dict = self.build_data_dict(
            fake_person_id=12345, fake_supervision_violation_id=23456
        )

        self.run_test_pipeline(
            data_dict, expected_metric_types={ViolationMetricType.VIOLATION}
        )

    def testViolationPipelineWithFilterSet(self) -> None:
        """Tests the violation pipeline with a proper filter set."""
        data_dict = self.build_data_dict(
            fake_person_id=12345, fake_supervision_violation_id=23456
        )

        self.run_test_pipeline(
            data_dict,
            expected_metric_types={ViolationMetricType.VIOLATION},
            root_entity_id_filter_set={12345},
        )

    def testViolationPipelineWithNoViolations(self) -> None:
        """Tests the violation pipeline when a person does not have any violations."""
        data_dict = self.build_data_dict(
            fake_person_id=12345, fake_supervision_violation_id=23456
        )
        data_dict[schema.StateSupervisionViolation.__tablename__] = []
        data_dict[schema.StateSupervisionViolationResponse.__tablename__] = []
        data_dict[schema.StateSupervisionViolationTypeEntry.__tablename__] = []
        data_dict[
            schema.StateSupervisionViolationResponseDecisionEntry.__tablename__
        ] = []

        self.run_test_pipeline(data_dict, expected_metric_types=set())


class TestClassifyViolationEvents(unittest.TestCase):
    """Tests the ClassifyViolationEvents DoFn."""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            violation_identifier
        )
        self.state_code = StateCode.US_XX
        self.fake_person_id = 12345
        self.fake_supervision_violation_id = 23456

        self.fake_person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=self.fake_person_id,
            gender=StateGender.FEMALE,
            birthdate=date(1985, 2, 1),
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )
        self.identifier = ViolationIdentifier(self.state_code)
        self.pipeline_class = pipeline.ViolationMetricsPipeline

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def testClassifyViolationEvents(self) -> None:
        """Tests the ClassifyViolationEvents DoFn."""
        violation_type = (
            normalized_entities.NormalizedStateSupervisionViolationTypeEntry(
                supervision_violation_type_entry_id=1,
                state_code="US_XX",
                violation_type=StateSupervisionViolationType.FELONY,
            )
        )
        violation_decision = normalized_entities.NormalizedStateSupervisionViolationResponseDecisionEntry(
            supervision_violation_response_decision_entry_id=1,
            state_code="US_XX",
            decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
        )
        violation_response = (
            normalized_entities.NormalizedStateSupervisionViolationResponse(
                supervision_violation_response_id=1,
                state_code="US_XX",
                external_id="svr1",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2021, 1, 4),
                is_draft=False,
                supervision_violation_response_decisions=[violation_decision],
                sequence_num=0,
            )
        )
        violation = normalized_entities.NormalizedStateSupervisionViolation(
            state_code="US_XX",
            external_id="sv1",
            supervision_violation_id=self.fake_supervision_violation_id,
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[violation_type],
            supervision_violation_responses=[violation_response],
        )
        violation_decision.supervision_violation_response = violation_response
        violation_response.supervision_violation = violation
        violation_type.supervision_violation = violation

        violation_events = [
            ViolationWithResponseEvent(
                state_code="US_XX",
                supervision_violation_id=self.fake_supervision_violation_id,
                event_date=date(2021, 1, 4),
                violation_date=date(2021, 1, 1),
                violation_type=StateSupervisionViolationType.FELONY,
                violation_type_subtype="FELONY",
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
                is_most_severe_response_decision_of_all_violations=True,
                is_most_severe_violation_type_of_all_violations=True,
            )
        ]

        correct_output: Iterable[
            Tuple[NormalizedStatePerson, Iterable[ViolationWithResponseEvent]]
        ] = [(self.fake_person, violation_events)]

        person_violations = {
            NormalizedStatePerson.__name__: [self.fake_person],
            NormalizedStateSupervisionViolation.__name__: [violation],
        }
        test_pipeline = create_test_pipeline()
        output = (
            test_pipeline
            | beam.Create([(self.fake_person_id, person_violations)])
            | "Identify Violation Events"
            >> beam.ParDo(
                ClassifyResults(),
                identifier=self.identifier,
                included_result_classes={ViolationWithResponseEvent},
            )
        )

        assert_that(output, equal_to(correct_output))
        test_pipeline.run()

    def testClassifyViolationEventsNoViolations(self) -> None:
        """Tests the ClassifyViolationEvents DoFn with no violations for a person."""

        correct_output: Iterable[
            Tuple[NormalizedStatePerson, Iterable[ViolationWithResponseEvent]]
        ] = []

        person_violations = {
            NormalizedStatePerson.__name__: [self.fake_person],
            NormalizedStateSupervisionViolation.__name__: [],
        }
        test_pipeline = create_test_pipeline()
        output = (
            test_pipeline
            | beam.Create([(self.fake_person_id, person_violations)])
            | "Identify Violation Events"
            >> beam.ParDo(
                ClassifyResults(),
                identifier=self.identifier,
                included_result_classes={ViolationWithResponseEvent},
            )
        )
        assert_that(output, equal_to(correct_output))
        test_pipeline.run()

    def testClassifyViolationEventsNoResponses(self) -> None:
        """Tests the ClassifyViolationEvents DoFn with a violation that has no responses for a person."""
        violation_type = (
            normalized_entities.NormalizedStateSupervisionViolationTypeEntry(
                supervision_violation_type_entry_id=1,
                state_code="US_XX",
                violation_type=StateSupervisionViolationType.FELONY,
            )
        )
        violation = normalized_entities.NormalizedStateSupervisionViolation(
            state_code="US_XX",
            external_id="sv1",
            supervision_violation_id=self.fake_supervision_violation_id,
            violation_date=date(2021, 1, 1),
            is_violent=False,
            is_sex_offense=False,
            supervision_violation_types=[violation_type],
        )
        violation_type.supervision_violation = violation

        correct_output: Iterable[
            Tuple[NormalizedStatePerson, Iterable[ViolationWithResponseEvent]]
        ] = []

        person_violations = {
            NormalizedStatePerson.__name__: [self.fake_person],
            NormalizedStateSupervisionViolation.__name__: [violation],
        }
        test_pipeline = create_test_pipeline()
        output = (
            test_pipeline
            | beam.Create([(self.fake_person_id, person_violations)])
            | "Identify Violation Events"
            >> beam.ParDo(
                ClassifyResults(),
                identifier=self.identifier,
                included_result_classes={ViolationWithResponseEvent},
            )
        )
        assert_that(output, equal_to(correct_output))
        test_pipeline.run()


class TestProduceViolationMetrics(unittest.TestCase):
    """Tests the Produce ViolationMetrics DoFn in the pipeline."""

    def setUp(self) -> None:
        self.fake_person_id = 12345
        self.fake_supervision_violation_id = 23456

        self.job_id_patcher = mock.patch(
            "recidiviz.pipelines.metrics.base_metric_pipeline.job_id"
        )
        self.mock_job_id = self.job_id_patcher.start()
        self.mock_job_id.return_value = "job_id"

        self.metric_producer = ViolationMetricProducer()

        self.pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-456",
            state_code="US_XX",
            pipeline="violation_metrics",
            metric_types="ALL",
            region="region",
            worker_zone="zone",
            person_filter_ids=None,
            calculation_month_count=-1,
        )

    def testProduceViolationMetrics(self) -> None:
        """Tests the ProduceViolationMetrics DoFn."""

        fake_person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=self.fake_person_id,
            gender=StateGender.FEMALE,
            birthdate=date(1985, 2, 1),
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )

        violation_events = [
            ViolationWithResponseEvent(
                state_code="US_XX",
                event_date=date(2011, 4, 3),
                supervision_violation_id=self.fake_supervision_violation_id,
                violation_type=StateSupervisionViolationType.ABSCONDED,
                is_most_severe_violation_type=True,
                is_violent=False,
                is_sex_offense=False,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            )
        ]

        expected_metric_counts = {ViolationMetricType.VIOLATION.value: 1}

        test_pipeline = create_test_pipeline()

        inputs = [(fake_person, violation_events)]

        output = (
            test_pipeline
            | beam.Create(inputs)
            | "Produce Violation Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_parameters.project,
                self.pipeline_parameters.region,
                self.pipeline_parameters.job_name,
                {ViolationMetricType.VIOLATION},
                self.pipeline_parameters.calculation_month_count,
                self.metric_producer,
            )
        )

        assert_that(
            output,
            AssertMatchers.count_metrics(expected_metric_counts),
            "Assert number of metrics is expected value",
        )

        test_pipeline.run()

    def testProduceViolationMetricsNoInput(self) -> None:
        """Tests the ProduceViolationMetrics when there is no input to the function."""

        test_pipeline = create_test_pipeline()

        output = (
            test_pipeline
            | beam.Create([])
            | "Produce ViolationMetrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_parameters.project,
                self.pipeline_parameters.region,
                self.pipeline_parameters.job_name,
                {ViolationMetricType.VIOLATION},
                self.pipeline_parameters.calculation_month_count,
                self.metric_producer,
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class AssertMatchers:
    """Functions to be used by Apache Beam testing `assert_that` functions to validate
    pipeline outputs."""

    @staticmethod
    def validate_pipeline_test() -> Callable[[Dict[str, int], bool], None]:
        def _validate_pipeline_test(
            output: Dict[str, int], allow_empty: bool = False
        ) -> None:
            if not allow_empty and not output:
                raise BeamAssertException("Output metrics unexpectedly empty.")

            for metric in output:
                if not isinstance(metric, ViolationMetric):
                    raise BeamAssertException(
                        "Failed assert. Output is not of type ViolationMetric."
                    )

        return _validate_pipeline_test

    @staticmethod
    def count_metrics(
        expected_metric_counts: Dict[str, int]
    ) -> Callable[[Iterable[RecidivizMetric]], None]:
        """Asserts that the number of metric combinations matches the expected
        counts."""

        def _count_metrics(output: Iterable[RecidivizMetric]) -> None:
            actual_combination_counts = {}

            for key in expected_metric_counts.keys():
                actual_combination_counts[key] = 0

            for metric in output:
                if not isinstance(metric, ViolationMetric):
                    raise ValueError(f"Found unexpected metric type [{type(metric)}].")

                metric_type = metric.metric_type

                actual_combination_counts[metric_type.value] = (
                    actual_combination_counts[metric_type.value] + 1
                )

            for key in expected_metric_counts:
                if expected_metric_counts[key] != actual_combination_counts[key]:
                    raise BeamAssertException(
                        "Failed assert. Count does not match expected value:"
                        f"{expected_metric_counts[key]} != {actual_combination_counts[key]}"
                    )

        return _count_metrics
