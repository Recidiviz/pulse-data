# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for population_spans/pipeline.py"""
import unittest
from collections import defaultdict
from datetime import date
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple
from unittest import mock
from unittest.mock import patch

import apache_beam as beam
from apache_beam.testing.util import BeamAssertException, assert_that, equal_to

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
    NormalizedStatePerson,
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.ingest.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.pipelines.metrics.base_metric_pipeline import (
    ClassifyResults,
    ProduceMetrics,
)
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.pipelines.metrics.population_spans import (
    identifier as population_spans_identifier,
)
from recidiviz.pipelines.metrics.population_spans import pipeline
from recidiviz.pipelines.metrics.population_spans.identifier import (
    PopulationSpanIdentifier,
)
from recidiviz.pipelines.metrics.population_spans.metric_producer import (
    PopulationSpanMetricProducer,
)
from recidiviz.pipelines.metrics.population_spans.metrics import (
    PopulationSpanMetricType,
)
from recidiviz.pipelines.metrics.population_spans.spans import (
    IncarcerationPopulationSpan,
    SupervisionPopulationSpan,
)
from recidiviz.pipelines.metrics.utils.metric_utils import RecidivizMetric
from recidiviz.pipelines.utils.execution_utils import RootEntityId
from recidiviz.pipelines.utils.identifier_models import Span
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

ALL_METRIC_INCLUSIONS_DICT = {
    metric_type: True for metric_type in PopulationSpanMetricType
}


class TestPopulationSpanPipeline(unittest.TestCase):
    """Tests the entire population spans pipeline."""

    def setUp(self) -> None:
        self.project_id = "test-project"
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id

        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()
        self.fake_bq_sink_factory = FakeWriteToBigQueryFactory(
            FakeWriteMetricsToBigQuery
        )
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            population_spans_identifier
        )
        self.pipeline_class = pipeline.PopulationSpanMetricsPipeline

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()
        self.project_id_patcher.stop()

    def _stop_state_specific_delegate_patchers(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def build_data_dict(
        self,
        fake_person_id: int,
        fake_incarceration_period_id: int,
        fake_supervision_period_id: int,
    ) -> Dict[str, Iterable[Any]]:
        """Builds a data_dict for a basic run of the pipeline."""
        fake_person = schema.StatePerson(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=StateGender.FEMALE,
            birthdate=date(1985, 2, 1),
        )
        persons_data = [normalized_database_base_dict(fake_person)]

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

        ethnicity = schema.StatePersonEthnicity(
            person_ethnicity_id=111,
            state_code="US_XX",
            ethnicity=StateEthnicity.NOT_HISPANIC,
            person_id=fake_person_id,
        )

        ethnicity_data = normalized_database_base_dict_list([ethnicity])

        incarceration_period = schema.StateIncarcerationPeriod(
            incarceration_period_id=fake_incarceration_period_id,
            external_id=f"external_id_{fake_incarceration_period_id}",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            county_code="124",
            facility="FA",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_date=date(2020, 1, 1),
            release_date=date(2022, 6, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id,
        )

        incarceration_periods_data = [
            normalized_database_base_dict(incarceration_period, {"sequence_num": 0})
        ]

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=fake_supervision_period_id,
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            state_code="US_XX",
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MEDIUM",
            supervision_site="level 1",
            start_date=date(2018, 1, 1),
            termination_date=date(2020, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            person_id=fake_person_id,
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
        )

        supervision_periods_data = [
            normalized_database_base_dict(supervision_period, {"sequence_num": 0})
        ]

        data_dict = default_data_dict_for_pipeline_class(self.pipeline_class)
        data_dict_overrides = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StatePersonRace.__tablename__: races_data,
            schema.StatePersonEthnicity.__tablename__: ethnicity_data,
            schema.StateIncarcerationPeriod.__tablename__: incarceration_periods_data,
            schema.StateSupervisionPeriod.__tablename__: supervision_periods_data,
        }
        # Given the empty dictionaries, we ignore the overall type until we fill in data.
        data_dict.update(data_dict_overrides)  # type:ignore
        return data_dict

    def run_test_pipeline(
        self,
        data_dict: DataTablesDict,
        expected_metric_types: Set[PopulationSpanMetricType],
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
            include_calculation_limit_args=False,
        )

    def test_population_spans_pipeline(self) -> None:
        """Tests the population_spans_pipeline."""
        data_dict = self.build_data_dict(
            fake_person_id=12345,
            fake_incarceration_period_id=23456,
            fake_supervision_period_id=34567,
        )
        self.run_test_pipeline(
            data_dict,
            expected_metric_types={
                PopulationSpanMetricType.INCARCERATION_POPULATION_SPAN,
                PopulationSpanMetricType.SUPERVISION_POPULATION_SPAN,
            },
        )

    def test_population_spans_pipeline_with_filter_set(self) -> None:
        """Tests the population spans pipeline with a proper filter set."""
        data_dict = self.build_data_dict(
            fake_person_id=12345,
            fake_incarceration_period_id=23456,
            fake_supervision_period_id=34567,
        )
        self.run_test_pipeline(
            data_dict,
            expected_metric_types={
                PopulationSpanMetricType.INCARCERATION_POPULATION_SPAN,
                PopulationSpanMetricType.SUPERVISION_POPULATION_SPAN,
            },
            root_entity_id_filter_set={12345},
        )

    def test_population_spans_pipeline_with_no_data(self) -> None:
        """Tests the population spans pipeline when a person doesn't have any period data."""
        data_dict = self.build_data_dict(
            fake_person_id=12345,
            fake_incarceration_period_id=23456,
            fake_supervision_period_id=34567,
        )
        data_dict[schema.StateIncarcerationPeriod.__tablename__] = []
        data_dict[schema.StateSupervisionPeriod.__tablename__] = []
        self.run_test_pipeline(data_dict, expected_metric_types=set())


class TestClassifyResults(unittest.TestCase):
    """Tests the ClassifyResults DoFn for Spans."""

    def setUp(self) -> None:
        self.state_code = StateCode.US_XX
        self.fake_person_id = 12345
        self.fake_incarceration_period_id = 23456

        self.fake_person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=self.fake_person_id,
            gender=StateGender.FEMALE,
            birthdate=date(1985, 2, 1),
        )
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            population_spans_identifier
        )

        self.identifier = PopulationSpanIdentifier(self.state_code)
        self.pipeline_class = pipeline.PopulationSpanMetricsPipeline

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    @staticmethod
    def load_person_entities_dict(
        person: NormalizedStatePerson,
        incarceration_periods: Optional[
            List[NormalizedStateIncarcerationPeriod]
        ] = None,
        supervision_periods: Optional[List[NormalizedStateSupervisionPeriod]] = None,
    ) -> Dict[str, List]:
        return {
            NormalizedStatePerson.__name__: [person],
            NormalizedStateIncarcerationPeriod.__name__: (
                incarceration_periods if incarceration_periods else []
            ),
            NormalizedStateSupervisionPeriod.__name__: (
                supervision_periods if supervision_periods else []
            ),
        }

    def test_classify_results(self) -> None:
        """Tests the ClassifyResults DoFn."""
        incarceration_period = normalized_entities.NormalizedStateIncarcerationPeriod(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            county_code="124",
            facility="FA",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_date=date(2020, 1, 1),
            release_date=date(2022, 6, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            sequence_num=0,
        )

        supervision_period = normalized_entities.NormalizedStateSupervisionPeriod(
            supervision_period_id=2222,
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            state_code="US_XX",
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MEDIUM",
            start_date=date(2018, 1, 1),
            termination_date=date(2020, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            supervision_site="site",
            sequence_num=0,
        )

        self.assertIsNotNone(incarceration_period.admission_date)
        self.assertIsNotNone(incarceration_period.admission_reason)
        self.assertIsNotNone(incarceration_period.release_date)
        self.assertIsNotNone(incarceration_period.release_reason)

        self.assertIsNotNone(supervision_period.start_date)
        self.assertIsNotNone(supervision_period.admission_reason)
        self.assertIsNotNone(supervision_period.termination_date)
        self.assertIsNotNone(supervision_period.termination_reason)

        spans = [
            IncarcerationPopulationSpan(
                included_in_state_population=True,
                state_code="US_XX",
                start_date_inclusive=date(2020, 1, 1),
                end_date_exclusive=date(2022, 6, 1),
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                facility="FA",
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                custodial_authority=StateCustodialAuthority.STATE_PRISON,
            ),
            SupervisionPopulationSpan(
                included_in_state_population=True,
                state_code="US_XX",
                start_date_inclusive=date(2018, 1, 1),
                end_date_exclusive=date(2020, 1, 1),
                supervision_level=StateSupervisionLevel.MEDIUM,
                supervision_level_raw_text="MEDIUM",
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                level_1_supervision_location_external_id="site",
                level_2_supervision_location_external_id=None,
                custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            ),
        ]

        correct_output = [(self.fake_person, spans)]

        person_entities = self.load_person_entities_dict(
            person=self.fake_person,
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period],
        )

        test_pipeline = create_test_pipeline()

        output = (
            test_pipeline
            | beam.Create([(self.fake_person_id, person_entities)])
            | "Identify Population Spans"
            >> beam.ParDo(
                ClassifyResults(),
                identifier=self.identifier,
                included_result_classes={
                    IncarcerationPopulationSpan,
                    SupervisionPopulationSpan,
                },
            )
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def test_classify_results_no_periods(self) -> None:
        """Tests the ClassifyResults DoFn with no periods for a person."""
        correct_output: List[Tuple[NormalizedStatePerson, List[Span]]] = []

        person_entities = self.load_person_entities_dict(
            person=self.fake_person,
        )

        test_pipeline = create_test_pipeline()

        output = (
            test_pipeline
            | beam.Create([(self.fake_person_id, person_entities)])
            | "Identify Population Spans"
            >> beam.ParDo(
                ClassifyResults(),
                identifier=self.identifier,
                included_result_classes={
                    IncarcerationPopulationSpan,
                    SupervisionPopulationSpan,
                },
            )
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()


class TestProduceMetrics(unittest.TestCase):
    """Tests the ProduceMetrics DoFn in the pipeline."""

    def setUp(self) -> None:
        self.fake_person_id = 12345
        self.incarceration_period_id = 23456

        self.job_id_patcher = mock.patch(
            "recidiviz.pipelines.metrics.base_metric_pipeline.job_id"
        )
        self.mock_job_id = self.job_id_patcher.start()
        self.mock_job_id.return_value = "job_id"
        self.metric_producer = PopulationSpanMetricProducer()

        self.pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-staging",
            state_code="US_XX",
            pipeline="population_span_metrics",
            metric_types="ALL",
            region="region",
            worker_zone="zone",
            person_filter_ids=None,
            calculation_month_count=-1,
        )

    def tearDown(self) -> None:
        self.job_id_patcher.stop()

    def test_produce_metrics(self) -> None:
        """Tests the ProduceMetrics DoFn."""
        fake_person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=self.fake_person_id,
            gender=StateGender.FEMALE,
            birthdate=date(1985, 2, 1),
        )
        population_span_events = [
            IncarcerationPopulationSpan(
                included_in_state_population=True,
                state_code="US_XX",
                start_date_inclusive=date(2020, 1, 1),
                end_date_exclusive=date(2022, 6, 1),
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                facility="FA",
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                custodial_authority=StateCustodialAuthority.STATE_PRISON,
            ),
            SupervisionPopulationSpan(
                included_in_state_population=True,
                state_code="US_XX",
                start_date_inclusive=date(2018, 1, 1),
                end_date_exclusive=date(2020, 1, 1),
                supervision_level=StateSupervisionLevel.MEDIUM,
                supervision_level_raw_text="MEDIUM",
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                level_1_supervision_location_external_id="site",
                level_2_supervision_location_external_id=None,
                custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            ),
        ]

        expected_metric_counts: Dict[PopulationSpanMetricType, int] = {
            PopulationSpanMetricType.INCARCERATION_POPULATION_SPAN: 4,
            PopulationSpanMetricType.SUPERVISION_POPULATION_SPAN: 4,
        }

        test_pipeline = create_test_pipeline()

        inputs = [(fake_person, population_span_events)]

        output = (
            test_pipeline
            | beam.Create(inputs)
            | "Produce Population Span Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_parameters.project,
                self.pipeline_parameters.region,
                self.pipeline_parameters.job_name,
                {
                    PopulationSpanMetricType.INCARCERATION_POPULATION_SPAN,
                    PopulationSpanMetricType.SUPERVISION_POPULATION_SPAN,
                },
                self.pipeline_parameters.calculation_month_count,
                self.metric_producer,
            )
        )

        assert_that(
            output,
            AssertMatchers.count_metrics(expected_metric_counts),
            "Assert number of metrics is expected value",
        )

    def test_produce_metrics_no_input(self) -> None:
        """Tests the ProduceMetrics when there is no input to the function."""

        test_pipeline = create_test_pipeline()

        output = (
            test_pipeline
            | beam.Create([])
            | "Produce Population Span Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_parameters.project,
                self.pipeline_parameters.region,
                self.pipeline_parameters.job_name,
                {
                    PopulationSpanMetricType.INCARCERATION_POPULATION_SPAN,
                    PopulationSpanMetricType.SUPERVISION_POPULATION_SPAN,
                },
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
    def count_metrics(
        expected_metric_counts: Dict[PopulationSpanMetricType, int]
    ) -> Callable[[Iterable[RecidivizMetric]], None]:
        """Asserts that the number of metric combinations matches the expected
        counts."""

        def _count_metrics(output: Iterable[Any]) -> None:
            actual_combination_counts: Dict[
                PopulationSpanMetricType, int
            ] = defaultdict()

            for metric in output:
                metric_type = metric.metric_type

                actual_combination_counts[metric_type] += 1

            for key in expected_metric_counts:
                if expected_metric_counts[key] != actual_combination_counts[key]:
                    raise BeamAssertException(
                        "Failed assert. Count does not match expected value."
                    )

        return _count_metrics
