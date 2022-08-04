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
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from unittest import mock

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException, assert_that, equal_to

from recidiviz.calculator.pipeline.metrics.base_metric_pipeline import (
    ClassifyResults,
    MetricPipelineJobArgs,
    ProduceMetrics,
)
from recidiviz.calculator.pipeline.metrics.population_spans import pipeline
from recidiviz.calculator.pipeline.metrics.population_spans.identifier import (
    PopulationSpanIdentifier,
)
from recidiviz.calculator.pipeline.metrics.population_spans.metric_producer import (
    PopulationSpanMetricProducer,
)
from recidiviz.calculator.pipeline.metrics.population_spans.metrics import (
    PopulationSpanMetricType,
)
from recidiviz.calculator.pipeline.metrics.population_spans.spans import (
    IncarcerationPopulationSpan,
    SupervisionPopulationSpan,
)
from recidiviz.calculator.pipeline.metrics.utils.metric_utils import (
    PersonMetadata,
    RecidivizMetric,
)
from recidiviz.calculator.pipeline.normalization.utils import normalized_entities
from recidiviz.calculator.pipeline.utils.beam_utils.person_utils import (
    PERSON_EVENTS_KEY,
    PERSON_METADATA_KEY,
    ExtractPersonEventsMetadata,
)
from recidiviz.calculator.pipeline.utils.beam_utils.pipeline_args_utils import (
    derive_apache_beam_pipeline_args,
)
from recidiviz.calculator.pipeline.utils.identifier_models import Span
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_metrics_producer_delegate import (
    StateSpecificIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_metrics_producer_delegate import (
    StateSpecificSupervisionMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_metrics_producer_delegate import (
    UsXxIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_supervision_metrics_producer_delegate import (
    UsXxSupervisionMetricsProducerDelegate,
)
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
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state import entities
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

ALL_METRIC_INCLUSIONS_DICT = {
    metric_type: True for metric_type in PopulationSpanMetricType
}


class TestPopulationSpanPipeline(unittest.TestCase):
    """Tests the entire population spans pipeline."""

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
        self.state_specific_metrics_producer_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.metrics.base_metric_pipeline.get_required_state_specific_metrics_producer_delegates",
            return_value={
                StateSpecificIncarcerationMetricsProducerDelegate.__name__: UsXxIncarcerationMetricsProducerDelegate(),
                StateSpecificSupervisionMetricsProducerDelegate.__name__: UsXxSupervisionMetricsProducerDelegate(),
            },
        )
        self.mock_get_required_state_metrics_producer_delegate = (
            self.state_specific_metrics_producer_delegate_patcher.start()
        )
        self.run_delegate_class = pipeline.PopulationSpanMetricsPipelineRunDelegate

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        self.state_specific_delegate_patcher.stop()
        self.state_specific_metrics_producer_delegate_patcher.stop()

    def build_data_dict(
        self,
        fake_person_id: int,
        fake_incarceration_period_id: int,
        fake_supervision_period_id: int,
    ) -> Dict[str, List[Any]]:
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

        incarceration_period_judicial_district_association_data = [
            {
                "state_code": "US_XX",
                "person_id": fake_person_id,
                "incarceration_period_id": fake_incarceration_period_id,
                "judicial_district_code": "NW",
            }
        ]

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=fake_supervision_period_id,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            state_code="US_XX",
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MEDIUM",
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

        supervision_period_judicial_district_association_data = [
            {
                "state_code": "US_XX",
                "person_id": fake_person_id,
                "supervision_period_id": fake_supervision_period_id,
                "judicial_district_code": "NW",
            }
        ]

        supervision_period_to_agent_association = [
            {
                "state_code": "US_XX",
                "person_id": fake_person_id,
                "supervision_period_id": fake_supervision_period_id,
                "agent_external_id": "OFFICER 1",
            }
        ]

        state_race_ethnicity_population_count_data = [
            {
                "state_code": "US_XX",
                "race_or_ethnicity": "ASIAN",
                "population_count": 1,
                "representation_priority": 1,
            }
        ]

        data_dict = default_data_dict_for_run_delegate(self.run_delegate_class)
        data_dict_overrides = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StatePersonRace.__tablename__: races_data,
            schema.StatePersonEthnicity.__tablename__: ethnicity_data,
            schema.StateIncarcerationPeriod.__tablename__: incarceration_periods_data,
            schema.StateSupervisionPeriod.__tablename__: supervision_periods_data,
            "incarceration_period_judicial_district_association": incarceration_period_judicial_district_association_data,
            "state_race_ethnicity_population_counts": state_race_ethnicity_population_count_data,
            "supervision_period_judicial_district_association": supervision_period_judicial_district_association_data,
            "supervision_period_to_agent_association": supervision_period_to_agent_association,
        }
        # Given the empty dictionaries, we ignore the overall type until we fill in data.
        data_dict.update(data_dict_overrides)  # type:ignore
        return data_dict

    def run_test_pipeline(
        self,
        data_dict: DataTablesDict,
        expected_metric_types: Set[PopulationSpanMetricType],
        unifying_id_field_filter_set: Optional[Set[int]] = None,
        metric_types_filter: Optional[Set[str]] = None,
    ) -> None:
        """Runs a test version of the violation pipeline."""
        project = "project"
        dataset = "dataset"
        normalized_dataset = "us_xx_normalized_state"

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
            unifying_id_field_filter_set={12345},
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
        self.state_code = "US_XX"
        self.fake_person_id = 12345
        self.fake_incarceration_period_id = 23456

        self.fake_person = entities.StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=self.fake_person_id,
            gender=StateGender.FEMALE,
            birthdate=date(1985, 2, 1),
        )
        self.identifier = PopulationSpanIdentifier()
        self.run_delegate_class = pipeline.PopulationSpanMetricsPipelineRunDelegate
        self.state_specific_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.metrics.base_metric_pipeline.get_required_state_specific_delegates",
            return_value=STATE_DELEGATES_FOR_TESTS,
        )
        self.mock_get_required_state_delegates = (
            self.state_specific_delegate_patcher.start()
        )

    def tearDown(self) -> None:
        self.state_specific_delegate_patcher.stop()

    @staticmethod
    def load_person_entities_dict(
        person: entities.StatePerson,
        incarceration_periods: List[entities.StateIncarcerationPeriod] = None,
        ip_to_judicial_district_kv: List[Dict[Any, Any]] = None,
        supervision_periods: List[entities.StateSupervisionPeriod] = None,
        sp_to_judicial_district_kv: List[Dict[Any, Any]] = None,
        sp_to_agent_kv: List[Dict[Any, Any]] = None,
    ) -> Dict[str, List]:
        return {
            entities.StatePerson.__name__: [person],
            entities.StateIncarcerationPeriod.__name__: incarceration_periods
            if incarceration_periods
            else [],
            "incarceration_period_judicial_district_association": ip_to_judicial_district_kv
            or [],
            "supervision_period_judicial_district_association": sp_to_judicial_district_kv
            or [],
            entities.StateSupervisionPeriod.__name__: supervision_periods
            if supervision_periods
            else [],
            "supervision_period_to_agent_association": sp_to_agent_kv or [],
        }

    def test_classify_results(self) -> None:
        """Tests the ClassifyResults DoFn."""
        incarceration_period = normalized_entities.NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
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

        incarceration_period_judicial_district_association_result = {
            "person_id": self.fake_person_id,
            "incarceration_period_id": 1111,
            "judicial_district_code": "NW",
        }

        supervision_period = (
            normalized_entities.NormalizedStateSupervisionPeriod.new_with_defaults(
                supervision_period_id=2222,
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
        )

        supervision_period_judicial_district_association_result = {
            "person_id": self.fake_person_id,
            "supervision_period_id": 2222,
            "judicial_district_code": "NW",
        }

        supervision_period_agent_association_result = {
            "person_id": self.fake_person_id,
            "agent_external_id": "OFFICER 1",
            "supervision_period_id": 2222,
        }

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
                facility="FA",
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                custodial_authority=StateCustodialAuthority.STATE_PRISON,
                judicial_district_code="NW",
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
                supervising_district_external_id="site",
                level_1_supervision_location_external_id="site",
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id="OFFICER 1",
                custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
                judicial_district_code="NW",
            ),
        ]

        correct_output = [(self.fake_person_id, (self.fake_person, spans))]

        person_entities = self.load_person_entities_dict(
            person=self.fake_person,
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period],
            ip_to_judicial_district_kv=[
                incarceration_period_judicial_district_association_result,
            ],
            sp_to_judicial_district_kv=[
                supervision_period_judicial_district_association_result
            ],
            sp_to_agent_kv=[supervision_period_agent_association_result],
        )

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(self.fake_person_id, person_entities)])
            | "Identify Population Spans"
            >> beam.ParDo(
                ClassifyResults(),
                state_code=self.state_code,
                identifier=self.identifier,
                pipeline_config=self.run_delegate_class.pipeline_config(),
            )
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def test_classify_results_no_periods(self) -> None:
        """Tests the ClassifyResults DoFn with no periods for a person."""
        correct_output: List[Tuple[int, Tuple[entities.StatePerson, List[Span]]]] = []

        person_entities = self.load_person_entities_dict(
            person=self.fake_person,
        )

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(self.fake_person_id, person_entities)])
            | "Identify Population Spans"
            >> beam.ParDo(
                ClassifyResults(),
                state_code=self.state_code,
                identifier=self.identifier,
                pipeline_config=self.run_delegate_class.pipeline_config(),
            )
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()


class TestProduceMetrics(unittest.TestCase):
    """Tests the ProduceMetrics DoFn in the pipeline."""

    def setUp(self) -> None:
        self.fake_person_id = 12345
        self.incarceration_period_id = 23456

        self.person_metadata = PersonMetadata(prioritized_race_or_ethnicity="ASIAN")

        self.job_id_patcher = mock.patch(
            "recidiviz.calculator.pipeline.metrics.base_metric_pipeline.job_id"
        )
        self.mock_job_id = self.job_id_patcher.start()
        self.mock_job_id.return_value = "job_id"
        self.state_specific_metrics_producer_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.metrics.base_metric_pipeline.get_required_state_specific_metrics_producer_delegates",
        )
        self.mock_state_specific_metrics_producer_delegate = (
            self.state_specific_metrics_producer_delegate_patcher.start()
        )
        self.mock_state_specific_metrics_producer_delegate.return_value = {
            StateSpecificIncarcerationMetricsProducerDelegate.__name__: UsXxIncarcerationMetricsProducerDelegate()
        }

        self.metric_producer = PopulationSpanMetricProducer()

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
        self.state_specific_metrics_producer_delegate_patcher.stop()

    def test_produce_metrics(self) -> None:
        """Tests the ProduceMetrics DoFn."""
        fake_person = entities.StatePerson.new_with_defaults(
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
                facility="FA",
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                custodial_authority=StateCustodialAuthority.STATE_PRISON,
                judicial_district_code="NW",
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
                supervising_district_external_id="site",
                level_1_supervision_location_external_id="site",
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id="OFFICER 1",
                custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
                judicial_district_code="NW",
            ),
        ]

        expected_metric_counts: Dict[PopulationSpanMetricType, int] = {
            PopulationSpanMetricType.INCARCERATION_POPULATION_SPAN: 4,
            PopulationSpanMetricType.SUPERVISION_POPULATION_SPAN: 4,
        }

        test_pipeline = TestPipeline()

        inputs = [
            (
                self.fake_person_id,
                {
                    PERSON_EVENTS_KEY: [(fake_person, population_span_events)],
                    PERSON_METADATA_KEY: [self.person_metadata],
                },
            )
        ]

        output = (
            test_pipeline
            | beam.Create(inputs)
            | beam.ParDo(ExtractPersonEventsMetadata())
            | "Produce Population Span Metrics"
            >> beam.ParDo(
                ProduceMetrics(), self.pipeline_job_args, self.metric_producer
            )
        )

        assert_that(
            output,
            AssertMatchers.count_metrics(expected_metric_counts),
            "Assert number of metrics is expected value",
        )

    def test_produce_metrics_no_input(self) -> None:
        """Tests the ProduceMetrics when there is no input to the function."""

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([])
            | beam.ParDo(ExtractPersonEventsMetadata())
            | "Produce Population Span Metrics"
            >> beam.ParDo(
                ProduceMetrics(), self.pipeline_job_args, self.metric_producer
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
    ) -> Callable[[List[RecidivizMetric]], None]:
        """Asserts that the number of metric combinations matches the expected
        counts."""

        def _count_metrics(output: List[Any]) -> None:
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
