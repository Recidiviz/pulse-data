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
"""Tests for recidivism/pipeline.py."""
import datetime
import unittest
from datetime import date
from enum import Enum
from typing import Any, Callable, Dict, Iterable, Optional, Set, Tuple
from unittest import mock
from unittest.mock import patch

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException, assert_that, equal_to
from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
    StateResidencyStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.state import schema
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
    RecidivizMetricWritableDict,
)
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.pipelines.metrics.recidivism import identifier, pipeline
from recidiviz.pipelines.metrics.recidivism.events import (
    NonRecidivismReleaseEvent,
    RecidivismReleaseEvent,
)
from recidiviz.pipelines.metrics.recidivism.metrics import (
    ReincarcerationRecidivismMetricType,
)
from recidiviz.pipelines.metrics.recidivism.metrics import (
    ReincarcerationRecidivismMetricType as MetricType,
)
from recidiviz.pipelines.metrics.recidivism.metrics import (
    ReincarcerationRecidivismRateMetric,
)
from recidiviz.pipelines.utils.execution_utils import RootEntityId
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
    MetricType.REINCARCERATION_RATE: True,
}


class TestRecidivismPipeline(unittest.TestCase):
    """Tests the entire recidivism pipeline."""

    def setUp(self) -> None:
        self.project_id = "test-project"
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id

        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()
        self.fake_bq_sink_factory = FakeWriteToBigQueryFactory(
            FakeWriteMetricsToBigQuery
        )

        self.delegate_patchers = start_pipeline_delegate_getter_patchers(identifier)
        self.pipeline_class = pipeline.RecidivismMetricsPipeline

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()
        self.project_id_patcher.stop()

    def build_data_dict(self, fake_person_id: int) -> Dict[str, Iterable]:
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
            state_code="ND",
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

        initial_incarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_date=date(2008, 11, 20),
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id,
        )

        first_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_date=date(2011, 4, 5),
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id,
        )

        subsequent_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=3333,
            external_id="ip3",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_date=date(2017, 1, 4),
            person_id=fake_person_id,
        )

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration, {"sequence_num": 0}),
            normalized_database_base_dict(first_reincarceration, {"sequence_num": 1}),
            normalized_database_base_dict(
                subsequent_reincarceration, {"sequence_num": 2}
            ),
        ]

        data_dict = default_data_dict_for_pipeline_class(self.pipeline_class)
        data_dict_overrides: Dict[str, Iterable[Dict[str, Any]]] = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StatePersonRace.__tablename__: races_data,
            schema.StatePersonEthnicity.__tablename__: ethnicity_data,
            schema.StateIncarcerationPeriod.__tablename__: incarceration_periods_data,
        }
        data_dict.update(data_dict_overrides)

        return data_dict

    def testRecidivismPipeline(self) -> None:
        """Tests the entire recidivism pipeline with one person and three
        incarceration periods."""

        fake_person_id = 12345

        data_dict = self.build_data_dict(fake_person_id)

        self.run_test_pipeline(data_dict)

    def testRecidivismPipelineWithFilterSet(self) -> None:
        """Tests the entire recidivism pipeline with one person and three
        incarceration periods."""

        fake_person_id = 12345

        data_dict = self.build_data_dict(fake_person_id)

        self.run_test_pipeline(data_dict)

    def testRecidivismPipeline_WithConditionalReturns(self) -> None:
        """Tests the entire RecidivismPipeline with two person and three
        incarceration periods each. One entities.StatePerson has a return from a
        technical supervision violation.
        """

        fake_person_id_1 = 12345

        fake_person_1 = schema.StatePerson(
            state_code="US_XX",
            person_id=fake_person_id_1,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        fake_person_id_2 = 6789

        fake_person_2 = schema.StatePerson(
            state_code="US_XX",
            person_id=fake_person_id_2,
            gender=StateGender.FEMALE,
            birthdate=date(1990, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        persons_data = [
            normalized_database_base_dict(fake_person_1),
            normalized_database_base_dict(fake_person_2),
        ]

        initial_incarceration_1 = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_date=date(2008, 11, 20),
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id_1,
        )

        first_reincarceration_1 = schema.StateIncarcerationPeriod(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_date=date(2011, 4, 5),
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id_1,
        )

        subsequent_reincarceration_1 = schema.StateIncarcerationPeriod(
            incarceration_period_id=3333,
            external_id="ip3",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_date=date(2017, 1, 4),
            person_id=fake_person_id_1,
        )

        initial_incarceration_2 = schema.StateIncarcerationPeriod(
            incarceration_period_id=4444,
            external_id="ip4",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_date=date(2004, 12, 20),
            release_date=date(2010, 6, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            person_id=fake_person_id_2,
        )

        first_reincarceration_2 = schema.StateIncarcerationPeriod(
            incarceration_period_id=5555,
            external_id="ip5",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_date=date(2011, 4, 5),
            release_date=date(2014, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id_2,
        )

        subsequent_reincarceration_2 = schema.StateIncarcerationPeriod(
            incarceration_period_id=6666,
            external_id="ip6",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_date=date(2018, 3, 9),
            person_id=fake_person_id_2,
        )

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration_1, {"sequence_num": 0}),
            normalized_database_base_dict(first_reincarceration_1, {"sequence_num": 1}),
            normalized_database_base_dict(
                subsequent_reincarceration_1, {"sequence_num": 2}
            ),
            normalized_database_base_dict(initial_incarceration_2, {"sequence_num": 0}),
            normalized_database_base_dict(first_reincarceration_2, {"sequence_num": 1}),
            normalized_database_base_dict(
                subsequent_reincarceration_2, {"sequence_num": 2}
            ),
        ]

        data_dict = default_data_dict_for_pipeline_class(self.pipeline_class)
        data_dict_overrides: Dict[str, Iterable[Dict[str, Any]]] = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StateIncarcerationPeriod.__tablename__: incarceration_periods_data,
        }
        data_dict.update(data_dict_overrides)

        self.run_test_pipeline(data_dict)

    def run_test_pipeline(
        self,
        data_dict: DataTablesDict,
        root_entity_id_filter_set: Optional[Set[RootEntityId]] = None,
        metric_types_filter: Optional[Set[str]] = None,
    ) -> None:
        """Runs a test version of the recidivism pipeline."""
        expected_metric_types: Set[MetricType] = {
            ReincarcerationRecidivismMetricType.REINCARCERATION_RATE,
        }

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


class TestClassifyReleaseEvents(unittest.TestCase):
    """Tests the ClassifyReleaseEvents DoFn in the pipeline."""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(identifier)
        self.state_code = "US_XX"
        self.identifier = identifier.RecidivismIdentifier(StateCode.US_XX)
        self.pipeline_class = pipeline.RecidivismMetricsPipeline

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def testClassifyReleaseEvents(self) -> None:
        """Tests the ClassifyReleaseEvents DoFn when there are two instances
        of recidivism."""

        fake_person_id = 12345

        fake_person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        initial_incarceration = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            sequence_num=0,
        )

        first_reincarceration = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=date(2011, 4, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            sequence_num=1,
        )

        subsequent_reincarceration = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=3333,
            external_id="ip3",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=date(2017, 1, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            sequence_num=2,
        )

        person_incarceration_periods = {
            NormalizedStatePerson.__name__: [fake_person],
            NormalizedStateSupervisionPeriod.__name__: [],
            NormalizedStateIncarcerationPeriod.__name__: [
                initial_incarceration,
                first_reincarceration,
                subsequent_reincarceration,
            ],
        }

        assert initial_incarceration.admission_date is not None
        assert initial_incarceration.release_date is not None
        assert first_reincarceration.admission_date is not None
        assert first_reincarceration.release_date is not None
        assert subsequent_reincarceration.admission_date is not None
        first_recidivism_release_event = RecidivismReleaseEvent(
            state_code="US_XX",
            original_admission_date=initial_incarceration.admission_date,
            release_date=initial_incarceration.release_date,
            release_facility=None,
            reincarceration_date=first_reincarceration.admission_date,
            reincarceration_facility=None,
        )

        second_recidivism_release_event = RecidivismReleaseEvent(
            state_code="US_XX",
            original_admission_date=first_reincarceration.admission_date,
            release_date=first_reincarceration.release_date,
            release_facility=None,
            reincarceration_date=subsequent_reincarceration.admission_date,
            reincarceration_facility=None,
        )

        correct_output = [
            (
                fake_person,
                {
                    initial_incarceration.release_date.year: [
                        first_recidivism_release_event
                    ],
                    first_reincarceration.release_date.year: [
                        second_recidivism_release_event
                    ],
                },
            ),
        ]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_incarceration_periods)])
            | "Identify Recidivism Events"
            >> beam.ParDo(
                ClassifyResults(),
                identifier=self.identifier,
                included_result_classes={
                    NonRecidivismReleaseEvent,
                    RecidivismReleaseEvent,
                },
            )
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyReleaseEvents_NoRecidivism(self) -> None:
        """Tests the ClassifyReleaseEvents DoFn in the pipeline when there
        is no instance of recidivism."""

        fake_person_id = 12345

        fake_person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        only_incarceration = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            sequence_num=0,
        )

        person_incarceration_periods = {
            NormalizedStatePerson.__name__: [fake_person],
            NormalizedStateSupervisionPeriod.__name__: [],
            NormalizedStateIncarcerationPeriod.__name__: [only_incarceration],
        }

        assert only_incarceration.admission_date is not None
        assert only_incarceration.release_date is not None
        non_recidivism_release_event = NonRecidivismReleaseEvent(
            "US_XX",
            only_incarceration.admission_date,
            only_incarceration.release_date,
            only_incarceration.facility,
        )

        correct_output = [
            (
                fake_person,
                {only_incarceration.release_date.year: [non_recidivism_release_event]},
            )
        ]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_incarceration_periods)])
            | "Identify Recidivism Events"
            >> beam.ParDo(
                ClassifyResults(),
                identifier=self.identifier,
                included_result_classes={
                    NonRecidivismReleaseEvent,
                    RecidivismReleaseEvent,
                },
            )
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyReleaseEvents_NoIncarcerationPeriods(self) -> None:
        """Tests the ClassifyReleaseEvents DoFn in the pipeline when there
        are no incarceration periods. The person in this case should be
        excluded from the calculations."""

        fake_person_id = 12345

        fake_person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        person_incarceration_periods = {
            NormalizedStatePerson.__name__: [fake_person],
            NormalizedStateSupervisionPeriod.__name__: [],
            NormalizedStateIncarcerationPeriod.__name__: [],
        }

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_incarceration_periods)])
            | "Identify Recidivism Events"
            >> beam.ParDo(
                ClassifyResults(),
                identifier=self.identifier,
                included_result_classes={
                    NonRecidivismReleaseEvent,
                    RecidivismReleaseEvent,
                },
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class TestProduceRecidivismMetrics(unittest.TestCase):
    """Tests for the ProduceRecidivismMetrics DoFn in the
    pipeline."""

    def setUp(self) -> None:
        self.fake_person_id = 12345

        self.job_id_patcher = mock.patch(
            "recidiviz.pipelines.metrics.base_metric_pipeline.job_id"
        )
        self.mock_job_id = self.job_id_patcher.start()
        self.mock_job_id.return_value = "job_id"
        self.metric_producer = pipeline.metric_producer.RecidivismMetricProducer()

        self.pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-staging",
            state_code="US_XX",
            pipeline="recidivism_metrics",
            metric_types="ALL",
            region="region",
            worker_zone="zone",
            person_filter_ids=None,
            calculation_month_count=-1,
        )

    def tearDown(self) -> None:
        self.job_id_patcher.stop()

    # TODO(#4813): This fails on dates after 2020-12-03 - is this a bug in the pipeline or in the test code?
    @freeze_time("2020-12-03 00:00:00-05:00")
    def testProduceRecidivismMetrics(self) -> None:
        """Tests the ProduceRecidivismMetrics DoFn in the pipeline."""
        fake_person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=self.fake_person_id,
            gender=StateGender.MALE,
            residency_status=StateResidencyStatus.PERMANENT,
        )

        first_recidivism_release_event = RecidivismReleaseEvent(
            state_code="US_XX",
            original_admission_date=date(2008, 11, 20),
            release_date=date(2010, 12, 4),
            release_facility=None,
            reincarceration_date=date(2011, 4, 5),
            reincarceration_facility=None,
        )

        second_recidivism_release_event = RecidivismReleaseEvent(
            state_code="US_XX",
            original_admission_date=date(2011, 4, 5),
            release_date=date(2014, 4, 14),
            release_facility=None,
            reincarceration_date=date(2017, 1, 4),
            reincarceration_facility=None,
        )

        person_release_events = [
            (
                fake_person,
                {
                    first_recidivism_release_event.release_date.year: [
                        first_recidivism_release_event
                    ],
                    second_recidivism_release_event.release_date.year: [
                        second_recidivism_release_event
                    ],
                },
            )
        ]

        # We do not track metrics for periods that start after today, so we need to subtract for some number of periods
        # that go beyond whatever today is.
        periods = relativedelta(date.today(), date(2010, 12, 4)).years + 1
        periods_with_single = 6
        periods_with_double = periods - periods_with_single

        expected_combinations_count_2010 = periods_with_single + (
            2 * periods_with_double
        )

        periods = relativedelta(date.today(), date(2014, 4, 14)).years + 1

        expected_combinations_count_2014 = periods

        expected_metric_counts = {
            2010: expected_combinations_count_2010,
            2014: expected_combinations_count_2014,
        }

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create(person_release_events)
            | "Produce Metric Combinations"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_parameters.project,
                self.pipeline_parameters.region,
                self.pipeline_parameters.job_name,
                {ReincarcerationRecidivismMetricType.REINCARCERATION_RATE},
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

    def testProduceRecidivismMetrics_NoResults(self) -> None:
        """Tests the ProduceRecidivismMetrics DoFn in the pipeline
        when there are no ReleaseEvents associated with the entities.StatePerson."""
        fake_person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=self.fake_person_id,
            gender=StateGender.MALE,
            residency_status=StateResidencyStatus.PERMANENT,
        )

        person_release_events: Iterable[Tuple[NormalizedStatePerson, Dict]] = [
            (fake_person, {})
        ]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create(person_release_events)
            | "Produce Metric Combinations"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_parameters.project,
                self.pipeline_parameters.region,
                self.pipeline_parameters.job_name,
                {ReincarcerationRecidivismMetricType.REINCARCERATION_RATE},
                self.pipeline_parameters.calculation_month_count,
                self.metric_producer,
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testProduceRecidivismMetrics_NoPersonEvents(self) -> None:
        """Tests the ProduceRecidivismMetrics DoFn in the pipeline
        when there is no entities.StatePerson and no ReleaseEvents."""
        test_pipeline = TestPipeline()
        output = (
            test_pipeline
            | beam.Create([])
            | "Produce Metric Combinations"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_parameters.project,
                self.pipeline_parameters.region,
                self.pipeline_parameters.job_name,
                {ReincarcerationRecidivismMetricType.REINCARCERATION_RATE},
                self.pipeline_parameters.calculation_month_count,
                self.metric_producer,
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class TestRecidivismMetricWritableDict(unittest.TestCase):
    """Tests the RecidivismMetricWritableDict DoFn in the pipeline."""

    def testRecidivismMetricWritableDict(self) -> None:
        """Tests converting the ReincarcerationRecidivismMetric to a dictionary
        that can be written to BigQuery."""
        metric = MetricGroup.recidivism_metric_with_age

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([metric])
            | "Convert to writable dict" >> beam.ParDo(RecidivizMetricWritableDict())
        )

        assert_that(output, AssertMatchers.validate_metric_writable_dict())

        test_pipeline.run()

    def testRecidivismMetricWritableDict_WithDateField(self) -> None:
        """Tests converting the ReincarcerationRecidivismMetric to a dictionary
        that can be written to BigQuery, where the metric contains a date
        attribute."""
        metric = MetricGroup.recidivism_metric_without_dimensions
        metric.created_on = date.today()

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([metric])
            | "Convert to writable dict" >> beam.ParDo(RecidivizMetricWritableDict())
        )

        assert_that(output, AssertMatchers.validate_metric_writable_dict())

        test_pipeline.run()

    def testRecidivismMetricWritableDict_WithEmptyDateField(self) -> None:
        """Tests converting the ReincarcerationRecidivismMetric to a dictionary
        that can be written to BigQuery, where the metric contains None on a
        date attribute."""
        metric = MetricGroup.recidivism_metric_without_dimensions
        metric.created_on = None

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([metric])
            | "Convert to writable dict" >> beam.ParDo(RecidivizMetricWritableDict())
        )

        assert_that(output, AssertMatchers.validate_metric_writable_dict())

        test_pipeline.run()


class MetricGroup:
    """Stores a set of metrics where every dimension is included for testing
    dimension filtering."""

    recidivism_metric_with_age = ReincarcerationRecidivismRateMetric(
        job_id="12345",
        state_code="US_XX",
        release_cohort=2015,
        follow_up_period=1,
        age=26,
        did_recidivate=True,
    )

    recidivism_metric_with_gender = ReincarcerationRecidivismRateMetric(
        job_id="12345",
        state_code="US_XX",
        release_cohort=2015,
        follow_up_period=1,
        gender=StateGender.MALE,
        did_recidivate=True,
    )

    recidivism_metric_with_release_facility = ReincarcerationRecidivismRateMetric(
        job_id="12345",
        state_code="US_XX",
        release_cohort=2015,
        follow_up_period=1,
        release_facility="Red",
        did_recidivate=True,
    )

    recidivism_metric_with_stay_length = ReincarcerationRecidivismRateMetric(
        job_id="12345",
        state_code="US_XX",
        release_cohort=2015,
        follow_up_period=1,
        stay_length_bucket="12-24",
        did_recidivate=True,
    )

    recidivism_metric_without_dimensions = ReincarcerationRecidivismRateMetric(
        job_id="12345",
        state_code="US_XX",
        release_cohort=2015,
        follow_up_period=1,
        did_recidivate=True,
    )

    recidivism_metric_event_based = ReincarcerationRecidivismRateMetric(
        job_id="12345",
        state_code="US_XX",
        release_cohort=2015,
        follow_up_period=1,
        did_recidivate=True,
    )

    @staticmethod
    def get_list() -> Iterable[ReincarcerationRecidivismRateMetric]:
        return [
            MetricGroup.recidivism_metric_with_age,
            MetricGroup.recidivism_metric_with_gender,
            MetricGroup.recidivism_metric_with_release_facility,
            MetricGroup.recidivism_metric_with_stay_length,
            MetricGroup.recidivism_metric_without_dimensions,
            MetricGroup.recidivism_metric_event_based,
        ]


class AssertMatchers:
    """Functions to be used by Apache Beam testing `assert_that` functions to
    validate pipeline outputs."""

    @staticmethod
    def count_metrics(expected_metric_counts: Dict[Any, Any]) -> Callable:
        """Asserts that the number of metric combinations matches the expected counts for each release cohort year."""

        def _count_metrics(output: Iterable) -> None:
            actual_combination_counts = {}

            for key in expected_metric_counts.keys():
                actual_combination_counts[key] = 0

            for metric in output:
                if isinstance(metric, ReincarcerationRecidivismRateMetric):
                    release_cohort_year = metric.release_cohort
                    actual_combination_counts[release_cohort_year] = (
                        actual_combination_counts[release_cohort_year] + 1
                    )

            for key in expected_metric_counts:
                if expected_metric_counts[key] != actual_combination_counts[key]:
                    raise BeamAssertException(
                        f"Failed assert. Count {actual_combination_counts[key]} does not match"
                        f" expected value {expected_metric_counts[key]}."
                    )

        return _count_metrics

    @staticmethod
    def validate_metric_writable_dict() -> Callable:
        def _validate_metric_writable_dict(output: Iterable) -> None:
            for metric_dict in output:
                for _, value in metric_dict.items():
                    if isinstance(value, Enum):
                        raise BeamAssertException(
                            "Failed assert. Dictionary"
                            "contains invalid Enum "
                            f"value: {value}"
                        )
                    if isinstance(value, datetime.date):
                        raise BeamAssertException(
                            "Failed assert. Dictionary"
                            "contains invalid date "
                            f"value: {value}"
                        )

        return _validate_metric_writable_dict
