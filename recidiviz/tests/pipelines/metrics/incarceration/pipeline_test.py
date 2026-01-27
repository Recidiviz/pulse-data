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
"""Tests for incarceration/pipeline.py"""
import unittest
from datetime import date
from typing import Any, Callable, Dict, Iterable, List, Optional, Set
from unittest import mock
from unittest.mock import patch

import apache_beam as beam
from apache_beam.testing.util import BeamAssertException, assert_that, equal_to
from freezegun import freeze_time

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET
from recidiviz.common.constants.state.state_assessment import StateAssessmentType
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
    StateResidencyStatus,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateAssessment,
    NormalizedStateIncarcerationPeriod,
    NormalizedStatePerson,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolationResponse,
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
from recidiviz.pipelines.metrics.incarceration import identifier
from recidiviz.pipelines.metrics.incarceration import (
    identifier as incarceration_identifier,
)
from recidiviz.pipelines.metrics.incarceration import pipeline
from recidiviz.pipelines.metrics.incarceration.events import (
    IncarcerationCommitmentFromSupervisionAdmissionEvent,
    IncarcerationReleaseEvent,
    IncarcerationStandardAdmissionEvent,
)
from recidiviz.pipelines.metrics.incarceration.metrics import (
    IncarcerationAdmissionMetric,
    IncarcerationMetric,
    IncarcerationMetricType,
    IncarcerationReleaseMetric,
)
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.pipelines.metrics.utils.metric_utils import RecidivizMetric
from recidiviz.pipelines.utils.execution_utils import RootEntityId
from recidiviz.tests.persistence.database import database_test_utils
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline
from recidiviz.tests.pipelines.calculator_test_utils import (
    normalized_database_base_dict,
    normalized_database_base_dict_list,
)
from recidiviz.tests.pipelines.fake_bigquery import (
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

_STATE_CODE = "US_XX"

ALL_METRICS_INCLUSIONS_DICT = {
    IncarcerationMetricType.INCARCERATION_ADMISSION: True,
    IncarcerationMetricType.INCARCERATION_COMMITMENT_FROM_SUPERVISION: True,
    IncarcerationMetricType.INCARCERATION_RELEASE: True,
}

ALL_METRIC_TYPES_SET = {
    IncarcerationMetricType.INCARCERATION_ADMISSION,
    IncarcerationMetricType.INCARCERATION_COMMITMENT_FROM_SUPERVISION,
    IncarcerationMetricType.INCARCERATION_RELEASE,
}

INCARCERATION_PIPELINE_PACKAGE_NAME = pipeline.__name__


class TestIncarcerationPipeline(unittest.TestCase):
    """Tests the entire incarceration pipeline."""

    def setUp(self) -> None:
        self.project_id = "test-project"
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id
        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()
        self.fake_bq_sink_factory = FakeWriteToBigQueryFactory(
            FakeWriteMetricsToBigQuery
        )
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            incarceration_identifier
        )

        self.pipeline_class = pipeline.IncarcerationMetricsPipeline

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()
        self.project_id_patcher.stop()

    def _stop_state_specific_delegate_patchers(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def build_incarceration_pipeline_data_dict(
        self, fake_person_id: int, state_code: str = "US_XX"
    ) -> Dict[str, Iterable]:
        """Builds a data_dict for a basic run of the pipeline."""
        fake_person = schema.StatePerson(
            state_code=state_code,
            person_id=fake_person_id,
            gender=StateGender.MALE,
            ethnicity=StateEthnicity.HISPANIC,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        persons_data = [normalized_database_base_dict(fake_person)]

        race_1 = schema.StatePersonRace(
            person_race_id=111,
            state_code=state_code,
            race=StateRace.BLACK,
            person_id=fake_person_id,
        )

        race_2 = schema.StatePersonRace(
            person_race_id=111,
            state_code=state_code,
            race=StateRace.WHITE,
            person_id=fake_person_id,
        )

        races_data = normalized_database_base_dict_list([race_1, race_2])

        initial_incarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_date=date(2008, 11, 20),
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id,
        )

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=1111,
            external_id="sp1",
            state_code=state_code,
            county_code="124",
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            start_date=date(2010, 12, 4),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            termination_date=date(2011, 4, 5),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_site="level 1",
            person_id=fake_person_id,
        )

        first_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            admission_date=date(2011, 4, 5),
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id,
        )

        subsequent_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=3333,
            external_id="ip3",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            admission_date=date(2015, 1, 4),
            person_id=fake_person_id,
        )

        assessment = schema.StateAssessment(
            assessment_id=298374,
            external_id="a1",
            state_code=state_code,
            assessment_date=date(2015, 1, 19),
            assessment_type=StateAssessmentType.LSIR,
            person_id=fake_person_id,
        )

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration, {"sequence_num": 0}),
            normalized_database_base_dict(first_reincarceration, {"sequence_num": 1}),
            normalized_database_base_dict(
                subsequent_reincarceration, {"sequence_num": 2}
            ),
        ]

        supervision_periods_data = [
            normalized_database_base_dict(supervision_period, {"sequence_num": 0})
        ]

        supervision_violation_response = (
            database_test_utils.generate_test_supervision_violation_response(
                fake_person_id
            )
        )

        supervision_violation = database_test_utils.generate_test_supervision_violation(
            fake_person_id, [supervision_violation_response]
        )

        supervision_violation_data = [
            normalized_database_base_dict(supervision_violation)
        ]

        supervision_violation_response.supervision_violation_id = (
            supervision_violation.supervision_violation_id
        )

        supervision_violation_response_data = [
            normalized_database_base_dict(
                supervision_violation_response, {"sequence_num": 0}
            )
        ]

        assessment_data = [
            normalized_database_base_dict(
                assessment,
                {"assessment_score_bucket": "NOT_ASSESSED", "sequence_num": 0},
            )
        ]

        data_dict = default_data_dict_for_pipeline_class(self.pipeline_class)
        data_dict_overrides = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StatePersonRace.__tablename__: races_data,
            schema.StateIncarcerationPeriod.__tablename__: incarceration_periods_data,
            schema.StateSupervisionViolationResponse.__tablename__: supervision_violation_response_data,
            schema.StateSupervisionViolation.__tablename__: supervision_violation_data,
            schema.StateSupervisionPeriod.__tablename__: supervision_periods_data,
            schema.StateAssessment.__tablename__: assessment_data,
        }
        data_dict.update(data_dict_overrides)
        return data_dict

    @freeze_time("2015-01-31 00:00:00-05:00")
    def testIncarcerationPipeline(self) -> None:
        fake_person_id = 12345
        data_dict = self.build_incarceration_pipeline_data_dict(
            fake_person_id=fake_person_id
        )

        self.run_test_pipeline(
            state_code=_STATE_CODE,
            data_dict=data_dict,
            expected_metric_types=ALL_METRIC_TYPES_SET,
        )

    def testIncarcerationPipelineWithFilterSet(self) -> None:
        fake_person_id = 12345
        data_dict = self.build_incarceration_pipeline_data_dict(
            fake_person_id=fake_person_id
        )

        self.run_test_pipeline(
            state_code=_STATE_CODE,
            data_dict=data_dict,
            expected_metric_types=ALL_METRIC_TYPES_SET,
            root_entity_id_filter_set={fake_person_id},
        )

    def testIncarcerationPipelineUsMo(self) -> None:
        self._stop_state_specific_delegate_patchers()

        fake_person_id = 12345
        data_dict = self.build_incarceration_pipeline_data_dict(
            fake_person_id=fake_person_id, state_code="US_MO"
        )

        self.run_test_pipeline(
            state_code="US_MO",
            data_dict=data_dict,
            expected_metric_types=ALL_METRIC_TYPES_SET,
        )

    def run_test_pipeline(
        self,
        state_code: str,
        data_dict: Dict[str, Iterable[Dict]],
        expected_metric_types: Set[IncarcerationMetricType],
        root_entity_id_filter_set: Optional[Set[RootEntityId]] = None,
        metric_types_filter: Optional[Set[str]] = None,
    ) -> None:
        """Runs a test version of the supervision pipeline."""
        read_from_bq_constructor = (
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                expected_entities_dataset=normalized_state_dataset_for_state_code(
                    StateCode(state_code.upper())
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
            state_code=state_code,
            project_id=self.project_id,
            read_from_bq_constructor=read_from_bq_constructor,
            write_to_bq_constructor=write_to_bq_constructor,
            root_entity_id_filter_set=root_entity_id_filter_set,
            metric_types_filter=metric_types_filter,
        )

    def build_incarceration_pipeline_data_dict_no_incarceration(
        self, fake_person_id: int
    ) -> Dict[str, Iterable]:
        """Builds a data_dict for a run of the pipeline where the person has no incarceration."""
        fake_person_1 = schema.StatePerson(
            state_code="US_XX",
            person_id=fake_person_id,
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
            normalized_database_base_dict(
                fake_person_1, {"ethnicity": "PRESENT_WITHOUT_INFO"}
            ),
            normalized_database_base_dict(
                fake_person_2, {"ethnicity": "PRESENT_WITHOUT_INFO"}
            ),
        ]

        data_dict = default_data_dict_for_pipeline_class(self.pipeline_class)
        data_dict_overrides = {
            schema.StatePerson.__tablename__: persons_data,
        }
        data_dict.update(data_dict_overrides)

        return data_dict

    def testIncarcerationPipelineNoIncarceration(self) -> None:
        """Tests the incarceration pipeline when a person does not have any
        incarceration periods."""
        fake_person_id = 12345
        data_dict = self.build_incarceration_pipeline_data_dict_no_incarceration(
            fake_person_id
        )

        self.run_test_pipeline(_STATE_CODE, data_dict, expected_metric_types=set())


class TestClassifyIncarcerationEvents(unittest.TestCase):
    """Tests the ClassifyEvents DoFn in the pipeline."""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            incarceration_identifier
        )
        self.state_code = StateCode.US_XX
        self.identifier = identifier.IncarcerationIdentifier(self.state_code)
        self.pipeline_class = pipeline.IncarcerationMetricsPipeline

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
        violation_responses: Optional[
            List[NormalizedStateSupervisionViolationResponse]
        ] = None,
        assessments: Optional[List[NormalizedStateAssessment]] = None,
    ) -> Dict[str, List]:
        return {
            NormalizedStatePerson.__name__: [person],
            NormalizedStateAssessment.__name__: assessments or [],
            NormalizedStateSupervisionPeriod.__name__: (
                supervision_periods if supervision_periods else []
            ),
            NormalizedStateIncarcerationPeriod.__name__: (
                incarceration_periods if incarceration_periods else []
            ),
            NormalizedStateSupervisionViolationResponse.__name__: (
                violation_responses if violation_responses else []
            ),
        }

    def testClassifyIncarcerationEvents(self) -> None:
        """Tests the ClassifyIncarcerationEvents DoFn."""
        fake_person_id = 12345

        fake_person = NormalizedStatePerson(
            state_code=self.state_code.value,
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )

        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=1111,
            external_id="sp1",
            state_code=self.state_code.value,
            county_code="124",
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            start_date=date(2010, 3, 14),
            termination_date=date(2010, 11, 20),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MEDM",
            supervision_site="10",
            person=fake_person,
            sequence_num=0,
        )

        incarceration_period = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=self.state_code.value,
            facility="PRISON XX",
            admission_date=date(2010, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2010, 11, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            sequence_num=0,
        )

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        assert incarceration_period.release_date is not None
        assert incarceration_period.release_reason is not None
        incarceration_events = [
            IncarcerationCommitmentFromSupervisionAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility=incarceration_period.facility,
                admission_reason=incarceration_period.admission_reason,
                admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                level_1_supervision_location_external_id="10",
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.release_date,
                facility=incarceration_period.facility,
                release_reason=incarceration_period.release_reason,
                admission_reason=incarceration_period.admission_reason,
                commitment_from_supervision_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                total_days_incarcerated=(
                    incarceration_period.release_date
                    - incarceration_period.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
        ]

        correct_output = [(fake_person, incarceration_events)]

        test_pipeline = create_test_pipeline()

        person_entities = self.load_person_entities_dict(
            person=fake_person,
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period],
        )

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_entities)])
            | "Identify Incarceration Events"
            >> beam.ParDo(
                ClassifyResults(),
                identifier=self.identifier,
                included_result_classes={
                    IncarcerationStandardAdmissionEvent,
                    IncarcerationCommitmentFromSupervisionAdmissionEvent,
                    IncarcerationReleaseEvent,
                },
            )
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyIncarcerationEvents_NoIncarcerationInfo(self) -> None:
        """Tests the ClassifyIncarcerationEvents DoFn when the person has no child
        entities.
        """
        fake_person = NormalizedStatePerson(
            state_code=self.state_code.value,
            person_id=123,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )

        person_entities = self.load_person_entities_dict(person=fake_person)

        test_pipeline = create_test_pipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person.person_id, person_entities)])
            | "Identify Incarceration Events"
            >> beam.ParDo(
                ClassifyResults(),
                identifier=self.identifier,
                included_result_classes={
                    IncarcerationStandardAdmissionEvent,
                    IncarcerationCommitmentFromSupervisionAdmissionEvent,
                    IncarcerationReleaseEvent,
                },
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class TestProduceIncarcerationMetrics(unittest.TestCase):
    """Tests the ProduceMetrics DoFn in the pipeline."""

    def setUp(self) -> None:
        self.fake_person_id = 12345

        self.job_id_patcher = mock.patch(
            "recidiviz.pipelines.metrics.base_metric_pipeline.job_id"
        )
        self.mock_job_id = self.job_id_patcher.start()
        self.mock_job_id.return_value = "job_id"
        self.metric_producer = pipeline.metric_producer.IncarcerationMetricProducer()

        self.pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-456",
            state_code="US_XX",
            pipeline="incarceration_metrics",
            metric_types="ALL",
            region="region",
            worker_zone="zone",
            person_filter_ids=None,
            calculation_month_count=-1,
        )

    def tearDown(self) -> None:
        self.job_id_patcher.stop()

    def testProduceIncarcerationMetrics(self) -> None:
        """Tests the ProduceIncarcerationMetrics DoFn."""
        fake_person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=123,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )

        incarceration_events = [
            IncarcerationStandardAdmissionEvent(
                state_code="US_XX",
                event_date=date(2001, 3, 16),
                facility="SAN QUENTIN",
                admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            ),
            IncarcerationReleaseEvent(
                state_code="US_XX",
                event_date=date(2002, 5, 26),
                facility="SAN QUENTIN",
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            ),
        ]

        expected_metric_count = 1

        expected_metric_counts = {
            "admissions": expected_metric_count,
            "releases": expected_metric_count,
        }

        test_pipeline = create_test_pipeline()

        inputs = [(fake_person, incarceration_events)]

        output = (
            test_pipeline
            | beam.Create(inputs)
            | "Produce Incarceration Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_parameters.project,
                self.pipeline_parameters.region,
                self.pipeline_parameters.job_name,
                {
                    IncarcerationMetricType.INCARCERATION_ADMISSION,
                    IncarcerationMetricType.INCARCERATION_COMMITMENT_FROM_SUPERVISION,
                    IncarcerationMetricType.INCARCERATION_RELEASE,
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

        test_pipeline.run()

    def testProduceIncarcerationMetrics_NoIncarceration(self) -> None:
        """Tests the ProduceIncarcerationMetrics when there are
        no incarceration_events. This should never happen because any person
        without incarceration events is dropped entirely from the pipeline."""
        fake_person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=123,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )

        test_pipeline = create_test_pipeline()

        inputs: list[tuple[NormalizedStatePerson, list[IncarcerationMetric]]] = [
            (fake_person, [])
        ]

        output = (
            test_pipeline
            | beam.Create(inputs)
            | "Produce Incarceration Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_parameters.project,
                self.pipeline_parameters.region,
                self.pipeline_parameters.job_name,
                {
                    IncarcerationMetricType.INCARCERATION_ADMISSION,
                    IncarcerationMetricType.INCARCERATION_COMMITMENT_FROM_SUPERVISION,
                    IncarcerationMetricType.INCARCERATION_RELEASE,
                },
                self.pipeline_parameters.calculation_month_count,
                self.metric_producer,
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testProduceIncarcerationMetrics_NoInput(self) -> None:
        """Tests the ProduceIncarcerationMetrics when there is
        no input to the function."""

        test_pipeline = create_test_pipeline()

        output = (
            test_pipeline
            | beam.Create([])
            | "Produce Incarceration Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_parameters.project,
                self.pipeline_parameters.region,
                self.pipeline_parameters.job_name,
                {
                    IncarcerationMetricType.INCARCERATION_ADMISSION,
                    IncarcerationMetricType.INCARCERATION_COMMITMENT_FROM_SUPERVISION,
                    IncarcerationMetricType.INCARCERATION_RELEASE,
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
    def validate_metric_type(allow_empty: bool = False) -> Callable:
        def _validate_metric_type(output: List[Any]) -> None:
            if not allow_empty and not output:
                raise BeamAssertException("Output metrics unexpectedly empty")

            for metric in output:
                if not isinstance(metric, IncarcerationMetric):
                    raise BeamAssertException(
                        "Failed assert. Output is not of type IncarcerationMetric."
                    )

        return _validate_metric_type

    @staticmethod
    def count_metrics(expected_metric_counts: Dict[Any, Any]) -> Callable:
        """Asserts that the number of metric combinations matches the expected
        counts."""

        def _count_metrics(output: Iterable[RecidivizMetric]) -> None:
            actual_combination_counts = {}

            for key in expected_metric_counts.keys():
                actual_combination_counts[key] = 0

            for metric in output:
                metric_type = metric.metric_type

                if metric_type == IncarcerationMetricType.INCARCERATION_ADMISSION:
                    actual_combination_counts["admissions"] = (
                        actual_combination_counts["admissions"] + 1
                    )
                elif metric_type == IncarcerationMetricType.INCARCERATION_RELEASE:
                    actual_combination_counts["releases"] = (
                        actual_combination_counts["releases"] + 1
                    )

            for key in expected_metric_counts:
                if expected_metric_counts[key] != actual_combination_counts[key]:
                    raise BeamAssertException(
                        "Failed assert. Count does not match expected value."
                    )

        return _count_metrics

    @staticmethod
    def validate_pipeline_test(
        expected_metric_types: Set[IncarcerationMetricType],
    ) -> Callable:
        """Asserts that the pipeline produced the expected types of metrics."""

        def _validate_pipeline_test(output: Iterable[Dict[str, Any]]) -> None:
            observed_metric_types: Set[IncarcerationMetricType] = set()

            for metric in output:
                if not isinstance(metric, IncarcerationMetric):
                    raise BeamAssertException(
                        "Failed assert. Output is not of type SupervisionMetric."
                    )

                if isinstance(metric, IncarcerationAdmissionMetric):
                    observed_metric_types.add(
                        IncarcerationMetricType.INCARCERATION_ADMISSION
                    )
                elif isinstance(metric, IncarcerationReleaseMetric):
                    observed_metric_types.add(
                        IncarcerationMetricType.INCARCERATION_RELEASE
                    )

            if observed_metric_types != expected_metric_types:
                raise BeamAssertException(
                    f"Failed assert. Expected metric types {expected_metric_types} does not equal"
                    f" observed metric types {observed_metric_types}."
                )

        return _validate_pipeline_test
