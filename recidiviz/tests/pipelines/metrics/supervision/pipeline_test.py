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
"""Tests for supervision/pipeline.py"""
import unittest
from datetime import date
from typing import Any, Callable, Collection, Dict, Iterable, Optional, Set
from unittest import mock

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException, assert_that, equal_to
from freezegun import freeze_time
from more_itertools import one

from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_person import (
    StateGender,
    StateRace,
    StateResidencyStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType as ViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationSentence,
    StatePerson,
)
from recidiviz.pipelines.metrics.base_metric_pipeline import (
    ClassifyResults,
    ProduceMetrics,
)
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.pipelines.metrics.supervision import identifier, pipeline
from recidiviz.pipelines.metrics.supervision.events import (
    SupervisionEvent,
    SupervisionPopulationEvent,
)
from recidiviz.pipelines.metrics.supervision.metrics import (
    SupervisionMetric,
    SupervisionMetricType,
)
from recidiviz.pipelines.metrics.supervision.supervision_case_compliance import (
    SupervisionCaseCompliance,
)
from recidiviz.pipelines.metrics.utils.metric_utils import (
    PersonMetadata,
    RecidivizMetric,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.assessment_normalization_manager import (
    DEFAULT_ASSESSMENT_SCORE_BUCKET,
)
from recidiviz.pipelines.normalization.utils.normalized_entities import (
    NormalizedStateAssessment,
    NormalizedStateIncarcerationPeriod,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionSentence,
)
from recidiviz.pipelines.utils.beam_utils.person_utils import (
    PERSON_EVENTS_KEY,
    PERSON_METADATA_KEY,
    ExtractPersonEventsMetadata,
)
from recidiviz.pipelines.utils.state_utils.state_specific_supervision_metrics_producer_delegate import (
    StateSpecificSupervisionMetricsProducerDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_supervision_metrics_producer_delegate import (
    UsXxSupervisionMetricsProducerDelegate,
)
from recidiviz.tests.persistence.database import database_test_utils
from recidiviz.tests.pipelines.calculator_test_utils import (
    normalized_database_base_dict,
)
from recidiviz.tests.pipelines.fake_bigquery import (
    DataTablesDict,
    FakeReadFromBigQueryFactory,
    FakeWriteMetricsToBigQuery,
    FakeWriteToBigQueryFactory,
)
from recidiviz.tests.pipelines.metrics.supervision import identifier_test
from recidiviz.tests.pipelines.metrics.supervision.identifier_test import (
    create_projected_completion_event_from_period,
    create_termination_event_from_period,
)
from recidiviz.tests.pipelines.utils.run_pipeline_test_utils import (
    default_data_dict_for_pipeline_class,
    run_test_pipeline,
)
from recidiviz.tests.pipelines.utils.state_utils.state_calculation_config_manager_test import (
    STATE_DELEGATES_FOR_TESTS,
)

SUPERVISION_PIPELINE_PACKAGE_NAME = pipeline.__name__
_STATE_CODE = "US_XX"


class TestSupervisionPipeline(unittest.TestCase):
    """Tests the entire supervision pipeline."""

    def setUp(self) -> None:
        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()
        self.fake_bq_sink_factory = FakeWriteToBigQueryFactory(
            SupervisionPipelineFakeWriteMetricsToBigQuery
        )

        self.metric_inclusions_dict: Dict[str, bool] = {
            metric_type.value: True for metric_type in SupervisionMetricType
        }
        self.state_specific_delegate_patcher = mock.patch(
            "recidiviz.pipelines.metrics.base_metric_pipeline.get_required_state_specific_delegates",
            return_value=STATE_DELEGATES_FOR_TESTS,
        )
        self.mock_get_required_state_delegates = (
            self.state_specific_delegate_patcher.start()
        )
        self.state_specific_metrics_producer_delegate_patcher = mock.patch(
            "recidiviz.pipelines.metrics.base_metric_pipeline.get_required_state_specific_metrics_producer_delegates",
            return_value={
                StateSpecificSupervisionMetricsProducerDelegate.__name__: UsXxSupervisionMetricsProducerDelegate()
            },
        )
        self.mock_get_required_state_metrics_producer_delegate = (
            self.state_specific_metrics_producer_delegate_patcher.start()
        )
        self.metric_producer_supervision_delegate_patcher = mock.patch(
            "recidiviz.pipelines.utils.state_utils"
            ".state_calculation_config_manager._get_state_specific_supervision_delegate"
        )
        self.mock_metric_producer_supervision_delegate = (
            self.metric_producer_supervision_delegate_patcher.start()
        )
        self.mock_metric_producer_supervision_delegate.return_value = (
            UsXxSupervisionDelegate([])
        )
        self.pipeline_class = pipeline.SupervisionMetricsPipeline

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        self.state_specific_delegate_patcher.stop()
        self.state_specific_metrics_producer_delegate_patcher.stop()

    def build_supervision_pipeline_data_dict(
        self, state_code: str, fake_person_id: int, fake_supervision_period_id: int
    ) -> Dict[str, Iterable[Any]]:
        """Builds a data_dict for a basic run of the pipeline."""
        fake_person = schema.StatePerson(
            state_code=state_code,
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        persons_data = [normalized_database_base_dict(fake_person)]

        fake_person_race = schema.StatePersonRace(
            state_code=state_code,
            person_id=fake_person_id,
            race=StateRace.BLACK,
            race_raw_text=StateRace.BLACK.name,
        )
        person_race_data = [normalized_database_base_dict(fake_person_race)]

        initial_incarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
            external_id="ip1",
            state_code=state_code,
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
            state_code=state_code,
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
            state_code=state_code,
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_date=date(2017, 1, 4),
            person_id=fake_person_id,
        )

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=fake_supervision_period_id,
            external_id="sp1",
            state_code=state_code,
            county_code="124",
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            start_date=date(2015, 3, 14),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            termination_date=date(2016, 12, 29),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            person_id=fake_person_id,
        )

        supervision_sentence = schema.StateSupervisionSentence(
            supervision_sentence_id=1122,
            external_id="ss1",
            state_code=state_code,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            effective_date=date(2015, 3, 1),
            projected_completion_date=date(2016, 12, 31),
            completion_date=date(2016, 12, 29),
            status=StateSentenceStatus.COMPLETED,
            person_id=fake_person_id,
        )

        incarceration_sentence = schema.StateIncarcerationSentence(
            incarceration_sentence_id=123,
            external_id="is1",
            state_code=state_code,
            person_id=fake_person_id,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        charge = database_test_utils.generate_test_charge(
            person_id=fake_person_id,
            charge_id=1234523,
        )

        assessment = schema.StateAssessment(
            assessment_id=298374,
            external_id="a1",
            state_code=state_code,
            assessment_date=date(2015, 3, 19),
            assessment_type=StateAssessmentType.LSIR,
            person_id=fake_person_id,
        )

        supervision_contact = schema.StateSupervisionContact(
            state_code=state_code,
            external_id="c1",
            contact_date=supervision_period.start_date,
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

        supervision_sentences_data = [
            normalized_database_base_dict(supervision_sentence)
        ]

        incarceration_sentences_data = [
            normalized_database_base_dict(incarceration_sentence)
        ]

        charge_data = [normalized_database_base_dict(charge)]

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

        supervision_contact_data = [normalized_database_base_dict(supervision_contact)]

        supervision_locations_to_names_data = [
            {
                "state_code": "US_XX",
                "level_1_supervision_location_external_id": "level 1",
                "level_2_supervision_location_external_id": "level 2",
            }
        ]

        state_race_ethnicity_population_count_data = [
            {
                "state_code": state_code,
                "race_or_ethnicity": "BLACK",
                "population_count": 1,
                "representation_priority": 1,
            }
        ]

        us_mo_sentence_status_data: Iterable[Dict[str, Any]] = [
            {
                "state_code": state_code,
                "person_id": fake_person_id,
                "sentence_external_id": "is-123",
                "sentence_status_external_id": "is-123-1",
                "status_code": "10I1000",
                "status_date": "20081120",
                "status_description": "New Court Comm-Institution",
            },
            {
                "state_code": state_code,
                "person_id": fake_person_id,
                "sentence_external_id": "is-123",
                "sentence_status_external_id": "is-123-2",
                "status_code": "40O1010",
                "status_date": "20101204",
                "status_description": "Parole Release",
            },
            {
                "state_code": state_code,
                "person_id": fake_person_id,
                "sentence_external_id": "is-123",
                "sentence_status_external_id": "is-123-2",
                "status_code": "45O1060",
                "status_date": "20110405",
                "status_description": "Parole Ret-Treatment Center",
            },
            {
                "state_code": state_code,
                "person_id": fake_person_id,
                "sentence_external_id": "is-123",
                "sentence_status_external_id": "is-123-3",
                "status_code": "40O1030",
                "status_date": "20140414",
                "status_description": "Parole Re-Release",
            },
            {
                "state_code": state_code,
                "person_id": fake_person_id,
                "sentence_external_id": "ss-1122",
                "sentence_status_external_id": "ss-1122-1",
                "status_code": "25I1000",
                "status_date": "20150314",
                "status_description": "Court Probation - Addl Chg",
            },
            {
                "state_code": state_code,
                "person_id": fake_person_id,
                "sentence_external_id": "ss-1122",
                "sentence_status_external_id": "ss-1122-2",
                "status_code": "45O7000",
                "status_date": "20170104",
                "status_description": "Field to DAI-Other Sentence",
            },
            {
                "state_code": state_code,
                "person_id": fake_person_id,
                "sentence_external_id": "is-123",
                "sentence_status_external_id": "is-123-2",
                "status_code": "45O1010",
                "status_date": "20170104",
                "status_description": "Parole Ret-Tech Viol",
            },
        ]

        data_dict = default_data_dict_for_pipeline_class(self.pipeline_class)
        data_dict_overrides = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StatePersonRace.__tablename__: person_race_data,
            schema.StateIncarcerationPeriod.__tablename__: incarceration_periods_data,
            schema.StateSupervisionViolationResponse.__tablename__: supervision_violation_response_data,
            schema.StateSupervisionViolation.__tablename__: supervision_violation_data,
            schema.StateSupervisionPeriod.__tablename__: supervision_periods_data,
            schema.StateSupervisionSentence.__tablename__: supervision_sentences_data,
            schema.StateIncarcerationSentence.__tablename__: incarceration_sentences_data,
            schema.StateCharge.__tablename__: charge_data,
            schema.StateAssessment.__tablename__: assessment_data,
            schema.StateSupervisionContact.__tablename__: supervision_contact_data,
            "state_race_ethnicity_population_counts": state_race_ethnicity_population_count_data,
            "us_mo_sentence_statuses": us_mo_sentence_status_data,
            "supervision_location_ids_to_names": supervision_locations_to_names_data,
        }
        data_dict.update(data_dict_overrides)

        return data_dict

    @freeze_time("2017-01-31")
    def testSupervisionPipeline(self) -> None:
        fake_person_id = 12345
        fake_supervision_period_id = 1111

        data_dict = self.build_supervision_pipeline_data_dict(
            _STATE_CODE, fake_person_id, fake_supervision_period_id
        )

        expected_metric_types = {
            SupervisionMetricType.SUPERVISION_POPULATION,
            SupervisionMetricType.SUPERVISION_SUCCESS,
            SupervisionMetricType.SUPERVISION_START,
            SupervisionMetricType.SUPERVISION_TERMINATION,
        }

        self.run_test_pipeline(_STATE_CODE, data_dict, expected_metric_types)

    def testSupervisionPipelineUsMo(self) -> None:
        self._stop_state_specific_delegate_patchers()

        fake_person_id = 12345
        fake_supervision_period_id = 1111

        data_dict = self.build_supervision_pipeline_data_dict(
            fake_person_id=fake_person_id,
            state_code="US_MO",
            fake_supervision_period_id=fake_supervision_period_id,
        )

        expected_metric_types = {
            SupervisionMetricType.SUPERVISION_POPULATION,
            SupervisionMetricType.SUPERVISION_SUCCESS,
            SupervisionMetricType.SUPERVISION_START,
            SupervisionMetricType.SUPERVISION_TERMINATION,
        }

        self.run_test_pipeline(
            state_code="US_MO",
            data_dict=data_dict,
            expected_metric_types=expected_metric_types,
        )

    @freeze_time("2017-01-31")
    def testSupervisionPipelineWithPersonIdFilterSet(self) -> None:
        fake_person_id = 12345
        fake_supervision_period_id = 1111

        data_dict = self.build_supervision_pipeline_data_dict(
            _STATE_CODE, fake_person_id, fake_supervision_period_id
        )

        expected_metric_types = {
            SupervisionMetricType.SUPERVISION_POPULATION,
            SupervisionMetricType.SUPERVISION_SUCCESS,
            SupervisionMetricType.SUPERVISION_TERMINATION,
            SupervisionMetricType.SUPERVISION_START,
        }

        self.run_test_pipeline(
            _STATE_CODE,
            data_dict,
            expected_metric_types,
            unifying_id_field_filter_set={fake_person_id},
        )

    def run_test_pipeline(
        self,
        state_code: str,
        data_dict: DataTablesDict,
        expected_metric_types: Set[SupervisionMetricType],
        expected_violation_types: Optional[Set[ViolationType]] = None,
        unifying_id_field_filter_set: Optional[Set[int]] = None,
        metric_types_filter: Optional[Set[str]] = None,
    ) -> None:
        """Runs a test version of the supervision pipeline."""
        project = "recidiviz-staging"
        dataset = "dataset"
        normalized_dataset = f"{state_code.lower()}_normalized_state"

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
                expected_violation_types=expected_violation_types,
            )
        )
        run_test_pipeline(
            pipeline_cls=self.pipeline_class,
            state_code=state_code,
            project_id=project,
            dataset_id=dataset,
            read_from_bq_constructor=read_from_bq_constructor,
            write_to_bq_constructor=write_to_bq_constructor,
            unifying_id_field_filter_set=unifying_id_field_filter_set,
            metric_types_filter=metric_types_filter,
        )

    @freeze_time("2017-01-31")
    def testSupervisionPipeline_withMetricTypesFilter(self) -> None:
        fake_person_id = 12345
        fake_svr_id = 56789
        fake_violation_id = 345789

        fake_person = schema.StatePerson(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=StateGender.FEMALE,
            birthdate=date(1990, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        persons_data = [normalized_database_base_dict(fake_person)]

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

        # This probation supervision period ended in a revocation
        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=1111,
            external_id="sp1",
            state_code="US_XX",
            county_code="124",
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            start_date=date(2015, 3, 14),
            termination_date=date(2017, 1, 4),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            person_id=fake_person_id,
        )

        supervision_sentence = schema.StateSupervisionSentence(
            supervision_sentence_id=1122,
            external_id="ss-1122",
            state_code="US_XX",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            effective_date=date(2008, 11, 20),
            projected_completion_date=date(2017, 12, 31),
            person_id=fake_person_id,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_sentence = schema.StateIncarcerationSentence(
            incarceration_sentence_id=123,
            external_id="is-123",
            state_code="US_XX",
            effective_date=date(2008, 11, 20),
            person_id=fake_person_id,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        charge = database_test_utils.generate_test_charge(
            person_id=fake_person_id,
            state_code="US_XX",
            charge_id=1234523,
        )

        ssvr = schema.StateSupervisionViolationResponse(
            supervision_violation_response_id=fake_svr_id,
            external_id="svr1",
            state_code="US_XX",
            person_id=fake_person_id,
            supervision_violation_id=fake_violation_id,
        )

        violation_report = schema.StateSupervisionViolationResponse(
            supervision_violation_response_id=99999,
            external_id="svr2",
            state_code="US_XX",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            is_draft=False,
            response_date=date(2017, 1, 1),
            person_id=fake_person_id,
        )

        supervision_violation_type = schema.StateSupervisionViolationTypeEntry(
            person_id=fake_person_id,
            state_code="US_XX",
            violation_type=StateSupervisionViolationType.FELONY,
        )

        supervision_violation = schema.StateSupervisionViolation(
            supervision_violation_id=fake_violation_id,
            external_id="sv1",
            state_code="US_XX",
            person_id=fake_person_id,
            supervision_violation_responses=[violation_report, ssvr],
            supervision_violation_types=[supervision_violation_type],
        )

        violation_report.supervision_violation_id = (
            supervision_violation.supervision_violation_id
        )

        # This incarceration period was due to a probation revocation
        revocation_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=3333,
            external_id="ip3",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_date=date(2017, 1, 4),
            person_id=fake_person_id,
        )

        assessment = schema.StateAssessment(
            assessment_id=298374,
            external_id="a1",
            state_code="US_XX",
            assessment_date=date(2015, 3, 19),
            assessment_type=StateAssessmentType.LSIR,
            person_id=fake_person_id,
        )

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration, {"sequence_num": 0}),
            normalized_database_base_dict(first_reincarceration, {"sequence_num": 1}),
            normalized_database_base_dict(
                revocation_reincarceration, {"sequence_num": 2}
            ),
        ]

        supervision_periods_data = [
            normalized_database_base_dict(supervision_period, {"sequence_num": 0})
        ]

        supervision_violation_type_data = [
            normalized_database_base_dict(supervision_violation_type)
        ]

        supervision_violation_response_data = [
            normalized_database_base_dict(ssvr, {"sequence_num": 0}),
            normalized_database_base_dict(violation_report, {"sequence_num": 1}),
        ]

        supervision_violation_data = [
            normalized_database_base_dict(supervision_violation)
        ]

        supervision_sentences_data = [
            normalized_database_base_dict(supervision_sentence)
        ]

        incarceration_sentences_data = [
            normalized_database_base_dict(incarceration_sentence)
        ]

        charge_data = [normalized_database_base_dict(charge)]

        assessment_data = [
            normalized_database_base_dict(
                assessment,
                {"assessment_score_bucket": "NOT_ASSESSED", "sequence_num": 0},
            )
        ]

        supervision_locations_to_names_data = [
            {
                "state_code": "US_XX",
                "level_1_supervision_location_external_id": "level 1",
                "level_2_supervision_location_external_id": "level 2",
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

        data_dict = default_data_dict_for_pipeline_class(self.pipeline_class)
        data_dict_overrides = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StateIncarcerationPeriod.__tablename__: incarceration_periods_data,
            schema.StateSupervisionViolationResponse.__tablename__: supervision_violation_response_data,
            schema.StateSupervisionViolation.__tablename__: supervision_violation_data,
            schema.StateSupervisionViolationTypeEntry.__tablename__: supervision_violation_type_data,
            schema.StateSupervisionPeriod.__tablename__: supervision_periods_data,
            schema.StateSupervisionSentence.__tablename__: supervision_sentences_data,
            schema.StateIncarcerationSentence.__tablename__: incarceration_sentences_data,
            schema.StateCharge.__tablename__: charge_data,
            schema.StateAssessment.__tablename__: assessment_data,
            "state_race_ethnicity_population_counts": state_race_ethnicity_population_count_data,
            "supervision_location_ids_to_names": supervision_locations_to_names_data,
        }
        data_dict.update(data_dict_overrides)

        expected_metric_types = {
            SupervisionMetricType.SUPERVISION_POPULATION,
            SupervisionMetricType.SUPERVISION_TERMINATION,
            SupervisionMetricType.SUPERVISION_START,
        }

        metric_types_filter = {metric.value for metric in expected_metric_types}

        self.run_test_pipeline(
            _STATE_CODE,
            data_dict,
            expected_metric_types,
            metric_types_filter=metric_types_filter,
        )

    @freeze_time("2019-11-26")
    def testSupervisionPipelineNoSupervision(self) -> None:
        """Tests the supervision pipeline when a person doesn't have any supervision periods."""
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
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="ip1",
            state_code="US_XX",
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_date=date(2008, 11, 20),
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id_1,
        )

        supervision_period__1 = schema.StateSupervisionPeriod(
            supervision_period_id=1111,
            state_code="US_XX",
            external_id="sp1",
            county_code="124",
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            start_date=date(2016, 3, 14),
            termination_date=date(2016, 12, 29),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            person_id=fake_person_id_1,
        )

        supervision_sentence = schema.StateSupervisionSentence(
            supervision_sentence_id=1122,
            state_code="US_XX",
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            effective_date=date(2016, 3, 1),
            projected_completion_date=date(2017, 12, 31),
            completion_date=date(2016, 12, 29),
            person_id=fake_person_id_1,
        )

        incarceration_sentence = schema.StateIncarcerationSentence(
            incarceration_sentence_id=123,
            external_id="is1",
            state_code="US_XX",
            person_id=fake_person_id_1,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        charge = database_test_utils.generate_test_charge(
            person_id=fake_person_id_1,
            charge_id=1234523,
        )

        supervision_violation_response = (
            database_test_utils.generate_test_supervision_violation_response(
                fake_person_id_2
            )
        )

        supervision_violation = database_test_utils.generate_test_supervision_violation(
            fake_person_id_2, [supervision_violation_response]
        )

        supervision_violation_response.supervision_violation_id = (
            supervision_violation.supervision_violation_id
        )

        supervision_violation_response_data = [
            normalized_database_base_dict(
                supervision_violation_response, {"sequence_num": 0}
            )
        ]

        supervision_violation_data = [
            normalized_database_base_dict(supervision_violation)
        ]

        supervision_sentences_data = [
            normalized_database_base_dict(supervision_sentence)
        ]

        incarceration_sentences_data = [
            normalized_database_base_dict(incarceration_sentence)
        ]

        charge_data = [normalized_database_base_dict(charge)]

        first_reincarceration_2 = schema.StateIncarcerationPeriod(
            incarceration_period_id=5555,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            external_id="ip5",
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
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            external_id="ip6",
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_date=date(2018, 3, 9),
            person_id=fake_person_id_2,
        )

        assessment = schema.StateAssessment(
            assessment_id=298374,
            external_id="a1",
            state_code="US_XX",
            assessment_date=date(2015, 3, 19),
            assessment_type=StateAssessmentType.LSIR,
            person_id=fake_person_id_1,
        )

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration_1, {"sequence_num": 0}),
            normalized_database_base_dict(first_reincarceration_2, {"sequence_num": 1}),
            normalized_database_base_dict(
                subsequent_reincarceration_2, {"sequence_num": 2}
            ),
        ]

        supervision_periods_data = [
            normalized_database_base_dict(supervision_period__1, {"sequence_num": 0})
        ]

        assessment_data = [
            normalized_database_base_dict(
                assessment,
                {"assessment_score_bucket": "NOT_ASSESSED", "sequence_num": 0},
            )
        ]

        supervision_locations_to_names_data = [
            {
                "state_code": "US_XX",
                "level_1_supervision_location_external_id": "level 1",
                "level_2_supervision_location_external_id": "level 2",
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

        data_dict = default_data_dict_for_pipeline_class(self.pipeline_class)
        data_dict_overrides = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StateIncarcerationPeriod.__tablename__: incarceration_periods_data,
            schema.StateSupervisionViolationResponse.__tablename__: supervision_violation_response_data,
            schema.StateSupervisionViolation.__tablename__: supervision_violation_data,
            schema.StateSupervisionPeriod.__tablename__: supervision_periods_data,
            schema.StateSupervisionSentence.__tablename__: supervision_sentences_data,
            schema.StateIncarcerationSentence.__tablename__: incarceration_sentences_data,
            schema.StateCharge.__tablename__: charge_data,
            schema.StateAssessment.__tablename__: assessment_data,
            "state_race_ethnicity_population_counts": state_race_ethnicity_population_count_data,
            "supervision_location_ids_to_names": supervision_locations_to_names_data,
        }
        data_dict.update(data_dict_overrides)

        expected_metric_types = {
            SupervisionMetricType.SUPERVISION_POPULATION,
            SupervisionMetricType.SUPERVISION_SUCCESS,
            SupervisionMetricType.SUPERVISION_TERMINATION,
            SupervisionMetricType.SUPERVISION_START,
        }

        self.run_test_pipeline(_STATE_CODE, data_dict, expected_metric_types)


class TestClassifyEvents(unittest.TestCase):
    """Tests the ClassifyEvents DoFn in the pipeline."""

    def setUp(self) -> None:
        self.state_code = "US_XX"
        self.state_specific_delegate_patcher = mock.patch(
            "recidiviz.pipelines.metrics.base_metric_pipeline.get_required_state_specific_delegates",
            return_value=STATE_DELEGATES_FOR_TESTS,
        )
        self.mock_get_required_state_delegates = (
            self.state_specific_delegate_patcher.start()
        )
        self.identifier = identifier.SupervisionIdentifier()
        self.pipeline_class = pipeline.SupervisionMetricsPipeline

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        self.state_specific_delegate_patcher.stop()

    @staticmethod
    def load_person_entities_dict(
        person: StatePerson,
        supervision_periods: Optional[Iterable[entities.StateSupervisionPeriod]] = None,
        incarceration_periods: Optional[
            Iterable[entities.StateIncarcerationPeriod]
        ] = None,
        incarceration_sentences: Optional[
            Iterable[entities.StateIncarcerationSentence]
        ] = None,
        supervision_sentences: Optional[
            Iterable[entities.StateSupervisionSentence]
        ] = None,
        violation_responses: Optional[
            Iterable[entities.StateSupervisionViolationResponse]
        ] = None,
        assessments: Optional[Iterable[entities.StateAssessment]] = None,
        supervision_contacts: Optional[
            Iterable[entities.StateSupervisionContact]
        ] = None,
        supervision_location_to_names_association: Optional[
            Iterable[Dict[Any, Any]]
        ] = None,
    ) -> Dict[str, Iterable[Any]]:
        return {
            entities.StatePerson.__name__: [person],
            entities.StateSupervisionPeriod.__name__: supervision_periods
            if supervision_periods
            else [],
            entities.StateAssessment.__name__: assessments if assessments else [],
            entities.StateIncarcerationPeriod.__name__: incarceration_periods
            if incarceration_periods
            else [],
            entities.StateIncarcerationSentence.__name__: incarceration_sentences
            if incarceration_sentences
            else [],
            entities.StateSupervisionSentence.__name__: supervision_sentences
            if supervision_sentences
            else [],
            entities.StateSupervisionViolationResponse.__name__: violation_responses
            if violation_responses
            else [],
            entities.StateSupervisionContact.__name__: supervision_contacts
            if supervision_contacts
            else [],
            "supervision_location_ids_to_names": supervision_location_to_names_association
            or [],
        }

    def testClassifyEvents(self) -> None:
        """Tests the ClassifyEvents DoFn."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        incarceration_period = NormalizedStateIncarcerationPeriod.new_with_defaults(
            external_id="ip1",
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            sequence_num=0,
        )

        supervision_period_termination_date = date(2015, 5, 29)
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            external_id="sp1",
            state_code="US_XX",
            county_code="124",
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            start_date=date(2015, 3, 14),
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MEDM",
            supervision_site="10",
            person=fake_person,
            sequence_num=0,
        )

        effective_date = date(2008, 1, 1)
        completion_date = date(2015, 5, 29)
        supervision_sentence = NormalizedStateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            effective_date=effective_date,
            projected_completion_date=date(2015, 5, 30),
            completion_date=completion_date,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=123,
            external_id="is1",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = NormalizedStateAssessment.new_with_defaults(
            state_code="US_XX",
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2015, 3, 10),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        person_entities = self.load_person_entities_dict(
            person=fake_person,
            supervision_periods=[supervision_period],
            assessments=[assessment],
            incarceration_periods=[incarceration_period],
            incarceration_sentences=[incarceration_sentence],
            supervision_sentences=[supervision_sentence],
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        expected_events: Iterable[SupervisionEvent] = [
            create_projected_completion_event_from_period(
                supervision_period,
                event_date=date(2015, 5, 31),
                supervision_type=supervision_type,
                supervising_district_external_id="10",
                level_1_supervision_location_external_id="10",
                successful_completion=True,
                incarcerated_during_sentence=True,
                sentence_days_served=(completion_date - effective_date).days,
            ),
            # We have to add these expected events in this order because there is no unsorted-list equality check in the
            # Apache Beam testing utils
            *identifier_test.expected_population_events(
                supervision_period,
                supervision_type,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                level_1_supervision_location_external_id="10",
                projected_supervision_completion_date=supervision_sentence.projected_completion_date,
            ),
            create_termination_event_from_period(
                supervision_period,
                supervision_type=supervision_type,
                supervising_district_external_id="10",
                level_1_supervision_location_external_id="10",
                in_supervision_population_on_date=True,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            identifier_test.create_start_event_from_period(
                supervision_period,
                supervising_district_external_id="10",
            ),
        ]

        correct_output = [(fake_person.person_id, (fake_person, expected_events))]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_entities)])
            | "Identify Supervision Events"
            >> beam.ParDo(
                ClassifyResults(),
                state_code=self.state_code,
                identifier=self.identifier,
                state_specific_required_delegates=self.pipeline_class.state_specific_required_delegates(),
            )
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()


class TestProduceSupervisionMetrics(unittest.TestCase):
    """Tests the ProduceSupervisionMetrics DoFn in the pipeline."""

    def setUp(self) -> None:
        self.fake_person_id = 12345

        self.metric_inclusions_dict: Dict[SupervisionMetricType, bool] = {
            metric_type: True for metric_type in SupervisionMetricType
        }

        self.supervision_delegate_patcher = mock.patch(
            "recidiviz.pipelines.utils.state_utils"
            ".state_calculation_config_manager._get_state_specific_supervision_delegate"
        )
        self.mock_supervision_delegate = self.supervision_delegate_patcher.start()
        self.mock_supervision_delegate.return_value = UsXxSupervisionDelegate([])

        self.person_metadata = PersonMetadata(prioritized_race_or_ethnicity="BLACK")

        self.job_id_patcher = mock.patch(
            "recidiviz.pipelines.metrics.base_metric_pipeline.job_id"
        )
        self.mock_job_id = self.job_id_patcher.start()
        self.mock_job_id.return_value = "job_id"
        self.state_specific_metrics_producer_delegate_patcher = mock.patch(
            "recidiviz.pipelines.metrics.base_metric_pipeline.get_required_state_specific_metrics_producer_delegates",
        )
        self.mock_state_specific_metrics_producer_delegate = (
            self.state_specific_metrics_producer_delegate_patcher.start()
        )
        self.mock_state_specific_metrics_producer_delegate.return_value = {
            StateSpecificSupervisionMetricsProducerDelegate.__name__: UsXxSupervisionMetricsProducerDelegate()
        }

        self.metric_producer = pipeline.metric_producer.SupervisionMetricProducer()

        self.pipeline_parameters = MetricsPipelineParameters(
            project="recidiviz-456",
            state_code="US_XX",
            pipeline="supervision_metrics",
            state_data_input="dataset_id",
            normalized_input="dataset_id",
            reference_view_input="dataset_id",
            static_reference_input="dataset_id",
            output="dataset_id",
            metric_types="ALL",
            region="region",
            job_name="job",
            person_filter_ids=None,
            calculation_month_count=-1,
        )

    def tearDown(self) -> None:
        self.supervision_delegate_patcher.stop()
        self.job_id_patcher.stop()
        self.state_specific_metrics_producer_delegate_patcher.stop()

    def testProduceSupervisionMetrics(self) -> None:
        """Tests the ProduceSupervisionMetrics DoFn."""
        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=self.fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        supervision_time_events = [
            SupervisionPopulationEvent(
                state_code="US_XX",
                year=2015,
                month=3,
                event_date=date(2015, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MINIMUM,
                supervision_level_raw_text="MIN",
                case_compliance=SupervisionCaseCompliance(
                    date_of_evaluation=date(2015, 3, 31),
                    next_recommended_assessment_date=None,
                ),
                projected_end_date=None,
            ),
        ]

        expected_population_metric_count = len(supervision_time_events)
        expected_compliance_metric_count = len(supervision_time_events)

        expected_metric_counts = {
            SupervisionMetricType.SUPERVISION_POPULATION.value: expected_population_metric_count,
            SupervisionMetricType.SUPERVISION_COMPLIANCE.value: expected_compliance_metric_count,
        }

        test_pipeline = TestPipeline()

        inputs = [
            (
                self.fake_person_id,
                {
                    PERSON_EVENTS_KEY: [(fake_person, supervision_time_events)],
                    PERSON_METADATA_KEY: [self.person_metadata],
                },
            )
        ]

        output = (
            test_pipeline
            | beam.Create(inputs)
            | beam.ParDo(ExtractPersonEventsMetadata())
            | "Calculate Supervision Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_parameters.project,
                self.pipeline_parameters.region,
                self.pipeline_parameters.job_name,
                self.pipeline_parameters.state_code,
                self.pipeline_parameters.metric_types,
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

    def testProduceSupervisionMetrics_NoSupervision(self) -> None:
        """Tests the ProduceSupervisionMetrics when there are
        no supervision months. This should never happen because any person
        without supervision time is dropped entirely from the pipeline."""
        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=self.fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        inputs = [
            (
                self.fake_person_id,
                {
                    PERSON_EVENTS_KEY: [(fake_person, [])],
                    PERSON_METADATA_KEY: [self.person_metadata],
                },
            )
        ]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create(inputs)
            | beam.ParDo(ExtractPersonEventsMetadata())
            | "Calculate Supervision Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_parameters.project,
                self.pipeline_parameters.region,
                self.pipeline_parameters.job_name,
                self.pipeline_parameters.state_code,
                self.pipeline_parameters.metric_types,
                self.pipeline_parameters.calculation_month_count,
                self.metric_producer,
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testProduceSupervisionMetrics_NoInput(self) -> None:
        """Tests the ProduceSupervisionMetrics when there is
        no input to the function."""

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([])
            | beam.ParDo(ExtractPersonEventsMetadata())
            | "Calculate Supervision Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_parameters.project,
                self.pipeline_parameters.region,
                self.pipeline_parameters.job_name,
                self.pipeline_parameters.state_code,
                self.pipeline_parameters.metric_types,
                self.pipeline_parameters.calculation_month_count,
                self.metric_producer,
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class SupervisionPipelineFakeWriteMetricsToBigQuery(FakeWriteMetricsToBigQuery):
    def __init__(
        self,
        output_table: str,
        expected_output_tags: Collection[str],
        expected_violation_types: Optional[Set[ViolationType]] = None,
    ):
        super().__init__(output_table, expected_output_tags)
        self._expected_violation_types = expected_violation_types
        self._table = output_table

    def expand(self, input_or_inputs: Iterable[Any]) -> Iterable[RecidivizMetric]:
        ret = super().expand(input_or_inputs)

        if self._expected_violation_types:
            for expected_violation_type in self._expected_violation_types:
                assert_that(
                    input_or_inputs,
                    AssertMatchers.assert_source_violation_type_set(
                        expected_violation_type
                    ),
                    f"Assert source violation {expected_violation_type} type is set",
                )
        return ret


class AssertMatchers:
    """Functions to be used by Apache Beam testing `assert_that` functions to
    validate pipeline outputs."""

    @staticmethod
    def assert_source_violation_type_set(expected_violation: ViolationType) -> Callable:
        """Asserts that there are some population metrics with the
        source_violation_type set."""

        def _assert_source_violation_type_set(output: Iterable[Dict[Any, Any]]) -> None:
            if not output:
                return

            metric_type = one({metric["metric_type"] for metric in output})
            if metric_type != SupervisionMetricType.SUPERVISION_POPULATION.value:
                return

            with_violation_types = [
                metric
                for metric in output
                if metric["most_severe_violation_type"] == expected_violation.value
            ]

            if len(with_violation_types) == 0:
                raise BeamAssertException(
                    f"No metrics with source violation type {expected_violation} set."
                )

        return _assert_source_violation_type_set

    @staticmethod
    def count_metrics(expected_metric_counts: Dict[Any, Any]) -> Callable:
        """Asserts that the number of metric combinations matches the expected
        counts."""

        def _count_metrics(output: Iterable[RecidivizMetric]) -> None:
            actual_combination_counts = {}

            for key in expected_metric_counts.keys():
                actual_combination_counts[key] = 0

            for metric in output:
                if not isinstance(metric, SupervisionMetric):
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
