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
from typing import Any, Callable, Dict, List, Optional, Set
from unittest import mock

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException, assert_that, equal_to
from freezegun import freeze_time

from recidiviz.calculator.pipeline.metrics.base_metric_pipeline import (
    ClassifyResults,
    MetricPipelineJobArgs,
    ProduceMetrics,
)
from recidiviz.calculator.pipeline.metrics.incarceration import identifier, pipeline
from recidiviz.calculator.pipeline.metrics.incarceration.events import (
    IncarcerationCommitmentFromSupervisionAdmissionEvent,
    IncarcerationReleaseEvent,
    IncarcerationStandardAdmissionEvent,
    IncarcerationStayEvent,
)
from recidiviz.calculator.pipeline.metrics.incarceration.metrics import (
    IncarcerationAdmissionMetric,
    IncarcerationMetric,
    IncarcerationMetricType,
    IncarcerationPopulationMetric,
    IncarcerationReleaseMetric,
)
from recidiviz.calculator.pipeline.metrics.utils.metric_utils import PersonMetadata
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
    NormalizedStateSupervisionPeriod,
)
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
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_metrics_producer_delegate import (
    StateSpecificIncarcerationMetricsProducerDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_metrics_producer_delegate import (
    UsXxIncarcerationMetricsProducerDelegate,
)
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
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StatePerson,
    StateSupervisionPeriod,
)
from recidiviz.tests.calculator.calculator_test_utils import (
    normalized_database_base_dict,
    normalized_database_base_dict_list,
)
from recidiviz.tests.calculator.pipeline.fake_bigquery import (
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

_COUNTY_OF_RESIDENCE = "county_of_residence"
_STATE_CODE = "US_XX"

ALL_METRICS_INCLUSIONS_DICT = {
    IncarcerationMetricType.INCARCERATION_ADMISSION: True,
    IncarcerationMetricType.INCARCERATION_COMMITMENT_FROM_SUPERVISION: True,
    IncarcerationMetricType.INCARCERATION_POPULATION: True,
    IncarcerationMetricType.INCARCERATION_RELEASE: True,
}

ALL_METRIC_TYPES_SET = {
    IncarcerationMetricType.INCARCERATION_ADMISSION,
    IncarcerationMetricType.INCARCERATION_COMMITMENT_FROM_SUPERVISION,
    IncarcerationMetricType.INCARCERATION_POPULATION,
    IncarcerationMetricType.INCARCERATION_RELEASE,
}

INCARCERATION_PIPELINE_PACKAGE_NAME = pipeline.__name__


class TestIncarcerationPipeline(unittest.TestCase):
    """Tests the entire incarceration pipeline."""

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
                StateSpecificIncarcerationMetricsProducerDelegate.__name__: UsXxIncarcerationMetricsProducerDelegate()
            },
        )
        self.mock_get_required_state_metrics_producer_delegate = (
            self.state_specific_metrics_producer_delegate_patcher.start()
        )
        self.run_delegate_class = pipeline.IncarcerationMetricsPipelineRunDelegate

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        self.state_specific_delegate_patcher.stop()
        self.state_specific_metrics_producer_delegate_patcher.stop()

    def build_incarceration_pipeline_data_dict(
        self, fake_person_id: int, state_code: str = "US_XX"
    ) -> Dict[str, List]:
        """Builds a data_dict for a basic run of the pipeline."""
        fake_person = schema.StatePerson(
            state_code=state_code,
            person_id=fake_person_id,
            gender=StateGender.MALE,
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

        ethnicity = schema.StatePersonEthnicity(
            person_ethnicity_id=111,
            state_code=state_code,
            ethnicity=StateEthnicity.HISPANIC,
            person_id=fake_person_id,
        )

        ethnicity_data = normalized_database_base_dict_list([ethnicity])

        initial_incarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
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
            state_code=state_code,
            county_code="124",
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            start_date=date(2010, 12, 4),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            termination_date=date(2011, 4, 5),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            person_id=fake_person_id,
        )

        first_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=2222,
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
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            admission_date=date(2017, 1, 4),
            person_id=fake_person_id,
        )

        assessment = schema.StateAssessment(
            assessment_id=298374,
            state_code=state_code,
            assessment_date=date(2015, 3, 19),
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

        assessment_data = [normalized_database_base_dict(assessment)]

        fake_person_id_to_county_query_result = [
            {
                "state_code": state_code,
                "person_id": fake_person_id,
                "county_of_residence": _COUNTY_OF_RESIDENCE,
            }
        ]

        incarceration_period_judicial_district_association_data = [
            {
                "state_code": state_code,
                "person_id": fake_person_id,
                "incarceration_period_id": 123,
                "judicial_district_code": "NW",
            }
        ]

        supervision_period_to_agent_data = [
            {
                "state_code": state_code,
                "agent_id": 1010,
                "person_id": fake_person_id,
                "agent_external_id": "OFFICER0009",
                "supervision_period_id": supervision_period.supervision_period_id,
                "agent_start_date": supervision_period.start_date,
                "agent_end_date": supervision_period.termination_date,
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

        data_dict = default_data_dict_for_run_delegate(self.run_delegate_class)
        data_dict_overrides = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StatePersonRace.__tablename__: races_data,
            schema.StatePersonEthnicity.__tablename__: ethnicity_data,
            schema.StateIncarcerationPeriod.__tablename__: incarceration_periods_data,
            schema.StateSupervisionViolationResponse.__tablename__: supervision_violation_response_data,
            schema.StateSupervisionViolation.__tablename__: supervision_violation_data,
            schema.StateSupervisionPeriod.__tablename__: supervision_periods_data,
            schema.StateAssessment.__tablename__: assessment_data,
            "persons_to_recent_county_of_residence": fake_person_id_to_county_query_result,
            "incarceration_period_judicial_district_association": incarceration_period_judicial_district_association_data,
            "state_race_ethnicity_population_counts": state_race_ethnicity_population_count_data,
            "supervision_period_to_agent_association": supervision_period_to_agent_data,
        }
        data_dict.update(data_dict_overrides)
        return data_dict

    @freeze_time("2015-01-31")
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

    @freeze_time("2015-01-31")
    def testIncarcerationPipelineFilterMetrics(self) -> None:
        fake_person_id = 12345
        data_dict = self.build_incarceration_pipeline_data_dict(
            fake_person_id=fake_person_id
        )

        expected_metric_types = {IncarcerationMetricType.INCARCERATION_ADMISSION}
        metric_types_filter = {IncarcerationMetricType.INCARCERATION_ADMISSION.value}

        self.run_test_pipeline(
            state_code=_STATE_CODE,
            data_dict=data_dict,
            expected_metric_types=expected_metric_types,
            metric_types_filter=metric_types_filter,
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
            unifying_id_field_filter_set={fake_person_id},
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
        data_dict: Dict[str, List[Dict]],
        expected_metric_types: Set[IncarcerationMetricType],
        unifying_id_field_filter_set: Optional[Set[int]] = None,
        metric_types_filter: Optional[Set[str]] = None,
    ) -> None:
        """Runs a test version of the supervision pipeline."""
        project = "project"
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
            )
        )

        run_test_pipeline(
            run_delegate=self.run_delegate_class,
            state_code=state_code,
            project_id=project,
            dataset_id=dataset,
            read_from_bq_constructor=read_from_bq_constructor,
            write_to_bq_constructor=write_to_bq_constructor,
            unifying_id_field_filter_set=unifying_id_field_filter_set,
            metric_types_filter=metric_types_filter,
        )

    def build_incarceration_pipeline_data_dict_no_incarceration(
        self, fake_person_id: int
    ) -> Dict[str, List]:
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
            normalized_database_base_dict(fake_person_1),
            normalized_database_base_dict(fake_person_2),
        ]

        fake_person_id_to_county_query_result = [
            {
                "state_code": "US_XX",
                "person_id": fake_person_id,
                "county_of_residence": _COUNTY_OF_RESIDENCE,
            }
        ]

        incarceration_period_judicial_district_association_data = [
            {
                "state_code": "US_XX",
                "person_id": fake_person_id,
                "incarceration_period_id": None,
                "judicial_district_code": "NW",
            }
        ]

        supervision_period_to_agent_data = [
            {
                "state_code": "US_XX",
                "agent_id": 1010,
                "person_id": fake_person_id,
                "agent_external_id": "OFFICER0009",
                "supervision_period_id": None,
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
        data_dict_overrides = {
            schema.StatePerson.__tablename__: persons_data,
            "persons_to_recent_county_of_residence": fake_person_id_to_county_query_result,
            "incarceration_period_judicial_district_association": incarceration_period_judicial_district_association_data,
            "state_race_ethnicity_population_counts": state_race_ethnicity_population_count_data,
            "supervision_period_to_agent_association": supervision_period_to_agent_data,
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
        self.state_specific_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.metrics.base_metric_pipeline.get_required_state_specific_delegates",
            return_value=STATE_DELEGATES_FOR_TESTS,
        )
        self.mock_get_required_state_delegates = (
            self.state_specific_delegate_patcher.start()
        )
        self.identifier = identifier.IncarcerationIdentifier()
        self.run_delegate_class = pipeline.IncarcerationMetricsPipelineRunDelegate

    def tearDown(self) -> None:
        self.state_specific_delegate_patcher.stop()

    @staticmethod
    def load_person_entities_dict(
        person: StatePerson,
        incarceration_periods: List[StateIncarcerationPeriod] = None,
        supervision_periods: List[StateSupervisionPeriod] = None,
        violation_responses: List[entities.StateSupervisionViolationResponse] = None,
        assessments: List[entities.StateAssessment] = None,
        ip_to_judicial_district_kv: List[Dict[Any, Any]] = None,
        supervision_period_to_agent_associations_as_kv: List[Dict[Any, Any]] = None,
        person_id_to_county_kv: List[Dict[Any, Any]] = None,
    ) -> Dict[str, List]:
        return {
            StatePerson.__name__: [person],
            entities.StateAssessment.__name__: assessments or [],
            entities.StateSupervisionPeriod.__name__: supervision_periods
            if supervision_periods
            else [],
            entities.StateIncarcerationPeriod.__name__: incarceration_periods
            if incarceration_periods
            else [],
            entities.StateSupervisionViolationResponse.__name__: violation_responses
            if violation_responses
            else [],
            "incarceration_period_judicial_district_association": ip_to_judicial_district_kv
            or [],
            "supervision_period_to_agent_association": supervision_period_to_agent_associations_as_kv
            or [],
            "persons_to_recent_county_of_residence": person_id_to_county_kv or [],
        }

    def testClassifyIncarcerationEvents(self) -> None:
        """Tests the ClassifyIncarcerationEvents DoFn."""
        fake_person_id = 12345
        state_code = "US_XX"

        fake_person = StatePerson.new_with_defaults(
            state_code=state_code,
            person_id=fake_person_id,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            state_code=state_code,
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

        incarceration_period = NormalizedStateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON XX",
            admission_date=date(2010, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2010, 11, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            sequence_num=0,
        )

        fake_person_id_to_county_query_result = {
            "person_id": fake_person_id,
            "county_of_residence": _COUNTY_OF_RESIDENCE,
        }

        fake_incarceration_period_judicial_district_association_result = {
            "person_id": fake_person_id,
            "incarceration_period_id": 123,
            "judicial_district_code": "NW",
        }

        supervision_period_to_agent_map = {
            "agent_id": 1010,
            "person_id": fake_person_id,
            "agent_external_id": "OFFICER0009",
            "supervision_period_id": supervision_period.supervision_period_id,
        }

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        assert incarceration_period.release_date is not None
        assert incarceration_period.release_reason is not None
        incarceration_events = [
            IncarcerationCommitmentFromSupervisionAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=incarceration_period.admission_reason,
                admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_district_external_id="10",
                level_1_supervision_location_external_id="10",
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                supervising_officer_external_id="OFFICER0009",
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.release_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=incarceration_period.release_reason,
                admission_reason=incarceration_period.admission_reason,
                commitment_from_supervision_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                total_days_incarcerated=(
                    incarceration_period.release_date
                    - incarceration_period.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            IncarcerationStayEvent(
                admission_reason=incarceration_period.admission_reason,
                admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                commitment_from_supervision_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            ),
        ]

        correct_output = [(fake_person_id, (fake_person, incarceration_events))]

        test_pipeline = TestPipeline()

        person_entities = self.load_person_entities_dict(
            person=fake_person,
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period],
            ip_to_judicial_district_kv=[
                fake_incarceration_period_judicial_district_association_result
            ],
            supervision_period_to_agent_associations_as_kv=[
                supervision_period_to_agent_map
            ],
            person_id_to_county_kv=[fake_person_id_to_county_query_result],
        )

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_entities)])
            | "Identify Incarceration Events"
            >> beam.ParDo(
                ClassifyResults(),
                state_code=state_code,
                identifier=self.identifier,
                pipeline_config=self.run_delegate_class.pipeline_config(),
            )
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyIncarcerationEvents_NoSentenceGroups(self) -> None:
        """Tests the ClassifyIncarcerationEvents DoFn when the person has no sentence groups."""
        state_code = "US_XX"

        fake_person = StatePerson.new_with_defaults(
            state_code=state_code,
            person_id=123,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        person_entities = self.load_person_entities_dict(
            person=fake_person,
        )

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person.person_id, person_entities)])
            | "Identify Incarceration Events"
            >> beam.ParDo(
                ClassifyResults(),
                state_code=state_code,
                identifier=self.identifier,
                pipeline_config=self.run_delegate_class.pipeline_config(),
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class TestProduceIncarcerationMetrics(unittest.TestCase):
    """Tests the ProduceMetrics DoFn in the pipeline."""

    def setUp(self) -> None:
        self.fake_person_id = 12345

        self.person_metadata = PersonMetadata(prioritized_race_or_ethnicity="BLACK")

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

        self.metric_producer = pipeline.metric_producer.IncarcerationMetricProducer()

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
            metric_inclusions=ALL_METRICS_INCLUSIONS_DICT,
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

    def testProduceIncarcerationMetrics(self) -> None:
        """Tests the ProduceIncarcerationMetrics DoFn."""
        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=123,
            gender=StateGender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=StateResidencyStatus.PERMANENT,
        )

        incarceration_events = [
            IncarcerationStandardAdmissionEvent(
                state_code="US_XX",
                event_date=date(2001, 3, 16),
                facility="SAN QUENTIN",
                county_of_residence="county_of_residence",
                admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            ),
            IncarcerationReleaseEvent(
                state_code="US_XX",
                event_date=date(2002, 5, 26),
                facility="SAN QUENTIN",
                county_of_residence="county_of_residence",
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            ),
        ]

        expected_metric_count = 1

        expected_metric_counts = {
            "admissions": expected_metric_count,
            "releases": expected_metric_count,
        }

        test_pipeline = TestPipeline()

        inputs = [
            (
                self.fake_person_id,
                {
                    PERSON_EVENTS_KEY: [(fake_person, incarceration_events)],
                    PERSON_METADATA_KEY: [self.person_metadata],
                },
            )
        ]

        output = (
            test_pipeline
            | beam.Create(inputs)
            | beam.ParDo(ExtractPersonEventsMetadata())
            | "Produce Incarceration Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_job_args,
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
                    "person_incarceration_events": [(fake_person, [])],
                    PERSON_METADATA_KEY: [self.person_metadata],
                },
            )
        ]

        output = (
            test_pipeline
            | beam.Create(inputs)
            | beam.ParDo(ExtractPersonEventsMetadata())
            | "Produce Incarceration Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_job_args,
                self.metric_producer,
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testProduceIncarcerationMetrics_NoInput(self) -> None:
        """Tests the ProduceIncarcerationMetrics when there is
        no input to the function."""

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([])
            | "Produce Incarceration Metrics"
            >> beam.ParDo(
                ProduceMetrics(),
                self.pipeline_job_args,
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

        def _count_metrics(output: List[Any]) -> None:
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

        def _validate_pipeline_test(output: List[Any]) -> None:
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
                elif isinstance(metric, IncarcerationPopulationMetric):
                    observed_metric_types.add(
                        IncarcerationMetricType.INCARCERATION_POPULATION
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
