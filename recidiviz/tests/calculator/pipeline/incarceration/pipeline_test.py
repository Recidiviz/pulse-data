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
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException, assert_that, equal_to
from freezegun import freeze_time

from recidiviz.calculator.pipeline.base_pipeline import ClassifyEvents, ProduceMetrics
from recidiviz.calculator.pipeline.incarceration import identifier, pipeline
from recidiviz.calculator.pipeline.incarceration.events import (
    IncarcerationCommitmentFromSupervisionAdmissionEvent,
    IncarcerationReleaseEvent,
    IncarcerationStandardAdmissionEvent,
    IncarcerationStayEvent,
)
from recidiviz.calculator.pipeline.incarceration.metrics import (
    IncarcerationAdmissionMetric,
    IncarcerationMetric,
    IncarcerationMetricType,
    IncarcerationPopulationMetric,
    IncarcerationReleaseMetric,
)
from recidiviz.calculator.pipeline.utils.assessment_utils import (
    DEFAULT_ASSESSMENT_SCORE_BUCKET,
)
from recidiviz.calculator.pipeline.utils.beam_utils.person_utils import (
    PERSON_EVENTS_KEY,
    PERSON_METADATA_KEY,
    ExtractPersonEventsMetadata,
)
from recidiviz.calculator.pipeline.utils.metric_utils import PersonMetadata
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_commitment_from_supervision_utils import (
    UsXxCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_delegate import (
    UsXxIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_incarceration_period_pre_processing_delegate import (
    UsXxIncarcerationPreProcessingDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_supervision_period_pre_processing_delegate import (
    UsXxSupervisionPreProcessingDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_violation_response_preprocessing_delegate import (
    UsXxViolationResponsePreprocessingDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_violations_delegate import (
    UsXxViolationDelegate,
)
from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationFacilitySecurityLevel,
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import (
    Ethnicity,
    Gender,
    Race,
    ResidencyStatus,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StateSupervisionPeriod,
    StateSupervisionSentence,
)
from recidiviz.tests.calculator.calculator_test_utils import (
    normalized_database_base_dict,
    normalized_database_base_dict_list,
)
from recidiviz.tests.calculator.pipeline.fake_bigquery import (
    FakeReadFromBigQueryFactory,
    FakeWriteToBigQuery,
    FakeWriteToBigQueryFactory,
)
from recidiviz.tests.calculator.pipeline.utils.run_pipeline_test_utils import (
    default_data_dict_for_root_schema_classes,
    run_test_pipeline,
    test_pipeline_options,
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

ROOT_SCHEMA_CLASSES_FOR_PIPELINE = [
    schema.StatePerson,
    schema.StateIncarcerationPeriod,
    schema.StateSupervisionPeriod,
    schema.StateSupervisionViolation,
    schema.StateSupervisionViolationResponse,
    schema.StateSupervisionSentence,
    schema.StateIncarcerationSentence,
    schema.StateAssessment,
]


class TestIncarcerationPipeline(unittest.TestCase):
    """Tests the entire incarceration pipeline."""

    def setUp(self) -> None:
        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()
        self.fake_bq_sink_factory = FakeWriteToBigQueryFactory(FakeWriteToBigQuery)

        self.incarceration_pre_processing_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_pre_processing_utils.get_state_specific_incarceration_period_pre_processing_delegate"
        )
        self.mock_incarceration_pre_processing_delegate = (
            self.incarceration_pre_processing_delegate_patcher.start()
        )
        self.mock_incarceration_pre_processing_delegate.return_value = (
            UsXxIncarcerationPreProcessingDelegate()
        )
        self.supervision_pre_processing_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_pre_processing_utils.get_state_specific_supervision_period_pre_processing_delegate"
        )
        self.mock_supervision_pre_processing_delegate = (
            self.supervision_pre_processing_delegate_patcher.start()
        )
        self.mock_supervision_pre_processing_delegate.return_value = (
            UsXxSupervisionPreProcessingDelegate()
        )
        self.pre_processing_incarceration_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_pre_processing_utils"
            ".get_state_specific_incarceration_delegate"
        )
        self.mock_incarceration_delegate = (
            self.pre_processing_incarceration_delegate_patcher.start()
        )
        self.mock_incarceration_delegate.return_value = UsXxIncarcerationDelegate()

        self.commitment_from_supervision_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.incarceration.identifier.get_state_specific_commitment_from_supervision_delegate"
        )
        self.mock_commitment_from_supervision_delegate = (
            self.commitment_from_supervision_delegate_patcher.start()
        )
        self.mock_commitment_from_supervision_delegate.return_value = (
            UsXxCommitmentFromSupervisionDelegate()
        )
        self.violation_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.incarceration.identifier.get_state_specific_violation_delegate"
        )
        self.mock_violation_delegate = self.violation_delegate_patcher.start()
        self.mock_violation_delegate.return_value = UsXxViolationDelegate()
        self.violation_pre_processing_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_pre_processing_utils.get_state_specific_violation_response_preprocessing_delegate"
        )
        self.mock_violation_pre_processing_delegate = (
            self.violation_pre_processing_delegate_patcher.start()
        )
        self.mock_violation_pre_processing_delegate.return_value = (
            UsXxViolationResponsePreprocessingDelegate()
        )
        self.supervision_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.incarceration.identifier.get_state_specific_supervision_delegate"
        )
        self.mock_supervision_delegate = self.supervision_delegate_patcher.start()
        self.mock_supervision_delegate.return_value = UsXxSupervisionDelegate()
        self.incarceration_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.incarceration.identifier.get_state_specific_incarceration_delegate"
        )
        self.mock_incarceration_delegate = self.incarceration_delegate_patcher.start()
        self.mock_incarceration_delegate.return_value = UsXxIncarcerationDelegate()

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        self.incarceration_pre_processing_delegate_patcher.stop()
        self.supervision_pre_processing_delegate_patcher.stop()
        self.commitment_from_supervision_delegate_patcher.stop()
        self.violation_delegate_patcher.stop()
        self.violation_pre_processing_delegate_patcher.stop()
        self.supervision_delegate_patcher.stop()
        self.incarceration_delegate_patcher.stop()
        self.pre_processing_incarceration_delegate_patcher.stop()

    @staticmethod
    def build_incarceration_pipeline_data_dict(
        fake_person_id: int, state_code: str = "US_XX"
    ) -> Dict[str, List]:
        """Builds a data_dict for a basic run of the pipeline."""
        fake_person = schema.StatePerson(
            state_code=state_code,
            person_id=fake_person_id,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        persons_data = [normalized_database_base_dict(fake_person)]

        race_1 = schema.StatePersonRace(
            person_race_id=111,
            state_code=state_code,
            race=Race.BLACK,
            person_id=fake_person_id,
        )

        race_2 = schema.StatePersonRace(
            person_race_id=111,
            state_code=state_code,
            race=Race.WHITE,
            person_id=fake_person_id,
        )

        races_data = normalized_database_base_dict_list([race_1, race_2])

        ethnicity = schema.StatePersonEthnicity(
            person_ethnicity_id=111,
            state_code=state_code,
            ethnicity=Ethnicity.HISPANIC,
            person_id=fake_person_id,
        )

        ethnicity_data = normalized_database_base_dict_list([ethnicity])

        initial_incarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            county_code="124",
            facility="San Quentin",
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2008, 11, 20),
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id,
        )

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=1111,
            state_code=state_code,
            county_code="124",
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
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
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
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
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2017, 1, 4),
            person_id=fake_person_id,
        )

        incarceration_sentence = schema.StateIncarcerationSentence(
            incarceration_sentence_id=1111,
            state_code=state_code,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            person_id=fake_person_id,
        )

        supervision_sentence = schema.StateSupervisionSentence(
            supervision_sentence_id=123,
            state_code=state_code,
            person_id=fake_person_id,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = schema.StateAssessment(
            assessment_id=298374,
            state_code=state_code,
            assessment_date=date(2015, 3, 19),
            assessment_type=StateAssessmentType.LSIR,
            person_id=fake_person_id,
        )

        incarceration_sentence_data = [
            normalized_database_base_dict(incarceration_sentence)
        ]

        supervision_sentence_data = [
            normalized_database_base_dict(supervision_sentence)
        ]

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration),
            normalized_database_base_dict(first_reincarceration),
            normalized_database_base_dict(subsequent_reincarceration),
        ]

        supervision_periods_data = [normalized_database_base_dict(supervision_period)]

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
            normalized_database_base_dict(supervision_violation_response)
        ]

        assessment_data = [normalized_database_base_dict(assessment)]

        fake_person_id_to_county_query_result = [
            {
                "state_code": state_code,
                "person_id": fake_person_id,
                "county_of_residence": _COUNTY_OF_RESIDENCE,
            }
        ]

        us_mo_sentence_status_data: List[Dict[str, Any]] = (
            [
                {
                    "state_code": state_code,
                    "person_id": fake_person_id,
                    "sentence_external_id": "XXX",
                    "sentence_status_external_id": "YYY",
                    "status_code": "ZZZ",
                    "status_date": "not_a_date",
                    "status_description": "XYZ",
                }
            ]
            if state_code == "US_MO"
            else []
        )

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

        data_dict = default_data_dict_for_root_schema_classes(
            ROOT_SCHEMA_CLASSES_FOR_PIPELINE
        )
        data_dict_overrides = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StatePersonRace.__tablename__: races_data,
            schema.StatePersonEthnicity.__tablename__: ethnicity_data,
            schema.StateIncarcerationSentence.__tablename__: incarceration_sentence_data,
            schema.StateSupervisionSentence.__tablename__: supervision_sentence_data,
            schema.StateIncarcerationPeriod.__tablename__: incarceration_periods_data,
            schema.StateSupervisionViolationResponse.__tablename__: supervision_violation_response_data,
            schema.StateSupervisionViolation.__tablename__: supervision_violation_data,
            schema.StateSupervisionPeriod.__tablename__: supervision_periods_data,
            schema.StateAssessment.__tablename__: assessment_data,
            "persons_to_recent_county_of_residence": fake_person_id_to_county_query_result,
            "incarceration_period_judicial_district_association": incarceration_period_judicial_district_association_data,
            "state_race_ethnicity_population_counts": state_race_ethnicity_population_count_data,
            "us_mo_sentence_statuses": us_mo_sentence_status_data,
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
        dataset = "recidiviz-123.state"

        self.run_test_pipeline(
            state_code=_STATE_CODE,
            dataset=dataset,
            data_dict=data_dict,
            expected_metric_types=ALL_METRIC_TYPES_SET,
        )

    @freeze_time("2015-01-31")
    def testIncarcerationPipelineFilterMetrics(self) -> None:
        fake_person_id = 12345
        data_dict = self.build_incarceration_pipeline_data_dict(
            fake_person_id=fake_person_id
        )
        dataset = "recidiviz-123.state"

        expected_metric_types = {IncarcerationMetricType.INCARCERATION_ADMISSION}
        metric_types_filter = {IncarcerationMetricType.INCARCERATION_ADMISSION.value}

        self.run_test_pipeline(
            state_code=_STATE_CODE,
            dataset=dataset,
            data_dict=data_dict,
            expected_metric_types=expected_metric_types,
            metric_types_filter=metric_types_filter,
        )

    def testIncarcerationPipelineUsMo(self) -> None:
        self._stop_state_specific_delegate_patchers()

        fake_person_id = 12345
        data_dict = self.build_incarceration_pipeline_data_dict(
            fake_person_id=fake_person_id, state_code="US_MO"
        )
        dataset = "recidiviz-123.state"

        self.run_test_pipeline(
            state_code="US_MO",
            dataset=dataset,
            data_dict=data_dict,
            expected_metric_types=ALL_METRIC_TYPES_SET,
        )

    def testIncarcerationPipelineWithFilterSet(self) -> None:
        fake_person_id = 12345
        data_dict = self.build_incarceration_pipeline_data_dict(
            fake_person_id=fake_person_id
        )
        dataset = "recidivz-staging.state"

        self.run_test_pipeline(
            state_code=_STATE_CODE,
            dataset=dataset,
            data_dict=data_dict,
            expected_metric_types=ALL_METRIC_TYPES_SET,
            unifying_id_field_filter_set={fake_person_id},
        )

    def run_test_pipeline(
        self,
        state_code: str,
        dataset: str,
        data_dict: Dict[str, List[Dict]],
        expected_metric_types: Set[IncarcerationMetricType],
        unifying_id_field_filter_set: Optional[Set[int]] = None,
        metric_types_filter: Optional[Set[str]] = None,
    ) -> None:
        """Runs a test version of the supervision pipeline."""
        read_from_bq_constructor = (
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict
            )
        )
        write_to_bq_constructor = (
            self.fake_bq_sink_factory.create_fake_bq_sink_constructor(
                dataset,
                expected_output_metric_types=expected_metric_types,
            )
        )

        run_test_pipeline(
            pipeline=pipeline.IncarcerationPipeline(),
            state_code=state_code,
            dataset=dataset,
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
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        fake_person_id_2 = 6789

        fake_person_2 = schema.StatePerson(
            state_code="US_XX",
            person_id=fake_person_id_2,
            gender=Gender.FEMALE,
            birthdate=date(1990, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        persons_data = [
            normalized_database_base_dict(fake_person_1),
            normalized_database_base_dict(fake_person_2),
        ]

        incarceration_sentence = schema.StateIncarcerationSentence(
            incarceration_sentence_id=1111,
            state_code="US_XX",
            person_id=fake_person_id,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence = schema.StateSupervisionSentence(
            supervision_sentence_id=123,
            state_code="US_XX",
            person_id=fake_person_id,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_sentence_data = [
            normalized_database_base_dict(incarceration_sentence)
        ]

        supervision_sentence_data = [
            normalized_database_base_dict(supervision_sentence)
        ]

        fake_person_id_to_county_query_result = [
            {
                "state_code": "US_XX",
                "person_id": fake_person_id,
                "county_of_residence": _COUNTY_OF_RESIDENCE,
            }
        ]

        us_mo_sentence_status_data: List[Dict[str, Any]] = [
            {
                "state_code": "US_MO",
                "person_id": fake_person_id,
                "sentence_external_id": "XXX",
                "sentence_status_external_id": "YYY",
                "status_code": "ZZZ",
                "status_date": "not_a_date",
                "status_description": "XYZ",
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

        data_dict = default_data_dict_for_root_schema_classes(
            ROOT_SCHEMA_CLASSES_FOR_PIPELINE
        )
        data_dict_overrides = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StateIncarcerationSentence.__tablename__: incarceration_sentence_data,
            schema.StateSupervisionSentence.__tablename__: supervision_sentence_data,
            "persons_to_recent_county_of_residence": fake_person_id_to_county_query_result,
            "incarceration_period_judicial_district_association": incarceration_period_judicial_district_association_data,
            "state_race_ethnicity_population_counts": state_race_ethnicity_population_count_data,
            "us_mo_sentence_statuses": us_mo_sentence_status_data,
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
        dataset = "recidiviz-123.state"

        self.run_test_pipeline(
            _STATE_CODE, dataset, data_dict, expected_metric_types=set()
        )


class TestClassifyIncarcerationEvents(unittest.TestCase):
    """Tests the ClassifyEvents DoFn in the pipeline."""

    def setUp(self) -> None:
        self.incarceration_pre_processing_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_pre_processing_utils.get_state_specific_incarceration_period_pre_processing_delegate"
        )
        self.mock_incarceration_pre_processing_delegate = (
            self.incarceration_pre_processing_delegate_patcher.start()
        )
        self.mock_incarceration_pre_processing_delegate.return_value = (
            UsXxIncarcerationPreProcessingDelegate()
        )
        self.supervision_pre_processing_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_pre_processing_utils.get_state_specific_supervision_period_pre_processing_delegate"
        )
        self.mock_supervision_pre_processing_delegate = (
            self.supervision_pre_processing_delegate_patcher.start()
        )
        self.mock_supervision_pre_processing_delegate.return_value = (
            UsXxSupervisionPreProcessingDelegate()
        )
        self.pre_processing_incarceration_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_pre_processing_utils"
            ".get_state_specific_incarceration_delegate"
        )
        self.mock_incarceration_delegate = (
            self.pre_processing_incarceration_delegate_patcher.start()
        )
        self.mock_incarceration_delegate.return_value = UsXxIncarcerationDelegate()
        self.identifier = identifier.IncarcerationIdentifier()

        self.commitment_from_supervision_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.incarceration.identifier.get_state_specific_commitment_from_supervision_delegate"
        )
        self.mock_commitment_from_supervision_delegate = (
            self.commitment_from_supervision_delegate_patcher.start()
        )
        self.mock_commitment_from_supervision_delegate.return_value = (
            UsXxCommitmentFromSupervisionDelegate()
        )
        self.violation_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.incarceration.identifier.get_state_specific_violation_delegate"
        )
        self.mock_violation_delegate = self.violation_delegate_patcher.start()
        self.mock_violation_delegate.return_value = UsXxViolationDelegate()
        self.violation_pre_processing_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_pre_processing_utils.get_state_specific_violation_response_preprocessing_delegate"
        )
        self.mock_violation_pre_processing_delegate = (
            self.violation_pre_processing_delegate_patcher.start()
        )
        self.mock_violation_pre_processing_delegate.return_value = (
            UsXxViolationResponsePreprocessingDelegate()
        )
        self.supervision_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.incarceration.identifier.get_state_specific_supervision_delegate"
        )
        self.mock_supervision_delegate = self.supervision_delegate_patcher.start()
        self.mock_supervision_delegate.return_value = UsXxSupervisionDelegate()
        self.incarceration_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.incarceration.identifier.get_state_specific_incarceration_delegate"
        )
        self.mock_incarceration_delegate = self.incarceration_delegate_patcher.start()
        self.mock_incarceration_delegate.return_value = UsXxIncarcerationDelegate()

    def tearDown(self) -> None:
        self.incarceration_pre_processing_delegate_patcher.stop()
        self.supervision_pre_processing_delegate_patcher.stop()
        self.commitment_from_supervision_delegate_patcher.stop()
        self.violation_delegate_patcher.stop()
        self.violation_pre_processing_delegate_patcher.stop()
        self.supervision_delegate_patcher.stop()
        self.incarceration_delegate_patcher.stop()
        self.pre_processing_incarceration_delegate_patcher.stop()

    @staticmethod
    def load_person_entities_dict(
        person: StatePerson,
        incarceration_periods: List[StateIncarcerationPeriod] = None,
        supervision_sentences: List[StateSupervisionSentence] = None,
        incarceration_sentences: List[StateIncarcerationSentence] = None,
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
            entities.StateIncarcerationSentence.__name__: incarceration_sentences
            if incarceration_sentences
            else [],
            entities.StateSupervisionSentence.__name__: supervision_sentences
            if supervision_sentences
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
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
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
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            facility="PRISON XX",
            admission_date=date(2010, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2010, 11, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=state_code,
            incarceration_sentence_id=123,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            start_date=date(2009, 2, 9),
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
            incarceration_sentences=[incarceration_sentence],
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
            >> beam.ParDo(ClassifyEvents(), self.identifier)
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyIncarcerationEvents_NoSentenceGroups(self) -> None:
        """Tests the ClassifyIncarcerationEvents DoFn when the person has no sentence groups."""
        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=123,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        person_entities = self.load_person_entities_dict(
            person=fake_person,
        )

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person.person_id, person_entities)])
            | "Identify Incarceration Events"
            >> beam.ParDo(ClassifyEvents(), self.identifier)
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testClassifyIncarcerationEventsWithPeriodsAfterDeath(self) -> None:
        """Tests the ClassifyIncarcerationEvents DoFn for when a person has periods
        that occur after a period ending in their death."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        incarceration_period_with_death = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON XX",
            admission_date=date(2010, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2010, 11, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.DEATH,
        )

        post_mortem_incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON XX",
            admission_date=date(2010, 11, 22),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2010, 11, 23),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        post_mortem_incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON XX",
            admission_date=date(2010, 11, 24),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=123,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            start_date=date(2009, 2, 9),
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

        assert incarceration_period_with_death.admission_date is not None
        assert incarceration_period_with_death.release_date is not None
        assert incarceration_period_with_death.admission_reason is not None
        assert incarceration_period_with_death.release_reason is not None
        incarceration_events = [
            IncarcerationCommitmentFromSupervisionAdmissionEvent(
                state_code=incarceration_period_with_death.state_code,
                event_date=incarceration_period_with_death.admission_date,
                facility=incarceration_period_with_death.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=incarceration_period_with_death.admission_reason,
                admission_reason_raw_text=incarceration_period_with_death.admission_reason_raw_text,
                supervision_type=StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period_with_death.state_code,
                event_date=incarceration_period_with_death.release_date,
                facility=incarceration_period_with_death.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=incarceration_period_with_death.release_reason,
                admission_reason=incarceration_period_with_death.admission_reason,
                commitment_from_supervision_supervision_type=StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
                total_days_incarcerated=(
                    incarceration_period_with_death.release_date
                    - incarceration_period_with_death.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            IncarcerationStayEvent(
                admission_reason=incarceration_period_with_death.admission_reason,
                admission_reason_raw_text=incarceration_period_with_death.admission_reason_raw_text,
                state_code=incarceration_period_with_death.state_code,
                event_date=incarceration_period_with_death.admission_date,
                facility=incarceration_period_with_death.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                commitment_from_supervision_supervision_type=StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
            ),
        ]

        correct_output = [(fake_person_id, (fake_person, incarceration_events))]

        test_pipeline = TestPipeline()

        person_entities = self.load_person_entities_dict(
            person=fake_person,
            incarceration_periods=[
                incarceration_period_with_death,
                post_mortem_incarceration_period_1,
                post_mortem_incarceration_period_2,
            ],
            incarceration_sentences=[incarceration_sentence],
            ip_to_judicial_district_kv=[
                fake_incarceration_period_judicial_district_association_result
            ],
            person_id_to_county_kv=[fake_person_id_to_county_query_result],
        )

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_entities)])
            | "Identify Incarceration Events"
            >> beam.ParDo(ClassifyEvents(), self.identifier)
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()


class TestProduceIncarcerationMetrics(unittest.TestCase):
    """Tests the ProduceMetrics DoFn in the pipeline."""

    def setUp(self) -> None:
        self.fake_person_id = 12345

        self.person_metadata = PersonMetadata(prioritized_race_or_ethnicity="BLACK")
        self.pipeline_config = pipeline.IncarcerationPipeline().pipeline_config

    def testProduceIncarcerationMetrics(self) -> None:
        """Tests the ProduceIncarcerationMetrics DoFn."""
        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=123,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
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
                self.pipeline_config,
                ALL_METRICS_INCLUSIONS_DICT,
                test_pipeline_options(),
                None,
                -1,
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
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
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
                self.pipeline_config,
                ALL_METRICS_INCLUSIONS_DICT,
                test_pipeline_options(),
                None,
                -1,
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
                self.pipeline_config,
                ALL_METRICS_INCLUSIONS_DICT,
                test_pipeline_options(),
                None,
                -1,
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
                        "Failed assert. Output is not of type" "IncarcerationMetric."
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
                        "Failed assert. Count does not" "match expected value."
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
