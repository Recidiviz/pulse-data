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
from typing import Any, Callable, Collection, Dict, List, Optional, Set, Type
from unittest import mock

import apache_beam as beam
import attr
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException, assert_that, equal_to
from freezegun import freeze_time
from mock import patch
from more_itertools import one

from recidiviz.calculator.pipeline.base_pipeline import ClassifyEvents, ProduceMetrics
from recidiviz.calculator.pipeline.supervision import identifier, pipeline
from recidiviz.calculator.pipeline.supervision.events import (
    ProjectedSupervisionCompletionEvent,
    SupervisionEvent,
    SupervisionPopulationEvent,
    SupervisionTerminationEvent,
)
from recidiviz.calculator.pipeline.supervision.metrics import (
    SupervisionMetric,
    SupervisionMetricType,
)
from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import (
    SupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.metric_utils import (
    RecidivizMetric,
    RecidivizMetricType,
)
from recidiviz.calculator.pipeline.utils.person_utils import (
    ExtractPersonEventsMetadata,
    PersonMetadata,
)
from recidiviz.common.constants.person_characteristics import Race
from recidiviz.common.constants.state.state_assessment import StateAssessmentType
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationFacilitySecurityLevel,
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
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
    Gender,
    ResidencyStatus,
    StateAssessment,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StateSupervisionSentence,
)
from recidiviz.tests.calculator.calculator_test_utils import (
    normalized_database_base_dict,
)
from recidiviz.tests.calculator.pipeline.fake_bigquery import (
    DataTablesDict,
    FakeReadFromBigQueryFactory,
    FakeWriteToBigQuery,
    FakeWriteToBigQueryFactory,
)
from recidiviz.tests.calculator.pipeline.supervision import identifier_test
from recidiviz.tests.calculator.pipeline.utils.run_pipeline_test_utils import (
    default_data_dict_for_root_schema_classes,
    run_test_pipeline,
    test_pipeline_options,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.us_xx.us_xx_incarceration_period_pre_processing_delegate import (
    UsXxIncarcerationPreProcessingDelegate,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.us_xx.us_xx_supervision_period_pre_processing_delegate import (
    UsXxSupervisionPreProcessingDelegate,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.us_xx.us_xx_violation_response_preprocessing_delegate import (
    UsXxViolationResponsePreprocessingDelegate,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.us_xx.us_xx_violations_delegate import (
    UsXxViolationDelegate,
)
from recidiviz.tests.persistence.database import database_test_utils

SUPERVISION_PIPELINE_PACKAGE_NAME = pipeline.__name__

ROOT_SCHEMA_CLASSES_FOR_PIPELINE = [
    schema.StatePerson,
    schema.StateSupervisionPeriod,
    schema.StateIncarcerationPeriod,
    schema.StateSupervisionViolation,
    schema.StateSupervisionViolationResponse,
    schema.StateSupervisionSentence,
    schema.StateIncarcerationSentence,
    schema.StateAssessment,
    schema.StateSupervisionContact,
]


class TestSupervisionPipeline(unittest.TestCase):
    """Tests the entire supervision pipeline."""

    def setUp(self) -> None:
        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()
        self.fake_bq_sink_factory = FakeWriteToBigQueryFactory(
            SupervisionPipelineFakeWriteToBigQuery
        )

        self.metric_inclusions_dict: Dict[str, bool] = {
            metric_type.value: True for metric_type in SupervisionMetricType
        }

        self.incarceration_pre_processing_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_pre_processing_utils"
            ".get_state_specific_incarceration_period_pre_processing_delegate"
        )
        self.mock_incarceration_pre_processing_delegate = (
            self.incarceration_pre_processing_delegate_patcher.start()
        )
        self.mock_incarceration_pre_processing_delegate.return_value = (
            UsXxIncarcerationPreProcessingDelegate()
        )
        self.supervision_pre_processing_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_pre_processing_utils"
            ".get_state_specific_supervision_period_pre_processing_delegate"
        )
        self.mock_supervision_pre_processing_delegate = (
            self.supervision_pre_processing_delegate_patcher.start()
        )
        self.mock_supervision_pre_processing_delegate.return_value = (
            UsXxSupervisionPreProcessingDelegate()
        )
        self.violation_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.supervision.identifier.get_state_specific_violation_delegate"
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
        self.identifier_supervision_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.supervision.identifier.get_state_specific_supervision_delegate"
        )
        self.mock_identifier_supervision_delegate = (
            self.identifier_supervision_delegate_patcher.start()
        )
        self.mock_identifier_supervision_delegate.return_value = (
            UsXxSupervisionDelegate()
        )
        self.metric_producer_supervision_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.supervision.metric_producer.get_state_specific_supervision_delegate"
        )
        self.mock_metric_producer_supervision_delegate = (
            self.metric_producer_supervision_delegate_patcher.start()
        )
        self.mock_metric_producer_supervision_delegate.return_value = (
            UsXxSupervisionDelegate()
        )

    def tearDown(self) -> None:
        self.incarceration_pre_processing_delegate_patcher.stop()
        self.supervision_pre_processing_delegate_patcher.stop()
        self.violation_delegate_patcher.stop()
        self.violation_pre_processing_delegate_patcher.stop()
        self.identifier_supervision_delegate_patcher.stop()
        self.metric_producer_supervision_delegate_patcher.stop()

    @staticmethod
    def build_supervision_pipeline_data_dict(
        fake_person_id: int, fake_supervision_period_id: int
    ) -> Dict[str, List[Any]]:
        """Builds a data_dict for a basic run of the pipeline."""
        fake_person = schema.StatePerson(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        persons_data = [normalized_database_base_dict(fake_person)]

        fake_person_race = schema.StatePersonRace(
            state_code="US_XX",
            person_id=fake_person_id,
            race=Race.BLACK,
            race_raw_text=Race.BLACK.name,
        )
        person_race_data = [normalized_database_base_dict(fake_person_race)]

        initial_incarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
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

        first_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            county_code="124",
            facility="San Quentin",
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2011, 4, 5),
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id,
        )

        subsequent_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=3333,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            county_code="124",
            facility="San Quentin",
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2017, 1, 4),
            person_id=fake_person_id,
        )

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=fake_supervision_period_id,
            state_code="US_XX",
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
            state_code="US_XX",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            supervision_periods=[supervision_period],
            start_date=date(2015, 3, 1),
            projected_completion_date=date(2016, 12, 31),
            completion_date=date(2016, 12, 29),
            status=StateSentenceStatus.COMPLETED,
            person_id=fake_person_id,
        )

        incarceration_sentence = schema.StateIncarcerationSentence(
            incarceration_sentence_id=123,
            state_code="US_XX",
            person_id=fake_person_id,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[
                initial_incarceration,
                subsequent_reincarceration,
                first_reincarceration,
            ],
        )

        supervision_sentence_supervision_period_association = [
            {
                "supervision_period_id": fake_supervision_period_id,
                "supervision_sentence_id": 1122,
            }
        ]

        charge = database_test_utils.generate_test_charge(
            person_id=fake_person_id,
            charge_id=1234523,
            court_case=None,
        )

        assessment = schema.StateAssessment(
            assessment_id=298374,
            state_code="US_XX",
            assessment_date=date(2015, 3, 19),
            assessment_type=StateAssessmentType.LSIR,
            person_id=fake_person_id,
        )

        supervision_contact = schema.StateSupervisionContact(
            state_code="US_XX",
            contact_date=supervision_period.start_date,
            person_id=fake_person_id,
        )

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration),
            normalized_database_base_dict(first_reincarceration),
            normalized_database_base_dict(subsequent_reincarceration),
        ]

        supervision_periods_data = [normalized_database_base_dict(supervision_period)]

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
            normalized_database_base_dict(supervision_violation_response)
        ]

        assessment_data = [normalized_database_base_dict(assessment)]

        supervision_contact_data = [normalized_database_base_dict(supervision_contact)]

        supervision_period_to_agent_data = [
            {
                "state_code": "US_XX",
                "agent_id": 1010,
                "person_id": fake_person_id,
                "agent_external_id": "OFFICER0009",
                "supervision_period_id": fake_supervision_period_id,
            }
        ]

        supervision_period_judicial_district_association_data = [
            {
                "state_code": "US_XX",
                "person_id": fake_person_id,
                "supervision_period_id": fake_supervision_period_id,
                "judicial_district_code": "XXX",
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

        us_mo_sentence_status_data: List[Dict[str, Any]] = [
            {
                "person_id": fake_person_id,
                "sentence_external_id": "is-123",
                "sentence_status_external_id": "is-123-1",
                "status_code": "10I1000",
                "status_date": "20081120",
                "status_description": "New Court Comm-Institution",
            },
            {
                "person_id": fake_person_id,
                "sentence_external_id": "is-123",
                "sentence_status_external_id": "is-123-2",
                "status_code": "40O1010",
                "status_date": "20101204",
                "status_description": "Parole Release",
            },
            {
                "person_id": fake_person_id,
                "sentence_external_id": "is-123",
                "sentence_status_external_id": "is-123-2",
                "status_code": "45O1060",
                "status_date": "20110405",
                "status_description": "Parole Ret-Treatment Center",
            },
            {
                "person_id": fake_person_id,
                "sentence_external_id": "is-123",
                "sentence_status_external_id": "is-123-3",
                "status_code": "40O1030",
                "status_date": "20140414",
                "status_description": "Parole Re-Release",
            },
            {
                "person_id": fake_person_id,
                "sentence_external_id": "ss-1122",
                "sentence_status_external_id": "ss-1122-1",
                "status_code": "25I1000",
                "status_date": "20150314",
                "status_description": "Court Probation - Addl Chg",
            },
            {
                "person_id": fake_person_id,
                "sentence_external_id": "ss-1122",
                "sentence_status_external_id": "ss-1122-2",
                "status_code": "45O7000",
                "status_date": "20170104",
                "status_description": "Field to DAI-Other Sentence",
            },
            {
                "person_id": fake_person_id,
                "sentence_external_id": "is-123",
                "sentence_status_external_id": "is-123-2",
                "status_code": "45O1010",
                "status_date": "20170104",
                "status_description": "Parole Ret-Tech Viol",
            },
        ]

        data_dict = default_data_dict_for_root_schema_classes(
            ROOT_SCHEMA_CLASSES_FOR_PIPELINE
        )
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
            schema.state_supervision_sentence_supervision_period_association_table.name: supervision_sentence_supervision_period_association,
            schema.StateAssessment.__tablename__: assessment_data,
            schema.StateSupervisionContact.__tablename__: supervision_contact_data,
            "supervision_period_to_agent_association": supervision_period_to_agent_data,
            "supervision_period_judicial_district_association": supervision_period_judicial_district_association_data,
            "state_race_ethnicity_population_counts": state_race_ethnicity_population_count_data,
            "us_mo_sentence_statuses": us_mo_sentence_status_data,
        }
        data_dict.update(data_dict_overrides)

        return data_dict

    @freeze_time("2017-01-31")
    def testSupervisionPipeline(self) -> None:
        fake_person_id = 12345
        fake_supervision_period_id = 1111

        data_dict = self.build_supervision_pipeline_data_dict(
            fake_person_id, fake_supervision_period_id
        )
        dataset = "recidiviz-123.state"

        expected_metric_types = {
            SupervisionMetricType.SUPERVISION_POPULATION,
            SupervisionMetricType.SUPERVISION_SUCCESS,
            SupervisionMetricType.SUPERVISION_START,
            SupervisionMetricType.SUPERVISION_TERMINATION,
        }

        self.run_test_pipeline(dataset, data_dict, expected_metric_types)

    @freeze_time("2017-01-31")
    def testSupervisionPipelineWithPersonIdFilterSet(self) -> None:
        fake_person_id = 12345
        fake_supervision_period_id = 1111

        data_dict = self.build_supervision_pipeline_data_dict(
            fake_person_id, fake_supervision_period_id
        )
        dataset = "recidiviz-123.state"

        expected_metric_types = {
            SupervisionMetricType.SUPERVISION_POPULATION,
            SupervisionMetricType.SUPERVISION_SUCCESS,
            SupervisionMetricType.SUPERVISION_TERMINATION,
            SupervisionMetricType.SUPERVISION_START,
        }

        self.run_test_pipeline(
            dataset,
            data_dict,
            expected_metric_types,
            unifying_id_field_filter_set={fake_person_id},
        )

    def run_test_pipeline(
        self,
        dataset: str,
        data_dict: DataTablesDict,
        expected_metric_types: Set[SupervisionMetricType],
        expected_violation_types: Set[ViolationType] = None,
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
                expected_violation_types=expected_violation_types,
            )
        )
        with patch(
            f"{SUPERVISION_PIPELINE_PACKAGE_NAME}.ReadFromBigQuery",
            read_from_bq_constructor,
        ):
            run_test_pipeline(
                pipeline=pipeline.SupervisionPipeline(),
                state_code="US_XX",
                dataset=dataset,
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
            gender=Gender.FEMALE,
            birthdate=date(1990, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        persons_data = [normalized_database_base_dict(fake_person)]

        initial_incarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
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

        first_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            county_code="124",
            facility="San Quentin",
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2011, 4, 5),
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id,
        )

        # This probation supervision period ended in a revocation
        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=1111,
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
            start_date=date(2008, 11, 20),
            projected_completion_date=date(2017, 12, 31),
            supervision_periods=[supervision_period],
            person_id=fake_person_id,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_sentence = schema.StateIncarcerationSentence(
            incarceration_sentence_id=123,
            external_id="is-123",
            state_code="US_XX",
            start_date=date(2008, 11, 20),
            person_id=fake_person_id,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence_supervision_period_association = [
            {
                "supervision_period_id": 1111,
                "supervision_sentence_id": 1122,
            }
        ]

        charge = database_test_utils.generate_test_charge(
            person_id=fake_person_id,
            state_code="US_XX",
            charge_id=1234523,
            court_case=None,
        )

        ssvr = schema.StateSupervisionViolationResponse(
            supervision_violation_response_id=fake_svr_id,
            state_code="US_XX",
            person_id=fake_person_id,
            supervision_violation_id=fake_violation_id,
        )

        state_agent = database_test_utils.generate_test_assessment_agent()

        ssvr.decision_agents = [state_agent]

        violation_report = schema.StateSupervisionViolationResponse(
            supervision_violation_response_id=99999,
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
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            county_code="124",
            facility="San Quentin",
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2017, 1, 4),
            person_id=fake_person_id,
        )

        assessment = schema.StateAssessment(
            assessment_id=298374,
            state_code="US_XX",
            assessment_date=date(2015, 3, 19),
            assessment_type=StateAssessmentType.LSIR,
            person_id=fake_person_id,
        )

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration),
            normalized_database_base_dict(first_reincarceration),
            normalized_database_base_dict(revocation_reincarceration),
        ]

        supervision_periods_data = [normalized_database_base_dict(supervision_period)]

        supervision_violation_type_data = [
            normalized_database_base_dict(supervision_violation_type)
        ]

        supervision_violation_response_data = [
            normalized_database_base_dict(ssvr),
            normalized_database_base_dict(violation_report),
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

        assessment_data = [normalized_database_base_dict(assessment)]

        supervision_period_to_agent_data = [
            {
                "state_code": "US_XX",
                "agent_id": 1010,
                "person_id": fake_person_id,
                "agent_external_id": "OFFICER0009",
                "supervision_period_id": supervision_period.supervision_period_id,
            }
        ]

        supervision_period_judicial_district_association_data = [
            {
                "state_code": "US_XX",
                "person_id": fake_person_id,
                "supervision_period_id": supervision_period.supervision_period_id,
                "judicial_district_code": "XXX",
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
            schema.StateIncarcerationPeriod.__tablename__: incarceration_periods_data,
            schema.StateSupervisionViolationResponse.__tablename__: supervision_violation_response_data,
            schema.StateSupervisionViolation.__tablename__: supervision_violation_data,
            schema.StateSupervisionViolationTypeEntry.__tablename__: supervision_violation_type_data,
            schema.StateSupervisionPeriod.__tablename__: supervision_periods_data,
            schema.StateSupervisionSentence.__tablename__: supervision_sentences_data,
            schema.StateIncarcerationSentence.__tablename__: incarceration_sentences_data,
            schema.StateCharge.__tablename__: charge_data,
            schema.state_supervision_sentence_supervision_period_association_table.name: supervision_sentence_supervision_period_association,
            schema.StateAssessment.__tablename__: assessment_data,
            "supervision_period_to_agent_association": supervision_period_to_agent_data,
            "supervision_period_judicial_district_association": supervision_period_judicial_district_association_data,
            "state_race_ethnicity_population_counts": state_race_ethnicity_population_count_data,
        }
        data_dict.update(data_dict_overrides)

        dataset = "recidiviz-123.state"

        expected_metric_types = {
            SupervisionMetricType.SUPERVISION_POPULATION,
            SupervisionMetricType.SUPERVISION_TERMINATION,
            SupervisionMetricType.SUPERVISION_START,
        }

        metric_types_filter = {metric.value for metric in expected_metric_types}

        self.run_test_pipeline(
            dataset,
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

        initial_incarceration_1 = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id="ip1",
            state_code="US_XX",
            county_code="124",
            facility="San Quentin",
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
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
            supervision_periods=[supervision_period__1],
            start_date=date(2016, 3, 1),
            projected_completion_date=date(2017, 12, 31),
            completion_date=date(2016, 12, 29),
            person_id=fake_person_id_1,
        )

        incarceration_sentence = schema.StateIncarcerationSentence(
            incarceration_sentence_id=123,
            state_code="US_XX",
            incarceration_periods=[initial_incarceration_1],
            person_id=fake_person_id_1,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence_supervision_period_association = [
            {
                "supervision_period_id": 1111,
                "supervision_sentence_id": 1122,
            }
        ]

        charge = database_test_utils.generate_test_charge(
            person_id=fake_person_id_1,
            charge_id=1234523,
            court_case=None,
        )

        supervision_violation_response = (
            database_test_utils.generate_test_supervision_violation_response(
                fake_person_id_2
            )
        )

        state_agent = database_test_utils.generate_test_assessment_agent()

        supervision_violation_response.decision_agents = [state_agent]

        supervision_violation = database_test_utils.generate_test_supervision_violation(
            fake_person_id_2, [supervision_violation_response]
        )

        supervision_violation_response.supervision_violation_id = (
            supervision_violation.supervision_violation_id
        )

        supervision_violation_response_data = [
            normalized_database_base_dict(supervision_violation_response)
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
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            external_id="ip5",
            county_code="124",
            facility="San Quentin",
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2011, 4, 5),
            release_date=date(2014, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id_2,
        )

        subsequent_reincarceration_2 = schema.StateIncarcerationPeriod(
            incarceration_period_id=6666,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            external_id="ip6",
            county_code="124",
            facility="San Quentin",
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2018, 3, 9),
            person_id=fake_person_id_2,
        )

        assessment = schema.StateAssessment(
            assessment_id=298374,
            state_code="US_XX",
            assessment_date=date(2015, 3, 19),
            assessment_type=StateAssessmentType.LSIR,
            person_id=fake_person_id_1,
        )

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration_1),
            normalized_database_base_dict(first_reincarceration_2),
            normalized_database_base_dict(subsequent_reincarceration_2),
        ]

        supervision_periods_data = [
            normalized_database_base_dict(supervision_period__1)
        ]

        assessment_data = [normalized_database_base_dict(assessment)]

        supervision_period_to_agent_data = [
            {
                "state_code": "US_XX",
                "agent_id": 1010,
                "person_id": supervision_period__1.person_id,
                "agent_external_id": "OFFICER0009",
                "supervision_period_id": supervision_period__1.supervision_period_id,
            }
        ]

        supervision_period_judicial_district_association_data = [
            {
                "state_code": "US_XX",
                "person_id": fake_person_id_1,
                "supervision_period_id": supervision_period__1.supervision_period_id,
                "judicial_district_code": "XXX",
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
            schema.StateIncarcerationPeriod.__tablename__: incarceration_periods_data,
            schema.StateSupervisionViolationResponse.__tablename__: supervision_violation_response_data,
            schema.StateSupervisionViolation.__tablename__: supervision_violation_data,
            schema.StateSupervisionPeriod.__tablename__: supervision_periods_data,
            schema.StateSupervisionSentence.__tablename__: supervision_sentences_data,
            schema.StateIncarcerationSentence.__tablename__: incarceration_sentences_data,
            schema.StateCharge.__tablename__: charge_data,
            schema.state_supervision_sentence_supervision_period_association_table.name: supervision_sentence_supervision_period_association,
            schema.StateAssessment.__tablename__: assessment_data,
            "supervision_period_to_agent_association": supervision_period_to_agent_data,
            "supervision_period_judicial_district_association": supervision_period_judicial_district_association_data,
            "state_race_ethnicity_population_counts": state_race_ethnicity_population_count_data,
        }
        data_dict.update(data_dict_overrides)
        dataset = "recidiviz-123.state"

        expected_metric_types = {
            SupervisionMetricType.SUPERVISION_POPULATION,
            SupervisionMetricType.SUPERVISION_SUCCESS,
            SupervisionMetricType.SUPERVISION_TERMINATION,
            SupervisionMetricType.SUPERVISION_START,
        }

        self.run_test_pipeline(dataset, data_dict, expected_metric_types)


class TestClassifyEvents(unittest.TestCase):
    """Tests the ClassifyEvents DoFn in the pipeline."""

    def setUp(self) -> None:
        self.maxDiff = None

        self.incarceration_pre_processing_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_pre_processing_utils"
            ".get_state_specific_incarceration_period_pre_processing_delegate"
        )
        self.mock_incarceration_pre_processing_delegate = (
            self.incarceration_pre_processing_delegate_patcher.start()
        )
        self.mock_incarceration_pre_processing_delegate.return_value = (
            UsXxIncarcerationPreProcessingDelegate()
        )
        self.supervision_pre_processing_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.entity_pre_processing_utils"
            ".get_state_specific_supervision_period_pre_processing_delegate"
        )
        self.mock_supervision_pre_processing_delegate = (
            self.supervision_pre_processing_delegate_patcher.start()
        )
        self.mock_supervision_pre_processing_delegate.return_value = (
            UsXxSupervisionPreProcessingDelegate()
        )
        self.violation_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.supervision.identifier.get_state_specific_violation_delegate"
        )
        self.mock_violation_delegate = self.violation_delegate_patcher.start()
        self.mock_violation_delegate.return_value = UsXxViolationDelegate()
        self.identifier = identifier.SupervisionIdentifier()

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
            "recidiviz.calculator.pipeline.supervision.identifier.get_state_specific_supervision_delegate"
        )
        self.mock_supervision_delegate = self.supervision_delegate_patcher.start()
        self.mock_supervision_delegate.return_value = UsXxSupervisionDelegate()

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        self.incarceration_pre_processing_delegate_patcher.stop()
        self.supervision_pre_processing_delegate_patcher.stop()
        self.violation_pre_processing_delegate_patcher.stop()
        self.violation_delegate_patcher.stop()
        self.supervision_delegate_patcher.stop()

    @staticmethod
    def load_person_entities_dict(
        person: StatePerson,
        supervision_periods: List[entities.StateSupervisionPeriod] = None,
        incarceration_periods: List[entities.StateIncarcerationPeriod] = None,
        incarceration_sentences: List[entities.StateIncarcerationSentence] = None,
        supervision_sentences: List[entities.StateSupervisionSentence] = None,
        violation_responses: List[entities.StateSupervisionViolationResponse] = None,
        assessments: List[entities.StateAssessment] = None,
        supervision_contacts: List[entities.StateSupervisionContact] = None,
        supervision_period_judicial_district_association: List[Dict[Any, Any]] = None,
        supervision_period_to_agent_association: List[Dict[Any, Any]] = None,
    ) -> Dict[str, List[Any]]:
        return {
            "persons": [person],
            "supervision_periods": supervision_periods if supervision_periods else [],
            "assessments": assessments if assessments else [],
            "incarceration_periods": incarceration_periods
            if incarceration_periods
            else [],
            "incarceration_sentences": incarceration_sentences
            if incarceration_sentences
            else [],
            "supervision_sentences": supervision_sentences
            if supervision_sentences
            else [],
            "violation_responses": violation_responses if violation_responses else [],
            "supervision_contacts": supervision_contacts
            if supervision_contacts
            else [],
            "supervision_period_judicial_district_association": (
                supervision_period_judicial_district_association
                if supervision_period_judicial_district_association
                else []
            ),
            "supervision_period_to_agent_association": supervision_period_to_agent_association
            or [],
        }

    def testClassifyEvents(self) -> None:
        """Tests the ClassifyEvents DoFn."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="ip1",
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        supervision_period_termination_date = date(2015, 5, 29)
        supervision_period = entities.StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
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
        )

        start_date = date(2008, 1, 1)
        completion_date = date(2015, 5, 29)
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            start_date=start_date,
            projected_completion_date=date(2015, 5, 30),
            completion_date=completion_date,
            supervision_periods=[supervision_period],
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=123,
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2015, 3, 10),
        )

        judicial_district_code = "NORTHEAST"

        supervision_period_to_judicial_district_row = {
            "person_id": fake_person_id,
            "supervision_period_id": supervision_period.supervision_period_id,
            "judicial_district_code": judicial_district_code,
        }

        supervision_period_to_agent_map = {
            "agent_id": 1010,
            "person_id": fake_person_id,
            "agent_external_id": "OFFICER0009",
            "supervision_period_id": supervision_period.supervision_period_id,
        }

        person_entities = self.load_person_entities_dict(
            person=fake_person,
            supervision_periods=[supervision_period],
            assessments=[assessment],
            incarceration_periods=[incarceration_period],
            incarceration_sentences=[incarceration_sentence],
            supervision_sentences=[supervision_sentence],
            supervision_period_judicial_district_association=[
                supervision_period_to_judicial_district_row
            ],
            supervision_period_to_agent_association=[supervision_period_to_agent_map],
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        expected_events: List[SupervisionEvent] = [
            ProjectedSupervisionCompletionEvent(
                state_code=supervision_period.state_code,
                year=2015,
                month=5,
                event_date=date(2015, 5, 31),
                supervision_type=supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="OFFICER0009",
                supervising_district_external_id="10",
                level_1_supervision_location_external_id="10",
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                successful_completion=True,
                incarcerated_during_sentence=True,
                judicial_district_code=judicial_district_code,
                sentence_days_served=(completion_date - start_date).days,
            )
        ]

        # We have to add these expected events in this order because there is no unsorted-list equality check in the
        # Apache Beam testing utils
        expected_events.extend(
            identifier_test.expected_population_events(
                supervision_period,
                supervision_type,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                supervising_officer_external_id="OFFICER0009",
                level_1_supervision_location_external_id="10",
                judicial_district_code=judicial_district_code,
                projected_supervision_completion_date=supervision_sentence.projected_completion_date,
            )
        )

        expected_events.append(
            SupervisionTerminationEvent(
                state_code=supervision_period.state_code,
                year=supervision_period_termination_date.year,
                month=supervision_period_termination_date.month,
                event_date=supervision_period_termination_date,
                supervision_type=supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                supervising_officer_external_id="OFFICER0009",
                supervising_district_external_id="10",
                level_1_supervision_location_external_id="10",
                judicial_district_code=judicial_district_code,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                in_supervision_population_on_date=True,
            )
        )
        expected_events.append(
            identifier_test.create_start_event_from_period(
                supervision_period,
                supervising_officer_external_id="OFFICER0009",
                supervising_district_external_id="10",
                judicial_district_code=judicial_district_code,
            )
        )

        correct_output = [(fake_person.person_id, (fake_person, expected_events))]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_entities)])
            | "Identify Supervision Events"
            >> beam.ParDo(ClassifyEvents(), identifier=self.identifier)
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyEvents_withPeriodsStartingAfterDeath(self) -> None:
        """Tests the ClassifyEvents DoFn when the person has supervision
        periods after their death."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="ip1",
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        # This probation supervision period ended in a death
        termination_date = date(2017, 1, 4)
        supervision_period_with_death = (
            entities.StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=1111,
                state_code="US_XX",
                county_code="124",
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                start_date=date(2017, 1, 3),
                termination_date=termination_date,
                termination_reason=StateSupervisionPeriodTerminationReason.DEATH,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                person=fake_person,
            )
        )

        closed_post_mortem_supervision_period = entities.StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1112,
            state_code="US_XX",
            county_code="124",
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION,
            start_date=date(2017, 1, 5),
            termination_date=date(2017, 1, 6),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            person=fake_person,
        )

        open_post_mortem_supervision_period = entities.StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1113,
            state_code="US_XX",
            county_code="124",
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=date(2017, 1, 6),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            person=fake_person,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=1122,
            external_id="ss-1122",
            state_code="US_XX",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            start_date=date(2008, 11, 20),
            projected_completion_date=date(2017, 12, 31),
            supervision_periods=[
                supervision_period_with_death,
                closed_post_mortem_supervision_period,
                open_post_mortem_supervision_period,
            ],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=123,
            external_id="is-123",
            state_code="US_XX",
            start_date=date(2008, 11, 20),
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_to_agent_data = [
            {
                "state_code": "US_XX",
                "agent_id": 1010,
                "person_id": fake_person_id,
                "agent_external_id": "OFFICER0009",
                "supervision_period_id": supervision_period_with_death.supervision_period_id,
            },
            {
                "state_code": "US_XX",
                "agent_id": 1010,
                "person_id": fake_person_id,
                "agent_external_id": "OFFICER0009",
                "supervision_period_id": closed_post_mortem_supervision_period.supervision_period_id,
            },
            {
                "state_code": "US_XX",
                "agent_id": 1010,
                "person_id": fake_person_id,
                "agent_external_id": "OFFICER0009",
                "supervision_period_id": open_post_mortem_supervision_period.supervision_period_id,
            },
        ]

        supervision_period_judicial_district_association_data = [
            {
                "state_code": "US_XX",
                "person_id": fake_person_id,
                "supervision_period_id": supervision_period_with_death.supervision_period_id,
                "judicial_district_code": "XXX",
            },
            {
                "state_code": "US_XX",
                "person_id": fake_person_id,
                "supervision_period_id": closed_post_mortem_supervision_period.supervision_period_id,
                "judicial_district_code": "XXX",
            },
            {
                "state_code": "US_XX",
                "person_id": fake_person_id,
                "supervision_period_id": open_post_mortem_supervision_period.supervision_period_id,
                "judicial_district_code": "XXX",
            },
        ]

        person_entities = self.load_person_entities_dict(
            person=fake_person,
            supervision_periods=[
                supervision_period_with_death,
                closed_post_mortem_supervision_period,
                open_post_mortem_supervision_period,
            ],
            incarceration_periods=[incarceration_period],
            incarceration_sentences=[incarceration_sentence],
            supervision_sentences=[supervision_sentence],
            supervision_period_judicial_district_association=supervision_period_judicial_district_association_data,
            supervision_period_to_agent_association=supervision_period_to_agent_data,
        )

        expected_events: List[SupervisionEvent] = [
            *identifier_test.expected_population_events(
                supervision_period_with_death,
                StateSupervisionPeriodSupervisionType.PROBATION,
                supervising_officer_external_id="OFFICER0009",
                judicial_district_code="XXX",
                projected_supervision_completion_date=supervision_sentence.projected_completion_date,
            )
        ]

        expected_events.append(
            SupervisionTerminationEvent(
                state_code=supervision_period_with_death.state_code,
                year=termination_date.year,
                month=termination_date.month,
                event_date=termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period_with_death.termination_reason,
                supervising_officer_external_id="OFFICER0009",
                judicial_district_code="XXX",
                supervision_level=supervision_period_with_death.supervision_level,
                supervision_level_raw_text=supervision_period_with_death.supervision_level_raw_text,
                in_supervision_population_on_date=True,
            )
        )

        expected_events.append(
            identifier_test.create_start_event_from_period(
                supervision_period_with_death,
                supervising_officer_external_id="OFFICER0009",
                judicial_district_code="XXX",
            )
        )

        correct_output = [(fake_person.person_id, (fake_person, expected_events))]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_entities)])
            | "Identify Supervision Events"
            >> beam.ParDo(ClassifyEvents(), identifier=self.identifier)
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyEvents_withOpenPeriodStartDateCapturedByPeriodEndingInDeath(
        self,
    ) -> None:
        """Tests the ClassifyEvents DoFn when the person has open period
        within the range of the period ending in their death."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="ip1",
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        # This probation supervision period has start date within the range of the period ending in death
        open_supervision_period = entities.StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1113,
            state_code="US_XX",
            county_code="124",
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=date(2017, 1, 4),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            person=fake_person,
        )

        # This probation supervision period ended in a death
        termination_date = date(2017, 1, 5)
        supervision_period_with_death = (
            entities.StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=1111,
                state_code="US_XX",
                county_code="124",
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                start_date=date(2017, 1, 3),
                termination_date=termination_date,
                termination_reason=StateSupervisionPeriodTerminationReason.DEATH,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                person=fake_person,
            )
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=1122,
            external_id="ss-1122",
            state_code="US_XX",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            start_date=date(2017, 1, 1),
            projected_completion_date=date(2017, 1, 31),
            supervision_periods=[
                supervision_period_with_death,
                open_supervision_period,
            ],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=123,
            external_id="is-123",
            state_code="US_XX",
            start_date=date(2008, 11, 20),
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_to_agent_data = [
            {
                "state_code": "US_XX",
                "agent_id": 1010,
                "person_id": fake_person_id,
                "agent_external_id": "OFFICER0009",
                "supervision_period_id": supervision_period_with_death.supervision_period_id,
            },
            {
                "state_code": "US_XX",
                "agent_id": 1010,
                "person_id": fake_person_id,
                "agent_external_id": "OFFICER0009",
                "supervision_period_id": open_supervision_period.supervision_period_id,
            },
        ]

        supervision_period_judicial_district_association_data = [
            {
                "state_code": "US_XX",
                "person_id": fake_person_id,
                "supervision_period_id": supervision_period_with_death.supervision_period_id,
                "judicial_district_code": "XXX",
            },
            {
                "state_code": "US_XX",
                "person_id": fake_person_id,
                "supervision_period_id": open_supervision_period.supervision_period_id,
                "judicial_district_code": "XXX",
            },
        ]

        person_entities = self.load_person_entities_dict(
            person=fake_person,
            supervision_periods=[
                supervision_period_with_death,
                open_supervision_period,
            ],
            incarceration_periods=[incarceration_period],
            incarceration_sentences=[incarceration_sentence],
            supervision_sentences=[supervision_sentence],
            supervision_period_judicial_district_association=supervision_period_judicial_district_association_data,
            supervision_period_to_agent_association=supervision_period_to_agent_data,
        )

        expected_events: List[SupervisionEvent] = [
            *identifier_test.expected_population_events(
                supervision_period_with_death,
                StateSupervisionPeriodSupervisionType.PROBATION,
                supervising_officer_external_id="OFFICER0009",
                judicial_district_code="XXX",
                projected_supervision_completion_date=supervision_sentence.projected_completion_date,
            )
        ]

        expected_events.append(
            SupervisionTerminationEvent(
                state_code=supervision_period_with_death.state_code,
                year=termination_date.year,
                month=termination_date.month,
                event_date=termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period_with_death.termination_reason,
                supervising_officer_external_id="OFFICER0009",
                judicial_district_code="XXX",
                supervision_level=supervision_period_with_death.supervision_level,
                supervision_level_raw_text=supervision_period_with_death.supervision_level_raw_text,
                in_supervision_population_on_date=True,
            )
        )

        expected_events.append(
            identifier_test.create_start_event_from_period(
                supervision_period_with_death,
                supervising_officer_external_id="OFFICER0009",
                judicial_district_code="XXX",
            )
        )

        expected_events.extend(
            identifier_test.expected_population_events(
                attr.evolve(
                    open_supervision_period,
                    termination_date=supervision_period_with_death.termination_date,
                ),
                StateSupervisionPeriodSupervisionType.PROBATION,
                supervising_officer_external_id="OFFICER0009",
                judicial_district_code="XXX",
                projected_supervision_completion_date=supervision_sentence.projected_completion_date,
            )
        )

        expected_events.append(
            SupervisionTerminationEvent(
                state_code=supervision_period_with_death.state_code,
                year=termination_date.year,
                month=termination_date.month,
                event_date=termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period_with_death.termination_reason,
                supervising_officer_external_id="OFFICER0009",
                judicial_district_code="XXX",
                supervision_level=supervision_period_with_death.supervision_level,
                supervision_level_raw_text=supervision_period_with_death.supervision_level_raw_text,
                in_supervision_population_on_date=True,
            )
        )

        expected_events.append(
            identifier_test.create_start_event_from_period(
                open_supervision_period,
                supervising_officer_external_id="OFFICER0009",
                judicial_district_code="XXX",
            )
        )

        correct_output = [(fake_person.person_id, (fake_person, expected_events))]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_entities)])
            | "Identify Supervision Events"
            >> beam.ParDo(ClassifyEvents(), identifier=self.identifier)
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyEvents_NoIncarcerationPeriods(self) -> None:
        """Tests the ClassifyEvents DoFn when the person has no
        incarceration periods."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        supervision_period_termination = date(2015, 5, 29)
        supervision_period = entities.StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            state_code="US_XX",
            county_code="124",
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            start_date=date(2015, 3, 14),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            termination_date=supervision_period_termination,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.INTERNAL_UNKNOWN,
            supervision_level_raw_text="XXXX",
            supervision_site="10",
            person=fake_person,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            external_id="ss1",
            start_date=date(2015, 3, 1),
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period],
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2015, 3, 13),
        )

        supervision_period_to_agent_map = {
            "agent_id": 1010,
            "person_id": fake_person_id,
            "agent_external_id": "OFFICER0009",
            "supervision_period_id": supervision_period.supervision_period_id,
        }

        person_entities = self.load_person_entities_dict(
            person=fake_person,
            assessments=[assessment],
            supervision_periods=[supervision_period],
            supervision_sentences=[supervision_sentence],
            supervision_period_to_agent_association=[supervision_period_to_agent_map],
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events: List[SupervisionEvent] = [
            *identifier_test.expected_population_events(
                supervision_period,
                supervision_type,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                supervising_officer_external_id="OFFICER0009",
                level_1_supervision_location_external_id="10",
            )
        ]

        expected_events.extend(
            [
                SupervisionTerminationEvent(
                    state_code=supervision_period.state_code,
                    year=supervision_period_termination.year,
                    month=supervision_period_termination.month,
                    event_date=supervision_period_termination,
                    supervision_type=supervision_type,
                    case_type=StateSupervisionCaseType.GENERAL,
                    termination_reason=supervision_period.termination_reason,
                    supervising_officer_external_id="OFFICER0009",
                    supervising_district_external_id="10",
                    level_1_supervision_location_external_id="10",
                    supervision_level=supervision_period.supervision_level,
                    supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                    in_supervision_population_on_date=True,
                ),
                identifier_test.create_start_event_from_period(
                    supervision_period,
                    supervising_officer_external_id="OFFICER0009",
                    supervising_district_external_id="10",
                ),
            ]
        )

        correct_output = [(fake_person.person_id, (fake_person, expected_events))]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_entities)])
            | "Identify Supervision Events"
            >> beam.ParDo(ClassifyEvents(), identifier=self.identifier)
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyEvents_NoAssessments(self) -> None:
        """Tests the ClassifyEvents DoFn when the person has no
        assessments."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        termination_date = date(2015, 5, 29)
        supervision_period = entities.StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            state_code="US_XX",
            county_code="124",
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            start_date=date(2015, 3, 14),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            termination_date=termination_date,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.INTERNAL_UNKNOWN,
            supervision_level_raw_text="XXXX",
            supervision_site="10",
            person=fake_person,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            external_id="ss1",
            start_date=date(2015, 3, 1),
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period],
        )

        supervision_period_to_agent_map = {
            "agent_id": 1010,
            "person_id": fake_person_id,
            "agent_external_id": "OFFICER0009",
            "supervision_period_id": supervision_period.supervision_period_id,
        }

        person_entities = self.load_person_entities_dict(
            person=fake_person,
            supervision_periods=[supervision_period],
            supervision_sentences=[supervision_sentence],
            supervision_period_to_agent_association=[supervision_period_to_agent_map],
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events: List[SupervisionEvent] = [
            *identifier_test.expected_population_events(
                supervision_period,
                supervision_type,
                supervising_officer_external_id="OFFICER0009",
                level_1_supervision_location_external_id="10",
            )
        ]

        expected_events.extend(
            [
                SupervisionTerminationEvent(
                    state_code=supervision_period.state_code,
                    year=termination_date.year,
                    month=termination_date.month,
                    event_date=termination_date,
                    supervision_type=supervision_type,
                    case_type=StateSupervisionCaseType.GENERAL,
                    termination_reason=supervision_period.termination_reason,
                    supervising_officer_external_id="OFFICER0009",
                    supervising_district_external_id="10",
                    level_1_supervision_location_external_id="10",
                    supervision_level=supervision_period.supervision_level,
                    supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                    in_supervision_population_on_date=True,
                ),
                identifier_test.create_start_event_from_period(
                    supervision_period,
                    supervising_officer_external_id="OFFICER0009",
                    supervising_district_external_id="10",
                ),
            ]
        )

        correct_output = [(fake_person.person_id, (fake_person, expected_events))]

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_entities)])
            | "Identify Supervision Events"
            >> beam.ParDo(ClassifyEvents(), identifier=self.identifier)
        )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifyEvents_NoSupervisionPeriods(self) -> None:
        """Tests the ClassifyEvents DoFn when the person
        has no supervision periods."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=fake_person_id,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id="ip1",
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2015, 3, 10),
        )

        supervision_period_to_agent_map = {
            "agent_id": 1010,
            "person_id": fake_person_id,
            "agent_external_id": "OFFICER0009",
            "supervision_period_id": 9999,
        }

        person_entities = self.load_person_entities_dict(
            person=fake_person,
            assessments=[assessment],
            incarceration_periods=[incarceration_period],
            supervision_period_to_agent_association=[supervision_period_to_agent_map],
        )

        correct_output: List = []

        test_pipeline = TestPipeline()

        output = (
            test_pipeline
            | beam.Create([(fake_person_id, person_entities)])
            | "Identify Supervision Events"
            >> beam.ParDo(ClassifyEvents(), identifier=self.identifier)
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
            "recidiviz.calculator.pipeline.supervision.metric_producer.get_state_specific_supervision_delegate"
        )
        self.mock_supervision_delegate = self.supervision_delegate_patcher.start()
        self.mock_supervision_delegate.return_value = UsXxSupervisionDelegate()

        self.person_metadata = PersonMetadata(prioritized_race_or_ethnicity="BLACK")
        self.pipeline_config = pipeline.SupervisionPipeline().pipeline_config

    def tearDown(self) -> None:
        self.supervision_delegate_patcher.stop()

    def testProduceSupervisionMetrics(self) -> None:
        """Tests the ProduceSupervisionMetrics DoFn."""
        fake_person = StatePerson.new_with_defaults(
            state_code="US_XX",
            person_id=self.fake_person_id,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
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
                    "person_events": [(fake_person, supervision_time_events)],
                    "person_metadata": [self.person_metadata],
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
                calculation_end_month=None,
                calculation_month_count=-1,
                metric_inclusions=self.metric_inclusions_dict,
                pipeline_options=test_pipeline_options(),
                pipeline_config=self.pipeline_config,
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
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT,
        )

        inputs = [
            (
                self.fake_person_id,
                {
                    "person_events": [(fake_person, [])],
                    "person_metadata": [self.person_metadata],
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
                calculation_end_month=None,
                calculation_month_count=-1,
                metric_inclusions=self.metric_inclusions_dict,
                pipeline_options=test_pipeline_options(),
                pipeline_config=self.pipeline_config,
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
                calculation_end_month=None,
                calculation_month_count=-1,
                metric_inclusions=self.metric_inclusions_dict,
                pipeline_options=test_pipeline_options(),
                pipeline_config=self.pipeline_config,
            )
        )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class SupervisionPipelineFakeWriteToBigQuery(FakeWriteToBigQuery):
    def __init__(
        self,
        output_table: str,
        expected_output_metric_types: Collection[RecidivizMetricType],
        expected_violation_types: Set[ViolationType] = None,
    ):
        super().__init__(output_table, expected_output_metric_types)
        self._expected_violation_types = expected_violation_types
        self._table = output_table

    def expand(self, input_or_inputs: List[Any]) -> List[RecidivizMetric]:
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

        def _assert_source_violation_type_set(output: List[Dict[Any, Any]]) -> None:
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

        def _count_metrics(output: List[SupervisionMetric]) -> None:
            actual_combination_counts = {}

            for key in expected_metric_counts.keys():
                actual_combination_counts[key] = 0

            for metric in output:
                assert isinstance(metric, SupervisionMetric)

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

    @staticmethod
    def validate_metric_is_expected_type(
        expected_metric_type: Type[SupervisionMetric],
    ) -> Callable:
        """Asserts that the SupervisionMetric is of the expected SupervisionMetricType."""

        def _validate_metric_is_expected_type(output: List[SupervisionMetric]) -> None:
            supervision_metric = output[0]

            if not isinstance(supervision_metric, expected_metric_type):
                raise BeamAssertException(
                    f"Failed assert. Metric not of expected type {expected_metric_type}."
                )

        return _validate_metric_is_expected_type
