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
# pylint: disable=wrong-import-order

"""Tests for supervision/pipeline.py"""
import unittest

from more_itertools import one
from typing import Set, Optional, Dict, List, Any, Collection
from unittest import mock

import apache_beam as beam
import pytest
from apache_beam.pvalue import AsDict
from apache_beam.testing.util import assert_that, equal_to, BeamAssertException
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.options.pipeline_options import PipelineOptions

import datetime
from datetime import date

import attr
from freezegun import freeze_time
from mock import patch

from recidiviz.calculator.pipeline.supervision import pipeline
from recidiviz.calculator.pipeline.supervision.metrics import \
    SupervisionMetricType, SupervisionRevocationMetric, \
    SupervisionRevocationViolationTypeAnalysisMetric, SupervisionPopulationMetric, \
    SupervisionRevocationAnalysisMetric, \
    SupervisionTerminationMetric, SupervisionSuccessMetric, \
    SuccessfulSupervisionSentenceDaysServedMetric, SupervisionCaseComplianceMetric
from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import SupervisionCaseCompliance
from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import \
    NonRevocationReturnSupervisionTimeBucket, \
    RevocationReturnSupervisionTimeBucket, \
    ProjectedSupervisionCompletionBucket, SupervisionTerminationBucket, SupervisionTimeBucket
from recidiviz.calculator.pipeline.utils.metric_utils import \
    MetricMethodologyType, RecidivizMetricType
from recidiviz.calculator.pipeline.utils.person_utils import PersonMetadata, ExtractPersonEventsMetadata
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_sentence_classification import SupervisionTypeSpan
from recidiviz.common.constants.person_characteristics import Race
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType
from recidiviz.common.constants.state.state_case_type import \
    StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, \
    StateIncarcerationFacilitySecurityLevel
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodTerminationReason, StateSupervisionPeriodSupervisionType, StateSupervisionLevel
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType as ViolationType, \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType as RevocationType, \
    StateSupervisionViolationResponseRevocationType, StateSupervisionViolationResponseType
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import \
    StateIncarcerationPeriod, Gender, ResidencyStatus, \
    StateSupervisionSentence, StateAssessment, StateSupervisionViolationResponse, StateSupervisionViolation, \
    StateSupervisionViolationTypeEntry, StateIncarcerationSentence
from recidiviz.persistence.entity.state.entities import StatePerson
from recidiviz.tests.calculator.calculator_test_utils import \
    normalized_database_base_dict
from recidiviz.tests.calculator.pipeline.fake_bigquery import FakeReadFromBigQueryFactory, FakeWriteToBigQueryFactory, \
    FakeWriteToBigQuery, DataTablesDict
from recidiviz.tests.calculator.pipeline.supervision import identifier_test
from recidiviz.tests.calculator.pipeline.utils.run_pipeline_test_utils import run_test_pipeline
from recidiviz.tests.calculator.pipeline.utils.us_mo_fakes import FakeUsMoSupervisionSentence
from recidiviz.tests.persistence.database import database_test_utils

SUPERVISION_PIPELINE_PACKAGE_NAME = pipeline.__name__


class TestSupervisionPipeline(unittest.TestCase):
    """Tests the entire supervision pipeline."""

    def setUp(self) -> None:
        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()
        self.fake_bq_sink_factory = FakeWriteToBigQueryFactory(SupervisionPipelineFakeWriteToBigQuery)

        self.metric_inclusions_dict: Dict[str, bool] = {
            metric_type.value: True for metric_type in SupervisionMetricType
        }

        self.assessment_types_patcher = mock.patch(
            'recidiviz.calculator.pipeline.program.identifier.assessment_utils.'
            '_assessment_types_of_class_for_state')
        self.mock_assessment_types = self.assessment_types_patcher.start()
        self.mock_assessment_types.return_value = [StateAssessmentType.ORAS, StateAssessmentType.LSIR]

    def tearDown(self) -> None:
        self.assessment_types_patcher.stop()

    @staticmethod
    def _default_data_dict():
        return {
            schema.StatePerson.__tablename__: [],
            schema.StateIncarcerationPeriod.__tablename__: [],
            schema.StateSupervisionViolationResponse.__tablename__: [],
            schema.StateSupervisionViolation.__tablename__: [],
            schema.StateSupervisionPeriod.__tablename__: [],
            schema.StateSupervisionSentence.__tablename__: [],
            schema.StateIncarcerationSentence.__tablename__: [],
            schema.StateCharge.__tablename__: [],
            schema.state_charge_supervision_sentence_association_table.name: [],
            schema.state_charge_incarceration_sentence_association_table.name: [],
            schema.state_incarceration_sentence_incarceration_period_association_table.name: [],
            schema.state_incarceration_sentence_supervision_period_association_table.name: [],
            schema.state_supervision_sentence_incarceration_period_association_table.name: [],
            schema.state_supervision_sentence_supervision_period_association_table.name: [],
            schema.StateAssessment.__tablename__: [],
            schema.StatePersonExternalId.__tablename__: [],
            schema.StatePersonAlias.__tablename__: [],
            schema.StatePersonRace.__tablename__: [],
            schema.StatePersonEthnicity.__tablename__: [],
            schema.StateSentenceGroup.__tablename__: [],
            schema.StateProgramAssignment.__tablename__: [],
            schema.StateFine.__tablename__: [],
            schema.StateIncarcerationIncident.__tablename__: [],
            schema.StateParoleDecision.__tablename__: [],
            schema.StateSupervisionViolationTypeEntry.__tablename__: [],
            schema.StateSupervisionViolatedConditionEntry.__tablename__: [],
            schema.StateSupervisionViolationResponseDecisionEntry.__tablename__: [],
            schema.state_incarceration_period_program_assignment_association_table.name: [],
            schema.StateEarlyDischarge.__tablename__: [],
            schema.StateSupervisionContact.__tablename__: [],
            schema.state_supervision_period_supervision_violation_association_table.name: [],
            schema.state_supervision_period_program_assignment_association_table.name: [],
            schema.StateSupervisionCaseTypeEntry.__tablename__: [],
            schema.state_supervision_period_supervision_contact_association_table.name: [],
        }

    def build_supervision_pipeline_data_dict(
            self, fake_person_id: int, fake_supervision_period_id: int, fake_svr_id: int):
        """Builds a data_dict for a basic run of the pipeline."""
        fake_person = schema.StatePerson(
            state_code='US_XX',
            person_id=fake_person_id,
            gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        persons_data = [normalized_database_base_dict(fake_person)]

        fake_person_race = schema.StatePersonRace(
            state_code='US_XX',
            person_id=fake_person_id,
            race=Race.BLACK,
            race_raw_text=Race.BLACK.name
        )
        person_race_data = [normalized_database_base_dict(fake_person_race)]

        initial_incarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_XX',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2008, 11, 20),
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id
        )

        first_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_XX',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2011, 4, 5),
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id)

        subsequent_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=3333,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='US_XX',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2017, 1, 4),
            person_id=fake_person_id)

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=fake_supervision_period_id,
            state_code='US_XX',
            county_code='124',
            start_date=date(2015, 3, 14),
            termination_date=date(2016, 12, 29),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            person_id=fake_person_id
        )

        supervision_sentence = schema.StateSupervisionSentence(
            supervision_sentence_id=1122,
            state_code='US_XX',
            supervision_type=StateSupervisionType.PROBATION,
            supervision_periods=[supervision_period],
            start_date=date(2015, 3, 1),
            projected_completion_date=date(2016, 12, 31),
            completion_date=date(2016, 12, 29),
            status=StateSentenceStatus.COMPLETED,
            person_id=fake_person_id
        )

        incarceration_sentence = schema.StateIncarcerationSentence(
            incarceration_sentence_id=123,
            state_code='US_XX',
            person_id=fake_person_id,
            incarceration_periods=[initial_incarceration,
                                   subsequent_reincarceration, first_reincarceration]
        )

        supervision_sentence_supervision_period_association = [
            {
                'supervision_period_id': fake_supervision_period_id,
                'supervision_sentence_id': 1122,
            }
        ]

        charge = database_test_utils.generate_test_charge(
            person_id=fake_person_id,
            charge_id=1234523,
            court_case=None,
            bond=None
        )

        assessment = schema.StateAssessment(
            assessment_id=298374,
            state_code='US_XX',
            assessment_date=date(2015, 3, 19),
            assessment_type=StateAssessmentType.LSIR,
            person_id=fake_person_id
        )

        supervision_contact = schema.StateSupervisionContact(
            state_code='US_XX',
            contact_date=supervision_period.start_date,
            person_id=fake_person_id
        )

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration),
            normalized_database_base_dict(first_reincarceration),
            normalized_database_base_dict(subsequent_reincarceration)
        ]

        supervision_periods_data = [
            normalized_database_base_dict(supervision_period)
        ]

        supervision_sentences_data = [
            normalized_database_base_dict(supervision_sentence)
        ]

        incarceration_sentences_data = [
            normalized_database_base_dict(incarceration_sentence)
        ]

        charge_data = [
            normalized_database_base_dict(charge)
        ]

        supervision_violation_response = \
            database_test_utils.generate_test_supervision_violation_response(
                fake_person_id)

        supervision_violation = \
            database_test_utils.generate_test_supervision_violation(
                fake_person_id,
                [supervision_violation_response]
            )

        supervision_violation_data = [
            normalized_database_base_dict(supervision_violation)
        ]

        supervision_violation_response.supervision_violation_id = \
            supervision_violation.supervision_violation_id

        supervision_violation_response_data = [
            normalized_database_base_dict(supervision_violation_response)
        ]

        assessment_data = [
            normalized_database_base_dict(assessment)
        ]

        supervision_contact_data = [
            normalized_database_base_dict(supervision_contact)
        ]

        ssvr_to_agent_data = [{
            'state_code': 'US_XX',
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_violation_response_id': fake_svr_id
        }]

        supervision_period_to_agent_data = [{
            'state_code': 'US_XX',
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_period_id': fake_supervision_period_id
        }]

        supervision_period_judicial_district_association_data = [{
            'state_code': 'US_XX',
            'person_id': fake_person_id,
            'supervision_period_id': fake_supervision_period_id,
            'judicial_district_code': 'XXX',
        }]

        state_race_ethnicity_population_count_data = [{
            'state_code': 'US_XX',
            'race_or_ethnicity': 'BLACK',
            'population_count': 1,
            'representation_priority': 1
        }]

        us_mo_sentence_status_data: List[Dict[str, Any]] = [
            {'person_id': fake_person_id, 'sentence_external_id': 'is-123', 'sentence_status_external_id': 'is-123-1',
             'status_code': '10I1000', 'status_date': '20081120',
             'status_description': 'New Court Comm-Institution'},
            {'person_id': fake_person_id, 'sentence_external_id': 'is-123', 'sentence_status_external_id': 'is-123-2',
             'status_code': '40O1010', 'status_date': '20101204', 'status_description': 'Parole Release'},
            {'person_id': fake_person_id, 'sentence_external_id': 'is-123', 'sentence_status_external_id': 'is-123-2',
             'status_code': '45O1060', 'status_date': '20110405', 'status_description': 'Parole Ret-Treatment Center'},
            {'person_id': fake_person_id, 'sentence_external_id': 'is-123', 'sentence_status_external_id': 'is-123-3',
             'status_code': '40O1030', 'status_date': '20140414', 'status_description': 'Parole Re-Release'},
            {'person_id': fake_person_id, 'sentence_external_id': 'ss-1122', 'sentence_status_external_id': 'ss-1122-1',
             'status_code': '25I1000', 'status_date': '20150314', 'status_description': 'Court Probation - Addl Chg'},
            {'person_id': fake_person_id, 'sentence_external_id': 'ss-1122', 'sentence_status_external_id': 'ss-1122-2',
             'status_code': '45O7000', 'status_date': '20170104', 'status_description': 'Field to DAI-Other Sentence'},
            {'person_id': fake_person_id, 'sentence_external_id': 'is-123', 'sentence_status_external_id': 'is-123-2',
             'status_code': '45O1010', 'status_date': '20170104', 'status_description': 'Parole Ret-Tech Viol'},
        ]

        data_dict = self._default_data_dict()
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
            schema.state_supervision_sentence_supervision_period_association_table.name:
            supervision_sentence_supervision_period_association,
            schema.StateAssessment.__tablename__: assessment_data,
            schema.StateSupervisionContact.__tablename__: supervision_contact_data,
            'ssvr_to_agent_association': ssvr_to_agent_data,
            'supervision_period_to_agent_association': supervision_period_to_agent_data,
            'supervision_period_judicial_district_association': supervision_period_judicial_district_association_data,
            'state_race_ethnicity_population_counts': state_race_ethnicity_population_count_data,
            'us_mo_sentence_statuses': us_mo_sentence_status_data,
        }
        data_dict.update(data_dict_overrides)

        return data_dict

    @freeze_time('2017-01-31')
    def testSupervisionPipeline(self):
        fake_person_id = 12345
        fake_supervision_period_id = 1111
        fake_svr_id = \
            database_test_utils.generate_test_supervision_violation_response(
                fake_person_id).supervision_violation_response_id

        data_dict = self.build_supervision_pipeline_data_dict(
            fake_person_id, fake_supervision_period_id, fake_svr_id)
        dataset = 'recidiviz-123.state'

        expected_metric_types = {
            SupervisionMetricType.SUPERVISION_POPULATION,
            SupervisionMetricType.SUPERVISION_SUCCESS,
            SupervisionMetricType.SUPERVISION_TERMINATION,
            SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED
        }

        self.run_test_pipeline(dataset,
                               data_dict,
                               expected_metric_types)

    @freeze_time('2017-01-31')
    def testSupervisionPipelineWithPersonIdFilterSet(self):
        fake_person_id = 12345
        fake_supervision_period_id = 1111
        fake_svr_id = \
            database_test_utils.generate_test_supervision_violation_response(
                fake_person_id).supervision_violation_response_id

        data_dict = self.build_supervision_pipeline_data_dict(
            fake_person_id, fake_supervision_period_id, fake_svr_id)
        dataset = 'recidiviz-123.state'

        expected_metric_types = {
            SupervisionMetricType.SUPERVISION_POPULATION,
            SupervisionMetricType.SUPERVISION_SUCCESS,
            SupervisionMetricType.SUPERVISION_TERMINATION
        }

        self.run_test_pipeline(dataset,
                               data_dict,
                               expected_metric_types,
                               unifying_id_field_filter_set={fake_person_id})

    def run_test_pipeline(self,
                          dataset: str,
                          data_dict: DataTablesDict,
                          expected_metric_types: Set[SupervisionMetricType],
                          expected_violation_types: Set[ViolationType] = None,
                          unifying_id_field_filter_set: Optional[Set[int]] = None,
                          metric_types_filter: Optional[Set[str]] = None) -> None:
        """Runs a test version of the supervision pipeline."""
        read_from_bq_constructor = self.fake_bq_source_factory.create_fake_bq_source_constructor(dataset, data_dict)
        write_to_bq_constructor = self.fake_bq_sink_factory.create_fake_bq_sink_constructor(
            dataset,
            expected_output_metric_types=expected_metric_types,
            expected_violation_types=expected_violation_types
        )
        with patch(f'{SUPERVISION_PIPELINE_PACKAGE_NAME}.ReadFromBigQuery', read_from_bq_constructor):
            run_test_pipeline(
                pipeline_module=pipeline,
                state_code='US_XX',
                dataset=dataset,
                read_from_bq_constructor=read_from_bq_constructor,
                write_to_bq_constructor=write_to_bq_constructor,
                unifying_id_field_filter_set=unifying_id_field_filter_set,
                metric_types_filter=metric_types_filter
            )


    @freeze_time('2017-01-31')
    def testSupervisionPipeline_withRevocations(self):
        fake_person_id = 12345
        fake_svr_id = 56789
        fake_violation_id = 345789

        fake_person = schema.StatePerson(
            state_code='US_XX',
            person_id=fake_person_id, gender=Gender.FEMALE,
            birthdate=date(1990, 1, 1),
            residency_status=ResidencyStatus.PERMANENT
        )

        persons_data = [normalized_database_base_dict(fake_person)]

        initial_incarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_XX',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2008, 11, 20),
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED,
            person_id=fake_person_id
        )

        first_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code='US_XX',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2011, 4, 5),
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id)

        # This probation supervision period ended in a revocation
        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=1111,
            state_code='US_XX',
            county_code='124',
            start_date=date(2015, 3, 14),
            termination_date=date(2017, 1, 4),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            person_id=fake_person_id
        )

        supervision_sentence = schema.StateSupervisionSentence(
            supervision_sentence_id=1122,
            external_id='ss-1122',
            state_code='US_XX',
            supervision_type=StateSupervisionType.PROBATION,
            start_date=date(2008, 11, 20),
            projected_completion_date=date(2017, 12, 31),
            supervision_periods=[supervision_period],
            person_id=fake_person_id
        )

        incarceration_sentence = schema.StateIncarcerationSentence(
            incarceration_sentence_id=123,
            external_id='is-123',
            state_code='US_XX',
            start_date=date(2008, 11, 20),
            person_id=fake_person_id
        )

        supervision_sentence_supervision_period_association = [
            {
                'supervision_period_id': 1111,
                'supervision_sentence_id': 1122,
            }
        ]

        charge = database_test_utils.generate_test_charge(
            person_id=fake_person_id,
            state_code='US_XX',
            charge_id=1234523,
            court_case=None,
            bond=None
        )

        ssvr = schema.StateSupervisionViolationResponse(
            supervision_violation_response_id=fake_svr_id,
            state_code='US_XX',
            person_id=fake_person_id,
            revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
            supervision_violation_id=fake_violation_id
        )

        state_agent = database_test_utils.generate_test_assessment_agent()

        ssvr.decision_agents = [state_agent]

        violation_report = schema.StateSupervisionViolationResponse(
            supervision_violation_response_id=99999,
            state_code='US_XX',
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            is_draft=False,
            response_date=date(2017, 1, 1),
            person_id=fake_person_id
        )

        supervision_violation_type = schema.StateSupervisionViolationTypeEntry(
            person_id=fake_person_id,
            state_code='US_XX',
            violation_type=StateSupervisionViolationType.FELONY
        )

        supervision_violation = schema.StateSupervisionViolation(
            supervision_violation_id=fake_violation_id,
            state_code='US_XX',
            person_id=fake_person_id,
            supervision_violation_responses=[violation_report, ssvr],
            supervision_violation_types=[supervision_violation_type]
        )

        supervision_violation_type.supervision_violation_id = supervision_violation.supervision_violation_id
        violation_report.supervision_violation_id = supervision_violation.supervision_violation_id

        # This incarceration period was due to a probation revocation
        revocation_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='US_XX',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2017, 1, 4),
            person_id=fake_person_id,
            source_supervision_violation_response_id=fake_svr_id)

        assessment = schema.StateAssessment(
            assessment_id=298374,
            state_code='US_XX',
            assessment_date=date(2015, 3, 19),
            assessment_type=StateAssessmentType.LSIR,
            person_id=fake_person_id
        )

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration),
            normalized_database_base_dict(first_reincarceration),
            normalized_database_base_dict(revocation_reincarceration)
        ]

        supervision_periods_data = [
            normalized_database_base_dict(supervision_period)
        ]

        supervision_violation_type_data = [
            normalized_database_base_dict(supervision_violation_type)
        ]

        supervision_violation_response_data = [
            normalized_database_base_dict(ssvr),
            normalized_database_base_dict(violation_report)
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

        charge_data = [
            normalized_database_base_dict(charge)
        ]

        assessment_data = [
            normalized_database_base_dict(assessment)
        ]

        ssvr_to_agent_data = [{
            'state_code': 'US_XX',
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_violation_response_id': fake_svr_id
        }]

        supervision_period_to_agent_data = [{
            'state_code': 'US_XX',
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_period_id': supervision_period.supervision_period_id
        }]

        supervision_period_judicial_district_association_data = [{
            'state_code': 'US_XX',
            'person_id': fake_person_id,
            'supervision_period_id': supervision_period.supervision_period_id,
            'judicial_district_code': 'XXX',
        }]

        state_race_ethnicity_population_count_data = [{
            'state_code': 'US_XX',
            'race_or_ethnicity': 'BLACK',
            'population_count': 1,
            'representation_priority': 1
        }]

        data_dict = self._default_data_dict()
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
            schema.state_supervision_sentence_supervision_period_association_table.name:
            supervision_sentence_supervision_period_association,
            schema.StateAssessment.__tablename__: assessment_data,
            'ssvr_to_agent_association': ssvr_to_agent_data,
            'supervision_period_to_agent_association': supervision_period_to_agent_data,
            'supervision_period_judicial_district_association': supervision_period_judicial_district_association_data,
            'state_race_ethnicity_population_counts': state_race_ethnicity_population_count_data,
        }
        data_dict.update(data_dict_overrides)

        dataset = 'recidiviz-123.state'

        expected_metric_types = {
            SupervisionMetricType.SUPERVISION_POPULATION,
            SupervisionMetricType.SUPERVISION_REVOCATION,
            SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS,
            SupervisionMetricType.SUPERVISION_REVOCATION_VIOLATION_TYPE_ANALYSIS,
            SupervisionMetricType.SUPERVISION_TERMINATION
        }

        self.run_test_pipeline(dataset,
                               data_dict,
                               expected_metric_types)

    @freeze_time('2017-07-31')
    def testSupervisionPipeline_withTechnicalRevocations(self):
        fake_person_id = 562
        fake_svr_id = 5582552
        fake_violation_id = 4702488

        fake_person = schema.StatePerson(
            state_code='US_XX',
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1991, 8, 16),
            residency_status=ResidencyStatus.PERMANENT)

        persons_data = [normalized_database_base_dict(fake_person)]

        initial_supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=5876524,
            state_code='US_XX',
            county_code='US_ND_RAMSEY',
            start_date=date(2014, 10, 31),
            termination_date=date(2015, 4, 21),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            person_id=fake_person_id
        )

        initial_supervision_sentence = schema.StateSupervisionSentence(supervision_sentence_id=105109,
                                                                       state_code='US_XX',
                                                                       supervision_periods=[
                                                                           initial_supervision_period],
                                                                       projected_completion_date=date(
                                                                           2016, 10, 31),
                                                                       person_id=fake_person_id
                                                                       )

        # violation of the first probation
        ssvr_1 = schema.StateSupervisionViolationResponse(
            supervision_violation_response_id=5578679,
            state_code='US_XX',
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            person_id=fake_person_id,
            revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
            supervision_violation_id=4698615
        )
        state_agent = database_test_utils.generate_test_assessment_agent()
        ssvr_1.decision_agents = [state_agent]
        supervision_violation_type_1 = schema.StateSupervisionViolationTypeEntry(
            person_id=fake_person_id,
            state_code='US_XX',
            violation_type=StateSupervisionViolationType.FELONY,
            supervision_violation_id=4698615
        )
        supervision_violation_1 = schema.StateSupervisionViolation(
            supervision_violation_id=4698615,
            state_code='US_XX',
            person_id=fake_person_id,
            supervision_violation_responses=[ssvr_1],
            supervision_violation_types=[supervision_violation_type_1]
        )
        initial_incarceration = schema.StateIncarcerationPeriod(
            external_id='11111',
            incarceration_period_id=2499739,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code='US_XX',
            county_code=None,
            facility='NDSP',
            facility_security_level=None,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            projected_release_reason=None,
            admission_date=date(2015, 4, 28),
            release_date=date(2016, 11, 22),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            person_id=fake_person_id,
            source_supervision_violation_response_id=ssvr_1.supervision_violation_response_id
        )

        # This probation supervision period ended in a revocation
        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=5887825,
            state_code='US_XX',
            county_code='US_XX_COUNTY',
            start_date=date(2016, 11, 22),
            termination_date=date(2017, 2, 6),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            person_id=fake_person_id
        )

        supervision_sentence = schema.StateSupervisionSentence(supervision_sentence_id=116412, state_code='US_XX',
                                                               supervision_periods=[
                                                                   supervision_period],
                                                               start_date=date(
                                                                   2016, 11, 22),
                                                               completion_date=date(
                                                                   2017, 2, 6),
                                                               projected_completion_date=date(
                                                                   2017, 7, 24),
                                                               status=StateSentenceStatus.COMPLETED,
                                                               person_id=fake_person_id)

        supervision_sentence_supervision_period_association = [
            {'supervision_period_id': 5876524, 'supervision_sentence_id': 105109},
            {'supervision_period_id': 5887825, 'supervision_sentence_id': 116412},
        ]

        # This incarceration period was due to a parole revocation
        revocation_reincarceration = schema.StateIncarcerationPeriod(
            external_id='3333',
            incarceration_period_id=2508394,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code='US_XX',
            county_code=None,
            facility='NDSP',
            facility_security_level=None,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            projected_release_reason=None,
            admission_date=date(2017, 1, 31),
            release_date=date(2017, 3, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            person_id=fake_person_id,
            source_supervision_violation_response_id=fake_svr_id
        )

        # violation on parole
        ssvr_2 = schema.StateSupervisionViolationResponse(
            supervision_violation_response_id=fake_svr_id,
            state_code='US_XX',
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            person_id=fake_person_id,
            revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
            supervision_violation_id=fake_violation_id
        )
        ssvr_2.decision_agents = [state_agent]

        violation_report = schema.StateSupervisionViolationResponse(
            state_code='US_XX',
            supervision_violation_response_id=99999,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            is_draft=False,
            response_date=date(2017, 1, 1),
            person_id=fake_person_id,
            supervision_violation_id=fake_violation_id
        )

        supervision_violation_type_2 = schema.StateSupervisionViolationTypeEntry(
            person_id=fake_person_id,
            state_code='US_XX',
            violation_type=StateSupervisionViolationType.TECHNICAL,

            supervision_violation_id=fake_violation_id
        )

        supervision_violation_condition = schema.StateSupervisionViolatedConditionEntry(
            person_id=fake_person_id,
            state_code='US_XX',
            condition='DRG',
            supervision_violation_id=fake_violation_id
        )

        supervision_violation = schema.StateSupervisionViolation(
            supervision_violation_id=fake_violation_id,
            state_code='US_XX',
            person_id=fake_person_id,
            supervision_violation_responses=[violation_report, ssvr_2],
            supervision_violation_types=[supervision_violation_type_2],
            supervision_violated_conditions=[supervision_violation_condition]
        )

        assessment = schema.StateAssessment(assessment_id=298374, state_code='US_XX',
                                            assessment_date=date(2016, 12, 20),
                                            assessment_score=32, assessment_type=StateAssessmentType.LSIR,
                                            person_id=fake_person_id)

        incarceration_sentence = schema.StateIncarcerationSentence(
            incarceration_sentence_id=123,
            state_code='US_XX',
            person_id=fake_person_id,
            incarceration_periods=[
                initial_incarceration, revocation_reincarceration]
        )

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration),
            normalized_database_base_dict(revocation_reincarceration)
        ]
        supervision_periods_data = [
            normalized_database_base_dict(initial_supervision_period),
            normalized_database_base_dict(supervision_period)
        ]
        supervision_violation_type_data = [
            normalized_database_base_dict(supervision_violation_type_1),
            normalized_database_base_dict(supervision_violation_type_2)
        ]

        supervision_violation_condition_entry_data = [
            normalized_database_base_dict(supervision_violation_condition)
        ]

        supervision_violation_response_data = [
            normalized_database_base_dict(violation_report),
            normalized_database_base_dict(ssvr_1),
            normalized_database_base_dict(ssvr_2)
        ]
        supervision_violation_data = [
            normalized_database_base_dict(supervision_violation_1),
            normalized_database_base_dict(supervision_violation)
        ]
        supervision_sentences_data = [
            normalized_database_base_dict(initial_supervision_sentence),
            normalized_database_base_dict(supervision_sentence)
        ]

        incarceration_sentences_data = [
            normalized_database_base_dict(incarceration_sentence)
        ]

        assessment_data = [
            normalized_database_base_dict(assessment)
        ]

        ssvr_to_agent_data = [{
            'state_code': 'US_XX',
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_violation_response_id': fake_svr_id
        }]

        supervision_period_to_agent_data = [{
            'state_code': 'US_XX',
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_period_id': supervision_period.supervision_period_id
        }]

        supervision_period_judicial_district_association_data = [{
            'state_code': 'US_XX',
            'person_id': fake_person_id,
            'supervision_period_id': supervision_period.supervision_period_id,
            'judicial_district_code': 'XXX',
        }]

        state_race_ethnicity_population_count_data = [{
            'state_code': 'US_XX',
            'race_or_ethnicity': 'BLACK',
            'population_count': 1,
            'representation_priority': 1
        }]

        data_dict = self._default_data_dict()
        data_dict_overrides = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StateIncarcerationPeriod.__tablename__: incarceration_periods_data,
            schema.StateSupervisionViolationResponse.__tablename__: supervision_violation_response_data,
            schema.StateSupervisionViolation.__tablename__: supervision_violation_data,
            schema.StateSupervisionPeriod.__tablename__: supervision_periods_data,
            schema.StateSupervisionSentence.__tablename__: supervision_sentences_data,
            schema.StateIncarcerationSentence.__tablename__: incarceration_sentences_data,
            schema.StateSupervisionViolationTypeEntry.__tablename__: supervision_violation_type_data,
            schema.StateSupervisionViolatedConditionEntry.__tablename__: supervision_violation_condition_entry_data,
            schema.state_supervision_sentence_supervision_period_association_table.name:
            supervision_sentence_supervision_period_association,
            schema.StateAssessment.__tablename__: assessment_data,
            'ssvr_to_agent_association': ssvr_to_agent_data,
            'supervision_period_to_agent_association': supervision_period_to_agent_data,
            'supervision_period_judicial_district_association': supervision_period_judicial_district_association_data,
            'state_race_ethnicity_population_counts': state_race_ethnicity_population_count_data,
        }
        data_dict.update(data_dict_overrides)

        dataset = 'recidiviz-123.state'

        expected_metric_types = {
            SupervisionMetricType.SUPERVISION_POPULATION,
            SupervisionMetricType.SUPERVISION_SUCCESS,
            SupervisionMetricType.SUPERVISION_REVOCATION,
            SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS,
            SupervisionMetricType.SUPERVISION_REVOCATION_VIOLATION_TYPE_ANALYSIS,
            SupervisionMetricType.SUPERVISION_TERMINATION
        }

        expected_violation_types = {
            ViolationType.FELONY, ViolationType.TECHNICAL}

        self.run_test_pipeline(dataset,
                               data_dict,
                               expected_metric_types,
                               expected_violation_types)

    @freeze_time('2017-01-31')
    def testSupervisionPipeline_withMetricTypesFilter(self):
        fake_person_id = 12345
        fake_svr_id = 56789
        fake_violation_id = 345789

        fake_person = schema.StatePerson(
            state_code='US_XX',
            person_id=fake_person_id, gender=Gender.FEMALE,
            birthdate=date(1990, 1, 1),
            residency_status=ResidencyStatus.PERMANENT
        )

        persons_data = [normalized_database_base_dict(fake_person)]

        initial_incarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_XX',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2008, 11, 20),
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED,
            person_id=fake_person_id
        )

        first_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=2222,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code='US_XX',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2011, 4, 5),
            release_date=date(2014, 4, 14),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id)

        # This probation supervision period ended in a revocation
        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=1111,
            state_code='US_XX',
            county_code='124',
            start_date=date(2015, 3, 14),
            termination_date=date(2017, 1, 4),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            person_id=fake_person_id
        )

        supervision_sentence = schema.StateSupervisionSentence(
            supervision_sentence_id=1122,
            external_id='ss-1122',
            state_code='US_XX',
            supervision_type=StateSupervisionType.PROBATION,
            start_date=date(2008, 11, 20),
            projected_completion_date=date(2017, 12, 31),
            supervision_periods=[supervision_period],
            person_id=fake_person_id
        )

        incarceration_sentence = schema.StateIncarcerationSentence(
            incarceration_sentence_id=123,
            external_id='is-123',
            state_code='US_XX',
            start_date=date(2008, 11, 20),
            person_id=fake_person_id
        )

        supervision_sentence_supervision_period_association = [
            {
                'supervision_period_id': 1111,
                'supervision_sentence_id': 1122,
            }
        ]

        charge = database_test_utils.generate_test_charge(
            person_id=fake_person_id,
            state_code='US_XX',
            charge_id=1234523,
            court_case=None,
            bond=None
        )

        ssvr = schema.StateSupervisionViolationResponse(
            supervision_violation_response_id=fake_svr_id,
            state_code='US_XX',
            person_id=fake_person_id,
            revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
            supervision_violation_id=fake_violation_id
        )

        state_agent = database_test_utils.generate_test_assessment_agent()

        ssvr.decision_agents = [state_agent]

        violation_report = schema.StateSupervisionViolationResponse(
            supervision_violation_response_id=99999,
            state_code='US_XX',
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            is_draft=False,
            response_date=date(2017, 1, 1),
            person_id=fake_person_id
        )

        supervision_violation_type = schema.StateSupervisionViolationTypeEntry(
            person_id=fake_person_id,
            state_code='US_XX',
            violation_type=StateSupervisionViolationType.FELONY
        )

        supervision_violation = schema.StateSupervisionViolation(
            supervision_violation_id=fake_violation_id,
            state_code='US_XX',
            person_id=fake_person_id,
            supervision_violation_responses=[violation_report, ssvr],
            supervision_violation_types=[supervision_violation_type]
        )

        violation_report.supervision_violation_id = supervision_violation.supervision_violation_id

        # This incarceration period was due to a probation revocation
        revocation_reincarceration = schema.StateIncarcerationPeriod(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='US_XX',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2017, 1, 4),
            person_id=fake_person_id,
            source_supervision_violation_response_id=fake_svr_id)

        assessment = schema.StateAssessment(
            assessment_id=298374,
            state_code='US_XX',
            assessment_date=date(2015, 3, 19),
            assessment_type=StateAssessmentType.LSIR,
            person_id=fake_person_id
        )

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration),
            normalized_database_base_dict(first_reincarceration),
            normalized_database_base_dict(revocation_reincarceration)
        ]

        supervision_periods_data = [
            normalized_database_base_dict(supervision_period)
        ]

        supervision_violation_type_data = [
            normalized_database_base_dict(supervision_violation_type)
        ]

        supervision_violation_response_data = [
            normalized_database_base_dict(ssvr),
            normalized_database_base_dict(violation_report)
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

        charge_data = [
            normalized_database_base_dict(charge)
        ]

        assessment_data = [
            normalized_database_base_dict(assessment)
        ]

        ssvr_to_agent_data = [{
            'state_code': 'US_XX',
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_violation_response_id': fake_svr_id
        }]

        supervision_period_to_agent_data = [{
            'state_code': 'US_XX',
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_period_id': supervision_period.supervision_period_id
        }]

        supervision_period_judicial_district_association_data = [{
            'state_code': 'US_XX',
            'person_id': fake_person_id,
            'supervision_period_id': supervision_period.supervision_period_id,
            'judicial_district_code': 'XXX',
        }]

        state_race_ethnicity_population_count_data = [{
            'state_code': 'US_XX',
            'race_or_ethnicity': 'BLACK',
            'population_count': 1,
            'representation_priority': 1
        }]

        data_dict = self._default_data_dict()
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
            schema.state_supervision_sentence_supervision_period_association_table.name:
            supervision_sentence_supervision_period_association,
            schema.StateAssessment.__tablename__: assessment_data,
            'ssvr_to_agent_association': ssvr_to_agent_data,
            'supervision_period_to_agent_association': supervision_period_to_agent_data,
            'supervision_period_judicial_district_association': supervision_period_judicial_district_association_data,
            'state_race_ethnicity_population_counts': state_race_ethnicity_population_count_data,
        }
        data_dict.update(data_dict_overrides)

        dataset = 'recidiviz-123.state'

        expected_metric_types = {
            SupervisionMetricType.SUPERVISION_POPULATION,
            SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS,
            SupervisionMetricType.SUPERVISION_TERMINATION
        }

        metric_types_filter = {
            metric.value for metric in expected_metric_types
        }

        self.run_test_pipeline(dataset,
                               data_dict,
                               expected_metric_types,
                               metric_types_filter=metric_types_filter)

    @freeze_time('2019-11-26')
    def testSupervisionPipelineNoSupervision(self):
        """Tests the supervision pipeline when a person doesn't have any supervision periods."""
        fake_person_id_1 = 12345

        fake_person_1 = schema.StatePerson(
            state_code='US_XX',
            person_id=fake_person_id_1, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        fake_person_id_2 = 6789

        fake_person_2 = schema.StatePerson(
            state_code='US_XX',
            person_id=fake_person_id_2, gender=Gender.FEMALE,
            birthdate=date(1990, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        persons_data = [normalized_database_base_dict(fake_person_1),
                        normalized_database_base_dict(fake_person_2)]

        initial_incarceration_1 = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            external_id='ip1',
            state_code='US_XX',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2008, 11, 20),
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id_1)

        supervision_period__1 = schema.StateSupervisionPeriod(
            supervision_period_id=1111,
            state_code='US_XX',
            external_id='sp1',
            county_code='124',
            start_date=date(2016, 3, 14),
            termination_date=date(2016, 12, 29),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            person_id=fake_person_id_1
        )

        supervision_sentence = schema.StateSupervisionSentence(
            supervision_sentence_id=1122,
            state_code='US_XX',
            external_id='ss1',
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_periods=[supervision_period__1],
            start_date=date(2016, 3, 1),
            projected_completion_date=date(2017, 12, 31),
            completion_date=date(2016, 12, 29),
            person_id=fake_person_id_1
        )

        incarceration_sentence = schema.StateIncarcerationSentence(
            incarceration_sentence_id=123,
            state_code='US_XX',
            incarceration_periods=[
                initial_incarceration_1
            ],
            person_id=fake_person_id_1
        )

        supervision_sentence_supervision_period_association = [
            {
                'supervision_period_id': 1111,
                'supervision_sentence_id': 1122,
            }
        ]

        charge = database_test_utils.generate_test_charge(
            person_id=fake_person_id_1,
            charge_id=1234523,
            court_case=None,
            bond=None
        )

        supervision_violation_response = database_test_utils.generate_test_supervision_violation_response(
            fake_person_id_2)

        state_agent = database_test_utils.generate_test_assessment_agent()

        supervision_violation_response.decision_agents = [state_agent]

        supervision_violation = database_test_utils.generate_test_supervision_violation(
            fake_person_id_2, [supervision_violation_response])

        supervision_violation_response.supervision_violation_id = supervision_violation.supervision_violation_id

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

        charge_data = [
            normalized_database_base_dict(charge)
        ]

        first_reincarceration_2 = schema.StateIncarcerationPeriod(
            incarceration_period_id=5555,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code='US_XX',
            external_id='ip5',
            county_code='124',
            facility='San Quentin',
            facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            admission_date=date(2011, 4, 5),
            release_date=date(2014, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            source_supervision_violation_response_id=supervision_violation_response.supervision_violation_response_id,
            person_id=fake_person_id_2)

        subsequent_reincarceration_2 = \
            schema.StateIncarcerationPeriod(
                incarceration_period_id=6666,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code='US_XX',
                external_id='ip6',
                county_code='124',
                facility='San Quentin',
                facility_security_level=StateIncarcerationFacilitySecurityLevel.MAXIMUM,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                projected_release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
                admission_date=date(2018, 3, 9),
                person_id=fake_person_id_2)

        assessment = schema.StateAssessment(
            assessment_id=298374,
            state_code='US_XX',
            assessment_date=date(2015, 3, 19),
            assessment_type=StateAssessmentType.LSIR,
            person_id=fake_person_id_1
        )

        incarceration_periods_data = [
            normalized_database_base_dict(initial_incarceration_1),
            normalized_database_base_dict(first_reincarceration_2),
            normalized_database_base_dict(subsequent_reincarceration_2)
        ]

        supervision_periods_data = [
            normalized_database_base_dict(supervision_period__1)
        ]

        assessment_data = [
            normalized_database_base_dict(assessment)
        ]

        ssvr_to_agent_data = [{
            'state_code': 'US_XX',
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_violation_response_id': supervision_violation_response.supervision_violation_response_id
        }]

        supervision_period_to_agent_data = [{
            'state_code': 'US_XX',
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_period_id': supervision_period__1.supervision_period_id
        }]

        supervision_period_judicial_district_association_data = [{
            'state_code': 'US_XX',
            'person_id': fake_person_id_1,
            'supervision_period_id': supervision_period__1.supervision_period_id,
            'judicial_district_code': 'XXX',
        }]

        state_race_ethnicity_population_count_data = [{
            'state_code': 'US_XX',
            'race_or_ethnicity': 'BLACK',
            'population_count': 1,
            'representation_priority': 1
        }]

        data_dict = self._default_data_dict()
        data_dict_overrides = {
            schema.StatePerson.__tablename__: persons_data,
            schema.StateIncarcerationPeriod.__tablename__: incarceration_periods_data,
            schema.StateSupervisionViolationResponse.__tablename__: supervision_violation_response_data,
            schema.StateSupervisionViolation.__tablename__: supervision_violation_data,
            schema.StateSupervisionPeriod.__tablename__: supervision_periods_data,
            schema.StateSupervisionSentence.__tablename__: supervision_sentences_data,
            schema.StateIncarcerationSentence.__tablename__: incarceration_sentences_data,
            schema.StateCharge.__tablename__: charge_data,
            schema.state_supervision_sentence_supervision_period_association_table.name:
                supervision_sentence_supervision_period_association,
            schema.StateAssessment.__tablename__: assessment_data,
            'ssvr_to_agent_association': ssvr_to_agent_data,
            'supervision_period_to_agent_association': supervision_period_to_agent_data,
            'supervision_period_judicial_district_association': supervision_period_judicial_district_association_data,
            'state_race_ethnicity_population_counts': state_race_ethnicity_population_count_data,
        }
        data_dict.update(data_dict_overrides)
        dataset = 'recidiviz-123.state'

        expected_metric_types = {
            SupervisionMetricType.SUPERVISION_POPULATION,
            SupervisionMetricType.SUPERVISION_SUCCESS,
            SupervisionMetricType.SUPERVISION_REVOCATION,
            SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS,
            SupervisionMetricType.SUPERVISION_TERMINATION,
            SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED
        }

        self.run_test_pipeline(dataset,
                               data_dict,
                               expected_metric_types)


class TestClassifySupervisionTimeBuckets(unittest.TestCase):
    """Tests the ClassifySupervisionTimeBuckets DoFn in the pipeline."""

    def setUp(self):
        self.maxDiff = None

        self.assessment_types_patcher = mock.patch(
            'recidiviz.calculator.pipeline.supervision.identifier.assessment_utils.'
            '_assessment_types_of_class_for_state')
        self.mock_assessment_types = self.assessment_types_patcher.start()
        self.mock_assessment_types.return_value = [StateAssessmentType.ORAS, StateAssessmentType.LSIR]

    def tearDown(self) -> None:
        self.assessment_types_patcher.stop()

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
            supervision_period_judicial_district_association: List[Dict[Any, Any]] = None
    ):
        return {
            'person': [person],
            'supervision_periods': supervision_periods if supervision_periods else [],
            'assessments': assessments if assessments else [],
            'incarceration_periods': incarceration_periods if incarceration_periods else [],
            'incarceration_sentences': incarceration_sentences if incarceration_sentences else [],
            'supervision_sentences': supervision_sentences if supervision_sentences else [],
            'violation_responses': violation_responses if violation_responses else [],
            'supervision_contacts': supervision_contacts if supervision_contacts else [],
            'supervision_period_judicial_district_association': (supervision_period_judicial_district_association
                                                                 if supervision_period_judicial_district_association
                                                                 else [])
        }

    def testClassifySupervisionTimeBuckets(self):
        """Tests the ClassifySupervisionTimeBuckets DoFn."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id='ip1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code='US_XX',
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        supervision_period = entities.StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            state_code='US_XX',
            county_code='124',
            start_date=date(2015, 3, 14),
            termination_date=date(2015, 5, 29),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text='MEDM',
            person=fake_person
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            external_id='ss1',
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            start_date=date(2008, 1, 1),
            projected_completion_date=date(2015, 5, 30),
            completion_date=date(2015, 5, 29),
            supervision_periods=[supervision_period]
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=123,
            incarceration_periods=[incarceration_period]
        )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_XX',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2015, 3, 10)
        )

        judicial_district_code = 'NORTHEAST'

        supervision_period_to_judicial_district_row = {
            'person_id': fake_person_id,
            'supervision_period_id': supervision_period.supervision_period_id,
            'judicial_district_code': judicial_district_code
        }

        person_entities = self.load_person_entities_dict(
            person=fake_person,
            supervision_periods=[supervision_period],
            assessments=[assessment],
            incarceration_periods=[incarceration_period],
            incarceration_sentences=[incarceration_sentence],
            supervision_sentences=[supervision_sentence],
            supervision_period_judicial_district_association=[supervision_period_to_judicial_district_row]
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        expected_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code=supervision_period.state_code,
                year=2015, month=5,
                bucket_date=date(2015, 5, 31),
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id='OFFICER0009',
                supervising_district_external_id='10',
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                successful_completion=True,
                incarcerated_during_sentence=True,
                judicial_district_code=judicial_district_code,
                sentence_days_served=(
                    supervision_sentence.completion_date - supervision_sentence.start_date).days,
                )
        ]

        # We have to add these expected buckets in this order because there is no unsorted-list equality check in the
        # Apache Beam testing utils
        expected_buckets.extend(identifier_test.expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type,
            supervising_officer_external_id='OFFICER0009',
            supervising_district_external_id='10',
            judicial_district_code=judicial_district_code
        ))

        expected_buckets.append(SupervisionTerminationBucket(
            state_code=supervision_period.state_code,
            year=supervision_period.termination_date.year,
            month=supervision_period.termination_date.month,
            bucket_date=supervision_period.termination_date,
            supervision_type=supervision_period_supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            termination_reason=supervision_period.termination_reason,
            supervising_officer_external_id='OFFICER0009',
            supervising_district_external_id='10',
            judicial_district_code=judicial_district_code,
            supervision_level=supervision_period.supervision_level,
            supervision_level_raw_text=supervision_period.supervision_level_raw_text,
        ))

        correct_output = [(fake_person.person_id, (fake_person, expected_buckets))]

        test_pipeline = TestPipeline()

        ssvr_to_agent_map = {
            'agent_id': 000,
            'agent_external_id': 'XXX',
            'district_external_id': 'X',
            'supervision_violation_response_id': 999
        }

        ssvr_to_agent_associations = (
            test_pipeline
            | 'Create SSVR to Agent table' >>
            beam.Create([ssvr_to_agent_map])
        )

        ssvr_agent_associations_as_kv = (
            ssvr_to_agent_associations |
            'Convert SSVR to Agent table to KV tuples' >>
            beam.ParDo(pipeline.ConvertDictToKVTuple(),
                       'supervision_violation_response_id')
        )

        supervision_period_to_agent_map = {
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_period_id':
                supervision_period.supervision_period_id
        }

        supervision_period_to_agent_associations = (
            test_pipeline
            | 'Create SupervisionPeriod to Agent table' >>
            beam.Create([supervision_period_to_agent_map])
        )

        supervision_periods_to_agent_associations_as_kv = (
            supervision_period_to_agent_associations |
            'Convert SupervisionPeriod to Agent table to KV tuples' >>
            beam.ParDo(pipeline.ConvertDictToKVTuple(),
                       'supervision_period_id')
        )

        output = (test_pipeline
                  | beam.Create([(fake_person_id, person_entities)])
                  | 'Identify Supervision Time Buckets' >>
                  beam.ParDo(
                      pipeline.ClassifySupervisionTimeBuckets(),
                      AsDict(ssvr_agent_associations_as_kv),
                      AsDict(supervision_periods_to_agent_associations_as_kv))
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifySupervisionTimeBucketsRevocation(self):
        """Tests the ClassifySupervisionTimeBuckets DoFn when there is an instance of revocation."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        supervision_period = entities.StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            state_code='US_XX',
            county_code='124',
            start_date=date(2015, 3, 14),
            termination_date=date(2015, 5, 29),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text='H',
            person=fake_person
        )

        source_supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code='US_XX',
            supervision_violation_response_id=999
        )

        violation_report = StateSupervisionViolationResponse.new_with_defaults(
            state_code='US_XX',
            supervision_violation_response_id=111,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2015, 5, 24),
            supervision_violation=StateSupervisionViolation.new_with_defaults(
                state_code='US_XX',
                supervision_violation_id=123,
                supervision_violation_types=[StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code='US_XX',
                    violation_type=StateSupervisionViolationType.MISDEMEANOR
                )]
            )
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_XX',
            admission_date=date(2015, 5, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2018, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            source_supervision_violation_response=source_supervision_violation_response)

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            external_id='ss1',
            start_date=date(2015, 1, 1),
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period]
        )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_XX',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2015, 3, 10)
        )

        person_entities = self.load_person_entities_dict(
            person=fake_person,
            assessments=[assessment],
            supervision_periods=[supervision_period],
            incarceration_periods=[incarceration_period],
            supervision_sentences=[supervision_sentence],
            violation_responses=[violation_report]
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        expected_buckets: List[SupervisionTimeBucket] = identifier_test.expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            end_date=violation_report.response_date,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type,
            supervising_officer_external_id='OFFICER0009',
            supervising_district_external_id='10',
        )

        expected_buckets.extend(identifier_test.expected_non_revocation_return_time_buckets(
            attr.evolve(supervision_period, start_date=violation_report.response_date),
            supervision_period_supervision_type,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type,
            supervising_officer_external_id='OFFICER0009',
            supervising_district_external_id='10',
            response_count=1,
            most_severe_violation_type=StateSupervisionViolationType.MISDEMEANOR,
            most_severe_violation_type_subtype=StateSupervisionViolationType.MISDEMEANOR.value
        ))

        expected_buckets.extend([
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                termination_reason=supervision_period.termination_reason,
                supervising_officer_external_id='OFFICER0009',
                supervising_district_external_id='10',
                most_severe_violation_type=StateSupervisionViolationType.MISDEMEANOR,
                most_severe_violation_type_subtype=StateSupervisionViolationType.MISDEMEANOR.value,
                response_count=1
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2015, month=5,
                bucket_date=incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                most_severe_violation_type=StateSupervisionViolationType.MISDEMEANOR,
                most_severe_violation_type_subtype=StateSupervisionViolationType.MISDEMEANOR.value,
                violation_history_description='1misdemeanor',
                response_count=1,
                violation_type_frequency_counter=[['MISDEMEANOR']],
                case_type=StateSupervisionCaseType.GENERAL,
                revocation_type=RevocationType.REINCARCERATION,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                supervising_officer_external_id='OFFICER0009',
                supervising_district_external_id='10',
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text='H',
                is_on_supervision_last_day_of_month=False,
            )
        ])

        correct_output = [(fake_person.person_id, (fake_person, expected_buckets))]

        test_pipeline = TestPipeline()

        ssvr_to_agent_map = {
            'agent_id': 000,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_violation_response_id': 999
        }

        ssvr_to_agent_associations = (
            test_pipeline
            | 'Create SSVR to Agent table' >>
            beam.Create([ssvr_to_agent_map])
        )

        ssvr_agent_associations_as_kv = (
            ssvr_to_agent_associations |
            'Convert SSVR to Agent table to KV tuples' >>
            beam.ParDo(pipeline.ConvertDictToKVTuple(),
                       'supervision_violation_response_id')
        )

        supervision_period_to_agent_map = {
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_period_id':
                supervision_period.supervision_period_id
        }

        supervision_period_to_agent_associations = (
            test_pipeline
            | 'Create SupervisionPeriod to Agent table' >>
            beam.Create([supervision_period_to_agent_map])
        )

        supervision_periods_to_agent_associations_as_kv = (
            supervision_period_to_agent_associations |
            'Convert SupervisionPeriod to Agent table to KV tuples' >>
            beam.ParDo(pipeline.ConvertDictToKVTuple(),
                       'supervision_period_id')
        )

        output = (test_pipeline
                  | beam.Create([(fake_person_id, person_entities)])
                  | 'Identify Supervision Time Buckets' >>
                  beam.ParDo(
                      pipeline.ClassifySupervisionTimeBuckets(),
                      AsDict(ssvr_agent_associations_as_kv),
                      AsDict(supervision_periods_to_agent_associations_as_kv))
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifySupervisionTimeBucketsRevocation_US_MO(self):
        """Tests the ClassifySupervisionTimeBuckets DoFn when there is an instance of revocation."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        supervision_period = entities.StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            state_code='US_MO',
            county_code='124',
            start_date=date(2015, 3, 14),
            termination_date=date(2015, 5, 29),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.HIGH,
            supervision_level_raw_text='H',
            person=fake_person
        )

        source_supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code='US_MO',
            supervision_violation_response_id=999
        )

        violation_report = StateSupervisionViolationResponse.new_with_defaults(
            state_code='US_MO',
            supervision_violation_response_id=111,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype='INI',
            response_date=date(2015, 5, 24),
            supervision_violation=StateSupervisionViolation.new_with_defaults(
                state_code='US_MO',
                supervision_violation_id=123,
                supervision_violation_types=[StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code='US_MO',
                    violation_type=StateSupervisionViolationType.MISDEMEANOR
                )]
            )
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_MO',
            admission_date=date(2015, 5, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2018, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            source_supervision_violation_response=source_supervision_violation_response)

        supervision_sentence = \
            FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
                StateSupervisionSentence.new_with_defaults(
                    supervision_sentence_id=111,
                    external_id='ss1',
                    start_date=date(2015, 1, 1),
                    supervision_type=StateSupervisionType.PROBATION,
                    status=StateSentenceStatus.COMPLETED,
                    supervision_periods=[supervision_period]
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=date(2015, 1, 1),
                        end_date=None,
                        supervision_type=StateSupervisionType.PROBATION
                    )
                ]
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_MO',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2015, 3, 10)
        )

        person_entities = self.load_person_entities_dict(
            person=fake_person,
            assessments=[assessment],
            supervision_periods=[supervision_period],
            incarceration_periods=[incarceration_period],
            supervision_sentences=[supervision_sentence],
            violation_responses=[violation_report]
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets: List[SupervisionTimeBucket] = identifier_test.expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            end_date=violation_report.response_date,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type,
            supervising_officer_external_id='OFFICER0009',
            supervising_district_external_id='10',
        )

        expected_buckets.extend(identifier_test.expected_non_revocation_return_time_buckets(
            attr.evolve(supervision_period, start_date=violation_report.response_date),
            supervision_period_supervision_type,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type,
            supervising_officer_external_id='OFFICER0009',
            supervising_district_external_id='10',
            response_count=1,
            most_severe_violation_type=StateSupervisionViolationType.MISDEMEANOR,
            most_severe_violation_type_subtype=StateSupervisionViolationType.MISDEMEANOR.value,
        ))

        expected_buckets.extend([
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                termination_reason=supervision_period.termination_reason,
                supervising_officer_external_id='OFFICER0009',
                supervising_district_external_id='10',
                response_count=1,
                most_severe_violation_type=StateSupervisionViolationType.MISDEMEANOR,
                most_severe_violation_type_subtype=StateSupervisionViolationType.MISDEMEANOR.value,
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2015, month=5,
                bucket_date=incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                most_severe_violation_type=StateSupervisionViolationType.MISDEMEANOR,
                most_severe_violation_type_subtype=StateSupervisionViolationType.MISDEMEANOR.value,
                response_count=1,
                violation_history_description='1misd',
                violation_type_frequency_counter=[['MISDEMEANOR']],
                case_type=StateSupervisionCaseType.GENERAL,
                revocation_type=RevocationType.REINCARCERATION,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                supervising_officer_external_id='OFFICER0009',
                supervising_district_external_id='10',
                supervision_level=StateSupervisionLevel.HIGH,
                supervision_level_raw_text='H',
                is_on_supervision_last_day_of_month=False,
            )
        ])

        correct_output = [(fake_person.person_id, (fake_person, expected_buckets))]

        test_pipeline = TestPipeline()

        ssvr_to_agent_map = {
            'agent_id': 000,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_violation_response_id': 999
        }

        ssvr_to_agent_associations = (
            test_pipeline
            | 'Create SSVR to Agent table' >>
            beam.Create([ssvr_to_agent_map])
        )

        ssvr_agent_associations_as_kv = (
            ssvr_to_agent_associations |
            'Convert SSVR to Agent table to KV tuples' >>
            beam.ParDo(pipeline.ConvertDictToKVTuple(),
                       'supervision_violation_response_id')
        )

        supervision_period_to_agent_map = {
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_period_id':
                supervision_period.supervision_period_id
        }

        supervision_period_to_agent_associations = (
            test_pipeline
            | 'Create SupervisionPeriod to Agent table' >>
            beam.Create([supervision_period_to_agent_map])
        )

        supervision_periods_to_agent_associations_as_kv = (
            supervision_period_to_agent_associations |
            'Convert SupervisionPeriod to Agent table to KV tuples' >>
            beam.ParDo(pipeline.ConvertDictToKVTuple(),
                       'supervision_period_id')
        )

        output = (test_pipeline
                  | beam.Create([(fake_person_id, person_entities)])
                  | 'Identify Supervision Time Buckets' >>
                  beam.ParDo(
                      pipeline.ClassifySupervisionTimeBuckets(),
                      AsDict(ssvr_agent_associations_as_kv),
                      AsDict(supervision_periods_to_agent_associations_as_kv))
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifySupervisionTimeBuckets_NoIncarcerationPeriods(self):
        """Tests the ClassifySupervisionTimeBuckets DoFn when the person has no
        incarceration periods."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        supervision_period = entities.StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            state_code='US_XX',
            county_code='124',
            start_date=date(2015, 3, 14),
            termination_date=date(2015, 5, 29),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.INTERNAL_UNKNOWN,
            supervision_level_raw_text='XXXX',
            person=fake_person
        )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                external_id='ss1',
                start_date=date(2015, 3, 1),
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period]
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_XX',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2015, 3, 13)
        )

        person_entities = self.load_person_entities_dict(
            person=fake_person,
            assessments=[assessment],
            supervision_periods=[supervision_period],
            supervision_sentences=[supervision_sentence],
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets: List[SupervisionTimeBucket] = identifier_test.expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type,
            supervising_officer_external_id='OFFICER0009',
            supervising_district_external_id='10',
        )

        expected_buckets.extend([
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                supervising_officer_external_id='OFFICER0009',
                supervising_district_external_id='10',
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text
            )
        ])

        correct_output = [(fake_person.person_id, (fake_person, expected_buckets))]

        test_pipeline = TestPipeline()

        ssvr_to_agent_map = {
            'agent_id': 000,
            'agent_external_id': 'XXX',
            'district_external_id': 'X',
            'supervision_violation_response_id': 999
        }

        ssvr_to_agent_associations = (
            test_pipeline
            | 'Create SSVR to Agent table' >>
            beam.Create([ssvr_to_agent_map])
        )

        ssvr_agent_associations_as_kv = (
            ssvr_to_agent_associations |
            'Convert SSVR to Agent table to KV tuples' >>
            beam.ParDo(pipeline.ConvertDictToKVTuple(),
                       'supervision_violation_response_id')
        )

        supervision_period_to_agent_map = {
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_period_id':
                supervision_period.supervision_period_id
        }

        supervision_period_to_agent_associations = (
            test_pipeline
            | 'Create SupervisionPeriod to Agent table' >>
            beam.Create([supervision_period_to_agent_map])
        )

        supervision_periods_to_agent_associations_as_kv = (
            supervision_period_to_agent_associations |
            'Convert SupervisionPeriod to Agent table to KV tuples' >>
            beam.ParDo(pipeline.ConvertDictToKVTuple(),
                       'supervision_period_id')
        )

        output = (test_pipeline
                  | beam.Create([(fake_person_id, person_entities)])
                  | 'Identify Supervision Time Buckets' >>
                  beam.ParDo(
                      pipeline.ClassifySupervisionTimeBuckets(),
                      AsDict(ssvr_agent_associations_as_kv),
                      AsDict(supervision_periods_to_agent_associations_as_kv))
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifySupervisionTimeBuckets_NoAssessments(self):
        """Tests the ClassifySupervisionTimeBuckets DoFn when the person has no
        assessments."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        supervision_period = entities.StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            state_code='US_XX',
            county_code='124',
            start_date=date(2015, 3, 14),
            termination_date=date(2015, 5, 29),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.INTERNAL_UNKNOWN,
            supervision_level_raw_text='XXXX',
            person=fake_person
        )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                external_id='ss1',
                start_date=date(2015, 3, 1),
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period]
            )

        person_entities = self.load_person_entities_dict(
            person=fake_person,
            supervision_periods=[supervision_period],
            supervision_sentences=[supervision_sentence],
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets: List[SupervisionTimeBucket] = identifier_test.expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            supervising_officer_external_id='OFFICER0009',
            supervising_district_external_id='10',
        )

        expected_buckets.extend([
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                supervising_officer_external_id='OFFICER0009',
                supervising_district_external_id='10',
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text
            )
        ])

        correct_output = [(fake_person.person_id, (fake_person, expected_buckets))]

        test_pipeline = TestPipeline()

        ssvr_to_agent_map = {
            'agent_id': 000,
            'agent_external_id': 'XXX',
            'district_external_id': 'X',
            'supervision_violation_response_id': 999
        }

        ssvr_to_agent_associations = (
            test_pipeline
            | 'Create SSVR to Agent table' >>
            beam.Create([ssvr_to_agent_map])
        )

        ssvr_agent_associations_as_kv = (
            ssvr_to_agent_associations |
            'Convert SSVR to Agent table to KV tuples' >>
            beam.ParDo(pipeline.ConvertDictToKVTuple(),
                       'supervision_violation_response_id')
        )

        supervision_period_to_agent_map = {
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_period_id':
                supervision_period.supervision_period_id
        }

        supervision_period_to_agent_associations = (
            test_pipeline
            | 'Create SupervisionPeriod to Agent table' >>
            beam.Create([supervision_period_to_agent_map])
        )

        supervision_periods_to_agent_associations_as_kv = (
            supervision_period_to_agent_associations |
            'Convert SupervisionPeriod to Agent table to KV tuples' >>
            beam.ParDo(pipeline.ConvertDictToKVTuple(),
                       'supervision_period_id')
        )

        output = (test_pipeline
                  | beam.Create([(fake_person_id, person_entities)])
                  | 'Identify Supervision Time Buckets' >>
                  beam.ParDo(
                      pipeline.ClassifySupervisionTimeBuckets(),
                      AsDict(ssvr_agent_associations_as_kv),
                      AsDict(supervision_periods_to_agent_associations_as_kv))
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()

    def testClassifySupervisionTimeBuckets_NoSupervisionPeriods(self):
        """Tests the ClassifySupervisionTimeBuckets DoFn when the person
        has no supervision periods."""
        fake_person_id = 12345

        fake_person = StatePerson.new_with_defaults(
            person_id=fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            external_id='ip1',
            incarceration_period_id=1111,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code='US_XX',
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.
            NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.
            SENTENCE_SERVED)

        assessment = StateAssessment.new_with_defaults(
            state_code='US_XX',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2015, 3, 10)
        )

        person_entities = self.load_person_entities_dict(
            person=fake_person,
            assessments=[assessment],
            incarceration_periods=[incarceration_period],
        )

        correct_output = []

        test_pipeline = TestPipeline()

        ssvr_to_agent_map = {
            'agent_id': 000,
            'agent_external_id': 'XXX',
            'district_external_id': 'X',
            'supervision_violation_response_id': 999
        }

        ssvr_to_agent_associations = (
            test_pipeline
            | 'Create SSVR to Agent table' >>
            beam.Create([ssvr_to_agent_map])
        )

        ssvr_agent_associations_as_kv = (
            ssvr_to_agent_associations |
            'Convert SSVR to Agent table to KV tuples' >>
            beam.ParDo(pipeline.ConvertDictToKVTuple(),
                       'supervision_violation_response_id')
        )

        supervision_period_to_agent_map = {
            'agent_id': 1010,
            'agent_external_id': 'OFFICER0009',
            'district_external_id': '10',
            'supervision_period_id': 9999
        }

        supervision_period_to_agent_associations = (
            test_pipeline
            | 'Create SupervisionPeriod to Agent table' >>
            beam.Create([supervision_period_to_agent_map])
        )

        supervision_periods_to_agent_associations_as_kv = (
            supervision_period_to_agent_associations |
            'Convert SupervisionPeriod to Agent table to KV tuples' >>
            beam.ParDo(pipeline.ConvertDictToKVTuple(),
                       'supervision_period_id')
        )

        output = (test_pipeline
                  | beam.Create([(fake_person_id, person_entities)])
                  | 'Identify Supervision Time Buckets' >>
                  beam.ParDo(
                      pipeline.ClassifySupervisionTimeBuckets(),
                      AsDict(ssvr_agent_associations_as_kv),
                      AsDict(supervision_periods_to_agent_associations_as_kv))
                  )

        assert_that(output, equal_to(correct_output))

        test_pipeline.run()


class TestCalculateSupervisionMetricCombinations(unittest.TestCase):
    """Tests the CalculateSupervisionMetricCombinations DoFn in the pipeline."""

    def setUp(self) -> None:
        self.fake_person_id = 12345

        self.metric_inclusions_dict: Dict[SupervisionMetricType, bool] = {
            metric_type: True for metric_type in SupervisionMetricType
        }

        self.person_metadata = PersonMetadata(prioritized_race_or_ethnicity='BLACK')

    def testCalculateSupervisionMetricCombinations(self):
        """Tests the CalculateSupervisionMetricCombinations DoFn."""
        fake_person = StatePerson.new_with_defaults(
            person_id=self.fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='CA',
                year=2015, month=3,
                bucket_date=date(2015, 3, 31),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MINIMUM,
                supervision_level_raw_text='MIN',
                is_on_supervision_last_day_of_month=True,
                case_compliance=SupervisionCaseCompliance(
                    date_of_evaluation=date(2015, 3, 31),
                    assessment_up_to_date=True
                )
            ),
        ]

        # Each characteristic combination will be tracked for each of the months and the two methodology types
        expected_population_metric_count = len(supervision_time_buckets) * 2
        expected_compliance_metric_count = len(supervision_time_buckets) * 2

        expected_combination_counts = \
            {SupervisionMetricType.SUPERVISION_POPULATION.value: expected_population_metric_count,
             SupervisionMetricType.SUPERVISION_COMPLIANCE.value: expected_compliance_metric_count}

        test_pipeline = TestPipeline()

        inputs = [(self.fake_person_id, {
            'person_events': [(fake_person, supervision_time_buckets)],
            'person_metadata': [self.person_metadata]
        })]

        output = (test_pipeline
                  | beam.Create(inputs)
                  | beam.ParDo(ExtractPersonEventsMetadata())
                  | 'Calculate Supervision Metrics' >>
                  beam.ParDo(pipeline.CalculateSupervisionMetricCombinations(),
                             calculation_end_month=None,
                             calculation_month_count=-1,
                             metric_inclusions=self.metric_inclusions_dict)
                  )

        assert_that(output, AssertMatchers.
                    count_combinations(expected_combination_counts),
                    'Assert number of metrics is expected value')

        test_pipeline.run()

    def testCalculateSupervisionMetricCombinations_withRevocations(self):
        """Tests the CalculateSupervisionMetricCombinations DoFn where
        revocations are identified."""
        fake_person = StatePerson.new_with_defaults(
            person_id=self.fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        supervision_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code='CA', year=2015, month=2,
                bucket_date=date(2015, 2, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                revocation_type=RevocationType.REINCARCERATION,
                source_violation_type=ViolationType.TECHNICAL,
                is_on_supervision_last_day_of_month=False),
            RevocationReturnSupervisionTimeBucket(
                state_code='CA', year=2015, month=3,
                bucket_date=date(2015, 3, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                revocation_type=RevocationType.REINCARCERATION,
                source_violation_type=ViolationType.TECHNICAL,
                is_on_supervision_last_day_of_month=False),
        ]

        # Multiply by the number of months and by 2 (to account for methodology)
        expected_metric_count = len(supervision_time_buckets) * 2

        expected_combination_counts = {
            SupervisionMetricType.SUPERVISION_REVOCATION.value: expected_metric_count,
            SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS.value: expected_metric_count,
            SupervisionMetricType.SUPERVISION_POPULATION.value: expected_metric_count
        }

        test_pipeline = TestPipeline()

        inputs = [(self.fake_person_id, {
            'person_events': [(fake_person, supervision_time_buckets)],
            'person_metadata': [self.person_metadata]
        })]

        output = (test_pipeline
                  | beam.Create(inputs)
                  | beam.ParDo(ExtractPersonEventsMetadata())
                  | 'Calculate Supervision Metrics' >>
                  beam.ParDo(pipeline.CalculateSupervisionMetricCombinations(),
                             calculation_end_month=None,
                             calculation_month_count=-1,
                             metric_inclusions=self.metric_inclusions_dict)
                  )

        assert_that(output, AssertMatchers.count_combinations(expected_combination_counts),
                    'Assert number of metrics is expected value')

        test_pipeline.run()

    def testCalculateSupervisionMetricCombinations_NoSupervision(self):
        """Tests the CalculateSupervisionMetricCombinations when there are
        no supervision months. This should never happen because any person
        without supervision time is dropped entirely from the pipeline."""
        fake_person = StatePerson.new_with_defaults(
            person_id=self.fake_person_id, gender=Gender.MALE,
            birthdate=date(1970, 1, 1),
            residency_status=ResidencyStatus.PERMANENT)

        inputs = [(self.fake_person_id, {
            'person_events': [(fake_person, [])],
            'person_metadata': [self.person_metadata]
        })]

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create(inputs)
                  | beam.ParDo(ExtractPersonEventsMetadata())
                  | 'Calculate Supervision Metrics' >>
                  beam.ParDo(pipeline.CalculateSupervisionMetricCombinations(),
                             calculation_end_month=None,
                             calculation_month_count=-1,
                             metric_inclusions=self.metric_inclusions_dict)
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()

    def testCalculateSupervisionMetricCombinations_NoInput(self):
        """Tests the CalculateSupervisionMetricCombinations when there is
        no input to the function."""

        test_pipeline = TestPipeline()

        output = (test_pipeline
                  | beam.Create([])
                  | beam.ParDo(ExtractPersonEventsMetadata())
                  | 'Calculate Supervision Metrics' >>
                  beam.ParDo(pipeline.CalculateSupervisionMetricCombinations(),
                             calculation_end_month=None,
                             calculation_month_count=-1,
                             metric_inclusions=self.metric_inclusions_dict)
                  )

        assert_that(output, equal_to([]))

        test_pipeline.run()


class TestProduceSupervisionMetrics(unittest.TestCase):
    """Tests the ProduceSupervisionMetrics DoFn in the pipeline."""

    def testProduceSupervisionMetrics(self):
        metric_value_to_metric_type = {
            SupervisionMetricType.SUPERVISION_TERMINATION: SupervisionTerminationMetric,
            SupervisionMetricType.SUPERVISION_COMPLIANCE: SupervisionCaseComplianceMetric,
            SupervisionMetricType.SUPERVISION_POPULATION: SupervisionPopulationMetric,
            SupervisionMetricType.SUPERVISION_REVOCATION: SupervisionRevocationMetric,
            SupervisionMetricType.SUPERVISION_REVOCATION_ANALYSIS: SupervisionRevocationAnalysisMetric,
            SupervisionMetricType.SUPERVISION_REVOCATION_VIOLATION_TYPE_ANALYSIS:
                SupervisionRevocationViolationTypeAnalysisMetric,
            SupervisionMetricType.SUPERVISION_SUCCESS: SupervisionSuccessMetric,
            SupervisionMetricType.SUPERVISION_SUCCESSFUL_SENTENCE_DAYS_SERVED:
                SuccessfulSupervisionSentenceDaysServedMetric
        }

        for metric_type in SupervisionMetricType:
            assert metric_type in metric_value_to_metric_type.keys()

        metric_key_dict = {'gender': Gender.MALE,
                           'methodology': MetricMethodologyType.PERSON,
                           'year': 1999,
                           'month': 3,
                           'state_code': 'CA'}

        value = 1

        all_pipeline_options = PipelineOptions().get_all_options()

        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        for metric_value, metric_type in metric_value_to_metric_type.items():
            metric_key_dict['metric_type'] = metric_value

            test_pipeline = TestPipeline()

            output = (test_pipeline
                      | f"Create PCollection for {metric_value}" >> beam.Create([(metric_key_dict, value)])
                      | f"Produce {metric_type}" >>
                      beam.ParDo(pipeline.ProduceSupervisionMetrics(), **all_pipeline_options))

            assert_that(
                output, AssertMatchers.validate_metric_is_expected_type(metric_type))

            test_pipeline.run()

    def testProduceSupervisionMetrics_EmptyMetric(self):
        metric_key_dict = {}

        value = 1

        test_pipeline = TestPipeline()

        all_pipeline_options = PipelineOptions().get_all_options()

        job_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H_%M_%S.%f')
        all_pipeline_options['job_timestamp'] = job_timestamp

        # This should never happen, and we want the pipeline to fail loudly if it does.
        with pytest.raises(ValueError):
            _ = (test_pipeline
                 | beam.Create([(metric_key_dict, value)])
                 | 'Produce Supervision Metric' >>
                 beam.ParDo(pipeline.ProduceSupervisionMetrics(),
                            **all_pipeline_options)
                 )

            test_pipeline.run()


class SupervisionPipelineFakeWriteToBigQuery(FakeWriteToBigQuery):
    def __init__(self,
                 output_table: str,
                 expected_output_metric_types: Collection[RecidivizMetricType],
                 expected_violation_types: Set[ViolationType] = None):
        super().__init__(output_table, expected_output_metric_types)
        self._expected_violation_types = expected_violation_types
        self._table = output_table

    def expand(self, input_or_inputs):
        ret = super().expand(input_or_inputs)

        if self._expected_violation_types:
            for expected_violation_type in self._expected_violation_types:
                assert_that(input_or_inputs,
                            AssertMatchers.assert_source_violation_type_set(expected_violation_type),
                            f"Assert source violation {expected_violation_type} type is set")
        return ret


class AssertMatchers:
    """Functions to be used by Apache Beam testing `assert_that` functions to
    validate pipeline outputs."""

    @staticmethod
    def assert_source_violation_type_set(expected_violation: ViolationType):
        """Asserts that there are some revocation metrics with the source_violation_type set."""
        def _assert_source_violation_type_set(output):
            if not output:
                return

            metric_type = one({metric['metric_type'] for metric in output})
            if metric_type != SupervisionMetricType.SUPERVISION_REVOCATION.value:
                return

            with_violation_types = [metric
                                    for metric in output if metric['source_violation_type'] == expected_violation.value]

            if len(with_violation_types) == 0:
                raise BeamAssertException(
                    'No metrics with source violation type {} set.'.format(expected_violation))

        return _assert_source_violation_type_set

    @staticmethod
    def count_combinations(expected_combination_counts):
        """Asserts that the number of metric combinations matches the expected
        counts."""
        def _count_combinations(output):
            actual_combination_counts = {}

            for key in expected_combination_counts.keys():
                actual_combination_counts[key] = 0

            for result in output:
                combination_dict, _ = result

                metric_type = combination_dict.get('metric_type')

                actual_combination_counts[metric_type.value] = actual_combination_counts[metric_type.value] + 1

            for key in expected_combination_counts:
                if expected_combination_counts[key] != \
                        actual_combination_counts[key]:
                    raise BeamAssertException('Failed assert. Count does not match expected value:'
                                              f'{expected_combination_counts[key]} != {actual_combination_counts[key]}')

        return _count_combinations

    @staticmethod
    def validate_metric_is_expected_type(expected_metric_type):
        """Asserts that the SupervisionMetric is of the expected SupervisionMetricType."""
        def _validate_metric_is_expected_type(output):
            supervision_metric = output[0]

            if not isinstance(supervision_metric, expected_metric_type):
                raise BeamAssertException(
                    f"Failed assert. Metric not of expected type {expected_metric_type}.")

        return _validate_metric_is_expected_type
