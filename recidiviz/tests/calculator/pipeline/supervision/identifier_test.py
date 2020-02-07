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
# pylint: disable=unused-import,wrong-import-order,protected-access

"""Tests for supervision/identifier.py."""

from datetime import date

import unittest
from freezegun import freeze_time

from recidiviz.calculator.pipeline.supervision import identifier
from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import \
    NonRevocationReturnSupervisionTimeBucket, \
    RevocationReturnSupervisionTimeBucket,\
    ProjectedSupervisionCompletionBucket, SupervisionTerminationBucket
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType, StateAssessmentLevel
from recidiviz.common.constants.state.state_case_type import \
    StateSupervisionCaseType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason, \
    StateIncarcerationPeriodReleaseReason as ReleaseReason, \
    StateIncarcerationPeriodStatus
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus, StateSupervisionPeriodTerminationReason
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType, \
    StateSupervisionViolationResponseDecision, \
    StateSupervisionViolationResponseType
from recidiviz.persistence.entity.state.entities import \
    StateSupervisionPeriod, StateIncarcerationPeriod, \
    StateSupervisionViolationResponse, StateSupervisionViolation, \
    StateSupervisionViolationTypeEntry, \
    StateSupervisionViolationResponseDecisionEntry, StateSupervisionSentence, \
    StateAssessment, StateSupervisionCaseTypeEntry, StateSupervisionViolatedConditionEntry

DEFAULT_SSVR_AGENT_ASSOCIATIONS = {
    999: {
        'agent_id': 000,
        'agent_external_id': 'XXX',
        'district_external_id': 'X',
        'supervision_violation_response_id': 999
    }
}

DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS = {
    999: {
        'agent_id': 000,
        'agent_external_id': 'XXX',
        'district_external_id': 'X',
        'supervision_period_id': 999
    }
}


# TODO(2732): Implement more full test coverage of the officer and district
#  functionality and the supervision success classification
class TestClassifySupervisionTimeBuckets(unittest.TestCase):
    """Tests for the find_supervision_time_buckets function."""

    def test_find_supervision_time_buckets(self):
        """Tests the find_supervision_time_buckets function for a single
        supervision period with no incarceration periods."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                case_type_entries=[
                    StateSupervisionCaseTypeEntry.new_with_defaults(
                        state_code='UT',
                        case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE
                    )
                ],
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=
                StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period]
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10)
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = []
        assessments = [assessment]
        violation_responses = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, supervision_periods,
            incarceration_periods, assessments,
            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(len(supervision_time_buckets), 6)

        self.assertEqual(supervision_time_buckets, [
            ProjectedSupervisionCompletionBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018,
                month=5,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                successful_completion=True
            ),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                most_severe_violation_type_subtype='UNSET',
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=4,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                most_severe_violation_type_subtype='UNSET',
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                most_severe_violation_type_subtype='UNSET',
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                most_severe_violation_type_subtype='UNSET',
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type),
            SupervisionTerminationBucket.for_month(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason
            )
        ])

    def test_find_supervision_time_buckets_overlaps_year(self):
        """Tests the find_supervision_time_buckets function for a single
        supervision period with no incarceration periods, where the supervision
        period overlaps two calendar years."""
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2019, 1, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = []
        assessments = []

        violation_responses = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, supervision_periods,
            incarceration_periods, assessments,
            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(len(supervision_time_buckets), 14)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=4,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=6,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=7,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=8,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=9,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=10,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=11,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=12,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2019, month=1,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2019,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason
            )
        ])

    def test_find_supervision_time_buckets_two_supervision_periods(self):
        """Tests the find_supervision_time_buckets function for two supervision
        periods with no incarceration periods."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2019, 8, 5),
                termination_date=date(2019, 12, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[first_supervision_period,
                                     second_supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period,
                               second_supervision_period]
        incarceration_periods = []
        assessments = []
        violation_responses = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, supervision_periods,
            incarceration_periods, assessments,
            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(12, len(supervision_time_buckets))

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=3,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=4,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=5,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=first_supervision_period.state_code,
                year=2018,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                supervision_type=first_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason
            ),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2019, month=8,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2019, month=9,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2019, month=10,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2019, month=11,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2019, month=12,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=second_supervision_period.state_code,
                year=2019,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                supervision_type=second_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason
            )
        ])

    def test_find_supervision_time_buckets_overlapping_supervision_periods(self):
        """Tests the find_supervision_time_buckets function for two supervision
        periods with no incarceration periods, where the supervision
        periods are of the same type and have overlapping months."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 4, 15),
                termination_date=date(2018, 7, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[first_supervision_period,
                                     second_supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period,
                               second_supervision_period]
        incarceration_periods = []
        assessments = []
        violation_responses = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, supervision_periods,
            incarceration_periods, assessments,
            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(len(supervision_time_buckets), 11)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=3,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=4,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=5,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=first_supervision_period.state_code,
                year=2018,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                supervision_type=first_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason
            ),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code, year=2018, month=4,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code, year=2018, month=5,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code, year=2018, month=6,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code, year=2018, month=7,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=second_supervision_period.state_code, year=2018,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                supervision_type=second_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason
            ),
        ])

    def test_find_supervision_time_buckets_overlapping_periods_different_types(self):
        """Tests the find_supervision_time_buckets function for two supervision
        periods with no incarceration periods, where the supervision
        periods are of different types and have overlapping months."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 4, 15),
                termination_date=date(2018, 7, 19),
                supervision_type=StateSupervisionType.PAROLE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[first_supervision_period,
                                     second_supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period,
                               second_supervision_period]
        incarceration_periods = []
        assessments = []
        violation_responses = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, supervision_periods,
            incarceration_periods, assessments,
            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(len(supervision_time_buckets), 11)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=3,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=4,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=5,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=first_supervision_period.state_code,
                year=2018,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                supervision_type=first_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason
            ),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2018, month=4,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2018, month=5,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2018, month=6,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2018, month=7,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=second_supervision_period.state_code,
                year=2018,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                supervision_type=second_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason
            ),
        ])

    def test_find_supervision_time_buckets_multiple_periods(self):
        """Tests the find_supervision_time_buckets function for two supervision
        periods with two incarceration periods."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2017, 3, 5),
                termination_date=date(2017, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='UT',
                admission_date=date(2017, 5, 25),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2017, 8, 3),
                release_reason=ReleaseReason.SENTENCE_SERVED
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 8, 5),
                termination_date=date(2018, 12, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=222,
                state_code='UT',
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                admission_date=date(2018, 12, 25),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2019, 3, 3),
                release_reason=ReleaseReason.SENTENCE_SERVED
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[first_supervision_period,
                                     second_supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period,
                               second_supervision_period]
        incarceration_periods = [first_incarceration_period,
                                 second_incarceration_period]
        assessments = []
        violation_responses = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, supervision_periods,
            incarceration_periods, assessments,
            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(len(supervision_time_buckets), 12)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2017, month=3,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2017, month=4,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2017, month=5,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=first_supervision_period.state_code,
                year=2017,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                supervision_type=first_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason
            ),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2018, month=8,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2018, month=9,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2018, month=10,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2018, month=11,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2018, month=12,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=second_supervision_period.state_code,
                year=2018,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                supervision_type=second_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason
            ),
        ])

    def test_find_supervision_time_buckets_multiple_admissions_in_month(self):
        """Tests the find_supervision_time_buckets function for a supervision
        period with two incarceration periods with admission dates in the
        same month."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2017, 3, 5),
                termination_date=date(2017, 5, 9),
                supervision_type=StateSupervisionType.PROBATION
            )

        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='UT',
                admission_date=date(2017, 5, 11),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2017, 5, 15),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE
            )

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=222,
                state_code='UT',
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                admission_date=date(2017, 5, 17),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2019, 3, 3),
                release_reason=ReleaseReason.SENTENCE_SERVED
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[first_supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period]
        incarceration_periods = [first_incarceration_period,
                                 second_incarceration_period]
        assessments = []
        violation_responses = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, supervision_periods,
            incarceration_periods, assessments,
            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(len(supervision_time_buckets), 5)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2017, month=3,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2017, month=4,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2017, month=5,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=first_supervision_period.state_code,
                year=2017,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                supervision_type=first_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason
            ),
        ])

    def test_find_supervision_time_buckets_incarceration_overlaps_periods(self):
        """Tests the find_supervision_time_buckets function for two supervision
        periods with an incarceration period that overlaps the end of one
        supervision period and the start of another."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2017, 3, 5),
                termination_date=date(2017, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='UT',
                admission_date=date(2017, 5, 15),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2017, 9, 20),
                release_reason=ReleaseReason.SENTENCE_SERVED
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2017, 8, 5),
                termination_date=date(2017, 12, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[first_supervision_period,
                                     second_supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period,
                               second_supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, supervision_periods,
            incarceration_periods, assessments,
            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(11, len(supervision_time_buckets))

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2017, month=3,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2017, month=4,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2017, month=5,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=first_supervision_period.state_code,
                year=2017,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                supervision_type=first_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason
            ),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2017, month=9,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2017, month=10,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2017, month=11,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2017, month=12,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=first_supervision_period.state_code,
                year=2017,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                supervision_type=second_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason
            ),
        ])

    def test_find_supervision_time_buckets_return_next_month(self):
        """Tests the find_supervision_time_buckets function
        when there is an incarceration period with a revocation admission
        the month after the supervision period's termination_date."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='UT',
                admission_date=date(2018, 6, 25),
                admission_reason=AdmissionReason.PROBATION_REVOCATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, supervision_periods,
            incarceration_periods, assessments,
            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )
        self.assertEqual(len(supervision_time_buckets), 6)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=4,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason
            ),
            RevocationReturnSupervisionTimeBucket.for_month(
                incarceration_period.state_code,
                year=2018, month=6,
                supervision_type=StateSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                most_severe_violation_type_subtype='UNSET',
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            RevocationReturnSupervisionTimeBucket.for_year(
                incarceration_period.state_code,
                year=2018, supervision_type=StateSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                most_severe_violation_type_subtype='UNSET',
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
        ])

    def test_find_supervision_time_buckets_return_next_year(self):
        """Tests the find_supervision_time_buckets function
        when there is an incarceration period with a revocation admission
        the month after the supervision period's termination_date, where the
        revocation admission happens in the next year."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 10, 5),
                termination_date=date(2018, 12, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='UT',
                admission_date=date(2019, 1, 5),
                admission_reason=AdmissionReason.PROBATION_REVOCATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, supervision_periods,
            incarceration_periods, assessments,
            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(len(supervision_time_buckets), 7)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=10,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=11,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=12,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018, supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason
            ),
            RevocationReturnSupervisionTimeBucket.for_month(
                incarceration_period.state_code,
                year=2019, month=1,
                supervision_type=StateSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                most_severe_violation_type_subtype='UNSET',
                revocation_type=
                StateSupervisionViolationResponseRevocationType.
                REINCARCERATION),
            RevocationReturnSupervisionTimeBucket.for_year(
                incarceration_period.state_code,
                year=2019,
                supervision_type=StateSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                most_severe_violation_type_subtype='UNSET',
                revocation_type=
                StateSupervisionViolationResponseRevocationType.
                REINCARCERATION),
        ])

    def test_find_supervision_time_buckets_return_months_later(self):
        """Tests the find_supervision_time_buckets function
        when there is an incarceration period with a revocation admission
        months after the supervision period's termination_date."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='UT',
                admission_date=date(2018, 10, 25),
                admission_reason=AdmissionReason.PROBATION_REVOCATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, supervision_periods,
            incarceration_periods, assessments,
            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(len(supervision_time_buckets), 6)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=4,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason
            ),
            RevocationReturnSupervisionTimeBucket.for_month(
                incarceration_period.state_code,
                year=2018, month=10,
                supervision_type=StateSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                most_severe_violation_type_subtype='UNSET',
                revocation_type=
                StateSupervisionViolationResponseRevocationType.
                REINCARCERATION),
            RevocationReturnSupervisionTimeBucket.for_year(
                incarceration_period.state_code,
                year=2018, supervision_type=StateSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                most_severe_violation_type_subtype='UNSET',
                revocation_type=
                StateSupervisionViolationResponseRevocationType.
                REINCARCERATION)
        ])

    def test_find_supervision_time_buckets_return_years_later(self):
        """Tests the find_supervision_time_buckets function
        when there is an incarceration period with a revocation admission
        years after the supervision period's termination_date."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2015, 3, 5),
                termination_date=date(2015, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='UT',
                admission_date=date(2017, 10, 25),
                admission_reason=AdmissionReason.PROBATION_REVOCATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, supervision_periods,
            incarceration_periods, assessments,
            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(len(supervision_time_buckets), 7)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2015, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2015, month=4,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2015, month=5,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2015,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason
            ),
            RevocationReturnSupervisionTimeBucket.for_month(
                incarceration_period.state_code,
                year=2017, month=10,
                supervision_type=StateSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                most_severe_violation_type_subtype='UNSET',
                revocation_type=
                StateSupervisionViolationResponseRevocationType.
                REINCARCERATION),
            RevocationReturnSupervisionTimeBucket.for_year(
                incarceration_period.state_code,
                year=2017, supervision_type=StateSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                most_severe_violation_type_subtype='UNSET',
                revocation_type=
                StateSupervisionViolationResponseRevocationType.
                REINCARCERATION),
        ])

    def test_find_supervision_time_buckets_multiple_periods_revocations(self):
        """Tests the find_supervision_time_buckets function
        when the person is revoked and returned to supervision twice in one
        year."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 12, 19),
                supervision_type=StateSupervisionType.PAROLE
            )

        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='UT',
                admission_date=date(2018, 3, 25),
                admission_reason=AdmissionReason.PAROLE_REVOCATION,
                release_date=date(2018, 8, 2),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE
            )

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='UT',
                admission_date=date(2018, 9, 25),
                admission_reason=AdmissionReason.PAROLE_REVOCATION,
                release_date=date(2018, 12, 2),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[first_supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period]
        incarceration_periods = [first_incarceration_period,
                                 second_incarceration_period]
        assessments = []
        violation_responses = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, supervision_periods,
            incarceration_periods, assessments,
            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )
        self.assertEqual(len(supervision_time_buckets), 10)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=1,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=2,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=3,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=8,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=9,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=12,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=first_supervision_period.state_code,
                year=2018,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=first_supervision_period.state_code,
                year=2018,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                first_incarceration_period.state_code,
                year=2018,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                supervision_type=first_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason
            ),
        ])

    def test_find_supervision_time_buckets_multiple_sentences_revocations(self):
        """Tests the find_supervision_time_buckets function
        when the person is revoked and returned to supervision twice in one
        year, and they have multiple supervision sentences."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 12, 19),
                supervision_type=StateSupervisionType.PAROLE
            )

        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='UT',
                admission_date=date(2018, 3, 25),
                admission_reason=AdmissionReason.PAROLE_REVOCATION,
                release_date=date(2018, 8, 2),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE
            )

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='UT',
                admission_date=date(2018, 9, 25),
                admission_reason=AdmissionReason.PAROLE_REVOCATION,
                release_date=date(2018, 12, 2),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=234,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2019, 1, 1),
                termination_date=date(2019, 1, 19),
                supervision_type=StateSupervisionType.PAROLE
            )

        first_supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[first_supervision_period]
            )

        second_supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=222,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[second_supervision_period]
            )

        supervision_sentences = [first_supervision_sentence,
                                 second_supervision_sentence]
        supervision_periods = [first_supervision_period,
                               second_supervision_period]
        incarceration_periods = [first_incarceration_period,
                                 second_incarceration_period]
        assessments = []
        violation_responses = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, supervision_periods,
            incarceration_periods, assessments,
            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(len(supervision_time_buckets), 13)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=1,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=2,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=3,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=8,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=9,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018, month=12,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=first_supervision_period.state_code,
                year=2018,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=first_supervision_period.state_code,
                year=2018,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                first_incarceration_period.state_code,
                year=2018,
                supervision_type=first_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                supervision_type=first_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason
            ),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2019, month=1,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL
            ),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=second_supervision_period.state_code,
                year=2019,
                supervision_type=second_supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL
            ),
            SupervisionTerminationBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                supervision_type=second_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason
            )
        ])

    def test_find_supervision_time_buckets_placeholders(self):
        """Tests the find_supervision_time_buckets function
        when there are placeholder supervision periods that should be dropped
        from the calculations."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_period_placeholder = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
                state_code='UT',
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='UT',
                admission_date=date(2018, 5, 25),
                admission_reason=AdmissionReason.PROBATION_REVOCATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period,
                                     supervision_period_placeholder]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, supervision_periods,
            incarceration_periods, assessments,
            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(len(supervision_time_buckets), 5)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=4,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                incarceration_period.state_code,
                year=2018,
                supervision_type=StateSupervisionType.PROBATION,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason
            ),
        ])

    def test_find_supervision_time_buckets_filter_state_code(self):
        """Tests the find_supervision_time_buckets function where the state_code
        does not match the state_code filter."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=
                StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_UT',
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period]
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_UT',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2018, 3, 10)
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = []
        assessments = [assessment]

        violation_responses = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            state_code='US_CA'
        )

        self.assertEqual(len(supervision_time_buckets), 0)
        self.assertEqual(supervision_time_buckets, [])

    def test_find_supervision_time_buckets_include_state(self):
        """Tests the find_supervision_time_buckets function where the state_code
        matches the state_code filter."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_CA',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=
                StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_CA',
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period]
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2018, 3, 10)
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = []
        assessments = [assessment]
        violation_responses = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            state_code='US_CA'
        )

        self.assertEqual(len(supervision_time_buckets), 6)

        self.assertEqual(supervision_time_buckets, [
            ProjectedSupervisionCompletionBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=5, supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL, successful_completion=True
            ),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code, year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=assessment.assessment_score,
                assessment_type=assessment.assessment_type),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=4,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=assessment.assessment_score,
                assessment_type=assessment.assessment_type),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=assessment.assessment_score,
                assessment_type=assessment.assessment_type),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=assessment.assessment_score,
                assessment_type=assessment.assessment_type),
            SupervisionTerminationBucket.for_month(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason
            )
        ])

    def test_remove_non_revocation_return_supervision_time_buckets(self):
        """Test that the appropriate NonRevocationReturnSupervisionTimeBuckets are removed from the list"""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_CA',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION
        )

        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code, year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=4,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL),
            # One time bucket to remove
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL),
            # One time bucket to remove
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason
            )
        ]
        output_supervision_time_buckets = identifier.remove_non_revocation_return_supervision_time_buckets(
            supervision_time_buckets,
            2018
        )

        self.assertEqual(output_supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code, year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=4,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL),
            SupervisionTerminationBucket.for_month(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason
            )
        ])


class TestFindMonthsForSupervisionPeriod(unittest.TestCase):
    """Tests for the find_months_for_supervision_period function."""

    def test_find_time_buckets_for_supervision_period_revocation(self):
        """Tests the find_time_buckets_for_supervision_period function
        when there is an incarceration period with a revocation admission
        in the same month as the supervision period's termination_date.
        Also ensures that the correct revocation_type, violation_type,
        supervising_officer_external_id, and supervising_district_external_id
        are set on the RevocationReturnSupervisionTimeBucket."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_MO',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_MO',
            violation_date=date(2018, 4, 20),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    violation_type=StateSupervisionViolationType.TECHNICAL
                ),
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    violation_type=StateSupervisionViolationType.FELONY
                )
            ]
        )

        source_supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=999,
            response_date=date(2018, 4, 23),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                )
            ],
            supervision_violation=supervision_violation
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='UT',
            admission_date=date(2018, 5, 25),
            admission_reason=AdmissionReason.PROBATION_REVOCATION,
            source_supervision_violation_response=source_supervision_violation_response
        )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2018, 3, 10)
        )

        indexed_incarceration_periods = identifier.index_incarceration_periods_by_admission_month(
            [incarceration_period])

        months_of_incarceration = identifier._identify_months_of_incarceration([incarceration_period])

        assessments = [assessment]
        violation_responses = [source_supervision_violation_response]

        supervision_time_buckets = identifier.find_time_buckets_for_supervision_period(
            supervision_period,
            indexed_incarceration_periods,
            months_of_incarceration,
            assessments,

            violation_responses,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(len(supervision_time_buckets), 4)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                case_type=StateSupervisionCaseType.GENERAL,
                most_severe_violation_type=None,
                most_severe_violation_type_subtype='UNSET',
                response_count=0
            ),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=4,
                supervision_type=supervision_period.supervision_type,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                case_type=StateSupervisionCaseType.GENERAL,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type_subtype='UNSET',
                response_count=1
            ),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                source_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type_subtype='UNSET',
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
                violation_history_description='1fel;1tech',
                supervising_officer_external_id=r'XXX',
                supervising_district_external_id='X'),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                source_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type_subtype='UNSET',
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
                violation_history_description='1fel;1tech',
                supervising_officer_external_id=r'XXX',
                supervising_district_external_id='X'),
        ])

    def test_us_mo_find_time_buckets_for_supervision_period_technical_revocation_with_subtype(self):
        """Tests the find_time_buckets_for_supervision_period function when there is an incarceration period with a
        revocation admission in the same month as the supervision period's termination_date. Also ensures that the
        correct revocation_type, violation_type, violation_subytype, supervising_officer_external_id, and
        supervising_district_external_id are set on the RevocationReturnSupervisionTimeBucket."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_MO',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_violation = \
            StateSupervisionViolation.new_with_defaults(
                supervision_violation_id=123455,
                violation_date=date(2018, 2, 20),
                state_code='US_MO',
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.TECHNICAL
                    ),
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(condition='DRG')]
            )

        source_supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=999,
                response_date=date(2018, 2, 23),
                supervision_violation_response_decisions=[
                    StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                        revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION
                    ),
                    StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                        revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION)
                ],
                supervision_violation=supervision_violation
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='US_MO',
                admission_date=date(2018, 5, 25),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                source_supervision_violation_response=source_supervision_violation_response
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2018, 3, 10)
        )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month([incarceration_period])

        months_of_incarceration = identifier._identify_months_of_incarceration([incarceration_period])

        assessments = [assessment]
        violation_responses = [source_supervision_violation_response]

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_period,
                indexed_incarceration_periods,
                months_of_incarceration,
                assessments,
                violation_responses,
                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        self.assertEqual(len(supervision_time_buckets), 4)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                case_type=StateSupervisionCaseType.GENERAL,
                most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
                most_severe_violation_type_subtype='SUBSTANCE_ABUSE',
                response_count=1),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=4,
                supervision_type=supervision_period.supervision_type,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                case_type=StateSupervisionCaseType.GENERAL,
                most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
                most_severe_violation_type_subtype='SUBSTANCE_ABUSE',
                response_count=1),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                source_violation_type=StateSupervisionViolationType.TECHNICAL,
                most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
                most_severe_violation_type_subtype='SUBSTANCE_ABUSE',
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
                violation_history_description='1subs',
                supervising_officer_external_id=r'XXX',
                supervising_district_external_id='X'),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                source_violation_type=StateSupervisionViolationType.TECHNICAL,
                most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
                most_severe_violation_type_subtype='SUBSTANCE_ABUSE',
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
                violation_history_description='1subs',
                supervising_officer_external_id=r'XXX',
                supervising_district_external_id='X'),
        ])

    def test_find_time_buckets_for_supervision_period_nested_revocation(self):
        """Tests the find_time_buckets_for_supervision_period function
        when there is an incarceration period with a revocation admission,
        a stay in prison, and a continued supervision period after release
        from incarceration."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 12, 10),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 5, 25),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2018, 10, 27)
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period])

        months_of_incarceration = identifier._identify_months_of_incarceration(
            [incarceration_period])

        assessments = []

        violation_reports = []

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_period, indexed_incarceration_periods,
                months_of_incarceration,
                assessments, violation_reports,
                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        self.assertEqual(len(supervision_time_buckets), 8)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=4,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=10,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=11,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=12,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
        ])

    def test_find_time_buckets_for_supervision_period_revocation_before_termination(self):
        """Tests the find_time_buckets_for_supervision_period function
        when there is an incarceration period with a revocation admission
        before the supervision period's termination_date, but in the same
        month as the termination_date."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 5, 3),
                admission_reason=AdmissionReason.PROBATION_REVOCATION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period])

        months_of_incarceration = identifier._identify_months_of_incarceration(
            [incarceration_period])

        assessments = []

        violation_reports = []

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_period, indexed_incarceration_periods,
                months_of_incarceration,
                assessments, violation_reports,
                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        self.assertEqual(len(supervision_time_buckets), 4)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=4,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018, supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL)
        ])

    def test_find_time_buckets_for_supervision_period_revocation_before_termination_month(self):
        """Tests the find_time_buckets_for_supervision_period function
        when there is an incarceration period with a revocation admission
        before the supervision period's termination_date, and in a different
        month as the termination_date."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 6, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 5, 10),
                admission_reason=AdmissionReason.PROBATION_REVOCATION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period])

        months_of_incarceration = identifier._identify_months_of_incarceration(
            [incarceration_period])

        assessments = []

        violation_reports = []

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_period, indexed_incarceration_periods,
                months_of_incarceration,
                assessments, violation_reports,
                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        self.assertEqual(len(supervision_time_buckets), 4)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=4,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018, supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL)
        ])

    def test_find_time_buckets_for_supervision_period_revocation_no_termination(self):
        """Tests the find_time_buckets_for_supervision_period function
        when there is an incarceration period with a revocation admission,
        where there is no termination_date on the supervision_period, and
        no release_date on the incarceration_period."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2003, 7, 5),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2003, 10, 10),
                admission_reason=AdmissionReason.PROBATION_REVOCATION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period])

        months_of_incarceration = identifier._identify_months_of_incarceration(
            [incarceration_period])

        assessments = []

        violation_reports = []

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_period, indexed_incarceration_periods,
                months_of_incarceration,
                assessments, violation_reports,
                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        self.assertEqual(len(supervision_time_buckets), 5)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2003, month=7,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2003, month=8,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2003, month=9,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2003, month=10,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2003,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL)
        ])

    @freeze_time('2019-11-03')
    def test_find_time_buckets_for_supervision_period_nested_revocation_no_termination(self):
        """Tests the find_time_buckets_for_supervision_period function
        when there is an incarceration period with a revocation admission,
        a stay in prison, and a continued supervision period after release
        from incarceration that has still not terminated."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2019, 3, 5),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2019, 5, 25),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2019, 10, 17)
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period])

        months_of_incarceration = identifier._identify_months_of_incarceration(
            [incarceration_period])
        assessments = []

        violation_reports = []

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_period, indexed_incarceration_periods,
                months_of_incarceration,
                assessments, violation_reports,
                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        self.assertEqual(len(supervision_time_buckets), 7)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2019, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2019, month=4,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2019, month=5,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2019, month=10,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2019, month=11,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2019,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2019,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
        ])

    @freeze_time('2019-09-04')
    def test_find_time_buckets_for_supervision_period_admission_today(self):
        """Tests the find_time_buckets_for_supervision_period function
        when there is an incarceration period with a revocation admission today,
        where there is no termination_date on the supervision_period, and
        no release_date on the incarceration_period."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2019, 6, 2),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date.today(),
                admission_reason=AdmissionReason.PROBATION_REVOCATION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period])

        months_of_incarceration = identifier._identify_months_of_incarceration(
            [incarceration_period])

        assessments = []

        violation_reports = []

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_period, indexed_incarceration_periods,
                months_of_incarceration,
                assessments, violation_reports,
                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        self.assertEqual(len(supervision_time_buckets), 5)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2019, month=6,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2019, month=7,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2019, month=8,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2019, month=9,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2019,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL)
        ])

    def test_find_time_buckets_for_supervision_period_admission_no_revocation(self):
        """Tests the find_time_buckets_for_supervision_period function
        when there is an incarceration period with a non-revocation admission
        before the supervision period's termination_date."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 6, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 6, 2),
                admission_reason=AdmissionReason.NEW_ADMISSION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period])

        months_of_incarceration = identifier._identify_months_of_incarceration(
            [incarceration_period])

        assessments = []

        violation_reports = []

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_period, indexed_incarceration_periods,
                months_of_incarceration,
                assessments, violation_reports,
                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        self.assertEqual(len(supervision_time_buckets), 5)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=4,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=6,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL)
        ])

    def test_find_time_buckets_for_supervision_period_multiple_incarcerations_in_year(self):
        """Tests the find_time_buckets_for_supervision_period function
        when there are multiple incarceration periods in the year of
        supervision."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 6, 19),
                supervision_type=StateSupervisionType.PAROLE
            )

        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 6, 2),
                admission_reason=AdmissionReason.PAROLE_REVOCATION,
                release_date=date(2018, 9, 3),
                release_reason=date(2018, 10, 30)
            )

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=222,
                state_code='UT',
                admission_date=date(2018, 11, 17),
                admission_reason=AdmissionReason.PAROLE_REVOCATION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [first_incarceration_period, second_incarceration_period])

        months_of_incarceration = identifier._identify_months_of_incarceration(
            [first_incarceration_period, second_incarceration_period])

        assessments = []

        violation_reports = []

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_period, indexed_incarceration_periods,
                months_of_incarceration,
                assessments, violation_reports,
                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        self.assertEqual(len(supervision_time_buckets), 5)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=4,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=6,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018, supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL)
        ])

    def test_find_time_buckets_for_supervision_period_multiple_incarcerations_overlap(
            self):
        """Tests the find_time_buckets_for_supervision_period function when
        there are multiple incarceration periods in the year of supervision,
        and the supervision overlaps both incarcerations."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 12, 30),
                supervision_type=StateSupervisionType.PROBATION
            )

        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 6, 2),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2018, 9, 3),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE
            )

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=222,
                state_code='UT',
                admission_date=date(2018, 11, 17),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2018, 12, 3),
                release_reason=ReleaseReason.SENTENCE_SERVED
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [first_incarceration_period, second_incarceration_period])

        months_of_incarceration = identifier._identify_months_of_incarceration(
            [first_incarceration_period, second_incarceration_period])

        assessments = []

        violation_reports = []

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_period, indexed_incarceration_periods,
                months_of_incarceration,
                assessments, violation_reports,
                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        self.assertEqual(len(supervision_time_buckets), 11)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=4,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=6,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=9,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=10,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=11,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=12,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018, supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL)
        ])

    def test_find_time_buckets_for_supervision_period_multiple_years(
            self):
        """Tests the find_time_buckets_for_supervision_period function when
        the supervision period overlaps multiple years, and there is an
        incarceration period during this time that also overlaps multiple
        years."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2008, 3, 5),
                termination_date=date(2010, 3, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2008, 6, 2),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2009, 12, 3),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [first_incarceration_period])

        months_of_incarceration = identifier._identify_months_of_incarceration(
            [first_incarceration_period])

        assessments = []

        violation_reports = []

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_period, indexed_incarceration_periods,
                months_of_incarceration,
                assessments, violation_reports,
                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        self.assertEqual(len(supervision_time_buckets), 11)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2008, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2008, month=4,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2008, month=5,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2008, month=6,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2009, month=12,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2010, month=1,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2010, month=2,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2010, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2008,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2009,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2010,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
        ])

    def test_find_time_buckets_for_supervision_period_ends_on_first(self):
        """Tests the find_time_buckets_for_supervision_period function for a
        supervision period with no incarceration periods, where the supervision
        period ends on the 1st day of a month."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2001, 1, 5),
                termination_date=date(2001, 7, 1),
                supervision_type=StateSupervisionType.PROBATION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [])

        months_of_incarceration = identifier._identify_months_of_incarceration(
            [])

        assessments = []

        violation_reports = []

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_period, indexed_incarceration_periods,
                months_of_incarceration,
                assessments, violation_reports,
                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        self.assertEqual(len(supervision_time_buckets), 8)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2001, month=1,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2001, month=2,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2001, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2001, month=4,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2001, month=5,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2001, month=6,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2001, month=7,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2001,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL)
        ])

    def test_find_time_buckets_for_supervision_period_multiple_assessments(self):
        """Tests the find_time_buckets_for_supervision_period function
        when there is an incarceration period with a revocation admission,
        a stay in prison, a continued supervision period after release
        from incarceration, and multiple assessments over this time period."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 12, 10),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 5, 25),
                admission_reason=AdmissionReason.PROBATION_REVOCATION,
                release_date=date(2018, 10, 27)
            )

        assessment_1 = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10)
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=24,
            assessment_level=StateAssessmentLevel.MEDIUM,
            assessment_date=date(2018, 11, 17)
        )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period])

        months_of_incarceration = identifier._identify_months_of_incarceration(
            [incarceration_period])

        assessments = [assessment_1, assessment_2]

        violation_responses = []

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_period, indexed_incarceration_periods,
                months_of_incarceration,
                assessments, violation_responses,
                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        self.assertEqual(len(supervision_time_buckets), 8)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                assessment_score=assessment_1.assessment_score,
                assessment_level=assessment_1.assessment_level,
                assessment_type=assessment_1.assessment_type,
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=4,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                assessment_score=assessment_1.assessment_score,
                assessment_level=assessment_1.assessment_level,
                assessment_type=assessment_1.assessment_type,
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                supervision_type=supervision_period.supervision_type,
                assessment_score=assessment_1.assessment_score,
                assessment_level=assessment_1.assessment_level,
                assessment_type=assessment_1.assessment_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=10,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                assessment_score=assessment_1.assessment_score,
                assessment_level=assessment_1.assessment_level,
                assessment_type=assessment_1.assessment_type,
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=11,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                assessment_score=assessment_2.assessment_score,
                assessment_level=assessment_2.assessment_level,
                assessment_type=assessment_2.assessment_type,
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=12,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                assessment_score=assessment_2.assessment_score,
                assessment_level=assessment_2.assessment_level,
                assessment_type=assessment_2.assessment_type,
                case_type=StateSupervisionCaseType.GENERAL),
            RevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018,
                supervision_type=supervision_period.supervision_type,
                assessment_score=assessment_2.assessment_score,
                assessment_level=assessment_2.assessment_level,
                assessment_type=assessment_2.assessment_type,
                most_severe_violation_type_subtype='UNSET',
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                assessment_score=assessment_2.assessment_score,
                assessment_level=assessment_2.assessment_level,
                assessment_type=assessment_2.assessment_type,
                case_type=StateSupervisionCaseType.GENERAL),
        ])

    def test_find_time_buckets_for_supervision_period_assessment_year_before(self):
        """Tests the find_time_buckets_for_supervision_period function
        when there is no revocation and the assessment date is the year before
        the start of the supervision."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                state_code='UT',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 3, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=24,
            assessment_date=date(2017, 12, 17)
        )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [])

        months_of_incarceration = identifier._identify_months_of_incarceration(
            [])

        assessments = [assessment]

        violation_responses = []

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_period, indexed_incarceration_periods,
                months_of_incarceration,
                assessments, violation_responses,
                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        self.assertEqual(len(supervision_time_buckets), 4)

        self.assertEqual(supervision_time_buckets, [
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=1,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=2,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018, month=3,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                case_type=StateSupervisionCaseType.GENERAL),
            NonRevocationReturnSupervisionTimeBucket.for_year(
                state_code=supervision_period.state_code,
                year=2018,
                supervision_type=supervision_period.supervision_type,
                most_severe_violation_type_subtype='UNSET',
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                case_type=StateSupervisionCaseType.GENERAL),
        ])


class TestIndexIncarcerationPeriodsByAdmissionMonth(unittest.TestCase):
    """Tests the index_incarceration_periods_by_admission_month function."""

    def test_index_incarceration_periods_by_admission_month(self):
        """Tests the index_incarceration_periods_by_admission_month function."""

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 6, 8),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2018, 12, 21)
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [incarceration_period]
            )

        self.assertEqual(indexed_incarceration_periods, {
            2018: {
                6: [incarceration_period]
            }
        })

    def test_index_incarceration_periods_by_admission_month_multiple(self):
        """Tests the index_incarceration_periods_by_admission_month function
        when there are multiple incarceration periods."""

        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 6, 8),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2018, 12, 21)
            )

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2019, 3, 2),
                admission_reason=AdmissionReason.NEW_ADMISSION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [first_incarceration_period, second_incarceration_period]
            )

        self.assertEqual(indexed_incarceration_periods, {
            2018: {
                6: [first_incarceration_period]
            },
            2019: {
                3: [second_incarceration_period]
            }
        })

    def test_index_incarceration_periods_by_admission_month_multiple_month(self):
        """Tests the index_incarceration_periods_by_admission_month function
        when there are multiple incarceration periods with admission dates
        in the same month."""

        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 6, 1),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2018, 6, 21)
            )

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 6, 30),
                admission_reason=AdmissionReason.NEW_ADMISSION
            )

        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month(
                [first_incarceration_period, second_incarceration_period]
            )

        self.assertEqual(indexed_incarceration_periods, {
            2018: {
                6: [first_incarceration_period, second_incarceration_period]
            },
        })

    def test_index_incarceration_periods_by_admission_month_none(self):
        """Tests the index_incarceration_periods_by_admission_month function
        when there are no incarceration periods."""
        indexed_incarceration_periods = \
            identifier.index_incarceration_periods_by_admission_month([])

        self.assertEqual(indexed_incarceration_periods, {})


class TestIdentifyMonthsOfIncarceration(unittest.TestCase):
    """Tests the identify_months_of_incarceration function."""

    def test_identify_months_of_incarceration_incarcerated(self):
        """Tests the identify_months_of_incarceration function."""
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 6, 8),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2018, 12, 21)
            )

        months_incarcerated = \
            identifier._identify_months_of_incarceration([incarceration_period])

        self.assertEqual(months_incarcerated, {
            (2018, 7), (2018, 8), (2018, 9), (2018, 10), (2018, 11)
        })

    def test_identify_months_of_incarceration_incarcerated_on_first(self):
        """Tests the identify_months_of_incarceration function where the person
        was incarcerated on the first of the month."""
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 8, 1),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2018, 12, 21)
            )

        months_incarcerated = \
            identifier._identify_months_of_incarceration([incarceration_period])

        self.assertEqual(months_incarcerated, {
            (2018, 8), (2018, 9), (2018, 10), (2018, 11)
        })

    def test_identify_months_of_incarceration_released_last_day(self):
        """Tests the identify_months_of_incarceration function where the person
        was released on the last day of a month."""
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2018, 8, 15),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2018, 10, 31)
            )

        months_incarcerated = \
            identifier._identify_months_of_incarceration([incarceration_period])

        self.assertEqual(months_incarcerated, {
            (2018, 9), (2018, 10)
        })

    def test_identify_months_of_incarceration_no_full_months(self):
        """Tests the identify_months_of_incarceration function where the person
        was not incarcerated for a full month."""
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(2013, 3, 1),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(2013, 3, 30)
            )

        months_incarcerated = \
            identifier._identify_months_of_incarceration([incarceration_period])

        self.assertEqual(months_incarcerated, set())

    def test_identify_months_of_incarceration_leap_year(self):
        """Tests the identify_months_of_incarceration function where the person
        was incarcerated until the 28th of February during a leap year, so they
        were not incarcerated for a full month."""
        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                state_code='UT',
                admission_date=date(1996, 2, 1),
                admission_reason=AdmissionReason.NEW_ADMISSION,
                release_date=date(1996, 2, 28)
            )

        months_incarcerated = \
            identifier._identify_months_of_incarceration([incarceration_period])

        self.assertEqual(months_incarcerated, set())


class TestClassifySupervisionSuccess(unittest.TestCase):
    """Tests the classify_supervision_success function."""
    def test_classify_supervision_success(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=
                StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 12, 25),
                supervision_periods=[supervision_period]
            )

        projected_completion_buckets = identifier.classify_supervision_success(
            [supervision_sentence],
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(1, len(projected_completion_buckets))

        self.assertEqual([
            ProjectedSupervisionCompletionBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018,
                month=12,
                supervision_type=supervision_period.supervision_type,
                successful_completion=True,
                case_type=StateSupervisionCaseType.GENERAL
            )
        ], projected_completion_buckets)

    def test_classify_supervision_success_unsuccessful(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=
                StateSupervisionPeriodTerminationReason.REVOCATION,
                supervision_type=StateSupervisionType.PAROLE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 12, 25),
                supervision_periods=[supervision_period]
            )

        projected_completion_buckets = identifier.classify_supervision_success(
            [supervision_sentence],
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(1, len(projected_completion_buckets))

        self.assertEqual([
            ProjectedSupervisionCompletionBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018,
                month=12,
                supervision_type=supervision_period.supervision_type,
                successful_completion=False,
                case_type=StateSupervisionCaseType.GENERAL
            )
        ], projected_completion_buckets)

    def test_classify_supervision_success_multiple_periods(self):
        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 8, 19),
                termination_reason=
                StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 9, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=
                StateSupervisionPeriodTerminationReason.REVOCATION,
                supervision_type=StateSupervisionType.PAROLE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 12, 25),
                supervision_periods=[first_supervision_period,
                                     second_supervision_period]
            )

        projected_completion_buckets = identifier.classify_supervision_success(
            [supervision_sentence],
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(1, len(projected_completion_buckets))

        self.assertEqual([
            ProjectedSupervisionCompletionBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2018,
                month=12,
                supervision_type=second_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=False,
            )
        ], projected_completion_buckets)

    def test_classify_supervision_success_multiple_sentences(self):
        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 8, 19),
                termination_reason=
                StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 9, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=
                StateSupervisionPeriodTerminationReason.REVOCATION,
                supervision_type=StateSupervisionType.PAROLE
            )

        first_supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 12, 25),
                supervision_periods=[first_supervision_period]
            )

        second_supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 12, 25),
                supervision_periods=[second_supervision_period]
            )

        projected_completion_buckets = identifier.classify_supervision_success(
            [first_supervision_sentence, second_supervision_sentence],
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(2, len(projected_completion_buckets))

        self.assertEqual([
            ProjectedSupervisionCompletionBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018,
                month=12,
                supervision_type=first_supervision_period.supervision_type,
                successful_completion=True,
                case_type=StateSupervisionCaseType.GENERAL
            ),
            ProjectedSupervisionCompletionBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2018,
                month=12,
                supervision_type=second_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=False,
            )
        ], projected_completion_buckets)

    def test_classify_supervision_success_multiple_sentence_types(self):
        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 8, 19),
                termination_reason=
                StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 9, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=
                StateSupervisionPeriodTerminationReason.REVOCATION,
                supervision_type=StateSupervisionType.PROBATION
            )

        first_supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 12, 25),
                supervision_periods=[first_supervision_period]
            )

        second_supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 12, 25),
                supervision_periods=[second_supervision_period]
            )

        projected_completion_buckets = identifier.classify_supervision_success(
            [first_supervision_sentence, second_supervision_sentence],
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(2, len(projected_completion_buckets))

        self.assertEqual([
            ProjectedSupervisionCompletionBucket.for_month(
                state_code=first_supervision_period.state_code,
                year=2018,
                month=12,
                supervision_type=first_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=True
            ),
            ProjectedSupervisionCompletionBucket.for_month(
                state_code=second_supervision_period.state_code,
                year=2018,
                month=12,
                supervision_type=second_supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=False
            )
        ], projected_completion_buckets)

    def test_classify_supervision_success_officer_district(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=
                StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 12, 25),
                supervision_periods=[supervision_period]
            )

        supervision_period_agent_association = {
            111: {
                'agent_id': 000,
                'agent_external_id': 'AGENTX',
                'district_external_id': 'DISTRICTX',
                'supervision_period_id': 111
            }
        }

        projected_completion_buckets = identifier.classify_supervision_success(
            [supervision_sentence],
            supervision_period_agent_association
        )

        self.assertEqual(1, len(projected_completion_buckets))

        self.assertEqual([
            ProjectedSupervisionCompletionBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018,
                month=12,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=True,
                supervising_officer_external_id='AGENTX',
                supervising_district_external_id='DISTRICTX'
            )
        ], projected_completion_buckets)

    def test_classify_supervision_success_empty_officer_district(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='UT',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=
                StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 12, 25),
                supervision_periods=[supervision_period]
            )

        supervision_period_agent_association = {
            111: {
                'agent_id': None,
                'agent_external_id': None,
                'district_external_id': None,
                'supervision_period_id': 111
            }
        }

        projected_completion_buckets = identifier.classify_supervision_success(
            [supervision_sentence],
            supervision_period_agent_association
        )

        self.assertEqual(1, len(projected_completion_buckets))

        self.assertEqual([
            ProjectedSupervisionCompletionBucket.for_month(
                state_code=supervision_period.state_code,
                year=2018,
                month=12,
                supervision_type=supervision_period.supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=True,
                supervising_officer_external_id=None,
                supervising_district_external_id=None
            )
        ], projected_completion_buckets)


class TestFindSupervisionTerminationBucket(unittest.TestCase):
    """Tests the find_supervision_termination_bucket function."""
    def test_find_supervision_termination_bucket(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='UT',
            start_date=date(2018, 3, 5),
            termination_date=date(2019, 5, 19),
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION
        )

        first_assessment = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2018, 3, 10)
        )

        first_reassessment = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2018, 5, 18)
        )

        last_assessment = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=19,
            assessment_date=date(2019, 5, 10)
        )

        assessments = [first_assessment, first_reassessment, last_assessment]

        indexed_supervision_periods = \
            identifier._index_supervision_periods_by_termination_month(
                [supervision_period]
            )

        termination_bucket = identifier.find_supervision_termination_bucket(
            supervision_period, indexed_supervision_periods, assessments,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        assessment_score_change = last_assessment.assessment_score - \
                                  first_reassessment.assessment_score

        expected_termination_bucket = SupervisionTerminationBucket.for_month(
            state_code=supervision_period.state_code,
            year=supervision_period.termination_date.year,
            month=supervision_period.termination_date.month,
            supervision_type=supervision_period.supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            assessment_score=last_assessment.assessment_score,
            assessment_type=last_assessment.assessment_type,
            termination_reason=supervision_period.termination_reason,
            assessment_score_change=assessment_score_change,
        )

        self.assertEqual(expected_termination_bucket, termination_bucket)

    def test_find_supervision_termination_bucket_no_assessments(self):

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='UT',
            start_date=date(2018, 3, 5),
            termination_date=date(2019, 5, 19),
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION
        )

        assessments = []

        indexed_supervision_periods = \
            identifier._index_supervision_periods_by_termination_month(
                [supervision_period]
            )

        termination_bucket = identifier.find_supervision_termination_bucket(
            supervision_period, indexed_supervision_periods, assessments,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        expected_termination_bucket = SupervisionTerminationBucket.for_month(
            state_code=supervision_period.state_code,
            year=supervision_period.termination_date.year,
            month=supervision_period.termination_date.month,
            supervision_type=supervision_period.supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            assessment_score=None,
            assessment_type=None,
            termination_reason=supervision_period.termination_reason,
            assessment_score_change=None
        )

        self.assertEqual(expected_termination_bucket, termination_bucket)

    def test_find_supervision_termination_bucket_insufficient_assessments(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='UT',
            start_date=date(2018, 3, 5),
            termination_date=date(2019, 5, 19),
            termination_reason=
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION
        )

        first_assessment = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2018, 3, 10)
        )

        assessments = [first_assessment]

        indexed_supervision_periods = \
            identifier._index_supervision_periods_by_termination_month(
                [supervision_period]
            )

        termination_bucket = identifier.find_supervision_termination_bucket(
            supervision_period, indexed_supervision_periods, assessments,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        expected_termination_bucket = SupervisionTerminationBucket.for_month(
            state_code=supervision_period.state_code,
            year=supervision_period.termination_date.year,
            month=supervision_period.termination_date.month,
            supervision_type=supervision_period.supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            assessment_score=None,
            assessment_type=None,
            termination_reason=supervision_period.termination_reason,
            assessment_score_change=None,
        )

        self.assertEqual(expected_termination_bucket, termination_bucket)

    def test_find_supervision_termination_bucket_no_termination(self):

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='UT',
            start_date=date(2018, 3, 5),
            supervision_type=StateSupervisionType.PROBATION
        )

        assessments = []

        indexed_supervision_periods = \
            identifier._index_supervision_periods_by_termination_month(
                [supervision_period]
            )

        termination_bucket = identifier.find_supervision_termination_bucket(
            supervision_period, indexed_supervision_periods, assessments,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        self.assertEqual(None, termination_bucket)

    def test_find_supervision_termination_bucket_multiple_in_month(self):
        """Tests that when multiple supervision periods end in the same month,
        the earliest start_date and the latest termination_date are used as
        the date boundaries for the assessments."""
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='UT',
            start_date=date(2018, 3, 5),
            termination_date=date(2019, 11, 23),
            supervision_type=StateSupervisionType.PROBATION
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='UT',
            start_date=date(2018, 1, 1),
            termination_date=date(2019, 11, 17),
            supervision_type=StateSupervisionType.PROBATION
        )

        too_early_assessment = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=38,
            assessment_date=date(2017, 12, 10)
        )

        assessment_1 = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2018, 1, 10)
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2018, 5, 18)
        )

        assessment_3 = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=19,
            assessment_date=date(2019, 11, 21)
        )

        assessments = [
            too_early_assessment,
            assessment_1,
            assessment_2,
            assessment_3
        ]

        indexed_supervision_periods = \
            identifier._index_supervision_periods_by_termination_month(
                [first_supervision_period, second_supervision_period]
            )

        termination_bucket = identifier.find_supervision_termination_bucket(
            first_supervision_period, indexed_supervision_periods, assessments,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
        )

        expected_termination_bucket = SupervisionTerminationBucket.for_month(
            state_code=first_supervision_period.state_code,
            year=first_supervision_period.termination_date.year,
            month=first_supervision_period.termination_date.month,
            supervision_type=first_supervision_period.supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            assessment_score=assessment_3.assessment_score,
            assessment_type=assessment_3.assessment_type,
            termination_reason=first_supervision_period.termination_reason,
            assessment_score_change=(assessment_3.assessment_score -
                                     assessment_2.assessment_score),
        )

        self.assertEqual(expected_termination_bucket, termination_bucket)


class TestGetViolationAndResponseHistory(unittest.TestCase):
    """Tests the get_violation_and_response_history function."""
    def test_get_violation_and_response_history(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_MO',
            violation_date=date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    violation_type=StateSupervisionViolationType.TECHNICAL
                ),
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    violation_type=StateSupervisionViolationType.FELONY
                ),
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    violation_type=StateSupervisionViolationType.ABSCONDED
                )
            ]
        )

        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=999,
            response_date=date(2009, 1, 7),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                )
            ],
            supervision_violation=supervision_violation
        )

        revocation_date = date(2009, 2, 13)

        most_severe_violation_type, \
            most_severe_violation_type_subtype, \
            most_severe_decision, \
            response_count, \
            violation_history_description = \
            identifier.get_violation_and_response_history(
                revocation_date, [supervision_violation_response])

        self.assertEqual(StateSupervisionViolationType.FELONY, most_severe_violation_type)
        self.assertEqual('UNSET', most_severe_violation_type_subtype)
        self.assertEqual(StateSupervisionViolationResponseDecision.REVOCATION, most_severe_decision)
        self.assertEqual(1, response_count)
        self.assertEqual(violation_history_description, '1fel;1absc;1tech')

    def test_get_violation_and_response_history_with_subtype(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_MO',
            violation_date=date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    violation_type=StateSupervisionViolationType.TECHNICAL),
            ],
            supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry.new_with_defaults(condition='DRG'),
                StateSupervisionViolatedConditionEntry.new_with_defaults(condition='OTHER'),
            ]
        )

        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=999,
            state_code='US_MO',
            response_date=date(2009, 1, 7),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                )
            ],
            supervision_violation=supervision_violation
        )

        revocation_date = date(2009, 2, 13)

        most_severe_violation_type, \
        most_severe_violation_type_subtype, \
        most_severe_decision, \
        response_count, \
        violation_history_description = \
            identifier.get_violation_and_response_history(revocation_date, [supervision_violation_response])

        self.assertEqual(StateSupervisionViolationType.TECHNICAL, most_severe_violation_type)
        self.assertEqual('SUBSTANCE_ABUSE', most_severe_violation_type_subtype)
        self.assertEqual(StateSupervisionViolationResponseDecision.REVOCATION, most_severe_decision)
        self.assertEqual(1, response_count)
        self.assertEqual(violation_history_description, '1subs')

    def test_get_violation_and_response_history_no_violations(self):
        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=999,
            response_date=date(2009, 1, 7),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.
                new_with_defaults(
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.
                    REINCARCERATION
                ),
                StateSupervisionViolationResponseDecisionEntry.
                new_with_defaults(
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.
                    SHOCK_INCARCERATION,
                )
            ],
        )

        revocation_date = date(2009, 2, 13)

        most_severe_violation_type, \
            most_severe_violation_type_subtype, \
            most_severe_decision, \
            response_count, \
            violation_history_description = \
            identifier.get_violation_and_response_history(revocation_date, [supervision_violation_response])

        self.assertIsNone(most_severe_violation_type)
        self.assertEqual('UNSET', most_severe_violation_type_subtype)
        self.assertEqual(StateSupervisionViolationResponseDecision.REVOCATION, most_severe_decision)
        self.assertEqual(1, response_count)
        self.assertIsNone(violation_history_description)

    def test_get_violation_and_response_history_no_responses(self):
        revocation_date = date(2009, 2, 13)

        most_severe_violation_type, \
            most_severe_violation_type_subtype, \
            most_severe_decision, \
            response_count, \
            violation_history_description = \
            identifier.get_violation_and_response_history(revocation_date,
                                                          [])

        self.assertIsNone(most_severe_violation_type)
        self.assertEqual('UNSET', most_severe_violation_type_subtype)
        self.assertIsNone(most_severe_decision)
        self.assertEqual(0, response_count)
        self.assertIsNone(violation_history_description)

    def test_get_violation_and_response_history_citation_date(self):
        supervision_violation = \
            StateSupervisionViolation.new_with_defaults(
                supervision_violation_id=123455,
                state_code='US_MO',
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.ABSCONDED
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.MISDEMEANOR
                    )
                ]
            )

        supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=999,
                response_type=StateSupervisionViolationResponseType.CITATION,
                response_date=date(2009, 1, 7),
                supervision_violation_response_decisions=[
                    StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                        revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION
                    ),
                    StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                        decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                        revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                    )
                ],
                supervision_violation=supervision_violation
            )

        revocation_date = date(2009, 2, 13)

        most_severe_violation_type, \
            most_severe_violation_type_subtype, \
            most_severe_decision, \
            response_count, \
            violation_history_description = \
            identifier.get_violation_and_response_history(revocation_date, [supervision_violation_response])

        self.assertEqual(StateSupervisionViolationType.MISDEMEANOR, most_severe_violation_type)
        self.assertEqual('UNSET', most_severe_violation_type_subtype)
        self.assertEqual(StateSupervisionViolationResponseDecision.REVOCATION, most_severe_decision)
        self.assertEqual(1, response_count)
        self.assertEqual(violation_history_description, '1misd;1absc')

    def test_get_violation_and_response_history_is_draft(self):
        supervision_violation = \
            StateSupervisionViolation.new_with_defaults(
                supervision_violation_id=123455,
                state_code='US_MO',
                violation_date=date(2009, 1, 7),
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.ABSCONDED
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.MISDEMEANOR
                    )
                ]
            )

        supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=999,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2009, 1, 7),
                is_draft=True,
                supervision_violation_response_decisions=[
                    StateSupervisionViolationResponseDecisionEntry.
                    new_with_defaults(
                        decision=
                        StateSupervisionViolationResponseDecision.REVOCATION,
                        revocation_type=
                        StateSupervisionViolationResponseRevocationType.
                        REINCARCERATION
                    ),
                    StateSupervisionViolationResponseDecisionEntry.
                    new_with_defaults(
                        decision=
                        StateSupervisionViolationResponseDecision.CONTINUANCE,
                        revocation_type=
                        StateSupervisionViolationResponseRevocationType.
                        SHOCK_INCARCERATION,
                    )
                ],
                supervision_violation=supervision_violation
            )

        revocation_date = date(2009, 2, 13)

        most_severe_violation_type, \
            most_severe_violation_type_subtype, \
            most_severe_decision, \
            response_count, \
            violation_history_description = \
            identifier.get_violation_and_response_history(
                revocation_date,
                [supervision_violation_response]
            )

        self.assertIsNone(most_severe_violation_type)
        self.assertEqual('UNSET', most_severe_violation_type_subtype)
        self.assertEqual(0, response_count)
        self.assertIsNone(most_severe_decision)
        self.assertIsNone(violation_history_description)


class TestIdentifyMostSevereRevocationType(unittest.TestCase):
    """Tests the _identify_most_severe_revocation_type function."""
    def test_identify_most_severe_revocation_type(self):
        response_decision_entries = [
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION
            ),
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION
            )]

        most_severe_revocation_type = identifier._identify_most_severe_revocation_type(response_decision_entries)

        self.assertEqual(most_severe_revocation_type,
                         StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION)

    def test_identify_most_severe_revocation_type_test_all_types(self):
        for revocation_type in StateSupervisionViolationResponseRevocationType:
            response_decision_entries = [
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    revocation_type=revocation_type
                )
            ]

            # RETURN_TO_SUPERVISION is not included as a revocation type for a revocation return to prison
            if revocation_type != StateSupervisionViolationResponseRevocationType.RETURN_TO_SUPERVISION:
                most_severe_revocation_type = identifier._identify_most_severe_revocation_type(
                    response_decision_entries)

                self.assertEqual(most_severe_revocation_type, revocation_type)


class TestIdentifyMostSevereCaseType(unittest.TestCase):
    """Tests the _identify_most_severe_case_type function."""
    def test_identify_most_severe_case_type(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE
                ),
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    case_type=StateSupervisionCaseType.SEX_OFFENDER
                )
            ]
        )

        most_severe_case_type = identifier._identify_most_severe_case_type(supervision_period)

        self.assertEqual(most_severe_case_type, StateSupervisionCaseType.SEX_OFFENDER)

    def test_identify_most_severe_case_type_test_all_types(self):
        for case_type in StateSupervisionCaseType:
            supervision_period = StateSupervisionPeriod.new_with_defaults(
                case_type_entries=[
                    StateSupervisionCaseTypeEntry.new_with_defaults(
                        case_type=case_type
                    ),
                ]
            )

            most_severe_case_type = identifier._identify_most_severe_case_type(supervision_period)

            self.assertEqual(most_severe_case_type, case_type)

    def test_identify_most_severe_case_type_no_type_entries(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            case_type_entries=[]
        )

        most_severe_case_type = identifier._identify_most_severe_case_type(supervision_period)

        self.assertEqual(most_severe_case_type, StateSupervisionCaseType.GENERAL)
