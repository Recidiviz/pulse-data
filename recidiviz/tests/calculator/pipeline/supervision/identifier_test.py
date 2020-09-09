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
from collections import defaultdict
from datetime import date

import unittest
from typing import Optional, List, Dict

import attr
from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

import recidiviz.calculator
from recidiviz.calculator.pipeline.supervision import identifier
from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import SupervisionCaseCompliance
from recidiviz.calculator.pipeline.utils.calculator_utils import last_day_of_month
from recidiviz.calculator.pipeline.utils.incarceration_period_index import IncarcerationPeriodIndex
from recidiviz.calculator.pipeline.supervision.metrics import SupervisionMetricType
from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import \
    NonRevocationReturnSupervisionTimeBucket, \
    RevocationReturnSupervisionTimeBucket,\
    ProjectedSupervisionCompletionBucket, SupervisionTerminationBucket
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_sentence_classification import SupervisionTypeSpan
from recidiviz.calculator.pipeline.utils.supervision_period_index import SupervisionPeriodIndex
from recidiviz.calculator.pipeline.utils.supervision_period_utils import SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType, StateAssessmentLevel
from recidiviz.common.constants.state.state_case_type import \
    StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason as AdmissionReason, \
    StateIncarcerationPeriodReleaseReason as ReleaseReason, \
    StateIncarcerationPeriodStatus, StateSpecializedPurposeForIncarceration, StateIncarcerationPeriodAdmissionReason
from recidiviz.common.constants.state.state_supervision_contact import StateSupervisionContactStatus, \
    StateSupervisionContactType
from recidiviz.common.constants.state.state_supervision_period import \
    StateSupervisionPeriodStatus, StateSupervisionPeriodTerminationReason, StateSupervisionPeriodSupervisionType, \
    StateSupervisionLevel, StateSupervisionPeriodAdmissionReason
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
    StateAssessment, StateSupervisionCaseTypeEntry, StateSupervisionViolatedConditionEntry,\
    StateIncarcerationSentence, StateSupervisionContact
from recidiviz.tests.calculator.pipeline.utils.us_mo_fakes import FakeUsMoSupervisionSentence, \
    FakeUsMoIncarcerationSentence

_DEFAULT_SUPERVISION_PERIOD_ID = 999
_DEFAULT_SSVR_ID = 999

DEFAULT_SSVR_AGENT_ASSOCIATIONS = {
    999: {
        'agent_id': 000,
        'agent_external_id': 'ZZZ',
        'district_external_id': 'Z',
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

DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS = [{
    'supervision_period_id': _DEFAULT_SUPERVISION_PERIOD_ID,
    'judicial_district_code': 'XXX',
}]


# TODO(2732): Implement more full test coverage of the officer and district
#  functionality and the supervision success classification
class TestClassifySupervisionTimeBuckets(unittest.TestCase):
    """Tests for the find_supervision_time_buckets function."""

    def setUp(self):
        self.maxDiff = None

    def test_find_supervision_time_buckets(self):
        """Tests the find_supervision_time_buckets function for a single
        supervision period with no incarceration periods."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                case_type_entries=[
                    StateSupervisionCaseTypeEntry.new_with_defaults(
                        state_code='US_ND',
                        case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE
                    )
                ],
                state_code='US_ND',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MEDIUM,
                supervision_level_raw_text='M'
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                state_code='US_ND',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                projected_completion_date=date(2018, 5, 19),
                completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period]
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ND',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 1)
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = []
        assessments = [assessment]
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=5,
                bucket_date=last_day_of_month(supervision_sentence.projected_completion_date),
                supervision_type=supervision_period_supervision_type,
                sentence_days_served=(
                    supervision_sentence.completion_date - supervision_sentence.start_date).days,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                successful_completion=True,
                incarcerated_during_sentence=False,
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_overlaps_year(self):
        """Tests the find_supervision_time_buckets function for a single
        supervision period with no incarceration periods, where the supervision
        period overlaps two calendar years."""
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 3, 5),
                termination_date=date(2019, 1, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = []
        assessments = []

        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_time_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason
            )]

        expected_time_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
        ))

        self.assertCountEqual(supervision_time_buckets, expected_time_buckets)

    def test_find_supervision_time_buckets_two_supervision_periods(self):
        """Tests the find_supervision_time_buckets function for two supervision
        periods with no incarceration periods."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                external_id='sp2',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2019, 8, 5),
                termination_date=date(2019, 12, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[first_supervision_period,
                                     second_supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period,
                               second_supervision_period]
        incarceration_periods = []
        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        first_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        second_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                bucket_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason
            ),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                bucket_date=second_supervision_period.termination_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            first_supervision_period,
            first_supervision_period_supervision_type
        ))

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            second_supervision_period,
            second_supervision_period_supervision_type
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_overlapping_supervision_periods(self):
        """Tests the find_supervision_time_buckets function for two supervision
        periods with no incarceration periods, where the supervision
        periods are of the same type and have overlapping months."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                external_id='sp2',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 4, 15),
                termination_date=date(2018, 7, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[first_supervision_period,
                                     second_supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period,
                               second_supervision_period]
        incarceration_periods = []
        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        first_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        second_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                bucket_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason
            ),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                bucket_date=second_supervision_period.termination_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            first_supervision_period,
            first_supervision_period_supervision_type
        ))

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            second_supervision_period,
            second_supervision_period_supervision_type
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_overlapping_periods_different_types(self):
        """Tests the find_supervision_time_buckets function for two supervision
        periods with no incarceration periods, where the supervision
        periods are of different types and have overlapping months."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                external_id='sp2',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 4, 15),
                termination_date=date(2018, 7, 19),
                supervision_type=StateSupervisionType.PAROLE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                completion_date=date(2018, 7, 19),
                supervision_periods=[first_supervision_period,
                                     second_supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period,
                               second_supervision_period]
        incarceration_periods = []
        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        first_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        overlapping_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                bucket_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason
            ),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                bucket_date=second_supervision_period.termination_date,
                supervision_type=overlapping_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            first_supervision_period,
            first_supervision_period_supervision_type
        ))

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            second_supervision_period,
            overlapping_supervision_period_supervision_type
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_findSupervisionTimeBuckets_usNd_ignoreTemporaryCustodyPeriod(self):
        """Tests the find_supervision_time_buckets function for state code US_ND to ensure that temporary
        custody periods are ignored."""
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ND',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionType.PROBATION
        )

        temporary_custody_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id='ip1',
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code='US_ND',
            admission_date=date(2017, 4, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2017, 5, 17),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ND',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED
        )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[first_supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [first_supervision_period]
        incarceration_periods = [temporary_custody_period,
                                 revocation_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, incarceration_sentences,
            supervision_periods, incarceration_periods,
            assessments, violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        first_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2017, month=5,
                bucket_date=revocation_period.admission_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                bucket_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason)]

        expected_time_buckets.extend(expected_non_revocation_return_time_buckets(
            first_supervision_period,
            first_supervision_period_supervision_type
        ))

        self.assertCountEqual(supervision_time_buckets, expected_time_buckets)

    def test_findSupervisionTimeBuckets_doNotCollapseTemporaryCustodyAndRevocation_us_mo(self):
        """Tests the find_supervision_time_buckets function to ensure temporary custody and revocation periods are
        not collapsed.
        """
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_MO',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionType.PAROLE
        )

        temporary_custody_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id='ip1',
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code='US_MO',
            admission_date=date(2017, 4, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2017, 5, 17),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_MO',
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[first_supervision_period]
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=first_supervision_period.start_date,
                    end_date=temporary_custody_period.admission_date,
                    supervision_type=StateSupervisionType.PAROLE
                ),
                SupervisionTypeSpan(
                    start_date=temporary_custody_period.admission_date,
                    end_date=revocation_period.admission_date,
                    supervision_type=None
                ),
                SupervisionTypeSpan(
                    start_date=revocation_period.admission_date,
                    end_date=None,
                    supervision_type=None
                )
            ]
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [first_supervision_period]
        incarceration_periods = [temporary_custody_period, revocation_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, incarceration_sentences,
            supervision_periods, incarceration_periods,
            assessments, violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        first_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        expected_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2017, month=5,
                bucket_date=revocation_period.admission_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                bucket_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason)]

        expected_time_buckets.extend(expected_non_revocation_return_time_buckets(
            first_supervision_period,
            first_supervision_period_supervision_type,
            temporary_custody_period.admission_date,
            case_type=StateSupervisionCaseType.GENERAL,
        ))

        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usId(self):
        """Tests the find_supervision_time_buckets function for state code US_ID."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text='MEDIUM'
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_contact = StateSupervisionContact.new_with_defaults(
            state_code='US_ID',
            external_id='contactX',
            contact_date=supervision_period.start_date,
            status=StateSupervisionContactStatus.COMPLETED,
            contact_type=StateSupervisionContactType.FACE_TO_FACE
        )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [supervision_period]
        incarceration_periods = [revocation_period]
        assessments = []
        violation_responses = []
        supervision_contacts = [supervision_contact]

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, incarceration_sentences,
            supervision_periods, incarceration_periods,
            assessments, violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        expected_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2017, month=5,
                bucket_date=revocation_period.admission_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason)]

        expected_time_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period.supervision_period_supervision_type,
            case_compliances={
                date(2017, 3, 31): SupervisionCaseCompliance(
                    date_of_evaluation=date(2017, 3, 31),
                    assessment_count=0,
                    assessment_up_to_date=True,
                    face_to_face_count=1,
                    face_to_face_frequency_sufficient=True
                ),
                date(2017, 4, 30): SupervisionCaseCompliance(
                    date_of_evaluation=date(2017, 4, 30),
                    assessment_count=0,
                    assessment_up_to_date=False,
                    face_to_face_count=0,
                    face_to_face_frequency_sufficient=True
                )
            }
        ))

        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usId_filterUnsetSupervisionTypePeriods(self):
        """Tests the find_supervision_time_buckets function for state code US_ID where the supervision periods with
        unset supervision_period_supervision_type values should be filtered out when looking for revocations."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM
        )

        # Supervision period with an unset supervision_period_supervision_type between the terminated period
        # and the revocation incarceration period
        supervision_period_type_unset = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2017, 5, 9),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=None
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period, supervision_period_type_unset]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [supervision_period, supervision_period_type_unset]
        incarceration_periods = [revocation_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, incarceration_sentences,
            supervision_periods, incarceration_periods,
            assessments, violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        expected_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2017, month=5,
                bucket_date=revocation_period.admission_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason)]

        expected_time_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period.supervision_period_supervision_type,
            case_compliances={
                date(2017, 3, 31): SupervisionCaseCompliance(
                    date_of_evaluation=date(2017, 3, 31),
                    assessment_count=0,
                    assessment_up_to_date=True,
                    face_to_face_count=0,
                    face_to_face_frequency_sufficient=False
                ),
                date(2017, 4, 30): SupervisionCaseCompliance(
                    date_of_evaluation=date(2017, 4, 30),
                    assessment_count=0,
                    assessment_up_to_date=False,
                    face_to_face_count=0,
                    face_to_face_frequency_sufficient=False
                )
            }
        ))

        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usId_filterInternalUnknownSupervisionTypePeriods(self):
        """Tests the find_supervision_time_buckets function for state code US_ID where the supervision periods with
        INTERNAL_UNKNOWN supervision_period_supervision_type values should be filtered out when looking for
        revocations."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE
        )

        # Supervision period with an internal unknown supervision_period_supervision_type between the terminated period
        # and the revocation incarceration period (this could signify a bench warrant, for example)
        supervision_period_type_unset = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2017, 5, 9),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_period_agent_associations = {
            111: {
                'agent_id': 123,
                'agent_external_id': 'Officer1',
                'district_external_id': 'X',
                'supervision_period_id': 111
            },
        }

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period, supervision_period_type_unset]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [supervision_period, supervision_period_type_unset]
        incarceration_periods = [revocation_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, incarceration_sentences,
            supervision_periods, incarceration_periods,
            assessments, violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            supervision_period_agent_associations,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        expected_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2017, month=5,
                bucket_date=revocation_period.admission_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervising_officer_external_id="Officer1",
                supervising_district_external_id="X",
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervising_officer_external_id="Officer1",
                supervising_district_external_id="X",
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                termination_reason=supervision_period.termination_reason)]

        expected_time_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period.supervision_period_supervision_type,
            supervising_officer_external_id='Officer1',
            supervising_district_external_id='X',
            case_compliances={
                date(2017, 3, 31): SupervisionCaseCompliance(
                    date_of_evaluation=date(2017, 3, 31),
                    assessment_count=0,
                    assessment_up_to_date=True,
                    face_to_face_count=0,
                    face_to_face_frequency_sufficient=False
                ),
                date(2017, 4, 30): SupervisionCaseCompliance(
                    date_of_evaluation=date(2017, 4, 30),
                    assessment_count=0,
                    assessment_up_to_date=False,
                    face_to_face_count=0,
                    face_to_face_frequency_sufficient=False
                )
            }
        ))

        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usId_unsortedIncarcerationPeriods(self):
        """Tests the find_supervision_time_buckets function for state code US_ID where the incarceration periods
        are not sorted prior to the calculations."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM
        )

        # Supervision period with an unset supervision_period_supervision_type between the terminated period
        # and the revocation incarceration period
        supervision_period_type_unset = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2017, 5, 9),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=None
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        earlier_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2011, 1, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 9, 13),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period, supervision_period_type_unset]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [supervision_period, supervision_period_type_unset]
        incarceration_periods = [revocation_period, earlier_incarceration_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, incarceration_sentences,
            supervision_periods, incarceration_periods,
            assessments, violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        expected_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2017, month=5,
                bucket_date=revocation_period.admission_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                is_on_supervision_last_day_of_month=False,
                case_type=StateSupervisionCaseType.GENERAL,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason)]

        expected_time_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period.supervision_period_supervision_type,
            case_compliances={
                date(2017, 3, 31): SupervisionCaseCompliance(
                    date_of_evaluation=date(2017, 3, 31),
                    assessment_count=0,
                    assessment_up_to_date=True,
                    face_to_face_count=0,
                    face_to_face_frequency_sufficient=False
                ),
                date(2017, 4, 30): SupervisionCaseCompliance(
                    date_of_evaluation=date(2017, 4, 30),
                    assessment_count=0,
                    assessment_up_to_date=False,
                    face_to_face_count=0,
                    face_to_face_frequency_sufficient=False
                )
            }
        ))

        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usId_admittedAfterInvestigation(self):
        """Tests the find_supervision_time_buckets function for state code US_ID where the person is admitted after
        being on INVESTIGATION supervision. These periods should produce no SupervisionTimeBuckets, and the admission
        to prison should not be counted as a revocation."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, incarceration_sentences,
            supervision_periods, incarceration_periods,
            assessments, violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        expected_time_buckets = []
        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usNd_admittedAfterPreConfinement(self):
        """Tests the find_supervision_time_buckets function for state code US_ND where the person is admitted after
        being on PRE-CONFINEMENT supervision. These periods should produce no SupervisionTimeBuckets, and the admission
        to prison should not be counted as a revocation.

        TODO(2891): This should be updated or removed once ND has been migrated to supervision_period_supervision_type
        """
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ND',
            custodial_authority='US_ND_DOCR',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionType.PRE_CONFINEMENT,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ND',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, incarceration_sentences,
            supervision_periods, incarceration_periods,
            assessments, violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        expected_time_buckets = []
        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usId_revokedAfterBoardHold(self):
        """Tests the find_supervision_time_buckets function for state code US_ID where the person is revoked after
        being held for a board hold."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM
        )

        board_hold_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2017, 6, 3),
            release_reason=ReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 6, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [supervision_period]
        incarceration_periods = [board_hold_period, revocation_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, incarceration_sentences,
            supervision_periods, incarceration_periods,
            assessments, violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        expected_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2017, month=6,
                bucket_date=revocation_period.admission_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason)]

        expected_time_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period.supervision_period_supervision_type,
            case_compliances={
                date(2017, 3, 31): SupervisionCaseCompliance(
                    date_of_evaluation=date(2017, 3, 31),
                    assessment_count=0,
                    assessment_up_to_date=True,
                    face_to_face_count=0,
                    face_to_face_frequency_sufficient=False
                ),
                date(2017, 4, 30): SupervisionCaseCompliance(
                    date_of_evaluation=date(2017, 4, 30),
                    assessment_count=0,
                    assessment_up_to_date=False,
                    face_to_face_count=0,
                    face_to_face_frequency_sufficient=False
                )
            }
        ))

        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usId_revokedAfterParoleBoardHoldMultipleTransfers(self):
        """Tests the find_supervision_time_buckets function for state code US_ID where the person is revoked after
               being incarcerated for parole board hold, where they were transferred multiple times while on parole
               board hold."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            custodial_authority='US_ID_DOC',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM
        )

        parole_board_hold_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            facility='XX',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=AdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2017, 6, 3),
            release_reason=ReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
        )

        parole_board_hold_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            facility='YY',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 6, 3),
            admission_reason=AdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2017, 6, 3),
            release_reason=ReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
        )

        parole_board_hold_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            facility='ZZ',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 6, 3),
            admission_reason=AdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2017, 11, 19),
            release_reason=ReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 11, 19),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [supervision_period]
        incarceration_periods = [parole_board_hold_period_1, parole_board_hold_period_2, parole_board_hold_period_3,
                                 revocation_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences, incarceration_sentences,
            supervision_periods, incarceration_periods,
            assessments, violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        expected_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2017, month=11,
                bucket_date=revocation_period.admission_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason)]

        expected_time_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period.supervision_period_supervision_type,
            case_compliances={
                date(2017, 3, 31): SupervisionCaseCompliance(
                    date_of_evaluation=date(2017, 3, 31),
                    assessment_count=0,
                    assessment_up_to_date=True,
                    face_to_face_count=0,
                    face_to_face_frequency_sufficient=False
                ),
                date(2017, 4, 30): SupervisionCaseCompliance(
                    date_of_evaluation=date(2017, 4, 30),
                    assessment_count=0,
                    assessment_up_to_date=False,
                    face_to_face_count=0,
                    face_to_face_frequency_sufficient=False
                )
            }
        ))

        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_multiple_periods(self):
        """Tests the find_supervision_time_buckets function for two supervision
        periods with two incarceration periods."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2017, 3, 5),
                termination_date=date(2017, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip1',
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2017, 5, 25),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                release_date=date(2017, 8, 3),
                release_reason=ReleaseReason.SENTENCE_SERVED
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                external_id='sp2',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 8, 5),
                termination_date=date(2018, 12, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=222,
                external_id='ip2',
                state_code='US_ND',
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                admission_date=date(2018, 12, 25),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                release_date=date(2019, 3, 3),
                release_reason=ReleaseReason.SENTENCE_SERVED
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
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
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        first_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        second_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2017, month=5,
                bucket_date=first_incarceration_period.admission_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                bucket_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code=second_supervision_period.state_code,
                year=2018, month=12,
                bucket_date=second_incarceration_period.admission_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                bucket_date=second_supervision_period.termination_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason
            ),
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            first_supervision_period,
            first_supervision_period_supervision_type
        ))

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            second_supervision_period,
            second_supervision_period_supervision_type
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_multiple_admissions_in_month(self):
        """Tests the find_supervision_time_buckets function for a supervision period with two incarceration periods
        with admission dates in the same month."""
        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2017, 3, 5),
                termination_date=date(2017, 5, 9),
                supervision_type=StateSupervisionType.PROBATION
            )

        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip1',
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2017, 5, 11),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                release_date=date(2017, 5, 15),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE
            )

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=222,
                external_id='ip2',
                state_code='US_ND',
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                admission_date=date(2017, 5, 17),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                release_date=date(2019, 3, 3),
                release_reason=ReleaseReason.SENTENCE_SERVED
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[first_supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period]
        incarceration_periods = [first_incarceration_period,
                                 second_incarceration_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        first_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2017, month=5,
                bucket_date=first_incarceration_period.admission_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2017, month=5,
                bucket_date=second_incarceration_period.admission_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                bucket_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason
            ),
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            first_supervision_period,
            first_supervision_period_supervision_type
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_incarceration_overlaps_periods(self):
        """Tests the find_supervision_time_buckets function for two supervision
        periods with an incarceration period that overlaps the end of one
        supervision period and the start of another."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2017, 3, 5),
                termination_date=date(2017, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip1',
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2017, 5, 15),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                release_date=date(2017, 9, 20),
                release_reason=ReleaseReason.SENTENCE_SERVED
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                external_id='sp2',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2017, 8, 5),
                termination_date=date(2017, 12, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                completion_date=date(2017, 12, 19),
                supervision_periods=[first_supervision_period,
                                     second_supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period,
                               second_supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        first_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        second_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2017, month=5,
                bucket_date=incarceration_period.admission_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                bucket_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason
            ),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                bucket_date=second_supervision_period.termination_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason
            ),
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            first_supervision_period,
            first_supervision_period_supervision_type,
            incarceration_period.admission_date
        ))

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            attr.evolve(second_supervision_period, start_date=incarceration_period.release_date),
            second_supervision_period_supervision_type
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_transition_from_parole_to_probation_in_month(self):
        """Tests the find_supervision_time_buckets function for transition between two supervision periods in a month.
        """
        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PAROLE
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                external_id='sp2',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 5, 19),
                termination_date=date(2018, 6, 20),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                state_code='US_ND',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[first_supervision_period,
                                     second_supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period,
                               second_supervision_period]
        incarceration_periods = []
        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        first_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        second_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                bucket_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason
            ),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                bucket_date=second_supervision_period.termination_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            first_supervision_period,
            first_supervision_period_supervision_type,
        ))

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            second_supervision_period,
            second_supervision_period_supervision_type
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_incarceration_overlaps_periods_multiple_us_nd(self):
        """Tests the find_supervision_time_buckets function for two supervision periods with an incarceration period
        that overlaps two supervision periods that were revoked, where there is state-specific logic for US_ND on how
        to determine the supervision type on the RevocationReturnSupervisionTimeBuckets."""
        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2017, 3, 5),
                termination_date=date(2017, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                external_id='sp2',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2017, 3, 5),
                termination_date=date(2017, 12, 19),
                supervision_type=StateSupervisionType.PAROLE
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip1',
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2017, 5, 15),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                release_date=date(2017, 9, 20),
                release_reason=ReleaseReason.SENTENCE_SERVED
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                completion_date=date(2017, 12, 19),
                supervision_periods=[first_supervision_period,
                                     second_supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period,
                               second_supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        first_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        second_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2017, month=5,
                bucket_date=incarceration_period.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2017, month=5,
                bucket_date=incarceration_period.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                bucket_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason
            ),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                bucket_date=second_supervision_period.termination_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            first_supervision_period,
            first_supervision_period_supervision_type,
            incarceration_period.admission_date
        ))

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            second_supervision_period,
            second_supervision_period_supervision_type,
            incarceration_period.admission_date
        ))

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            attr.evolve(second_supervision_period, start_date=incarceration_period.release_date),
            second_supervision_period_supervision_type
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_return_next_month(self):
        """Tests the find_supervision_time_buckets function
        when there is an incarceration period with a revocation admission
        the month after the supervision period's termination_date."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 26),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip1',
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code='US_ND',
                admission_date=date(2018, 6, 25),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                source_supervision_violation_response=StateSupervisionViolationResponse.new_with_defaults(
                    supervision_violation_response_id=_DEFAULT_SSVR_ID
                )
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_ND',
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                supervising_officer_external_id='XXX',
                judicial_district_code='XXX',
                supervising_district_external_id='X',
            ),
            RevocationReturnSupervisionTimeBucket(
                incarceration_period.state_code,
                year=2018, month=6,
                bucket_date=incarceration_period.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id='ZZZ',
                judicial_district_code='XXX',
                supervising_district_external_id='Z',
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                is_on_supervision_last_day_of_month=False),
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            supervising_officer_external_id='XXX',
            supervising_district_external_id='X',
            judicial_district_code='XXX'
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_return_next_month_different_supervision_type_us_nd(self):
        """Tests the find_supervision_time_buckets function when there is an incarceration period with a revocation
        admission the month after the supervision period's termination_date, and the admission_reason supervision
        type does not match the supervision type on the period. This tests state-specific logic for US_ND on how
        to determine the supervision type on the RevocationReturnSupervisionTimeBuckets"""
        self.maxDiff = None
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip1',
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code='US_ND',
                admission_date=date(2018, 6, 3),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
                source_supervision_violation_response=StateSupervisionViolationResponse.new_with_defaults(
                    supervision_violation_response_id=_DEFAULT_SSVR_ID
                )
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                supervising_officer_external_id='XXX',
                supervising_district_external_id='X',
                judicial_district_code='XXX'
            ),
            RevocationReturnSupervisionTimeBucket(
                incarceration_period.state_code,
                year=2018, month=6,
                bucket_date=incarceration_period.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id='ZZZ',
                supervising_district_external_id='Z',
                judicial_district_code='XXX',
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                is_on_supervision_last_day_of_month=False),
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            supervising_officer_external_id='XXX',
            supervising_district_external_id='X',
            judicial_district_code='XXX'
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_return_next_year(self):
        """Tests the find_supervision_time_buckets function
        when there is an incarceration period with a revocation admission
        the month after the supervision period's termination_date, where the
        revocation admission happens in the next year."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 10, 5),
                termination_date=date(2018, 12, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip1',
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2019, 1, 5),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                source_supervision_violation_response=StateSupervisionViolationResponse.new_with_defaults(
                    supervision_violation_response_id=_DEFAULT_SSVR_ID
                )
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [

            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                supervising_officer_external_id='XXX',
                supervising_district_external_id='X',
                judicial_district_code='XXX'
            ),
            RevocationReturnSupervisionTimeBucket(
                incarceration_period.state_code,
                year=2019, month=1,
                bucket_date=incarceration_period.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                supervising_officer_external_id='ZZZ',
                supervising_district_external_id='Z',
                judicial_district_code='XXX',
                is_on_supervision_last_day_of_month=False
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            supervising_officer_external_id='XXX',
            supervising_district_external_id='X',
            judicial_district_code='XXX'
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_return_months_later(self):
        """Tests the find_supervision_time_buckets function
        when there is an incarceration period with a revocation admission
        months after the supervision period's termination_date."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip1',
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2018, 10, 25),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason
            ),
            RevocationReturnSupervisionTimeBucket(
                incarceration_period.state_code,
                year=2018, month=10,
                bucket_date=incarceration_period.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                is_on_supervision_last_day_of_month=False),
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_return_years_later(self):
        """Tests the find_supervision_time_buckets function
        when there is an incarceration period with a revocation admission
        years after the supervision period's termination_date."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2015, 3, 5),
                termination_date=date(2015, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip1',
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2017, 10, 25),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2015, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason
            ),
            RevocationReturnSupervisionTimeBucket(
                incarceration_period.state_code,
                year=2017, month=10,
                bucket_date=incarceration_period.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                is_on_supervision_last_day_of_month=False),
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_multiple_periods_revocations(self):
        """Tests the find_supervision_time_buckets function
        when the person is revoked and returned to supervision twice in one
        year."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 12, 19),
                supervision_type=StateSupervisionType.PAROLE
            )

        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip1',
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2018, 3, 25),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
                release_date=date(2018, 8, 2),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE
            )

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip2',
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2018, 9, 25),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
                release_date=date(2018, 12, 2),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                completion_date=date(2018, 12, 19),
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = [first_incarceration_period,
                                 second_incarceration_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018, month=3,
                bucket_date=first_incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018, month=9,
                bucket_date=second_incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason
            ),
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            first_incarceration_period.admission_date
        ))

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            attr.evolve(supervision_period, start_date=first_incarceration_period.release_date),
            supervision_period_supervision_type,
            second_incarceration_period.admission_date
        ))

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            attr.evolve(supervision_period, start_date=second_incarceration_period.release_date),
            supervision_period_supervision_type,
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_multiple_sentences_revocations(self):
        """Tests the find_supervision_time_buckets function
        when the person is revoked and returned to supervision twice in one
        year, and they have multiple supervision sentences."""

        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 12, 19),
                supervision_type=StateSupervisionType.PAROLE
            )

        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip1',
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2018, 3, 25),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
                release_date=date(2018, 8, 2),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE
            )

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip2',
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2018, 9, 25),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
                release_date=date(2018, 12, 2),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=234,
                external_id='sp2',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2019, 1, 1),
                termination_date=date(2019, 1, 19),
                supervision_type=StateSupervisionType.PAROLE
            )

        first_supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                completion_date=date(2018, 12, 19),
                supervision_periods=[first_supervision_period]
            )

        second_supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=222,
                start_date=date(2017, 1, 1),
                external_id='ss2',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                completion_date=date(2019, 1, 19),
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
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        first_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE
        second_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2018, month=3,
                bucket_date=first_incarceration_period.admission_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2018, month=9,
                bucket_date=second_incarceration_period.admission_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                bucket_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason
            ),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                bucket_date=second_supervision_period.termination_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            first_supervision_period,
            first_supervision_period_supervision_type,
            first_incarceration_period.admission_date
        ))

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            attr.evolve(first_supervision_period, start_date=first_incarceration_period.release_date),
            first_supervision_period_supervision_type,
            second_incarceration_period.admission_date
        ))

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            attr.evolve(first_supervision_period, start_date=second_incarceration_period.release_date),
            first_supervision_period_supervision_type
        ))

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            second_supervision_period,
            second_supervision_period_supervision_type
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_placeholders(self):
        """Tests the find_supervision_time_buckets function
        when there are placeholder supervision periods that should be dropped
        from the calculations."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_period_placeholder = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
                state_code='US_ND',
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip1',
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2018, 5, 25),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[supervision_period,
                                     supervision_period_placeholder]
            )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                bucket_date=incarceration_period.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason
            ),
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_us_id(self):
        """Tests the find_supervision_time_buckets function where the supervision type should be taken from the
        supervision_period_supervision_type off of the supervision_period."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code='US_ID',
                    case_type=StateSupervisionCaseType.GENERAL
                )
            ],
            state_code='US_ID',
            supervision_site='DISTRICT_1|OFFICE_2',
            custodial_authority='US_ID_DOC',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text='LOW'
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id='ss1',
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period]
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=123,
            external_id='is1',
            start_date=date(2017, 1, 1),
            supervision_periods=[supervision_period]
        )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 1)
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = []
        assessments = [assessment]
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = [incarceration_sentence]

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                supervising_district_external_id='DISTRICT_1',
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period.supervision_period_supervision_type,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type,
            supervising_district_external_id='DISTRICT_1',
            case_compliances={
                date(2018, 3, 31): SupervisionCaseCompliance(
                    date_of_evaluation=date(2018, 3, 31),
                    assessment_count=1,
                    assessment_up_to_date=True,
                    face_to_face_count=0,
                    face_to_face_frequency_sufficient=False
                ),
                date(2018, 4, 30): SupervisionCaseCompliance(
                    date_of_evaluation=date(2018, 4, 30),
                    assessment_count=0,
                    assessment_up_to_date=True,
                    face_to_face_count=0,
                    face_to_face_frequency_sufficient=False
                )
            }
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_infer_supervision_type(self):
        """Tests the find_supervision_time_buckets function where the supervision type needs to be inferred from the
        sentence attached to the supervision period."""
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                case_type_entries=[
                    StateSupervisionCaseTypeEntry.new_with_defaults(
                        state_code='US_ND',
                        case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE
                    )
                ],
                state_code='US_ND',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                completion_date=date(2018, 12, 19),
                projected_completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period]
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ND',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 2, 10)
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = []
        assessments = [assessment]
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        expected_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=5,
                bucket_date=last_day_of_month(supervision_sentence.projected_completion_date),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                successful_completion=True,
                incarcerated_during_sentence=False,
                sentence_days_served=(
                    supervision_sentence.completion_date - supervision_sentence.start_date).days
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            StateSupervisionPeriodSupervisionType.PROBATION,
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_infer_supervision_type_parole(self):
        """Tests the find_supervision_time_buckets function where the supervision type needs to be inferred, the
        but the supervision period is not attached to any sentences, so the inferred type should be PAROLE."""
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                case_type_entries=[
                    StateSupervisionCaseTypeEntry.new_with_defaults(
                        state_code='US_ND',
                        case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE
                    )
                ],
                state_code='US_ND',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                projected_completion_date=date(2018, 5, 19),
                supervision_periods=[]
            )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=123,
            external_id='is1',
            start_date=date(2017, 1, 1),
            supervision_periods=[supervision_period]
        )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 1)
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = []
        assessments = [assessment]
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = [incarceration_sentence]

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )
        expected_buckets = [

            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_infer_supervision_type_dual_us_mo(self):
        """Tests the find_supervision_time_buckets function where the supervision type needs to be inferred, the
        but the supervision period is attached to both a supervision sentence of type PROBATION and an incarceration
        sentence, so the inferred type should be DUAL."""
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                case_type_entries=[
                    StateSupervisionCaseTypeEntry.new_with_defaults(
                        state_code='US_MO',
                        case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE
                    )
                ],
                state_code='US_MO',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
                StateSupervisionSentence.new_with_defaults(
                    supervision_sentence_id=111,
                    start_date=date(2017, 1, 1),
                    completion_date=date(2018, 5, 19),
                    external_id='ss1',
                    status=StateSentenceStatus.COMPLETED,
                    supervision_periods=[supervision_period],
                    supervision_type=StateSupervisionType.PROBATION
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=supervision_period.start_date,
                        end_date=supervision_period.termination_date,
                        supervision_type=StateSupervisionType.PROBATION
                    ),
                    SupervisionTypeSpan(
                        start_date=supervision_period.termination_date,
                        end_date=None,
                        supervision_type=None
                    )
                ]
            )

        incarceration_sentence = \
            FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
                StateIncarcerationSentence.new_with_defaults(
                    incarceration_sentence_id=123,
                    external_id='is1',
                    start_date=date(2017, 1, 1),
                    completion_date=date(2018, 5, 19),
                    supervision_periods=[supervision_period]
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=supervision_period.start_date,
                        end_date=supervision_period.termination_date,
                        supervision_type=StateSupervisionType.PAROLE
                    ),
                    SupervisionTypeSpan(
                        start_date=supervision_period.termination_date,
                        end_date=None,
                        supervision_type=None
                    )
                ]
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_MO',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 2, 10)
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = []
        assessments = [assessment]
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = [incarceration_sentence]

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            StateSupervisionPeriodSupervisionType.DUAL,
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_infer_supervision_type_dual_us_id(self):
        """Tests the find_supervision_time_buckets function where the supervision type is taken from a `DUAL`
        supervision period. Asserts that the DUAL buckets are NOT expanded into separate PROBATION and PAROLE buckets.
        """
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                state_code='US_ID',
                custodial_authority='US_ID_DOC',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None,
                supervision_level=StateSupervisionLevel.MINIMUM
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ID',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 1)
        )

        supervision_sentences = []
        supervision_periods = [supervision_period]
        incarceration_periods = []
        assessments = [assessment]
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                case_type=StateSupervisionCaseType.GENERAL,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                supervision_level=supervision_period.supervision_level,
                termination_reason=supervision_period.termination_reason
            ),
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            StateSupervisionPeriodSupervisionType.DUAL,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type,
            case_compliances={
                date(2018, 3, 31): SupervisionCaseCompliance(
                    date_of_evaluation=date(2018, 3, 31),
                    assessment_count=1,
                    assessment_up_to_date=True,
                    face_to_face_count=0,
                    face_to_face_frequency_sufficient=False
                ),
                date(2018, 4, 30): SupervisionCaseCompliance(
                    date_of_evaluation=date(2018, 4, 30),
                    assessment_count=0,
                    assessment_up_to_date=False,
                    face_to_face_count=0,
                    face_to_face_frequency_sufficient=False
                )
            }
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_infer_supervision_type_dual_us_nd(self):
        """Tests the find_supervision_time_buckets function where the supervision type needs to be inferred, the
        but the supervision period is attached to both a supervision sentence of type PROBATION and an incarceration
        sentence, so the inferred type should be DUAL. Also asserts that the DUAL buckets are expanded to have PAROLE,
        PROBATION, and DUAL buckets."""
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                case_type_entries=[
                    StateSupervisionCaseTypeEntry.new_with_defaults(
                        state_code='US_ND',
                        case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE
                    )
                ],
                state_code='US_ND',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period],
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=123,
            external_id='is1',
            start_date=date(2017, 1, 1),
            supervision_periods=[supervision_period]
        )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_ND',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 1)
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = []
        assessments = [assessment]
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = [incarceration_sentence]

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=supervision_period.supervision_level,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason
            ),
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            StateSupervisionPeriodSupervisionType.DUAL,
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type,
        ))

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type,
        ))

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            StateSupervisionPeriodSupervisionType.PROBATION,
            case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type,
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_no_supervision_when_no_sentences_supervision_spans_us_mo(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                case_type_entries=[
                    StateSupervisionCaseTypeEntry.new_with_defaults(
                        state_code='US_MO',
                        case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE
                    )
                ],
                state_code='US_MO',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10)
        )

        supervision_sentences = []
        supervision_periods = [supervision_period]
        incarceration_periods = []
        assessments = [assessment]
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        self.assertCountEqual(supervision_time_buckets, [])

    @freeze_time('2019-09-04')
    def test_find_supervision_time_buckets_admission_today(self):
        """Tests the find_time_buckets_for_supervision_period function when there is an incarceration period with a
        revocation admission today, where there is no termination_date on the supervision_period, and no release_date
        on the incarceration_period."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ND',
                start_date=date(2019, 6, 2),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                external_id='ip1',
                state_code='US_ND',
                admission_date=date.today(),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period]
            )

        assessments = []
        violation_reports = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            [supervision_sentence],
            incarceration_sentences,
            [supervision_period],
            [incarceration_period],
            assessments,
            violation_reports,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2019, month=9,
                bucket_date=incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            StateSupervisionPeriodSupervisionType.PROBATION,
            date.today()
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_supervision_time_buckets_multiple_incarcerations_in_year(self):
        """Tests the find_time_buckets_for_supervision_period function when there are multiple
        incarceration periods in the year of supervision."""
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 6, 19),
                supervision_type=StateSupervisionType.PAROLE
            )

        first_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip1',
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2018, 6, 2),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
                release_date=date(2018, 9, 3),
                release_reason=date(2018, 10, 30)
            )

        second_incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=222,
                external_id='ip2',
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                state_code='US_ND',
                admission_date=date(2018, 11, 17),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION
            )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=123,
            external_id='is1',
            start_date=date(2018, 1, 1),
            incarceration_periods=[
                first_incarceration_period, second_incarceration_period],
            supervision_periods=[supervision_period]
        )

        assessments = []
        violation_reports = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            [],
            [incarceration_sentence],
            [supervision_period],
            [first_incarceration_period, second_incarceration_period],
            assessments,
            violation_reports,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018, month=6,
                bucket_date=first_incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018, month=11,
                bucket_date=second_incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            first_incarceration_period.admission_date
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_supervision_time_buckets_revocation_details_us_mo(self):
        """Tests the supervision_time_buckets function when there is an incarceration period with
        a revocation admission in the same month as the supervision period's termination_date. Also ensures that the
        correct revocation_type, violation_count_type, supervising_officer_external_id, and
        supervising_district_external_id are set on the RevocationReturnSupervisionTimeBucket."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
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

        violation_report = StateSupervisionViolationResponse.new_with_defaults(
            state_code='US_MO',
            supervision_violation_response_id=888,
            response_date=date(2018, 4, 21),
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype='INI',
            supervision_violation=supervision_violation,
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
            ]
        )

        source_supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code='US_MO',
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_date=date(2018, 4, 23),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                )
            ],
            supervision_violation=supervision_violation
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id='ip1',
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code='US_MO',
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            source_supervision_violation_response=source_supervision_violation_response
        )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_MO',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2018, 3, 1)
        )

        supervision_sentence = \
            FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
                StateSupervisionSentence.new_with_defaults(
                    supervision_sentence_id=111,
                    start_date=date(2017, 1, 1),
                    external_id='ss1',
                    supervision_type=StateSupervisionType.PROBATION,
                    status=StateSentenceStatus.REVOKED,
                    completion_date=date(2018, 5, 19),
                    supervision_periods=[supervision_period]
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=supervision_period.start_date,
                        end_date=supervision_period.termination_date,
                        supervision_type=StateSupervisionType.PROBATION
                    ),
                    SupervisionTypeSpan(
                        start_date=supervision_period.termination_date,
                        end_date=None,
                        supervision_type=None
                    )
                ]
            )

        incarceration_sentence = \
            FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
                StateIncarcerationSentence.new_with_defaults(
                    incarceration_sentence_id=123,
                    external_id='is1',
                    start_date=date(2018, 5, 25),
                    incarceration_periods=[incarceration_period]
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=incarceration_period.admission_date,
                        end_date=None,
                        supervision_type=None
                    )
                ]
            )

        assessments = [assessment]
        violation_reports = [violation_report]
        incarceration_sentences = [incarceration_sentence]
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            [supervision_sentence],
            incarceration_sentences,
            [supervision_period],
            [incarceration_period],
            assessments,
            violation_reports,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                bucket_date=incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                source_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
                violation_history_description='1fel',
                violation_type_frequency_counter=[['FELONY']],
                supervising_officer_external_id=r'ZZZ',
                supervising_district_external_id='Z',
                is_on_supervision_last_day_of_month=False),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                response_count=1,
                most_severe_violation_type=StateSupervisionViolationType.FELONY
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            end_date=violation_report.response_date,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type
        ))

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            attr.evolve(supervision_period, start_date=violation_report.response_date),
            supervision_period_supervision_type,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type,
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            response_count=1
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_supervision_time_buckets_technical_revocation_with_subtype_us_mo(self):
        """Tests the find_supervision_time_buckets function when there is an incarceration period with a
        revocation admission in the same month as the supervision period's termination_date. Also ensures that the
        correct revocation_type, violation_count_type, violation_subtype, supervising_officer_external_id, and
        supervising_district_external_id are set on the RevocationReturnSupervisionTimeBucket."""
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
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
                    )
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(condition='DRG')]
            )

        violation_report = StateSupervisionViolationResponse.new_with_defaults(
            state_code='US_MO',
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_date=date(2018, 2, 21),
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype='INI',
            supervision_violation=supervision_violation,
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
            ]
        )

        source_supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
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
                external_id='ip1',
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                incarceration_type=StateIncarcerationType.STATE_PRISON,
                state_code='US_MO',
                admission_date=date(2018, 5, 25),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                source_supervision_violation_response=source_supervision_violation_response
            )

        assessment = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2018, 3, 1)
        )

        supervision_sentence = \
            FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
                StateSupervisionSentence.new_with_defaults(
                    supervision_sentence_id=111,
                    start_date=date(2017, 1, 1),
                    external_id='ss1',
                    supervision_type=StateSupervisionType.PROBATION,
                    status=StateSentenceStatus.COMPLETED,
                    completion_date=date(2018, 5, 19),
                    supervision_periods=[supervision_period]
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=supervision_period.start_date,
                        end_date=supervision_period.termination_date,
                        supervision_type=StateSupervisionType.PROBATION
                    ),
                    SupervisionTypeSpan(
                        start_date=supervision_period.termination_date,
                        end_date=None,
                        supervision_type=None
                    )
                ]
            )

        incarceration_sentence = \
            FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
                StateIncarcerationSentence.new_with_defaults(
                    incarceration_sentence_id=123,
                    external_id='is1',
                    start_date=date(2018, 5, 25),
                    incarceration_periods=[incarceration_period]
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=incarceration_period.admission_date,
                        end_date=None,
                        supervision_type=None
                    )
                ]
            )

        assessments = [assessment]
        violation_responses = [violation_report,
                               source_supervision_violation_response]
        incarceration_sentences = [incarceration_sentence]
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            [supervision_sentence],
            incarceration_sentences,
            [supervision_period],
            [incarceration_period],
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018, month=5,
                bucket_date=incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
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
                violation_type_frequency_counter=[['DRG']],
                supervising_officer_external_id=r'ZZZ',
                supervising_district_external_id='Z',
                is_on_supervision_last_day_of_month=False),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                response_count=1,
                most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
                most_severe_violation_type_subtype='SUBSTANCE_ABUSE'
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type,
            most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
            most_severe_violation_type_subtype='SUBSTANCE_ABUSE',
            response_count=1
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_supervision_time_buckets_no_supervision_period_end_of_month_us_mo_supervision_span_shows_supervision(self):
        """Tests that we do not mark someone as under supervision at the end of the month if there is no supervision
        period overlapping with the end of the month, even if the US_MO sentence supervision spans indicate that they
        are on supervision at a given time.
        """

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp2',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_MO',
                supervision_site='DISTRICTX',
                supervising_officer='AGENTX',
                start_date=date(2019, 10, 3),
                termination_date=date(2019, 10, 9),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
                StateSupervisionSentence.new_with_defaults(
                    supervision_sentence_id=111,
                    start_date=supervision_period.start_date,
                    external_id='ss1',
                    state_code='US_MO',
                    supervision_type=StateSupervisionType.PROBATION,
                    status=StateSentenceStatus.SERVING,
                    supervision_periods=[supervision_period]
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=supervision_period.start_date,
                        end_date=None,
                        supervision_type=StateSupervisionType.PROBATION
                    )
                ]
            )

        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []
        incarceration_periods = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            [supervision_sentence],
            incarceration_sentences,
            [supervision_period],
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_district_external_id=supervision_period.supervision_site,
                termination_reason=supervision_period.termination_reason
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            StateSupervisionPeriodSupervisionType.PROBATION,
            supervising_district_external_id=supervision_period.supervision_site
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_supervision_time_buckets_period_eom_us_mo_supervision_span_shows_no_supervision_eom(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp2',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_MO',
                supervision_site='DISTRICTX',
                supervising_officer='AGENTX',
                start_date=date(2019, 10, 3),
                termination_date=date(2019, 11, 9),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
                StateSupervisionSentence.new_with_defaults(
                    supervision_sentence_id=111,
                    start_date=supervision_period.start_date,
                    external_id='ss1',
                    state_code='US_MO',
                    supervision_type=StateSupervisionType.PROBATION,
                    status=StateSentenceStatus.SERVING,
                    supervision_periods=[supervision_period]
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=supervision_period.start_date,
                        end_date=date(2019, 10, 6),
                        supervision_type=StateSupervisionType.PROBATION
                    ),
                    SupervisionTypeSpan(
                        start_date=date(2019, 10, 6),
                        end_date=date(2019, 11, 6),
                        supervision_type=None
                    ),
                    SupervisionTypeSpan(
                        start_date=date(2019, 11, 6),
                        end_date=None,
                        supervision_type=StateSupervisionType.PROBATION
                    )
                ]
            )

        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []
        incarceration_periods = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            [supervision_sentence],
            incarceration_sentences,
            [supervision_period],
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_district_external_id=supervision_period.supervision_site,
                termination_reason=supervision_period.termination_reason
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            supervision_period,
            StateSupervisionPeriodSupervisionType.PROBATION,
            end_date=date(2019, 10, 6),
            supervising_district_external_id=supervision_period.supervision_site
        ))

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            attr.evolve(supervision_period, start_date=date(2019, 11, 6)),
            StateSupervisionPeriodSupervisionType.PROBATION,
            supervising_district_external_id=supervision_period.supervision_site
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_supervision_period_end_of_month_us_mo_supervision_span_shows_no_supervision_all_month(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp2',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_MO',
                supervision_site='DISTRICTX',
                supervising_officer='AGENTX',
                start_date=date(2019, 10, 3),
                termination_date=date(2019, 11, 9),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
                StateSupervisionSentence.new_with_defaults(
                    supervision_sentence_id=111,
                    start_date=supervision_period.start_date,
                    external_id='ss1',
                    state_code='US_MO',
                    supervision_type=StateSupervisionType.PROBATION,
                    status=StateSentenceStatus.SERVING,
                    supervision_periods=[supervision_period]
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=date(2019, 9, 6),
                        end_date=date(2019, 10, 3),
                        supervision_type=StateSupervisionType.PROBATION
                    ),
                    SupervisionTypeSpan(
                        start_date=date(2019, 10, 3),
                        end_date=date(2019, 11, 6),
                        supervision_type=None
                    ),
                    SupervisionTypeSpan(
                        start_date=date(2019, 11, 6),
                        end_date=None,
                        supervision_type=StateSupervisionType.PROBATION
                    )
                ]
            )

        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []
        incarceration_periods = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            [supervision_sentence],
            incarceration_sentences,
            [supervision_period],
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_district_external_id=supervision_period.supervision_site,
                termination_reason=supervision_period.termination_reason
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            attr.evolve(supervision_period, start_date=date(2019, 11, 6)),
            StateSupervisionPeriodSupervisionType.PROBATION,
            supervising_district_external_id=supervision_period.supervision_site
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_supervision_period_us_mo_supervision_spans_do_not_overlap(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp2',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_MO',
                supervision_site='DISTRICTX',
                supervising_officer='AGENTX',
                start_date=date(2019, 10, 3),
                termination_date=date(2019, 11, 9),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
                StateSupervisionSentence.new_with_defaults(
                    supervision_sentence_id=111,
                    start_date=supervision_period.start_date,
                    external_id='ss1',
                    state_code='US_MO',
                    supervision_type=StateSupervisionType.PROBATION,
                    status=StateSentenceStatus.SERVING,
                    supervision_periods=[supervision_period]
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=date(2019, 9, 6),
                        end_date=date(2019, 10, 3),
                        supervision_type=StateSupervisionType.PROBATION
                    ),
                    SupervisionTypeSpan(
                        start_date=date(2019, 10, 3),
                        end_date=date(2019, 11, 9),
                        supervision_type=None
                    ),
                    SupervisionTypeSpan(
                        start_date=date(2019, 11, 9),
                        end_date=None,
                        supervision_type=StateSupervisionType.PROBATION
                    )
                ]
            )

        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []
        incarceration_periods = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            [supervision_sentence],
            incarceration_sentences,
            [supervision_period],
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        self.assertCountEqual(supervision_time_buckets, [])

    def test_supervision_period_mid_month_us_mo_supervision_span_shows_supervision_eom(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp2',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_MO',
                supervision_site='DISTRICTX',
                supervising_officer='AGENTX',
                start_date=date(2019, 10, 3),
                termination_date=date(2019, 10, 20),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
                StateSupervisionSentence.new_with_defaults(
                    supervision_sentence_id=111,
                    start_date=supervision_period.start_date,
                    external_id='ss1',
                    state_code='US_MO',
                    supervision_type=StateSupervisionType.PROBATION,
                    status=StateSentenceStatus.SERVING,
                    supervision_periods=[supervision_period]
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=date(2019, 9, 6),
                        end_date=date(2019, 10, 15),
                        supervision_type=None
                    ),
                    SupervisionTypeSpan(
                        start_date=date(2019, 10, 15),
                        end_date=None,
                        supervision_type=StateSupervisionType.PROBATION
                    )
                ]
            )

        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []
        incarceration_periods = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            [supervision_sentence],
            incarceration_sentences,
            [supervision_period],
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SSVR_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                bucket_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_district_external_id=supervision_period.supervision_site,
                termination_reason=supervision_period.termination_reason
            )
        ]

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            attr.evolve(supervision_period, start_date=date(2019, 10, 15)),
            StateSupervisionPeriodSupervisionType.PROBATION,
            supervising_district_external_id=supervision_period.supervision_site
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)


class TestFindTimeBucketsForSupervisionPeriod(unittest.TestCase):
    """Tests for the find_time_buckets_for_supervision_period function."""

    def setUp(self):
        self.maxDiff = None

    def test_find_time_buckets_for_supervision_period_revocation_no_termination(self):
        """Tests the find_time_buckets_for_supervision_period function
        when there is an incarceration period with a revocation admission,
        where there is no termination_date on the supervision_period, and
        no release_date on the incarceration_period."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ND',
                start_date=date(2003, 7, 5),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                external_id='ip1',
                state_code='US_ND',
                admission_date=date(2003, 10, 10),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2003, 1, 1),
                external_id='ss1',
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.SERVING,
                supervision_periods=[supervision_period]
            )

        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=[incarceration_period])
        supervision_period_index = SupervisionPeriodIndex(supervision_periods=[supervision_period])

        assessments = []

        violation_reports = []
        supervision_contacts = []
        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_reports,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        self.assertCountEqual(supervision_time_buckets, expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            incarceration_period.admission_date
        ))

    def test_find_time_buckets_for_supervision_period_incarceration_ends_same_month(self):
        """Tests the find_time_buckets_for_supervision_period function when there is an incarceration period with a
        revocation admission before the supervision period's termination_date, and the supervision_period and the
        incarceration_period end in the same month."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ND',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip1',
                state_code='US_ND',
                admission_date=date(2018, 4, 25),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                release_date=date(2018, 5, 22),
                release_reason=ReleaseReason.SENTENCE_SERVED
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.COMPLETED,
                completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period]
            )

        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=[incarceration_period])
        supervision_period_index = SupervisionPeriodIndex(supervision_periods=[supervision_period])

        incarceration_sentences = []
        assessments = []
        violation_reports = []
        supervision_contacts = []

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                [supervision_sentence], incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_reports,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        self.assertCountEqual(supervision_time_buckets, expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            incarceration_period.admission_date
        ))

    @freeze_time('2019-11-03')
    def test_find_time_buckets_for_supervision_period_nested_revocation_no_termination(self):
        """Tests the find_time_buckets_for_supervision_period function when there is an incarceration period with
        a revocation admission, a stay in prison, and a continued supervision period after release from incarceration
        that has still not terminated."""
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ND',
                start_date=date(2019, 3, 5),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip1',
                state_code='US_ND',
                admission_date=date(2019, 5, 25),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                release_date=date(2019, 10, 17)
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.SERVING,
                supervision_periods=[supervision_period]
            )

        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=[incarceration_period])
        supervision_period_index = SupervisionPeriodIndex(supervision_periods=[supervision_period])
        assessments = []

        violation_reports = []
        supervision_contacts = []
        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_reports,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            incarceration_period.admission_date
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(supervision_period, start_date=incarceration_period.release_date),
                supervision_period_supervision_type
            )
        )

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_time_buckets_for_supervision_period_admission_no_revocation(self):
        """Tests the find_time_buckets_for_supervision_period function when there is an incarceration period with a
        non-revocation admission before the supervision period's termination_date."""
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ND',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 6, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                status=StateIncarcerationPeriodStatus.IN_CUSTODY,
                incarceration_period_id=111,
                external_id='ip1',
                state_code='US_ND',
                admission_date=date(2018, 6, 2),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.COMPLETED,
                completion_date=date(2018, 7, 19),
                supervision_periods=[supervision_period]
            )

        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=[incarceration_period])
        supervision_period_index = SupervisionPeriodIndex(supervision_periods=[supervision_period])

        assessments = []

        violation_reports = []
        supervision_contacts = []
        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_reports,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            incarceration_period.admission_date
        )

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_time_buckets_for_supervision_period_multiple_years(self):
        """Tests the find_time_buckets_for_supervision_period function when the supervision period overlaps
        multiple years, and there is an incarceration period during this time that also overlaps multiple years."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2008, 3, 5),
                termination_date=date(2010, 3, 19),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip1',
                state_code='US_ND',
                admission_date=date(2008, 6, 2),
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=date(2009, 12, 3),
                release_reason=ReleaseReason.CONDITIONAL_RELEASE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2007, 1, 1),
                external_id='ss1',
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.COMPLETED,
                completion_date=date(2010, 3, 19),
                supervision_periods=[supervision_period]
            )

        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=[incarceration_period])
        supervision_period_index = SupervisionPeriodIndex(supervision_periods=[supervision_period])

        assessments = []

        violation_reports = []
        supervision_contacts = []
        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_reports,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            incarceration_period.admission_date
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(supervision_period, start_date=incarceration_period.release_date),
                supervision_period_supervision_type
            )
        )

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_time_buckets_for_supervision_period_ends_on_first(self):
        """Tests the find_time_buckets_for_supervision_period function for a
        supervision period with no incarceration periods, where the supervision
        period ends on the 1st day of a month."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ND',
                start_date=date(2001, 1, 5),
                termination_date=date(2001, 7, 1),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2000, 1, 1),
                external_id='ss1',
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.COMPLETED,
                completion_date=date(2001, 7, 1),
                supervision_periods=[supervision_period]
            )

        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=[])
        supervision_period_index = SupervisionPeriodIndex(supervision_periods=[supervision_period])

        assessments = []

        violation_reports = []
        supervision_contacts = []
        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_reports,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type
        )

        self.assertEqual(expected_buckets[-1],
                         NonRevocationReturnSupervisionTimeBucket(
                             state_code=supervision_period.state_code,
                             year=2001, month=6,
                             bucket_date=date(2001, 6, 30),
                             supervision_type=supervision_period_supervision_type,
                             case_type=StateSupervisionCaseType.GENERAL,
                             is_on_supervision_last_day_of_month=True))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_time_buckets_for_supervision_period_ends_on_last(self):
        """Tests the find_time_buckets_for_supervision_period function for a
        supervision period with no incarceration periods, where the supervision
        period ends on the last day of a month."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ND',
                start_date=date(2001, 1, 5),
                termination_date=date(2001, 6, 30),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2000, 1, 1),
                external_id='ss1',
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.COMPLETED,
                completion_date=date(2001, 6, 30),
                supervision_periods=[supervision_period]
            )

        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=[])
        supervision_period_index = SupervisionPeriodIndex(supervision_periods=[supervision_period])

        assessments = []

        violation_reports = []
        supervision_contacts = []
        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_reports,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type
        )

        self.assertEqual(expected_buckets[-1],
                         NonRevocationReturnSupervisionTimeBucket(
                             state_code=supervision_period.state_code,
                             year=2001, month=6,
                             bucket_date=date(2001, 6, 29),
                             supervision_type=supervision_period_supervision_type,
                             case_type=StateSupervisionCaseType.GENERAL,
                             is_on_supervision_last_day_of_month=False))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_time_buckets_for_supervision_period_start_end_same_day(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ND',
                start_date=date(2001, 3, 3),
                termination_date=date(2001, 3, 3),
                supervision_type=StateSupervisionType.PROBATION
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2000, 1, 1),
                external_id='ss1',
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.COMPLETED,
                completion_date=date(2001, 3, 3),
                supervision_periods=[supervision_period]
            )

        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=[])
        supervision_period_index = SupervisionPeriodIndex(supervision_periods=[supervision_period])

        assessments = []

        violation_reports = []
        supervision_contacts = []
        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_reports,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        self.assertCountEqual(supervision_time_buckets, [])

    def test_find_time_buckets_for_supervision_period_multiple_assessments(self):
        """Tests the find_time_buckets_for_supervision_period function
        when there is an incarceration period with a revocation admission,
        a stay in prison, a continued supervision period after release
        from incarceration, and multiple assessments over this time period."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ND',
                start_date=date(2018, 3, 11),
                termination_date=date(2018, 12, 10),
                supervision_type=StateSupervisionType.PROBATION
            )

        incarceration_period = \
            StateIncarcerationPeriod.new_with_defaults(
                incarceration_period_id=111,
                external_id='ip1',
                state_code='US_ND',
                admission_date=date(2018, 5, 25),
                admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
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
            assessment_date=date(2018, 10, 27)
        )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.COMPLETED,
                completion_date=date(2018, 12, 10),
                supervision_periods=[supervision_period]
            )

        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=[incarceration_period])
        supervision_period_index = SupervisionPeriodIndex(supervision_periods=[supervision_period])

        assessments = [assessment_1, assessment_2]

        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                [supervision_sentence],
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_responses,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            end_date=incarceration_period.admission_date,
            assessment_score=assessment_1.assessment_score,
            assessment_level=assessment_1.assessment_level,
            assessment_type=assessment_1.assessment_type
        )

        expected_buckets.extend(expected_non_revocation_return_time_buckets(
            attr.evolve(supervision_period, start_date=incarceration_period.release_date),
            supervision_period_supervision_type,
            assessment_score=assessment_2.assessment_score,
            assessment_level=assessment_2.assessment_level,
            assessment_type=assessment_2.assessment_type,
        ))

        self.assertCountEqual(supervision_time_buckets, expected_buckets)

    def test_find_time_buckets_for_supervision_period_assessment_year_before(self):
        """Tests the find_time_buckets_for_supervision_period function
        when there is no revocation and the assessment date is the year before
        the start of the supervision."""

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                state_code='US_ND',
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

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                supervision_type=StateSupervisionType.PROBATION,
                completion_date=date(2018, 3, 19),
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period]
            )

        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=[])
        supervision_period_index = SupervisionPeriodIndex(supervision_periods=[supervision_period])

        assessments = [assessment]
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = \
            identifier.find_time_buckets_for_supervision_period(
                [supervision_sentence],
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_responses,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS
            )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_buckets = expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type
        )

        self.assertCountEqual(supervision_time_buckets, expected_buckets)


class TestClassifySupervisionSuccess(unittest.TestCase):
    """Tests the classify_supervision_success function."""

    def setUp(self) -> None:
        self.default_supervision_period_to_judicial_district_associations = {
            _DEFAULT_SUPERVISION_PERIOD_ID: DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS[0]
        }

    def test_classify_supervision_success(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                projected_completion_date=date(2018, 12, 25),
                completion_date=date(2018, 12, 25),
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex([]),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations
        )

        self.assertEqual(1, len(projected_completion_buckets))

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertEqual([
            ProjectedSupervisionCompletionBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=12,
                bucket_date=last_day_of_month(supervision_sentence.projected_completion_date),
                supervision_type=supervision_period_supervision_type,
                successful_completion=True,
                incarcerated_during_sentence=False,
                case_type=StateSupervisionCaseType.GENERAL,
                sentence_days_served=(
                    supervision_sentence.completion_date - supervision_sentence.start_date).days
            )
        ], projected_completion_buckets)

    def test_classify_supervision_success_unsuccessful(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
                supervision_type=StateSupervisionType.PAROLE
            )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id='ip1',
            state_code='US_ND',
            admission_date=date(2018, 6, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_date=date(2018, 6, 21)
        )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                projected_completion_date=date(2018, 12, 25),
                completion_date=date(2018, 12, 25),
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_periods = [incarceration_period]

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex(incarceration_periods),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations
        )

        self.assertEqual(1, len(projected_completion_buckets))

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertEqual([
            ProjectedSupervisionCompletionBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=12,
                bucket_date=last_day_of_month(supervision_sentence.projected_completion_date),
                supervision_type=supervision_period_supervision_type,
                successful_completion=False,
                incarcerated_during_sentence=True,
                case_type=StateSupervisionCaseType.GENERAL,
                sentence_days_served=(
                    supervision_sentence.completion_date - supervision_sentence.start_date).days
            )
        ], projected_completion_buckets)

    def test_classify_supervision_success_multiple_periods(self):
        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 8, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                external_id='sp2',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 9, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
                supervision_type=StateSupervisionType.PAROLE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                projected_completion_date=date(2018, 12, 25),
                completion_date=date(2018, 12, 25),
                supervision_periods=[first_supervision_period,
                                     second_supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_periods = []

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex(incarceration_periods),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations
        )

        self.assertEqual(1, len(projected_completion_buckets))

        second_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertEqual([
            ProjectedSupervisionCompletionBucket(
                state_code=second_supervision_period.state_code,
                year=2018,
                month=12,
                bucket_date=last_day_of_month(supervision_sentence.projected_completion_date),
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=False,
                incarcerated_during_sentence=False,
                sentence_days_served=(
                    supervision_sentence.completion_date - supervision_sentence.start_date).days
            )
        ], projected_completion_buckets)

    def test_classify_supervision_success_multiple_sentences(self):
        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 8, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                external_id='sp2',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 9, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
                supervision_type=StateSupervisionType.PAROLE
            )

        first_supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                projected_completion_date=date(2018, 12, 25),
                completion_date=date(2018, 12, 25),
                supervision_periods=[first_supervision_period]
            )

        second_supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss2',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                projected_completion_date=date(2018, 12, 25),
                completion_date=date(2018, 12, 25),
                supervision_periods=[second_supervision_period]
            )

        supervision_sentences = [
            first_supervision_sentence, second_supervision_sentence]
        incarceration_periods = []

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex(incarceration_periods),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations
        )

        self.assertEqual(2, len(projected_completion_buckets))

        first_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE
        second_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertEqual([
            ProjectedSupervisionCompletionBucket(
                state_code=first_supervision_period.state_code,
                year=2018,
                month=12,
                bucket_date=last_day_of_month(first_supervision_sentence.projected_completion_date),
                supervision_type=first_supervision_period_supervision_type,
                successful_completion=True,
                incarcerated_during_sentence=False,
                case_type=StateSupervisionCaseType.GENERAL,
                sentence_days_served=(first_supervision_sentence.completion_date -
                                      first_supervision_sentence.start_date).days
            ),
            ProjectedSupervisionCompletionBucket(
                state_code=second_supervision_period.state_code,
                year=2018,
                month=12,
                bucket_date=last_day_of_month(second_supervision_sentence.projected_completion_date),
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=False,
                incarcerated_during_sentence=False,
                sentence_days_served=(second_supervision_sentence.completion_date -
                                      second_supervision_sentence.start_date).days
            )
        ], projected_completion_buckets)

    def test_classify_supervision_success_multiple_sentence_types(self):
        first_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 8, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE
            )

        second_supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=222,
                external_id='sp2',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 9, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
                supervision_type=StateSupervisionType.PROBATION
            )

        first_supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                projected_completion_date=date(2018, 12, 25),
                completion_date=date(2018, 12, 25),
                supervision_periods=[first_supervision_period]
            )

        second_supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss2',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                projected_completion_date=date(2018, 12, 25),
                completion_date=date(2018, 12, 25),
                supervision_periods=[second_supervision_period]
            )

        supervision_sentences = [
            first_supervision_sentence, second_supervision_sentence]
        incarceration_periods = []

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex(incarceration_periods),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations
        )

        self.assertEqual(2, len(projected_completion_buckets))

        first_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE
        second_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        self.assertEqual([
            ProjectedSupervisionCompletionBucket(
                state_code=first_supervision_period.state_code,
                year=2018,
                month=12,
                bucket_date=last_day_of_month(first_supervision_sentence.projected_completion_date),
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=True,
                incarcerated_during_sentence=False,
                sentence_days_served=(first_supervision_sentence.completion_date -
                                      first_supervision_sentence.start_date).days
            ),
            ProjectedSupervisionCompletionBucket(
                state_code=second_supervision_period.state_code,
                year=2018,
                month=12,
                bucket_date=last_day_of_month(second_supervision_sentence.projected_completion_date),
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=False,
                incarcerated_during_sentence=False,
                sentence_days_served=(second_supervision_sentence.completion_date -
                                      second_supervision_sentence.start_date).days
            )
        ], projected_completion_buckets)

    def test_classify_supervision_success_officer_district(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                projected_completion_date=date(2018, 12, 25),
                completion_date=date(2018, 12, 25),
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

        supervision_period_to_judicial_district_associations = {
            111: {
                'supervision_period_id': 111,
                'judicial_district_code': 'NORTHWEST'
            }
        }

        supervision_sentences = [supervision_sentence]
        incarceration_periods = []

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex(incarceration_periods),
            supervision_period_agent_association,
            supervision_period_to_judicial_district_associations
        )

        self.assertEqual(1, len(projected_completion_buckets))

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertEqual([
            ProjectedSupervisionCompletionBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=12,
                bucket_date=last_day_of_month(supervision_sentence.projected_completion_date),
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=True,
                incarcerated_during_sentence=False,
                supervising_officer_external_id='AGENTX',
                supervising_district_external_id='DISTRICTX',
                judicial_district_code='NORTHWEST',
                sentence_days_served=(supervision_sentence.completion_date -
                                      supervision_sentence.start_date).days
            )
        ], projected_completion_buckets)

    def test_classify_supervision_success_empty_officer_district(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                projected_completion_date=date(2018, 12, 25),
                completion_date=date(2018, 12, 25),
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

        supervision_period_to_judicial_district_associations = {
            111: {
                'supervision_period_id': 111,
                'judicial_district_code': None
            }
        }

        supervision_sentences = [supervision_sentence]
        incarceration_periods = []

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex(incarceration_periods),
            supervision_period_agent_association,
            supervision_period_to_judicial_district_associations
        )
        self.assertEqual(1, len(projected_completion_buckets))

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertEqual([
            ProjectedSupervisionCompletionBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=12,
                bucket_date=last_day_of_month(supervision_sentence.projected_completion_date),
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                successful_completion=True,
                incarcerated_during_sentence=False,
                sentence_days_served=(
                    supervision_sentence.completion_date - supervision_sentence.start_date).days,
                supervising_officer_external_id=None,
                supervising_district_external_id=None,
                judicial_district_code=None
            )
        ], projected_completion_buckets)

    def test_classify_supervision_success_period_ends_after_projected_completion(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                projected_completion_date=date(2018, 12, 25),
                completion_date=date(2018, 10, 1),
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_periods = []

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex(incarceration_periods),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations
        )

        self.assertEqual(1, len(projected_completion_buckets))

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertEqual([
            ProjectedSupervisionCompletionBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=12,
                bucket_date=last_day_of_month(supervision_sentence.projected_completion_date),
                supervision_type=supervision_period_supervision_type,
                successful_completion=True,
                incarcerated_during_sentence=False,
                case_type=StateSupervisionCaseType.GENERAL,
                sentence_days_served=(
                    supervision_sentence.completion_date - supervision_sentence.start_date).days
            )
        ], projected_completion_buckets)

    def test_classify_supervision_success_exclude_termination_reason_death(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DEATH,
                supervision_type=StateSupervisionType.PAROLE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                projected_completion_date=date(2018, 12, 25),
                completion_date=date(2018, 12, 25),
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_periods = []

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            incarceration_periods,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations
        )

        self.assertEqual(0, len(projected_completion_buckets))

    def test_classify_supervision_success_exclude_no_completion_date(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.SUSPENSION,
                supervision_type=StateSupervisionType.PAROLE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                projected_completion_date=date(2018, 12, 25),
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_periods = []

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            incarceration_periods,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations
        )

        self.assertEqual(0, len(projected_completion_buckets))

    def test_classify_supervision_success_exclude_completion_before_start(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.SUSPENSION,
                supervision_type=StateSupervisionType.PAROLE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                completion_date=date(2016, 10, 3),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                projected_completion_date=date(2018, 12, 25),
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_periods = []

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            incarceration_periods,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations
        )

        self.assertEqual(0, len(projected_completion_buckets))

    def test_classify_supervision_success_was_incarcerated_during_sentence(self):
        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_ND',
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 12, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=StateSupervisionType.PAROLE
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                completion_date=date(2018, 12, 20),
                projected_completion_date=date(2018, 12, 25),
                supervision_periods=[supervision_period]
            )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            admission_date=date(2017, 6, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_date=date(2017, 10, 5)
        )

        supervision_sentences = [supervision_sentence]
        incarceration_periods = [incarceration_period]

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex(incarceration_periods),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations
        )

        self.assertEqual(1, len(projected_completion_buckets))

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        sentence_length = (supervision_sentence.completion_date -
                           supervision_sentence.start_date).days

        self.assertEqual([
            ProjectedSupervisionCompletionBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=12,
                bucket_date=last_day_of_month(supervision_sentence.projected_completion_date),
                supervision_type=supervision_period_supervision_type,
                successful_completion=True,
                incarcerated_during_sentence=True,
                case_type=StateSupervisionCaseType.GENERAL,
                sentence_days_served=sentence_length
            )
        ], projected_completion_buckets)


class TestFindSupervisionTerminationBucket(unittest.TestCase):
    """Tests the find_supervision_termination_bucket function."""

    def test_find_supervision_termination_bucket(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ND',
            start_date=date(2018, 3, 5),
            termination_date=date(2019, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
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

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                completion_date=date(2019, 5, 19),
                supervision_periods=[supervision_period]
            )

        assessments = [first_assessment, first_reassessment, last_assessment]

        supervision_period_index = SupervisionPeriodIndex(supervision_periods=[supervision_period])

        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]
        violation_responses = []

        termination_bucket = identifier.find_supervision_termination_bucket(
            supervision_sentences,
            incarceration_sentences,
            supervision_period,
            supervision_period_index,
            assessments,
            violation_responses,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            IncarcerationPeriodIndex(incarceration_periods=[])
        )

        assessment_score_change = last_assessment.assessment_score - \
            first_reassessment.assessment_score

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_termination_bucket = SupervisionTerminationBucket(
            state_code=supervision_period.state_code,
            year=supervision_period.termination_date.year,
            month=supervision_period.termination_date.month,
            bucket_date=supervision_period.termination_date,
            supervision_type=supervision_period_supervision_type,
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
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ND',
            start_date=date(2018, 3, 5),
            termination_date=date(2019, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION
        )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                completion_date=date(2019, 5, 19),
                supervision_periods=[supervision_period]
            )

        assessments = []

        supervision_period_index = SupervisionPeriodIndex(supervision_periods=[supervision_period])

        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]
        violation_responses = []

        termination_bucket = identifier.find_supervision_termination_bucket(
            supervision_sentences,
            incarceration_sentences,
            supervision_period,
            supervision_period_index,
            assessments,
            violation_responses,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            IncarcerationPeriodIndex(incarceration_periods=[])
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_termination_bucket = SupervisionTerminationBucket(
            state_code=supervision_period.state_code,
            year=supervision_period.termination_date.year,
            month=supervision_period.termination_date.month,
            bucket_date=supervision_period.termination_date,
            supervision_type=supervision_period_supervision_type,
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
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ND',
            start_date=date(2018, 3, 5),
            termination_date=date(2019, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION
        )

        first_assessment = StateAssessment.new_with_defaults(
            state_code='US_CA',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2018, 3, 10)
        )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                completion_date=date(2019, 5, 19),
                supervision_periods=[supervision_period]
            )

        assessments = [first_assessment]

        supervision_period_index = SupervisionPeriodIndex(supervision_periods=[supervision_period])

        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]
        violation_responses = []

        termination_bucket = identifier.find_supervision_termination_bucket(
            supervision_sentences,
            incarceration_sentences,
            supervision_period,
            supervision_period_index,
            assessments,
            violation_responses,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            IncarcerationPeriodIndex(incarceration_periods=[])
        )

        supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_termination_bucket = SupervisionTerminationBucket(
            state_code=supervision_period.state_code,
            year=supervision_period.termination_date.year,
            month=supervision_period.termination_date.month,
            bucket_date=supervision_period.termination_date,
            supervision_type=supervision_period_supervision_type,
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
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ND',
            start_date=date(2018, 3, 5),
            supervision_type=StateSupervisionType.PROBATION
        )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[supervision_period]
            )

        assessments = []

        supervision_period_index = SupervisionPeriodIndex(supervision_periods=[supervision_period])

        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]
        violation_responses = []

        termination_bucket = identifier.find_supervision_termination_bucket(
            supervision_sentences,
            incarceration_sentences,
            supervision_period,
            supervision_period_index,
            assessments,
            violation_responses,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            IncarcerationPeriodIndex(incarceration_periods=[])
        )

        self.assertEqual(None, termination_bucket)

    def test_find_supervision_termination_bucket_multiple_in_month(self):
        """Tests that when multiple supervision periods end in the same month, the earliest start_date and the latest
        termination_date are used as the date boundaries for the assessments, but the termination_date on the
        supervision_period is still on the SupervisionTerminationBucket."""
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ND',
            start_date=date(2018, 3, 5),
            termination_date=date(2019, 11, 17),
            supervision_type=StateSupervisionType.PROBATION
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp2',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ND',
            start_date=date(2018, 1, 1),
            termination_date=date(2019, 11, 23),
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

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                completion_date=date(2019, 12, 23),
                supervision_periods=[
                    first_supervision_period, second_supervision_period]
            )

        assessments = [
            too_early_assessment,
            assessment_1,
            assessment_2,
            assessment_3
        ]

        supervision_period_index = \
            SupervisionPeriodIndex(
                supervision_periods=[first_supervision_period, second_supervision_period]
            )

        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]
        violation_responses = []

        termination_bucket = identifier.find_supervision_termination_bucket(
            supervision_sentences,
            incarceration_sentences,
            first_supervision_period,
            supervision_period_index,
            assessments,
            violation_responses,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            IncarcerationPeriodIndex(incarceration_periods=[])
        )

        first_supervision_period_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_termination_bucket = SupervisionTerminationBucket(
            state_code=first_supervision_period.state_code,
            year=first_supervision_period.termination_date.year,
            month=first_supervision_period.termination_date.month,
            bucket_date=first_supervision_period.termination_date,
            supervision_type=first_supervision_period_supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            assessment_score=assessment_3.assessment_score,
            assessment_type=assessment_3.assessment_type,
            termination_reason=first_supervision_period.termination_reason,
            assessment_score_change=(assessment_3.assessment_score -
                                     assessment_2.assessment_score),
        )

        self.assertEqual(expected_termination_bucket, termination_bucket)

    def test_find_supervision_termination_bucket_incarceration_overlaps_full_supervision_period(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ND',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id='ip1',
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2018, 3, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2018, 5, 19),
            release_reason=ReleaseReason.COMMUTED,
            source_supervision_violation_response=None
        )

        supervision_period_index = SupervisionPeriodIndex(supervision_periods=[supervision_period])

        termination_bucket = identifier.find_supervision_termination_bucket(
            supervision_sentences=[],
            incarceration_sentences=[],
            supervision_period=supervision_period,
            supervision_period_index=supervision_period_index,
            assessments=[],
            violation_responses=[],
            supervision_period_to_agent_associations=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            incarceration_period_index=IncarcerationPeriodIndex(
                incarceration_periods=[incarceration_period])
        )

        self.assertEqual(None, termination_bucket)

    def test_find_supervision_termination_bucket_us_mo_suspension_span_overlaps_full_supervision_period(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_MO',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id='ss',
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[supervision_period]
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=date(2018, 2, 5),
                    end_date=supervision_period.start_date,
                    supervision_type=StateSupervisionType.PAROLE
                ),
                SupervisionTypeSpan(
                    start_date=supervision_period.start_date,
                    end_date=supervision_period.termination_date,
                    supervision_type=None
                ),
                SupervisionTypeSpan(
                    start_date=supervision_period.termination_date,
                    end_date=None,
                    supervision_type=StateSupervisionType.PAROLE
                )
            ]
        )

        supervision_period_index = SupervisionPeriodIndex(supervision_periods=[supervision_period])

        termination_bucket = identifier.find_supervision_termination_bucket(
            supervision_sentences=[supervision_sentence],
            incarceration_sentences=[],
            supervision_period=supervision_period,
            supervision_period_index=supervision_period_index,
            assessments=[],
            violation_responses=[],
            supervision_period_to_agent_associations=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            incarceration_period_index=IncarcerationPeriodIndex(
                incarceration_periods=[])
        )

        self.assertEqual(None, termination_bucket)


class TestGetViolationAndResponseHistory(unittest.TestCase):
    """Tests the get_violation_and_response_history function."""

    def test_get_violation_and_response_history(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_XX',
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
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code='US_XX',
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

        violation_history = identifier.get_violation_and_response_history(
            supervision_violation_response.state_code, revocation_date, [supervision_violation_response])

        self.assertEqual(StateSupervisionViolationType.FELONY,
                         violation_history.most_severe_violation_type)
        self.assertIsNone(violation_history.most_severe_violation_type_subtype)
        self.assertEqual(StateSupervisionViolationResponseDecision.REVOCATION,
                         violation_history.most_severe_response_decision)
        self.assertEqual(1, violation_history.response_count)
        self.assertIsNone(violation_history.violation_history_description)
        self.assertEqual(violation_history.violation_type_frequency_counter, [['FELONY', 'ABSCONDED']])

    def test_get_violation_and_response_history_decision_on_most_recent_response(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_XX',
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

        supervision_violation_response_1 = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code='US_XX',
            response_date=date(2009, 1, 7),
            supervision_violation_response_decisions=[
                # A REVOCATION decision is more severe than TREATMENT_IN_PRISON, but this is not the most
                # recent response
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION
                )
            ],
            supervision_violation=supervision_violation
        )

        supervision_violation_response_2 = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id='OTHER_ID',
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code='US_XX',
            response_date=date(2009, 1, 9),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    decision=StateSupervisionViolationResponseDecision.TREATMENT_IN_PRISON,
                    revocation_type=StateSupervisionViolationResponseRevocationType.TREATMENT_IN_PRISON,
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                )
            ],
            supervision_violation=supervision_violation
        )

        revocation_date = date(2009, 2, 13)

        violation_history = identifier.get_violation_and_response_history(
            supervision_violation_response_1.state_code,
            revocation_date,
            [supervision_violation_response_1, supervision_violation_response_2])

        self.assertEqual(StateSupervisionViolationType.FELONY, violation_history.most_severe_violation_type)
        self.assertIsNone(violation_history.most_severe_violation_type_subtype)
        self.assertEqual(StateSupervisionViolationResponseDecision.TREATMENT_IN_PRISON,
                         violation_history.most_severe_response_decision)
        self.assertEqual(2, violation_history.response_count)
        self.assertIsNone(violation_history.violation_history_description)
        self.assertEqual([['FELONY', 'ABSCONDED']], violation_history.violation_type_frequency_counter)

    def test_get_violation_and_response_history_date_before_cutoff(self):
        supervision_violation_old = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_XX',
            violation_date=date(2005, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    violation_type=StateSupervisionViolationType.MISDEMEANOR
                )
            ]
        )

        supervision_violation_response_old = StateSupervisionViolationResponse.new_with_defaults(
            state_code='US_XX',
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2005, 1, 7),
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
            supervision_violation=supervision_violation_old
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            state_code='US_XX',
            supervision_violation_id=123455,
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
            state_code='US_XX',
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
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

        violation_history = identifier.get_violation_and_response_history(
            supervision_violation_response.state_code,
            revocation_date,
            [supervision_violation_response_old, supervision_violation_response])

        self.assertEqual(StateSupervisionViolationType.FELONY,
                         violation_history.most_severe_violation_type)
        self.assertIsNone(violation_history.most_severe_violation_type_subtype)
        self.assertEqual(StateSupervisionViolationResponseDecision.REVOCATION,
                         violation_history.most_severe_response_decision)
        self.assertEqual(1, violation_history.response_count)
        self.assertIsNone(violation_history.violation_history_description)
        self.assertEqual(violation_history.violation_type_frequency_counter, [['FELONY', 'ABSCONDED']])

    def test_get_violation_and_response_history_date_in_cutoff(self):
        """The `old` response and violation fall within a year of the last violation response before the revocation,
        but not within a year of the revocation date. Test that the `old` response is included in the response
        history."""
        supervision_violation_old = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_XX',
            violation_date=date(2008, 12, 7),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    violation_type=StateSupervisionViolationType.FELONY
                )
            ]
        )

        supervision_violation_response_old = StateSupervisionViolationResponse.new_with_defaults(
            state_code='US_XX',
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2008, 12, 7),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION
                )
            ],
            supervision_violation=supervision_violation_old
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            state_code='US_XX',
            supervision_violation_id=6789,
            violation_date=date(2009, 12, 1),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    violation_type=StateSupervisionViolationType.TECHNICAL
                )
            ]
        )

        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code='US_XX',
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2009, 12, 1),
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

        revocation_date = date(2009, 12, 31)

        violation_history = identifier.get_violation_and_response_history(
            supervision_violation_response.state_code,
            revocation_date,
            [supervision_violation_response_old, supervision_violation_response])

        self.assertEqual(StateSupervisionViolationType.FELONY,
                         violation_history.most_severe_violation_type)
        self.assertIsNone(violation_history.most_severe_violation_type_subtype)
        self.assertEqual(StateSupervisionViolationResponseDecision.REVOCATION,
                         violation_history.most_severe_response_decision)
        self.assertEqual(2, violation_history.response_count)
        self.assertIsNone(violation_history.violation_history_description)
        self.assertEqual(violation_history.violation_type_frequency_counter, [['FELONY'], ['TECHNICAL_NO_CONDITIONS']])

    def test_get_violation_and_response_history_with_us_mo_subtype(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_MO',
            violation_date=date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    violation_type=StateSupervisionViolationType.TECHNICAL),
            ],
            supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    condition='DRG'),
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    condition='OTHER'),
            ]
        )

        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype='INI',
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

        violation_history = \
            identifier.get_violation_and_response_history('US_MO', revocation_date, [supervision_violation_response])

        self.assertEqual(StateSupervisionViolationType.TECHNICAL,
                         violation_history.most_severe_violation_type)
        self.assertEqual('SUBSTANCE_ABUSE',
                         violation_history.most_severe_violation_type_subtype)
        self.assertEqual(StateSupervisionViolationResponseDecision.REVOCATION,
                         violation_history.most_severe_response_decision)
        self.assertEqual(1, violation_history.response_count)
        self.assertEqual(
            violation_history.violation_history_description, '1subs')
        self.assertEqual(violation_history.violation_type_frequency_counter, [['DRG', 'OTHER']])

    def test_get_violation_and_response_history_no_violations(self):
        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code='US_XX',
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
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

        violation_history = identifier.get_violation_and_response_history(
            supervision_violation_response.state_code, revocation_date, [supervision_violation_response])

        self.assertIsNone(violation_history.most_severe_violation_type)
        self.assertIsNone(violation_history.most_severe_violation_type_subtype)
        self.assertEqual(StateSupervisionViolationResponseDecision.REVOCATION,
                         violation_history.most_severe_response_decision)
        self.assertEqual(1, violation_history.response_count)
        self.assertIsNone(violation_history.violation_history_description)
        self.assertIsNone(violation_history.violation_type_frequency_counter)

    def test_get_violation_and_response_history_us_mo_response_subtype_filter(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_MO',
            violation_date=date(2009, 1, 3),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    violation_type=StateSupervisionViolationType.TECHNICAL),
            ]
        )

        # This should only be included when looking at the decision entries
        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype='INI',
            state_code='US_MO',
            response_date=date(2009, 1, 7),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                )
            ],
            supervision_violation=supervision_violation
        )

        # This should only be included when looking at the decision entries
        supervision_violation_response_supplemental = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype='SUP',
            state_code='US_MO',
            response_date=date(2009, 1, 19),
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
        )

        revocation_date = date(2009, 2, 13)

        violation_history = \
            identifier.get_violation_and_response_history(
                'US_MO', revocation_date, [supervision_violation_response, supervision_violation_response_supplemental])

        self.assertEqual(StateSupervisionViolationType.TECHNICAL, violation_history.most_severe_violation_type)
        self.assertIsNone(violation_history.most_severe_violation_type_subtype)
        self.assertEqual(StateSupervisionViolationResponseDecision.REVOCATION,
                         violation_history.most_severe_response_decision)
        self.assertEqual(1, violation_history.response_count)
        self.assertEqual(violation_history.violation_history_description, '1tech')
        self.assertEqual(violation_history.violation_type_frequency_counter, [['TECHNICAL_NO_CONDITIONS']])

    def test_get_violation_and_response_history_no_responses(self):
        revocation_date = date(2009, 2, 13)

        violation_history = identifier.get_violation_and_response_history('US_XX', revocation_date, [])

        self.assertIsNone(violation_history.most_severe_violation_type)
        self.assertIsNone(violation_history.most_severe_violation_type_subtype)
        self.assertIsNone(violation_history.most_severe_response_decision)
        self.assertEqual(0, violation_history.response_count)
        self.assertIsNone(violation_history.violation_history_description)
        self.assertIsNone(violation_history.violation_type_frequency_counter)

    def test_get_violation_and_response_history_citation_date(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_XX',
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
                state_code='US_XX',
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
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

        violation_history = identifier.get_violation_and_response_history(
            supervision_violation_response.state_code, revocation_date, [supervision_violation_response])

        self.assertEqual(StateSupervisionViolationType.MISDEMEANOR,
                         violation_history.most_severe_violation_type)
        self.assertIsNone(violation_history.most_severe_violation_type_subtype)
        self.assertEqual(StateSupervisionViolationResponseDecision.REVOCATION,
                         violation_history.most_severe_response_decision)
        self.assertEqual(1, violation_history.response_count)
        self.assertIsNone(violation_history.violation_history_description)
        self.assertEqual([['ABSCONDED', 'MISDEMEANOR']],
                         violation_history.violation_type_frequency_counter)

    def test_get_violation_and_response_history_is_draft(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_XX',
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
                state_code='US_XX',
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2009, 1, 7),
                is_draft=True,
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
                supervision_violation=supervision_violation
            )

        revocation_date = date(2009, 2, 13)

        violation_history = identifier.get_violation_and_response_history(
            supervision_violation_response.state_code, revocation_date, [supervision_violation_response])

        self.assertIsNone(violation_history.most_severe_violation_type)
        self.assertIsNone(violation_history.most_severe_violation_type_subtype)
        self.assertEqual(0, violation_history.response_count)
        self.assertIsNone(violation_history.most_severe_response_decision)
        self.assertIsNone(violation_history.violation_history_description)
        self.assertIsNone(violation_history.violation_type_frequency_counter)

    def test_get_violation_and_response_history_us_mo_handle_citations(self):
        """Tests that US_MO citations with no violation types get TECHNICAL violation types added to them."""
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_MO',
            violation_date=date(2009, 1, 7)
        )

        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code='US_MO',
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.CITATION,
            response_date=date(2009, 1, 7),
            is_draft=False,
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION)
            ],
            supervision_violation=supervision_violation
        )

        revocation_date = date(2009, 2, 13)

        violation_history = identifier.get_violation_and_response_history(
            'US_MO', revocation_date, [supervision_violation_response])

        self.assertEqual(StateSupervisionViolationType.TECHNICAL, violation_history.most_severe_violation_type)
        self.assertIsNone(violation_history.most_severe_violation_type_subtype)
        self.assertEqual(StateSupervisionViolationResponseDecision.REVOCATION,
                         violation_history.most_severe_response_decision)
        self.assertEqual(1, violation_history.response_count)
        self.assertEqual('1tech', violation_history.violation_history_description)
        self.assertEqual([['TECHNICAL_NO_CONDITIONS']],
                         violation_history.violation_type_frequency_counter)

    def test_get_violation_and_response_history_us_mo_handle_law_citations(self):
        """Tests that a US_MO citation with no violation types and a LAW condition gets a TECHNICAL violation type added
        to it, a 'LAW_CITATION' condition added, and the violation subtype of LAW_CITATION is set."""
        supervision_violation = \
            StateSupervisionViolation.new_with_defaults(
                supervision_violation_id=123455,
                state_code='US_MO',
                violation_date=date(2009, 1, 7),
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='LAW'),
                ]
            )

        supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_type=StateSupervisionViolationResponseType.CITATION,
                response_date=date(2009, 1, 7),
                is_draft=False,
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
                supervision_violation=supervision_violation
            )

        revocation_date = date(2009, 2, 13)

        violation_history = identifier.get_violation_and_response_history(
            'US_MO', revocation_date, [supervision_violation_response])

        self.assertEqual(StateSupervisionViolationType.TECHNICAL,
                         violation_history.most_severe_violation_type)
        self.assertEqual(
            'LAW_CITATION', violation_history.most_severe_violation_type_subtype)
        self.assertEqual(StateSupervisionViolationResponseDecision.REVOCATION,
                         violation_history.most_severe_response_decision)
        self.assertEqual(1, violation_history.response_count)
        self.assertEqual(
            '1law_cit', violation_history.violation_history_description)
        self.assertEqual([['LAW_CITATION']],
                         violation_history.violation_type_frequency_counter)

    def test_get_violation_and_response_history_us_mo_handle_law_citations_most_severe(self):
        """Tests that a US_MO citation with no violation types and a LAW condition gets a TECHNICAL violation type added
        to it, a 'LAW_CITATION' condition added, and the violation subtype of LAW_CITATION is set. Also tests that a
        LAW_CITATION is ranked higher than an ABSCONDED and MUNICIPAL violation for US_MO."""
        supervision_violation_law_cit = \
            StateSupervisionViolation.new_with_defaults(
                supervision_violation_id=123455,
                state_code='US_MO',
                violation_date=date(2009, 1, 7),
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='LAW'),
                ]
            )

        supervision_violation_response_law = \
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_type=StateSupervisionViolationResponseType.CITATION,
                response_date=date(2009, 1, 7),
                is_draft=False,
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
                supervision_violation=supervision_violation_law_cit
            )

        supervision_violation_absc = \
            StateSupervisionViolation.new_with_defaults(
                supervision_violation_id=6789,
                state_code='US_MO',
                violation_date=date(2009, 1, 7),
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.ABSCONDED
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.MUNICIPAL
                    )
                ]
            )

        supervision_violation_response_absc = \
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype='INI',
                response_date=date(2009, 1, 10),
                is_draft=False,
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
                supervision_violation=supervision_violation_absc
            )

        revocation_date = date(2009, 2, 13)

        violation_history = identifier.get_violation_and_response_history(
            'US_MO', revocation_date, [supervision_violation_response_law, supervision_violation_response_absc])

        self.assertEqual(StateSupervisionViolationType.TECHNICAL,
                         violation_history.most_severe_violation_type)
        self.assertEqual('LAW_CITATION', violation_history.most_severe_violation_type_subtype)
        self.assertEqual(StateSupervisionViolationResponseDecision.REVOCATION,
                         violation_history.most_severe_response_decision)
        self.assertEqual(2, violation_history.response_count)
        self.assertEqual('1law_cit;1absc', violation_history.violation_history_description)
        self.assertEqual([['LAW_CITATION'], ['ABSCONDED', 'MUNICIPAL']],
                         violation_history.violation_type_frequency_counter)

    def test_get_violation_and_response_history_us_mo_handle_drg_citations(self):
        """Tests that a US_MO citation with no violation types and a DRG condition gets a TECHNICAL violation type added
        to it and the violation subtype of SUBSTANCE_ABUSE is returned."""
        supervision_violation = \
            StateSupervisionViolation.new_with_defaults(
                supervision_violation_id=123455,
                state_code='US_MO',
                violation_date=date(2009, 1, 7),
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='DRG'),
                ]
            )

        supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_type=StateSupervisionViolationResponseType.CITATION,
                response_date=date(2009, 1, 7),
                is_draft=False,
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
                supervision_violation=supervision_violation
            )

        revocation_date = date(2009, 2, 13)

        violation_history = identifier.get_violation_and_response_history(
            'US_MO', revocation_date, [supervision_violation_response])

        self.assertEqual(StateSupervisionViolationType.TECHNICAL,
                         violation_history.most_severe_violation_type)
        self.assertEqual('SUBSTANCE_ABUSE',
                         violation_history.most_severe_violation_type_subtype)
        self.assertEqual(StateSupervisionViolationResponseDecision.REVOCATION,
                         violation_history.most_severe_response_decision)
        self.assertEqual(1, violation_history.response_count)
        self.assertEqual(
            '1subs', violation_history.violation_history_description)
        self.assertEqual(
            [['DRG']], violation_history.violation_type_frequency_counter)

    def test_get_violation_and_response_history_us_mo_handle_law_technicals(self):
        """Tests that a US_MO violation report with a TECHNICAL type and a LAW condition is not treated like a
        citation with a LAW condition."""
        supervision_violation = \
            StateSupervisionViolation.new_with_defaults(
                supervision_violation_id=123455,
                state_code='US_MO',
                violation_date=date(2009, 1, 7),
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.TECHNICAL),
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='LAW'),
                ]
            )

        supervision_violation_response = \
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype='ITR',
                response_date=date(2009, 1, 7),
                is_draft=False,
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
                supervision_violation=supervision_violation
            )

        revocation_date = date(2009, 2, 13)

        violation_history = identifier.get_violation_and_response_history(
            'US_MO', revocation_date, [supervision_violation_response])

        self.assertEqual(StateSupervisionViolationType.TECHNICAL,
                         violation_history.most_severe_violation_type)
        self.assertIsNone(violation_history.most_severe_violation_type_subtype)
        self.assertEqual(StateSupervisionViolationResponseDecision.REVOCATION,
                         violation_history.most_severe_response_decision)
        self.assertEqual(1, violation_history.response_count)
        self.assertEqual(
            '1tech', violation_history.violation_history_description)
        self.assertEqual(
            [['LAW']], violation_history.violation_type_frequency_counter)


class TestGetResponsesInWindowBeforeRevocation(unittest.TestCase):
    """Test the get_responses_in_window_before_revocation function."""

    def test_get_responses_in_window_before_revocation(self):
        violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2000, 1, 1)
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2000, 2, 1)
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2000, 3, 1)
            )
        ]

        revocation_date = date(2001, 1, 17)

        responses_in_window = identifier._get_responses_in_window_before_revocation(
            revocation_date, violation_responses, include_follow_up_responses=False)

        self.assertEqual(violation_responses, responses_in_window)

    def test_get_responses_in_window_before_revocation_exclude_before_window(self):
        violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2000, 1, 1)
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(1998, 2, 1)
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(1997, 3, 1)
            )
        ]

        revocation_date = date(2010, 1, 17)

        responses_in_window = identifier._get_responses_in_window_before_revocation(
            revocation_date, violation_responses, include_follow_up_responses=False)

        self.assertEqual([
            StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2000, 1, 1)
            )
        ], responses_in_window)

    def test_get_responses_in_window_before_revocation_none_before_revocation(self):
        violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2000, 1, 1)
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(1998, 2, 1)
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(1997, 3, 1)
            )
        ]

        revocation_date = date(1900, 1, 17)

        responses_in_window = identifier._get_responses_in_window_before_revocation(
            revocation_date, violation_responses, include_follow_up_responses=False)

        self.assertEqual([], responses_in_window)

    def test_get_responses_in_window_before_revocation_exclude_permanent_decision(self):
        violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                response_date=date(2000, 1, 1)
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(1998, 2, 1)
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(1997, 3, 1)
            )
        ]

        revocation_date = date(2000, 1, 17)

        responses_in_window = identifier._get_responses_in_window_before_revocation(
            revocation_date, violation_responses, include_follow_up_responses=False)

        self.assertCountEqual([
            StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(1998, 2, 1)
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(1997, 3, 1)
            )
        ], responses_in_window)

    def test_get_responses_in_window_before_revocation_us_mo_do_not_include_supplemental(self):
        violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype='INI',
                response_date=date(1998, 2, 1)
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype='SUP',
                response_date=date(1998, 3, 1)
            )
        ]

        revocation_date = date(2000, 1, 17)

        responses_in_window = identifier._get_responses_in_window_before_revocation(
            revocation_date, violation_responses, include_follow_up_responses=False)

        self.assertCountEqual([
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype='INI',
                response_date=date(1998, 2, 1)
            )
        ], responses_in_window)

    def test_get_responses_in_window_before_revocation_us_mo_include_supplemental(self):
        violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype='INI',
                response_date=date(1998, 2, 1)
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype='SUP',
                response_date=date(1998, 3, 1)
            )
        ]

        revocation_date = date(2000, 1, 17)

        responses_in_window = identifier._get_responses_in_window_before_revocation(
            revocation_date, violation_responses, include_follow_up_responses=True)

        self.assertCountEqual([
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype='INI',
                response_date=date(1998, 2, 1)
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code='US_MO',
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype='SUP',
                response_date=date(1998, 3, 1)
            )
        ], responses_in_window)


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

        most_severe_revocation_type = identifier._identify_most_severe_revocation_type(
            response_decision_entries)

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

        most_severe_case_type = identifier._identify_most_severe_case_type(
            supervision_period)

        self.assertEqual(most_severe_case_type,
                         StateSupervisionCaseType.SEX_OFFENDER)

    def test_identify_most_severe_case_type_test_all_types(self):
        for case_type in StateSupervisionCaseType:
            supervision_period = StateSupervisionPeriod.new_with_defaults(
                case_type_entries=[
                    StateSupervisionCaseTypeEntry.new_with_defaults(
                        case_type=case_type
                    ),
                ]
            )

            most_severe_case_type = identifier._identify_most_severe_case_type(
                supervision_period)

            self.assertEqual(most_severe_case_type, case_type)

    def test_identify_most_severe_case_type_no_type_entries(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            case_type_entries=[]
        )

        most_severe_case_type = identifier._identify_most_severe_case_type(
            supervision_period)

        self.assertEqual(most_severe_case_type,
                         StateSupervisionCaseType.GENERAL)


class TestGetRevocationDetails(unittest.TestCase):
    """Tests the _get_revocation_details function."""

    def test_get_revocation_details(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_MO',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site='DISTRICT 999'
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_MO',
            violation_date=date(2018, 4, 20),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    violation_type=StateSupervisionViolationType.MISDEMEANOR
                )
            ]
        )

        source_supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
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
            external_id='ip1',
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            source_supervision_violation_response=source_supervision_violation_response
        )

        revocation_details = identifier._get_revocation_details(incarceration_period, supervision_period,
                                                                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                                                                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS)

        self.assertEqual(revocation_details,
                         identifier.RevocationDetails(
                             revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                             source_violation_type=StateSupervisionViolationType.MISDEMEANOR,
                             supervising_district_external_id=DEFAULT_SSVR_AGENT_ASSOCIATIONS.get(
                                 supervision_period.supervision_period_id).get('district_external_id'),
                             supervising_officer_external_id=DEFAULT_SSVR_AGENT_ASSOCIATIONS.get(
                                 supervision_period.supervision_period_id).get('agent_external_id')
                         ))

    def test_get_revocation_details_no_ssvr(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_XX',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site='DISTRICT 999'
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id='ip1',
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='US_XX',
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            source_supervision_violation_response=None
        )

        revocation_details = identifier._get_revocation_details(incarceration_period, supervision_period,
                                                                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                                                                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS)

        self.assertEqual(revocation_details,
                         identifier.RevocationDetails(
                             revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                             source_violation_type=None,
                             supervising_district_external_id=None,
                             supervising_officer_external_id=None,
                         ))

    def test_get_revocation_details_no_ssvr_us_mo(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_MO',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site='DISTRICT 999'
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id='ip1',
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='US_MO',
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            source_supervision_violation_response=None
        )

        revocation_details = identifier._get_revocation_details(incarceration_period, supervision_period,
                                                                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                                                                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS)

        self.assertEqual(revocation_details,
                         identifier.RevocationDetails(
                             revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                             source_violation_type=None,
                             supervising_district_external_id=supervision_period.supervision_site,
                             supervising_officer_external_id=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.get(
                                 supervision_period.supervision_period_id).get('agent_external_id')
                         ))

    def test_get_revocation_details_no_supervision_period_us_mo(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_MO',
            violation_date=date(2018, 4, 20),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    violation_type=StateSupervisionViolationType.MISDEMEANOR
                )
            ]
        )

        source_supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=000,
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
            external_id='ip1',
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='US_MO',
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            source_supervision_violation_response=source_supervision_violation_response
        )

        revocation_details = identifier._get_revocation_details(incarceration_period, None,
                                                                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                                                                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS)

        self.assertEqual(revocation_details,
                         identifier.RevocationDetails(
                             revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                             source_violation_type=StateSupervisionViolationType.MISDEMEANOR,
                             supervising_district_external_id=None,
                             supervising_officer_external_id=None,
                         ))

    def test_get_revocation_details_no_revocation_type(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_MO',
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site='DISTRICT 999'
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code='US_MO',
            violation_date=date(2018, 4, 20),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    violation_type=StateSupervisionViolationType.MISDEMEANOR
                )
            ]
        )

        source_supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_date=date(2018, 4, 23),
            supervision_violation_response_decisions=None,
            revocation_type=None,
            supervision_violation=supervision_violation
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id='ip1',
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code='US_ND',
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            source_supervision_violation_response=source_supervision_violation_response
        )

        revocation_details = identifier._get_revocation_details(incarceration_period, supervision_period,
                                                                DEFAULT_SSVR_AGENT_ASSOCIATIONS,
                                                                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS)

        self.assertEqual(revocation_details,
                         identifier.RevocationDetails(
                             revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                             source_violation_type=StateSupervisionViolationType.MISDEMEANOR,
                             supervising_district_external_id=DEFAULT_SSVR_AGENT_ASSOCIATIONS.get(
                                 supervision_period.supervision_period_id).get('district_external_id'),
                             supervising_officer_external_id=DEFAULT_SSVR_AGENT_ASSOCIATIONS.get(
                                 supervision_period.supervision_period_id).get('agent_external_id')
                         ))


class TestRevokedSupervisionPeriodsIfRevocationOccurred(unittest.TestCase):
    """tests the _revoked_supervision_periods_if_revocation_occurred function."""
    def test_revoked_supervision_periods_if_revocation_occurred(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_XX',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_date=date(2019, 5, 29),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods = []

        admission_is_revocation, revoked_periods = identifier._revoked_supervision_periods_if_revocation_occurred(
            incarceration_period, supervision_periods, None
        )

        self.assertTrue(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_occured_with_general_purpose_US_ID(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2019, 5, 29),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods = []

        admission_is_revocation, revoked_periods = identifier._revoked_supervision_periods_if_revocation_occurred(
            incarceration_period, supervision_periods, None
        )

        self.assertTrue(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_occurred_with_treatment_US_ID(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=AdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2019, 5, 29),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
        )

        supervision_periods = []

        admission_is_revocation, revoked_periods = identifier._revoked_supervision_periods_if_revocation_occurred(
            incarceration_period, supervision_periods, None
        )

        self.assertTrue(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_did_not_occur_with_treatment_transfer_US_ID(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            start_date=date(2017, 1, 1),
            termination_date=date(2017, 5, 17),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        treatment_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=AdmissionReason.RETURN_FROM_SUPERVISION,
            release_date=date(2017, 5, 29),
            release_reason=ReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON)

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 29),
            admission_reason=AdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods = [supervision_period]

        admission_is_revocation, revoked_periods = identifier._revoked_supervision_periods_if_revocation_occurred(
            incarceration_period, supervision_periods, treatment_period
        )

        self.assertFalse(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_occurred_US_ID_NoRecentSupervision(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
        )

        # Incarceration period that occurred more than SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT months after
        # the most recent supervision period ended
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=
            supervision_period.termination_date + relativedelta(months=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT + 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods = [supervision_period]

        admission_is_revocation, revoked_periods = identifier._revoked_supervision_periods_if_revocation_occurred(
            incarceration_period, supervision_periods, None
        )

        self.assertTrue(admission_is_revocation)
        self.assertEqual([], revoked_periods)

    def test_revoked_supervision_periods_if_revocation_occurred_US_ID_InvestigationSupervision(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id='sp1',
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code='US_ID',
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id='ip2',
            state_code='US_ID',
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 9),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL
        )

        supervision_periods = [supervision_period]

        admission_is_revocation, revoked_periods = identifier._revoked_supervision_periods_if_revocation_occurred(
            incarceration_period, supervision_periods, None
        )

        self.assertFalse(admission_is_revocation)
        self.assertEqual([], revoked_periods)


class TestGetViolationTypeFrequencyCounter(unittest.TestCase):
    """Tests the _get_violation_type_frequency_counter function."""

    def test_get_violation_type_frequency_counter(self):
        violations = [
            StateSupervisionViolation.new_with_defaults(
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.ABSCONDED
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.FELONY
                    )
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='DRG'
                    )
                ]
            )
        ]

        violation_type_frequency_counter = identifier._get_violation_type_frequency_counter(
            violations)

        self.assertEqual([['ABSCONDED', 'FELONY', 'DRG']],
                         violation_type_frequency_counter)

    def test_get_violation_type_frequency_counter_technical_only(self):
        violations = [
            StateSupervisionViolation.new_with_defaults(
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.TECHNICAL
                    )
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='DRG'
                    )
                ]
            )
        ]

        violation_type_frequency_counter = identifier._get_violation_type_frequency_counter(
            violations)

        self.assertEqual([['DRG']], violation_type_frequency_counter)

    def test_get_violation_type_frequency_counter_technical_only_no_conditions(self):
        violations = [
            StateSupervisionViolation.new_with_defaults(
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.TECHNICAL
                    )
                ]
            )
        ]

        violation_type_frequency_counter = identifier._get_violation_type_frequency_counter(
            violations)

        self.assertEqual([['TECHNICAL_NO_CONDITIONS']],
                         violation_type_frequency_counter)

    def test_get_violation_type_frequency_counter_multiple_violations(self):
        violations = [
            StateSupervisionViolation.new_with_defaults(
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.ABSCONDED
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.FELONY
                    )
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='WEA'
                    )
                ]
            ),
            StateSupervisionViolation.new_with_defaults(
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.MISDEMEANOR
                    ),
                    StateSupervisionViolationTypeEntry.new_with_defaults(
                        violation_type=StateSupervisionViolationType.TECHNICAL
                    )
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='DRG'
                    ),
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        condition='EMP'
                    )
                ]
            )
        ]

        violation_type_frequency_counter = identifier._get_violation_type_frequency_counter(
            violations)

        self.assertEqual([['ABSCONDED', 'FELONY', 'WEA'], ['MISDEMEANOR', 'DRG', 'EMP']],
                         violation_type_frequency_counter)


class TestConvertBucketsToDual(unittest.TestCase):
    """Tests the _convert_buckets_to_dual function."""

    def test_convert_buckets_to_dual_us_mo(self):
        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False,
            )
        ]

        updated_buckets = identifier._convert_buckets_to_dual(
            supervision_time_buckets=supervision_time_buckets)

        expected_output = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
            )
        ]

        self.assertCountEqual(updated_buckets, expected_output)

    def test_convert_buckets_to_dual_us_mo_one_dual(self):
        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False,
            )
        ]

        updated_buckets = identifier._convert_buckets_to_dual(
            supervision_time_buckets=supervision_time_buckets)

        expected_output = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
            )
        ]

        self.assertCountEqual(updated_buckets, expected_output)

    def test_convert_buckets_to_dual_us_mo_one_other_day(self):
        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 4),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False,
            ),
        ]

        updated_buckets = identifier._convert_buckets_to_dual(
            supervision_time_buckets=supervision_time_buckets)

        expected_output = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 4),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False,
            ),
        ]

        self.assertCountEqual(updated_buckets, expected_output)

    def test_convert_buckets_to_dual_us_mo_one_different_type(self):
        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=True,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=True,
            ),
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=True
            )
        ]

        updated_buckets = identifier._convert_buckets_to_dual(
            supervision_time_buckets=supervision_time_buckets)

        expected_output = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=True,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=True,
            ),
            ProjectedSupervisionCompletionBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=True
            )
        ]

        self.assertCountEqual(updated_buckets, expected_output)

    def test_convert_buckets_to_dual_us_mo_different_bucket_types_same_metric(self):
        supervision_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False,
            )
        ]

        updated_buckets = identifier._convert_buckets_to_dual(
            supervision_time_buckets=supervision_time_buckets)

        expected_output = [
            RevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code='US_MO',
                year=1900,
                month=1,
                bucket_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
            )
        ]

        self.assertCountEqual(updated_buckets, expected_output)

    def test_convert_buckets_to_dual_empty_input(self):
        supervision_time_buckets = []

        updated_buckets = identifier._convert_buckets_to_dual(
            supervision_time_buckets=supervision_time_buckets)

        expected_output = []

        self.assertCountEqual(updated_buckets, expected_output)

    def test_convert_buckets_to_dual_metric_type_coverage(self):
        """Asserts all SupervisionMetricTypes are handled in the function."""
        for metric_type in SupervisionMetricType:
            self.assertTrue(
                metric_type in identifier.BUCKET_TYPES_FOR_METRIC.keys())


class TestIncludeTerminationInSuccessMetric(unittest.TestCase):
    def test_include_termination_in_success_metric(self):
        for termination_reason in StateSupervisionPeriodTerminationReason:
            _ = identifier._include_termination_in_success_metric(
                termination_reason)


def expected_non_revocation_return_time_buckets(
        supervision_period: StateSupervisionPeriod,
        supervision_type: StateSupervisionPeriodSupervisionType,
        end_date: Optional[date] = None,
        case_type: Optional[StateSupervisionCaseType] = StateSupervisionCaseType.GENERAL,
        assessment_score: Optional[int] = None,
        assessment_level: Optional[StateAssessmentLevel] = None,
        assessment_type: Optional[StateAssessmentType] = None,
        most_severe_violation_type: Optional[StateSupervisionViolationType] = None,
        most_severe_violation_type_subtype: Optional[str] = None,
        response_count: Optional[int] = 0,
        supervising_officer_external_id: Optional[str] = None,
        supervising_district_external_id: Optional[str] = None,
        case_compliances: Dict[date, SupervisionCaseCompliance] = None,
        judicial_district_code: Optional[str] = None
) -> \
        List[NonRevocationReturnSupervisionTimeBucket]:
    """Returns the expected NonRevocationReturnSupervisionTimeBuckets based on the provided |supervision_period|
    and when the buckets should end."""

    expected_buckets = []

    if not case_compliances:
        case_compliances = defaultdict()

    if not end_date:
        end_date = (supervision_period.termination_date
                    if supervision_period.termination_date else date.today() + relativedelta(days=1))

    start_date = supervision_period.start_date

    if start_date:
        days_on_supervision = [start_date + relativedelta(days=x)
                               for x in range((end_date - start_date).days)]

        if days_on_supervision:
            # Ensuring we're not counting the end date
            assert max(days_on_supervision) < end_date

        for day_on_supervision in days_on_supervision:
            is_on_supervision_last_day_of_month = day_on_supervision == last_day_of_month(day_on_supervision)

            case_compliance = case_compliances.get(day_on_supervision)

            bucket = NonRevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=day_on_supervision.year,
                month=day_on_supervision.month,
                bucket_date=day_on_supervision,
                supervision_type=supervision_type,
                case_type=case_type,
                assessment_score=assessment_score,
                assessment_level=assessment_level,
                assessment_type=assessment_type,
                most_severe_violation_type=most_severe_violation_type,
                most_severe_violation_type_subtype=most_severe_violation_type_subtype,
                response_count=response_count,
                supervising_officer_external_id=supervising_officer_external_id,
                supervising_district_external_id=supervising_district_external_id,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                is_on_supervision_last_day_of_month=is_on_supervision_last_day_of_month,
                case_compliance=case_compliance,
                judicial_district_code=judicial_district_code
            )

            expected_buckets.append(bucket)

    return expected_buckets


class TestFindAssessmentScoreChange(unittest.TestCase):
    """Tests the find_assessment_score_change function."""
    def test_find_assessment_score_change(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code='US_ND',
            assessment_type=StateAssessmentType.ORAS,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_score=33,
            assessment_date=date(2015, 3, 23)
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code='US_ND',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2015, 11, 2)
        )

        assessment_3 = StateAssessment.new_with_defaults(
            state_code='US_ND',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=23,
            assessment_level=StateAssessmentLevel.MEDIUM,
            assessment_date=date(2016, 1, 13)
        )

        assessments = [assessment_1,
                       assessment_2,
                       assessment_3]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        assessment_score_change, \
            end_assessment_score, \
            end_assessment_level, \
            end_assessment_type = \
            identifier.find_assessment_score_change(
                assessment_1.state_code,
                start_date,
                termination_date,
                assessments
            )

        self.assertEqual(-6, assessment_score_change)
        self.assertEqual(assessment_3.assessment_score, end_assessment_score)
        self.assertEqual(assessment_3.assessment_level, end_assessment_level)
        self.assertEqual(assessment_3.assessment_type, end_assessment_type)

    def test_find_assessment_score_change_insufficient_assessments(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code='US_ND',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2015, 3, 23)
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code='US_ND',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2015, 11, 2)
        )

        assessments = [assessment_1,
                       assessment_2]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        assessment_score_change, \
            end_assessment_score, \
            end_assessment_level, \
            end_assessment_type = \
            identifier.find_assessment_score_change(
                assessment_1.state_code,
                start_date,
                termination_date,
                assessments
            )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)

    def test_find_assessment_score_change_first_reliable_assessment_is_first_assessment(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code='US_XX',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2015, 3, 23)
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code='US_XX',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2015, 11, 2)
        )

        assessments = [assessment_1, assessment_2]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        assessment_score_change, \
            end_assessment_score, \
            end_assessment_level, \
            end_assessment_type = \
            identifier.find_assessment_score_change(
                assessment_1.state_code,
                start_date,
                termination_date,
                assessments
            )

        self.assertEqual(-4, assessment_score_change)
        self.assertEqual(assessment_2.assessment_score, end_assessment_score)
        self.assertEqual(assessment_2.assessment_level, end_assessment_level)
        self.assertEqual(assessment_2.assessment_type, end_assessment_type)

    def test_find_assessment_score_change_different_type(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code='US_ND',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2015, 3, 23)
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code='US_ND',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2015, 11, 2)
        )

        assessment_3 = StateAssessment.new_with_defaults(
            state_code='US_ND',
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=23,
            assessment_date=date(2016, 1, 13)
        )

        assessments = [assessment_1,
                       assessment_2,
                       assessment_3]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        assessment_score_change, \
            end_assessment_score, \
            end_assessment_level, \
            end_assessment_type = \
            identifier.find_assessment_score_change(
                assessment_1.state_code,
                start_date,
                termination_date,
                assessments
            )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)

    def test_find_assessment_score_change_same_date(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code='US_ND',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2015, 3, 23)
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code='US_ND',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2015, 11, 2)
        )

        assessment_3 = StateAssessment.new_with_defaults(
            state_code='US_ND',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=23,
            assessment_date=date(2016, 11, 2)
        )

        assessments = [assessment_1,
                       assessment_2,
                       assessment_3]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        assessment_score_change, \
            end_assessment_score, \
            end_assessment_level, \
            end_assessment_type = \
            identifier.find_assessment_score_change(
                assessment_1.state_code,
                start_date,
                termination_date,
                assessments
            )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)

    def test_find_assessment_score_change_no_assessments(self):
        assessments = []

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        assessment_score_change, \
            end_assessment_score, \
            end_assessment_level, \
            end_assessment_type = \
            identifier.find_assessment_score_change(
                'US_XX',
                start_date,
                termination_date,
                assessments
            )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)

    def test_find_assessment_score_change_outside_boundary(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code='US_ND',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2011, 3, 23)
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code='US_ND',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2015, 11, 2)
        )

        assessment_3 = StateAssessment.new_with_defaults(
            state_code='US_ND',
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=23,
            assessment_date=date(2016, 1, 13)
        )

        assessments = [assessment_1,
                       assessment_2,
                       assessment_3]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        assessment_score_change, \
            end_assessment_score, \
            end_assessment_level, \
            end_assessment_type = \
            identifier.find_assessment_score_change(
                assessment_1.state_code,
                start_date,
                termination_date,
                assessments
            )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)
