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
# pylint: disable=unused-import,wrong-import-order,protected-access

"""Tests for supervision/identifier.py."""

from collections import defaultdict
from datetime import date

import unittest
from typing import Optional, List, Dict
from unittest import mock

import attr
from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

import recidiviz.calculator.pipeline.utils.supervision_period_utils
import recidiviz.calculator.pipeline.utils.violation_response_utils
import recidiviz.calculator.pipeline.utils.violation_utils
from recidiviz.calculator.pipeline.supervision import identifier
from recidiviz.calculator.pipeline.supervision.identifier import _get_projected_end_date
from recidiviz.calculator.pipeline.supervision.supervision_case_compliance import (
    SupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.incarceration_period_index import (
    IncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.supervision.metrics import SupervisionMetricType
from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import (
    NonRevocationReturnSupervisionTimeBucket,
    RevocationReturnSupervisionTimeBucket,
    ProjectedSupervisionCompletionBucket,
    SupervisionTerminationBucket,
    SupervisionStartBucket,
    SupervisionTimeBucket,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_supervising_officer_and_location_info_from_supervision_period,
    get_state_specific_case_compliance_manager,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_sentence_classification import (
    SupervisionTypeSpan,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_commitment_from_supervision_utils import (
    PURPOSE_FOR_INCARCERATION_PVC,
    SHOCK_INCARCERATION_9_MONTHS,
)

from recidiviz.calculator.pipeline.utils.supervision_case_compliance_manager import (
    StateSupervisionCaseComplianceManager,
)

from recidiviz.calculator.pipeline.utils.supervision_period_index import (
    SupervisionPeriodIndex,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentType,
    StateAssessmentLevel,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason as AdmissionReason,
    StateIncarcerationPeriodReleaseReason as ReleaseReason,
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodTerminationReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseRevocationType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
    StateSupervisionViolationResponseDecidingBodyType,
)
from recidiviz.common.date import last_day_of_month
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionPeriod,
    StateIncarcerationPeriod,
    StateSupervisionViolationResponse,
    StateSupervisionViolation,
    StateSupervisionViolationTypeEntry,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionSentence,
    StateAssessment,
    StateSupervisionCaseTypeEntry,
    StateSupervisionViolatedConditionEntry,
    StateIncarcerationSentence,
    StateSupervisionContact,
)
from recidiviz.tests.calculator.pipeline.utils.us_mo_fakes import (
    FakeUsMoSupervisionSentence,
    FakeUsMoIncarcerationSentence,
)

_DEFAULT_SUPERVISION_PERIOD_ID = 999
_DEFAULT_SSVR_ID = 999

DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS = {
    999: {"agent_id": 000, "agent_external_id": "XXX", "supervision_period_id": 999}
}

DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST = list(
    DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.values()
)

DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS = {
    _DEFAULT_SUPERVISION_PERIOD_ID: {
        "supervision_period_id": _DEFAULT_SUPERVISION_PERIOD_ID,
        "judicial_district_code": "XXX",
    }
}
DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST = list(
    DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS.values()
)


# TODO(#2732): Implement more full test coverage of the officer and district
#  functionality and the supervision success classification
class TestClassifySupervisionTimeBuckets(unittest.TestCase):
    """Tests for the find_supervision_time_buckets function."""

    def setUp(self):
        self.maxDiff = None

        self.assessment_types_patcher = mock.patch(
            "recidiviz.calculator.pipeline.supervision.identifier.assessment_utils."
            "_assessment_types_of_class_for_state"
        )
        self.mock_assessment_types = self.assessment_types_patcher.start()
        self.mock_assessment_types.return_value = [
            StateAssessmentType.ORAS,
            StateAssessmentType.LSIR,
        ]

    def tearDown(self):
        self.assessment_types_patcher.stop()

    def test_find_supervision_time_buckets(self):
        """Tests the find_supervision_time_buckets function for a single
        supervision period with no incarceration periods."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            state_code="US_XX",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            projected_completion_date=date(2018, 5, 19),
            completion_date=date(2018, 5, 19),
            supervision_periods=[supervision_period],
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 1),
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=5,
                event_date=last_day_of_month(
                    supervision_sentence.projected_completion_date
                ),
                supervision_type=supervision_period_supervision_type,
                sentence_days_served=(
                    supervision_sentence.completion_date
                    - supervision_sentence.start_date
                ).days,
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
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                supervision_period, case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                projected_supervision_completion_date=supervision_sentence.projected_completion_date,
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_overlaps_year(self):
        """Tests the find_supervision_time_buckets function for a single
        supervision period with no incarceration periods, where the supervision
        period overlaps two calendar years."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2019, 1, 19),
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_periods=[supervision_period],
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_time_buckets = [
            create_start_bucket_from_period(
                supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
            ),
        ]

        expected_time_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        self.assertCountEqual(supervision_time_buckets, expected_time_buckets)

    def test_find_supervision_time_buckets_two_supervision_periods(self):
        """Tests the find_supervision_time_buckets function for two supervision
        periods with no incarceration periods."""

        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2019, 8, 5),
            termination_date=date(2019, 12, 19),
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_periods=[first_supervision_period, second_supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period, second_supervision_period]
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        first_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )
        second_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                event_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason,
            ),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                event_date=second_supervision_period.termination_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            create_start_bucket_from_period(
                second_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                first_supervision_period,
                first_supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                second_supervision_period,
                second_supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_overlapping_supervision_periods(self):
        """Tests the find_supervision_time_buckets function for two supervision
        periods with no incarceration periods, where the supervision
        periods are of the same type and have overlapping months."""

        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 4, 15),
            termination_date=date(2018, 7, 19),
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_periods=[first_supervision_period, second_supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period, second_supervision_period]
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        first_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )
        second_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                event_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason,
            ),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                event_date=second_supervision_period.termination_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            create_start_bucket_from_period(
                second_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                first_supervision_period,
                first_supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                second_supervision_period,
                second_supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_overlapping_periods_different_types(self):
        """Tests the find_supervision_time_buckets function for two supervision
        periods with no incarceration periods, where the supervision
        periods are of different types and have overlapping months."""

        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 4, 15),
            termination_date=date(2018, 7, 19),
            supervision_type=StateSupervisionType.PAROLE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            completion_date=date(2018, 7, 19),
            supervision_periods=[first_supervision_period, second_supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period, second_supervision_period]
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        first_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )
        overlapping_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                event_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason,
            ),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                event_date=second_supervision_period.termination_date,
                supervision_type=overlapping_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            create_start_bucket_from_period(
                second_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                first_supervision_period,
                first_supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                second_supervision_period,
                overlapping_supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usNd_ignoreTemporaryCustodyPeriod(self):
        """Tests the find_supervision_time_buckets function for state code US_ND to ensure that temporary
        custody periods are ignored."""
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionType.PROBATION,
        )

        temporary_custody_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_ND",
            admission_date=date(2017, 4, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2017, 5, 17),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_periods=[first_supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [first_supervision_period]
        incarceration_periods = [temporary_custody_period, revocation_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        first_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2017,
                month=5,
                event_date=revocation_period.admission_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            create_start_bucket_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            ),
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                event_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason,
            ),
        ]

        expected_time_buckets.extend(
            expected_non_revocation_return_time_buckets(
                first_supervision_period,
                first_supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        self.assertCountEqual(supervision_time_buckets, expected_time_buckets)

    def test_findSupervisionTimeBuckets_doNotCollapseTemporaryCustodyAndRevocation_us_mo(
        self,
    ):
        """Tests the find_supervision_time_buckets function to ensure temporary custody and revocation periods are
        not collapsed.
        """
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_MO",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionType.PAROLE,
        )

        temporary_custody_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_MO",
            admission_date=date(2017, 4, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2017, 5, 17),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_MO",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code="US_XX",
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id="ss",
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[first_supervision_period],
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=first_supervision_period.start_date,
                    end_date=temporary_custody_period.admission_date,
                    supervision_type=StateSupervisionType.PAROLE,
                ),
                SupervisionTypeSpan(
                    start_date=temporary_custody_period.admission_date,
                    end_date=revocation_period.admission_date,
                    supervision_type=None,
                ),
                SupervisionTypeSpan(
                    start_date=revocation_period.admission_date,
                    end_date=None,
                    supervision_type=None,
                ),
            ],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [first_supervision_period]
        incarceration_periods = [temporary_custody_period, revocation_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        first_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PAROLE
        )

        expected_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2017,
                month=5,
                event_date=revocation_period.admission_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                event_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(first_supervision_period),
        ]

        expected_time_buckets.extend(
            expected_non_revocation_return_time_buckets(
                first_supervision_period,
                first_supervision_period_supervision_type,
                temporary_custody_period.admission_date,
                case_type=StateSupervisionCaseType.GENERAL,
            )
        )

        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usId(self):
        """Tests the find_supervision_time_buckets function for state code US_ID."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MEDIUM",
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        supervision_contact = StateSupervisionContact.new_with_defaults(
            state_code="US_ID",
            external_id="contactX",
            contact_date=supervision_period.start_date,
            status=StateSupervisionContactStatus.COMPLETED,
            contact_type=StateSupervisionContactType.FACE_TO_FACE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [supervision_period]
        incarceration_periods = [revocation_period]
        assessments = []
        violation_responses = []
        supervision_contacts = [supervision_contact]

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2017,
                month=5,
                event_date=revocation_period.admission_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            create_start_bucket_from_period(supervision_period),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
            ),
        ]

        expected_time_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period.supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    face_to_face_contacts=supervision_contacts,
                ),
            )
        )

        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usId_filterUnsetSupervisionTypePeriods(self):
        """Tests the find_supervision_time_buckets function for state code US_ID where the supervision periods with
        unset supervision_period_supervision_type values should be filtered out when looking for revocations."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
        )

        # Supervision period with an unset supervision_period_supervision_type between the terminated period
        # and the revocation incarceration period
        supervision_period_type_unset = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2017, 5, 9),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=None,
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period, supervision_period_type_unset],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [supervision_period, supervision_period_type_unset]
        incarceration_periods = [revocation_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2017,
                month=5,
                event_date=revocation_period.admission_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            create_start_bucket_from_period(supervision_period),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
            ),
            create_start_bucket_from_period(supervision_period_type_unset),
            SupervisionTerminationBucket(
                state_code=supervision_period_type_unset.state_code,
                year=supervision_period_type_unset.termination_date.year,
                month=supervision_period_type_unset.termination_date.month,
                event_date=supervision_period_type_unset.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
                supervision_level=supervision_period_type_unset.supervision_level,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period_type_unset.termination_reason,
            ),
        ]

        expected_time_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period.supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usId_filterInternalUnknownSupervisionTypePeriods(
        self,
    ):
        """Tests the find_supervision_time_buckets function for state code US_ID where the supervision periods with
        INTERNAL_UNKNOWN supervision_period_supervision_type values should be filtered out when looking for
        revocations."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            supervision_site="X|Y",
        )

        # Supervision period with an internal unknown supervision_period_supervision_type between the terminated period
        # and the revocation incarceration period (this could signify a bench warrant, for example)
        supervision_period_type_internal_unknown = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2017, 5, 9),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        supervision_period_agent_association_list = [
            {
                "agent_id": 123,
                "agent_external_id": "Officer1",
                "supervision_period_id": 111,
            },
        ]

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[
                supervision_period,
                supervision_period_type_internal_unknown,
            ],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [
            supervision_period,
            supervision_period_type_internal_unknown,
        ]
        incarceration_periods = [revocation_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            supervision_period_agent_association_list,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2017,
                month=5,
                event_date=revocation_period.admission_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervising_officer_external_id="Officer1",
                supervising_district_external_id="X",
                level_1_supervision_location_external_id="Y",
                level_2_supervision_location_external_id="X",
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            create_start_bucket_from_period(
                supervision_period,
                supervising_officer_external_id="Officer1",
                supervising_district_external_id="X",
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervising_officer_external_id="Officer1",
                supervising_district_external_id="X",
                level_1_supervision_location_external_id="Y",
                level_2_supervision_location_external_id="X",
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                termination_reason=supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(supervision_period_type_internal_unknown),
            SupervisionTerminationBucket(
                state_code=supervision_period_type_internal_unknown.state_code,
                year=supervision_period_type_internal_unknown.termination_date.year,
                month=supervision_period_type_internal_unknown.termination_date.month,
                event_date=supervision_period_type_internal_unknown.termination_date,
                supervision_type=supervision_period_type_internal_unknown.supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=supervision_period_type_internal_unknown.supervision_level,
                supervision_level_raw_text=supervision_period_type_internal_unknown.supervision_level_raw_text,
                termination_reason=supervision_period_type_internal_unknown.termination_reason,
            ),
        ]

        expected_time_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period.supervision_period_supervision_type,
                supervising_officer_external_id="Officer1",
                level_1_supervision_location_external_id="Y",
                level_2_supervision_location_external_id="X",
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usId_unsortedIncarcerationPeriods(self):
        """Tests the find_supervision_time_buckets function for state code US_ID where the incarceration periods
        are not sorted prior to the calculations."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
        )

        # Supervision period with an unset supervision_period_supervision_type between the terminated period
        # and the revocation incarceration period
        supervision_period_type_unset = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2017, 5, 9),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=None,
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        earlier_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2011, 1, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 9, 13),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period, supervision_period_type_unset],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [supervision_period, supervision_period_type_unset]
        incarceration_periods = [revocation_period, earlier_incarceration_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2017,
                month=5,
                event_date=revocation_period.admission_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                is_on_supervision_last_day_of_month=False,
                case_type=StateSupervisionCaseType.GENERAL,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            create_start_bucket_from_period(supervision_period),
            create_termination_bucket_from_period(supervision_period),
            create_start_bucket_from_period(supervision_period_type_unset),
            create_termination_bucket_from_period(
                supervision_period_type_unset,
                supervision_type=StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
            ),
        ]

        expected_time_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period.supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usId_supervisionAfterInvestigation(self):
        """Tests the find_supervision_time_buckets function for state code US_ID where the person starts supervision
        after being on INVESTIGATION. Investigative periods should produce no SupervisionTimeBuckets, and the start
        of probation should be marked as the beginning of supervision.
        """
        supervision_period_investigation = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            status=StateSupervisionPeriodStatus.TERMINATED,
            start_date=date(2017, 5, 9),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_date=date(2017, 5, 13),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period_investigation],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [supervision_period_investigation, supervision_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            [],
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_time_buckets = [
            create_start_bucket_from_period(
                supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            ),
        ]
        expected_time_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period.supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )
        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usId_admittedAfterInvestigation(self):
        """Tests the find_supervision_time_buckets function for state code US_ID where the person is admitted after
        being on INVESTIGATION supervision. These periods should produce no SupervisionTimeBuckets, and the admission
        to prison should not be counted as a revocation."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_time_buckets = []
        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usNd_admittedAfterPreConfinement(self):
        """Tests the find_supervision_time_buckets function for state code US_ND where the person is admitted after
        being on PRE-CONFINEMENT supervision. These periods should produce no SupervisionTimeBuckets, and the admission
        to prison should not be counted as a revocation.

        TODO(#2891): This should be updated or removed once ND has been migrated to supervision_period_supervision_type
        """
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            custodial_authority_raw_text="US_ND_DOCR",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionType.PRE_CONFINEMENT,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 9),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_time_buckets = []
        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usId_revokedAfterBoardHoldToGeneral(self):
        """Tests the find_supervision_time_buckets function for state code US_ID where the person is revoked
        to general incarceration after being held for a board hold."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
        )

        board_hold_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2017, 6, 3),
            release_reason=ReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 6, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [supervision_period]
        incarceration_periods = [board_hold_period, revocation_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2017,
                month=6,
                event_date=revocation_period.admission_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            create_start_bucket_from_period(supervision_period),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
            ),
        ]

        expected_time_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period.supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usId_revokedAfterBoardHoldToTreatment(self):
        """Tests the find_supervision_time_buckets function for state code US_ID where the person is revoked
        to treatment in prison after being held for a board hold."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
        )

        board_hold_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2017, 6, 3),
            release_reason=ReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 6, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [supervision_period]
        incarceration_periods = [board_hold_period, revocation_period]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2017,
                month=6,
                event_date=revocation_period.admission_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            create_start_bucket_from_period(supervision_period),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
            ),
        ]

        expected_time_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period.supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_findSupervisionTimeBuckets_usId_revokedAfterParoleBoardHoldMultipleTransfers(
        self,
    ):
        """Tests the find_supervision_time_buckets function for state code US_ID where the person is revoked after
        being incarcerated for parole board hold, where they were transferred multiple times while on parole
        board hold."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
        )

        parole_board_hold_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            facility="XX",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=AdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2017, 6, 3),
            release_reason=ReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        parole_board_hold_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            facility="YY",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 6, 3),
            admission_reason=AdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2017, 6, 3),
            release_reason=ReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        parole_board_hold_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            facility="ZZ",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 6, 3),
            admission_reason=AdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2017, 11, 19),
            release_reason=ReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 11, 19),
            admission_reason=AdmissionReason.TRANSFER,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []
        supervision_periods = [supervision_period]
        incarceration_periods = [
            parole_board_hold_period_1,
            parole_board_hold_period_2,
            parole_board_hold_period_3,
            revocation_period,
        ]
        assessments = []
        violation_responses = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2017,
                month=11,
                event_date=revocation_period.admission_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            create_start_bucket_from_period(supervision_period),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
            ),
        ]

        expected_time_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period.supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_time_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_multiple_periods(self):
        """Tests the find_supervision_time_buckets function for two supervision
        periods with two incarceration periods."""

        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
        )

        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2017, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2017, 8, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 8, 5),
            termination_date=date(2018, 12, 19),
            supervision_type=StateSupervisionType.PROBATION,
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_XX",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2018, 12, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_periods=[first_supervision_period, second_supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period, second_supervision_period]
        incarceration_periods = [
            first_incarceration_period,
            second_incarceration_period,
        ]
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        first_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )
        second_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2017,
                month=5,
                event_date=first_incarceration_period.admission_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                event_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason,
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code=second_supervision_period.state_code,
                year=2018,
                month=12,
                event_date=second_incarceration_period.admission_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                event_date=second_supervision_period.termination_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            create_start_bucket_from_period(
                second_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                first_supervision_period,
                first_supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                second_supervision_period,
                second_supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_multiple_admissions_in_month(self):
        """Tests the find_supervision_time_buckets function for a supervision period with two incarceration periods
        with admission dates in the same month."""
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionType.PROBATION,
        )

        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2017, 5, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2017, 5, 15),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE,
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_XX",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2017, 5, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_periods=[first_supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period]
        incarceration_periods = [
            first_incarceration_period,
            second_incarceration_period,
        ]
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        first_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2017,
                month=5,
                event_date=first_incarceration_period.admission_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2017,
                month=5,
                event_date=second_incarceration_period.admission_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                event_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                first_supervision_period,
                first_supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_incarceration_overlaps_periods(self):
        """Tests the find_supervision_time_buckets function for two supervision
        periods with an incarceration period that overlaps the end of one
        supervision period and the start of another."""

        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2017, 5, 15),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2017, 9, 20),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2017, 8, 5),
            termination_date=date(2017, 12, 19),
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            completion_date=date(2017, 12, 19),
            supervision_periods=[first_supervision_period, second_supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period, second_supervision_period]
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        first_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )
        second_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2017,
                month=5,
                event_date=incarceration_period.admission_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                event_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason,
            ),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                event_date=second_supervision_period.termination_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            create_start_bucket_from_period(
                second_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                first_supervision_period,
                first_supervision_period_supervision_type,
                incarceration_period.admission_date,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(
                    second_supervision_period,
                    start_date=incarceration_period.release_date,
                ),
                second_supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_supervision_authority_incarceration_overlaps_periods(
        self,
    ):
        """Tests the find_supervision_time_buckets function for two supervision periods with an incarceration period
        that overlaps the end of one supervision period and the start of another, where the incarceration period is
        under the custodial authority of a supervision entity so the period does not exclude the supervision periods
        from being counted towards the supervision population."""

        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 19),
            supervision_type=StateSupervisionType.PAROLE,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2017, 5, 15),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2017, 9, 20),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2017, 8, 5),
            termination_date=date(2017, 12, 19),
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            completion_date=date(2017, 12, 19),
            supervision_periods=[first_supervision_period, second_supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period, second_supervision_period]
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        first_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )
        second_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                event_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason,
            ),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                event_date=second_supervision_period.termination_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(first_supervision_period),
            create_start_bucket_from_period(second_supervision_period),
        ]

        # The entirety of both supervision periods should be counted as NonRevocationReturnSupervisionTimeBuckets
        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                first_supervision_period, first_supervision_period_supervision_type
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                second_supervision_period, second_supervision_period_supervision_type
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_transition_from_parole_to_probation_in_month(self):
        """Tests the find_supervision_time_buckets function for transition between two supervision periods in a
        month."""
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PAROLE,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 5, 19),
            termination_date=date(2018, 6, 20),
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            state_code="US_XX",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_periods=[first_supervision_period, second_supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period, second_supervision_period]
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        first_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )
        second_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                event_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason,
            ),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                event_date=second_supervision_period.termination_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            create_start_bucket_from_period(
                second_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                first_supervision_period,
                first_supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                second_supervision_period,
                second_supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_incarceration_overlaps_periods_multiple_us_nd(
        self,
    ):
        """Tests the find_supervision_time_buckets function for two supervision periods with an incarceration period
        that overlaps two supervision periods that were revoked, where there is state-specific logic for US_ND on how
        to determine the supervision type on the RevocationReturnSupervisionTimeBuckets."""
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 12, 19),
            supervision_type=StateSupervisionType.PAROLE,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_ND",
            admission_date=date(2017, 5, 15),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2017, 9, 20),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            completion_date=date(2017, 12, 19),
            supervision_periods=[first_supervision_period, second_supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period, second_supervision_period]
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        first_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )
        second_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2017,
                month=5,
                event_date=incarceration_period.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2017,
                month=5,
                event_date=incarceration_period.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            create_start_bucket_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            ),
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                event_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                second_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            ),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                event_date=second_supervision_period.termination_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                first_supervision_period,
                first_supervision_period_supervision_type,
                incarceration_period.admission_date,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                second_supervision_period,
                second_supervision_period_supervision_type,
                incarceration_period.admission_date,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(
                    second_supervision_period,
                    start_date=incarceration_period.release_date,
                ),
                second_supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=second_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_return_next_month(self):
        """Tests the find_supervision_time_buckets function
        when there is an incarceration period with a revocation admission
        the month after the supervision period's termination_date."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 26),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="X",
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=date(2018, 6, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_periods=[supervision_period],
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
                supervising_district_external_id="X",
                level_1_supervision_location_external_id="X",
                level_2_supervision_location_external_id=None,
            ),
            create_start_bucket_from_period(
                supervision_period,
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
                supervising_district_external_id="X",
                level_1_supervision_location_external_id="X",
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code=incarceration_period.state_code,
                year=2018,
                month=6,
                event_date=incarceration_period.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
                supervising_district_external_id="X",
                level_1_supervision_location_external_id="X",
                level_2_supervision_location_external_id=None,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
                is_on_supervision_last_day_of_month=False,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                supervising_officer_external_id="XXX",
                level_1_supervision_location_external_id="X",
                level_2_supervision_location_external_id=None,
                judicial_district_code="XXX",
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_return_next_month_different_supervision_type_us_nd(
        self,
    ):
        """Tests the find_supervision_time_buckets function when there is an incarceration period with a revocation
        admission the month after the supervision period's termination_date, and the admission_reason supervision
        type does not match the supervision type on the period. This tests state-specific logic for US_ND on how
        to determine the supervision type on the RevocationReturnSupervisionTimeBuckets"""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="X",
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_ND",
            admission_date=date(2018, 6, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_periods=[supervision_period],
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                supervising_officer_external_id="XXX",
                supervising_district_external_id="X",
                level_1_supervision_location_external_id="X",
                level_2_supervision_location_external_id=None,
                judicial_district_code="XXX",
            ),
            create_start_bucket_from_period(
                supervision_period,
                supervising_officer_external_id="XXX",
                supervising_district_external_id="X",
                level_1_supervision_location_external_id="X",
                judicial_district_code="XXX",
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code=incarceration_period.state_code,
                year=2018,
                month=6,
                event_date=incarceration_period.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_officer_external_id="XXX",
                supervising_district_external_id="X",
                level_1_supervision_location_external_id="X",
                level_2_supervision_location_external_id=None,
                judicial_district_code="XXX",
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
                is_on_supervision_last_day_of_month=False,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                supervising_officer_external_id="XXX",
                level_1_supervision_location_external_id="X",
                level_2_supervision_location_external_id=None,
                judicial_district_code="XXX",
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_return_next_year(self):
        """Tests the find_supervision_time_buckets function
        when there is an incarceration period with a revocation admission
        the month after the supervision period's termination_date, where the
        revocation admission happens in the next year."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 10, 5),
            termination_date=date(2018, 12, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="X",
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2019, 1, 5),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_periods=[supervision_period],
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            create_start_bucket_from_period(
                supervision_period,
                supervising_officer_external_id="XXX",
                supervising_district_external_id="X",
                level_1_supervision_location_external_id="X",
                judicial_district_code="XXX",
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                supervising_officer_external_id="XXX",
                supervising_district_external_id="X",
                level_1_supervision_location_external_id="X",
                level_2_supervision_location_external_id=None,
                judicial_district_code="XXX",
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code=incarceration_period.state_code,
                year=2019,
                month=1,
                event_date=incarceration_period.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
                supervising_officer_external_id="XXX",
                supervising_district_external_id="X",
                level_1_supervision_location_external_id="X",
                level_2_supervision_location_external_id=None,
                judicial_district_code="XXX",
                is_on_supervision_last_day_of_month=False,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                supervising_officer_external_id="XXX",
                level_1_supervision_location_external_id="X",
                level_2_supervision_location_external_id=None,
                judicial_district_code="XXX",
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_return_months_later(self):
        """Tests the find_supervision_time_buckets function
        when there is an incarceration period with a revocation admission
        months after the supervision period's termination_date."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2018, 10, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_periods=[supervision_period],
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code=incarceration_period.state_code,
                year=2018,
                month=10,
                event_date=incarceration_period.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
                is_on_supervision_last_day_of_month=False,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_return_years_later(self):
        """Tests the find_supervision_time_buckets function
        when there is an incarceration period with a revocation admission
        years after the supervision period's termination_date."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2015, 3, 5),
            termination_date=date(2015, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2017, 10, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2015, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_periods=[supervision_period],
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            create_start_bucket_from_period(
                supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code=incarceration_period.state_code,
                year=2017,
                month=10,
                event_date=incarceration_period.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
                is_on_supervision_last_day_of_month=False,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_multiple_periods_revocations(self):
        """Tests the find_supervision_time_buckets function
        when the person is revoked and returned to supervision twice in one
        year."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            supervision_type=StateSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2018, 3, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_date=date(2018, 8, 2),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE,
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip2",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2018, 9, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_date=date(2018, 12, 2),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            completion_date=date(2018, 12, 19),
            supervision_periods=[supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = [
            first_incarceration_period,
            second_incarceration_period,
        ]
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PAROLE
        )

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=3,
                event_date=first_incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                supervision_level=StateSupervisionLevel.MINIMUM,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=9,
                event_date=second_incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                supervision_level=StateSupervisionLevel.MINIMUM,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                supervision_level=StateSupervisionLevel.MINIMUM,
            ),
            create_start_bucket_from_period(
                supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                first_incarceration_period.admission_date,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(
                    supervision_period,
                    start_date=first_incarceration_period.release_date,
                ),
                supervision_period_supervision_type,
                second_incarceration_period.admission_date,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(
                    supervision_period,
                    start_date=second_incarceration_period.release_date,
                ),
                supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_multiple_sentences_revocations(self):
        """Tests the find_supervision_time_buckets function
        when the person is revoked and returned to supervision twice in one
        year, and they have multiple supervision sentences."""

        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            supervision_type=StateSupervisionType.PAROLE,
        )

        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2018, 3, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_date=date(2018, 8, 2),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE,
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip2",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2018, 9, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_date=date(2018, 12, 2),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=234,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2019, 1, 1),
            termination_date=date(2019, 1, 19),
            supervision_type=StateSupervisionType.PAROLE,
        )

        first_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            completion_date=date(2018, 12, 19),
            supervision_periods=[first_supervision_period],
        )

        second_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=222,
            start_date=date(2017, 1, 1),
            external_id="ss2",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            completion_date=date(2019, 1, 19),
            supervision_periods=[second_supervision_period],
        )

        supervision_sentences = [
            first_supervision_sentence,
            second_supervision_sentence,
        ]
        supervision_periods = [first_supervision_period, second_supervision_period]
        incarceration_periods = [
            first_incarceration_period,
            second_incarceration_period,
        ]
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        first_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PAROLE
        )
        second_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PAROLE
        )

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2018,
                month=3,
                event_date=first_incarceration_period.admission_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code=first_supervision_period.state_code,
                year=2018,
                month=9,
                event_date=second_incarceration_period.admission_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                event_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason,
            ),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                event_date=second_supervision_period.termination_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            create_start_bucket_from_period(
                second_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                first_supervision_period,
                first_supervision_period_supervision_type,
                first_incarceration_period.admission_date,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(
                    first_supervision_period,
                    start_date=first_incarceration_period.release_date,
                ),
                first_supervision_period_supervision_type,
                second_incarceration_period.admission_date,
                case_compliances=_generate_case_compliances(
                    start_date=first_incarceration_period.release_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(
                    first_supervision_period,
                    start_date=second_incarceration_period.release_date,
                ),
                first_supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=second_incarceration_period.release_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                second_supervision_period,
                second_supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=second_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_single_sentence_with_past_completion_date(
        self,
    ):
        """Tests the find_supervision_time_buckets function for a single
        supervision period with no incarceration periods. The supervision period extends past
        the supervision sentence."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            start_date=date(2018, 5, 1),
            # Termination date is after sentence's projected completion date
            termination_date=date(2018, 5, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            start_date=date(2018, 5, 1),
            external_id="ss1",
            state_code="US_XX",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            projected_completion_date=date(2018, 5, 10),
            supervision_periods=[supervision_period],
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 1),
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                supervision_period, case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                projected_supervision_completion_date=supervision_sentence.projected_completion_date,
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_multiple_sentences_past_sentence_completion_dates(
        self,
    ):
        """Tests the find_supervision_time_buckets function when there are multiple supervision periods and
        multiple supervision sentences. Both supervision periods extend past their corresponding supervision
        sentences."""

        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 1, 1),
            # termination date is after first supervision sentence's projected completion date
            termination_date=date(2018, 1, 3),
            supervision_type=StateSupervisionType.PAROLE,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=234,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2019, 2, 1),
            # termination date is after second supervision sentence's projected completion date
            termination_date=date(2019, 2, 3),
            supervision_type=StateSupervisionType.PAROLE,
        )

        first_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2018, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            completion_date=date(2018, 1, 2),
            supervision_periods=[first_supervision_period],
        )

        second_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=222,
            start_date=date(2019, 2, 1),
            external_id="ss2",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            completion_date=date(2019, 2, 2),
            supervision_periods=[second_supervision_period],
        )

        supervision_sentences = [
            first_supervision_sentence,
            second_supervision_sentence,
        ]
        supervision_periods = [first_supervision_period, second_supervision_period]
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        first_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PAROLE
        )
        second_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PAROLE
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=first_supervision_period.state_code,
                year=first_supervision_period.termination_date.year,
                month=first_supervision_period.termination_date.month,
                event_date=first_supervision_period.termination_date,
                supervision_type=first_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=first_supervision_period.termination_reason,
            ),
            SupervisionTerminationBucket(
                state_code=second_supervision_period.state_code,
                year=second_supervision_period.termination_date.year,
                month=second_supervision_period.termination_date.month,
                event_date=second_supervision_period.termination_date,
                supervision_type=second_supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=second_supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            create_start_bucket_from_period(
                second_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                first_supervision_period,
                first_supervision_period_supervision_type,
                projected_supervision_completion_date=first_supervision_sentence.projected_completion_date,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                second_supervision_period,
                second_supervision_period_supervision_type,
                projected_supervision_completion_date=second_supervision_sentence.projected_completion_date,
                case_compliances=_generate_case_compliances(
                    start_date=first_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_placeholders(self):
        """Tests the find_supervision_time_buckets function
        when there are placeholder supervision periods that should be dropped
        from the calculations."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_period_placeholder = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            state_code="US_XX",
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_periods=[supervision_period, supervision_period_placeholder],
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=5,
                event_date=incarceration_period.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_us_id(self):
        """Tests the find_supervision_time_buckets function where the supervision type should be taken from the
        supervision_period_supervision_type off of the supervision_period."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_ID", case_type=StateSupervisionCaseType.GENERAL
                )
            ],
            state_code="US_ID",
            supervision_site="DISTRICT_1|OFFICE_2",
            custodial_authority_raw_text="US_ID_DOC",
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period],
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=123,
            external_id="is1",
            start_date=date(2017, 1, 1),
            supervision_periods=[supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_ID",
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 1),
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_buckets = [
            create_start_bucket_from_period(
                supervision_period, supervising_district_external_id="DISTRICT_1"
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                supervising_district_external_id="DISTRICT_1",
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period.supervision_period_supervision_type,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    assessments=assessments,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_us_pa(self):
        """Tests the find_supervision_time_buckets function for periods in US_PA."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_PA", case_type=StateSupervisionCaseType.GENERAL
                )
            ],
            state_code="US_PA",
            supervision_site="DISTRICT_1|OFFICE_2|1345",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_PA",
            assessment_type=StateAssessmentType.LSIR,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_score=33,
            assessment_date=date(2018, 3, 1),
        )

        supervision_sentences = []
        supervision_periods = [supervision_period]
        incarceration_periods = []
        assessments = [assessment]
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        # TODO(#6314): Don't send in this temporary reference, revert references to
        #  DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST
        temporary_sp_agent_associations_list = [
            {
                "agent_id": 123,
                "agent_external_id": "DISTRICT_1|OFFICE_2|1345#XXX",
                "supervision_period_id": _DEFAULT_SUPERVISION_PERIOD_ID,
            }
        ]

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            temporary_sp_agent_associations_list,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_buckets = [
            create_start_bucket_from_period(
                supervision_period,
                supervising_district_external_id="DISTRICT_1",
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                supervising_district_external_id="DISTRICT_1",
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score=assessment.assessment_score,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=assessment.assessment_type,
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    assessments=assessments,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_us_pa_shock_incarceration_revocation(self):
        """Tests the find_supervision_time_buckets function for periods in US_PA, where
        there is a revocation return for shock incarceration."""
        state_code = "US_PA"

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code=state_code, case_type=StateSupervisionCaseType.GENERAL
                )
            ],
            state_code=state_code,
            supervision_site="DISTRICT_1|OFFICE_2|456",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=state_code,
            assessment_type=StateAssessmentType.LSIR,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_score=33,
            assessment_date=date(2018, 3, 1),
        )

        parole_board_decision_entry = StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
            state_code=state_code,
            decision_raw_text=SHOCK_INCARCERATION_9_MONTHS,
            revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
            decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            revocation_type_raw_text=SHOCK_INCARCERATION_9_MONTHS,
        )

        parole_board_permanent_decision = StateSupervisionViolationResponse.new_with_defaults(
            state_code=state_code,
            response_date=date(year=2018, month=5, day=16),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            response_type_raw_text="PERMANENT_DECISION",
            deciding_body_type=StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
            deciding_body_type_raw_text="PAROLE_BOARD",
            supervision_violation_response_decisions=[parole_board_decision_entry],
        )

        parole_board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code=state_code,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2018, 3, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2018, 5, 19),
            release_reason=ReleaseReason.TRANSFER,
        )

        shock_incarceration_revocation = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=state_code,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2018, 5, 19),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            # Program 46 indicates a revocation for a 6, 9 or 12 month stay
            specialized_purpose_for_incarceration_raw_text="CCIS-46",
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        supervision_sentences = []
        supervision_periods = [supervision_period]
        incarceration_periods = [parole_board_hold, shock_incarceration_revocation]
        assessments = [assessment]
        violation_responses = [parole_board_permanent_decision]
        supervision_contacts = []
        incarceration_sentences = []

        # TODO(#6314): Don't send in this temporary reference, revert references to
        #  DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST
        temporary_sp_agent_associations_list = [
            {
                "agent_id": 123,
                "agent_external_id": "DISTRICT_1|OFFICE_2|ORG_CODE#XXX",
                "supervision_period_id": _DEFAULT_SUPERVISION_PERIOD_ID,
            }
        ]

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            temporary_sp_agent_associations_list,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_buckets = [
            create_start_bucket_from_period(
                supervision_period,
                supervising_district_external_id="DISTRICT_1",
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                supervising_district_external_id="DISTRICT_1",
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                end_date=parole_board_hold.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score=assessment.assessment_score,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=assessment.assessment_type,
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    assessments=assessments,
                ),
            )
        )

        expected_buckets.append(
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=5,
                event_date=shock_incarceration_revocation.admission_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                supervising_district_external_id="DISTRICT_1",
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=assessment.assessment_score,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=assessment.assessment_type,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                revocation_type_subtype=SHOCK_INCARCERATION_9_MONTHS,
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_us_pa_shock_incarceration_revocation_to_pvc(
        self,
    ):
        """Tests the find_supervision_time_buckets function for periods in US_PA, where there is a revocation return
        to a Parole Violator Center (PVC) following a parole board hold and a single-day revocation admission
        before being transferred to the PVC."""
        state_code = "US_PA"

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code=state_code, case_type=StateSupervisionCaseType.GENERAL
                )
            ],
            state_code=state_code,
            supervision_site="DISTRICT_1|OFFICE_2|123",
            start_date=date(2017, 12, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
        )

        assessment = StateAssessment.new_with_defaults(
            state_code=state_code,
            assessment_type=StateAssessmentType.LSIR,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_score=33,
            assessment_date=date(2017, 12, 1),
        )

        parole_board_hold = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="ip1",
            state_code=state_code,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2018, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            release_date=date(2018, 1, 11),
            release_reason=ReleaseReason.TRANSFER,
        )

        invalid_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            external_id="1",
            state_code=state_code,
            admission_date=date(2018, 1, 11),
            admission_reason=AdmissionReason.PAROLE_REVOCATION,
            release_date=date(2018, 1, 11),
            release_reason=ReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION,
        )

        pvc_revocation = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=state_code,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            admission_date=date(2018, 1, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            specialized_purpose_for_incarceration_raw_text=PURPOSE_FOR_INCARCERATION_PVC,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
        )

        supervision_sentences = []
        supervision_periods = [supervision_period]
        incarceration_periods = [
            parole_board_hold,
            pvc_revocation,
            invalid_incarceration_period,
        ]
        assessments = [assessment]
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        # TODO(#6314): Don't send in this temporary reference, revert references to
        #  DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST
        temporary_sp_agent_associations_list = [
            {
                "agent_id": 123,
                "agent_external_id": "DISTRICT_1|OFFICE_2|ORG_CODE#XXX",
                "supervision_period_id": _DEFAULT_SUPERVISION_PERIOD_ID,
            }
        ]

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            temporary_sp_agent_associations_list,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_buckets = [
            create_start_bucket_from_period(
                supervision_period,
                supervising_district_external_id="DISTRICT_1",
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                supervising_district_external_id="DISTRICT_1",
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                end_date=parole_board_hold.admission_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score=assessment.assessment_score,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=assessment.assessment_type,
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    assessments=assessments,
                ),
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(
                    supervision_period,
                    start_date=pvc_revocation.admission_date,
                ),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score=assessment.assessment_score,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=assessment.assessment_type,
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    assessments=assessments,
                ),
            )
        )

        expected_buckets.append(
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=1,
                event_date=pvc_revocation.admission_date,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                supervising_district_external_id="DISTRICT_1",
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=assessment.assessment_score,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=assessment.assessment_type,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                revocation_type_subtype="PVC",
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_infer_supervision_type(self):
        """Tests the find_supervision_time_buckets function where the supervision type needs to be inferred from the
        sentence attached to the supervision period."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            completion_date=date(2018, 12, 19),
            projected_completion_date=date(2018, 5, 19),
            supervision_periods=[supervision_period],
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 2, 10),
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_buckets = [
            ProjectedSupervisionCompletionBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=5,
                event_date=last_day_of_month(
                    supervision_sentence.projected_completion_date
                ),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                successful_completion=True,
                incarcerated_during_sentence=False,
                sentence_days_served=(
                    supervision_sentence.completion_date
                    - supervision_sentence.start_date
                ).days,
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                supervision_period,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    assessments=assessments,
                ),
                projected_supervision_completion_date=supervision_sentence.projected_completion_date,
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_infer_supervision_type_parole(self):
        """Tests the find_supervision_time_buckets function where the supervision type needs to be inferred, the
        but the supervision period is not attached to any sentences, so the inferred type should be PAROLE."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            projected_completion_date=date(2018, 5, 19),
            supervision_periods=[],
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=123,
            external_id="is1",
            start_date=date(2017, 1, 1),
            supervision_periods=[supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_CA",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 1),
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )
        expected_buckets = [
            create_start_bucket_from_period(
                supervision_period,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    assessments=assessments,
                ),
                projected_supervision_completion_date=supervision_sentence.projected_completion_date,
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_infer_supervision_type_dual_us_mo(self):
        """Tests the find_supervision_time_buckets function where the supervision type needs to be inferred, the
        but the supervision period is attached to both a supervision sentence of type PROBATION and an incarceration
        sentence, so the inferred type should be DUAL."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_MO",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_MO",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code="US_MO",
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                completion_date=date(2018, 5, 19),
                external_id="ss1",
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period],
                supervision_type=StateSupervisionType.PROBATION,
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=supervision_period.start_date,
                    end_date=supervision_period.termination_date,
                    supervision_type=StateSupervisionType.PROBATION,
                ),
                SupervisionTypeSpan(
                    start_date=supervision_period.termination_date,
                    end_date=None,
                    supervision_type=None,
                ),
            ],
        )

        incarceration_sentence = (
            FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
                StateIncarcerationSentence.new_with_defaults(
                    state_code="US_MO",
                    incarceration_sentence_id=123,
                    external_id="is1",
                    start_date=date(2017, 1, 1),
                    completion_date=date(2018, 5, 19),
                    supervision_periods=[supervision_period],
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=supervision_period.start_date,
                        end_date=supervision_period.termination_date,
                        supervision_type=StateSupervisionType.PAROLE,
                    ),
                    SupervisionTypeSpan(
                        start_date=supervision_period.termination_date,
                        end_date=None,
                        supervision_type=None,
                    ),
                ],
            )
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_MO",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 2, 10),
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                supervision_period,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                StateSupervisionPeriodSupervisionType.DUAL,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_infer_supervision_type_dual_us_id(self):
        """Tests the find_supervision_time_buckets function where the supervision type is taken from a `DUAL`
        supervision period. Asserts that the DUAL buckets are NOT expanded into separate PROBATION and PAROLE buckets.
        """
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_ID",
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 1),
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_buckets = [
            create_start_bucket_from_period(supervision_period),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                case_type=StateSupervisionCaseType.GENERAL,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                termination_reason=supervision_period.termination_reason,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                StateSupervisionPeriodSupervisionType.DUAL,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    assessments=assessments,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_infer_supervision_type_dual_us_nd(self):
        """Tests the find_supervision_time_buckets function where the supervision type needs to be inferred, the
        but the supervision period is attached to both a supervision sentence of type PROBATION and an incarceration
        sentence, so the inferred type should be DUAL. Also asserts that the DUAL buckets are expanded to have PAROLE,
        PROBATION, and DUAL buckets."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_ND",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_ND",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period],
            supervision_type=StateSupervisionType.PROBATION,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_ND",
            incarceration_sentence_id=123,
            external_id="is1",
            start_date=date(2017, 1, 1),
            supervision_periods=[supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_ND",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 1),
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason,
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason,
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=supervision_period.supervision_level,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                supervision_period,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                admission_reason=None,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                StateSupervisionPeriodSupervisionType.DUAL,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    assessments=assessments,
                ),
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    assessments=assessments,
                ),
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    assessments=assessments,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_no_supervision_when_no_sentences_supervision_spans_us_mo(
        self,
    ):
        """This person"""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_MO",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_MO",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=None,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_MO",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10),
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        self.assertCountEqual(supervision_time_buckets, [])

    @freeze_time("2019-09-04")
    def test_find_supervision_time_buckets_admission_today(self):
        """Tests the find_time_buckets_for_supervision_period function when there is an incarceration period with a
        revocation admission today, where there is no termination_date on the supervision_period, and no release_date
        on the incarceration_period."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2019, 6, 2),
            supervision_type=StateSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date.today(),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period],
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            create_start_bucket_from_period(
                supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2019,
                month=9,
                event_date=incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                StateSupervisionPeriodSupervisionType.PROBATION,
                date.today(),
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    end_date_override=date(2019, 9, 3),
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_supervision_time_buckets_multiple_incarcerations_in_year(self):
        """Tests the find_time_buckets_for_supervision_period function when there are multiple
        incarceration periods in the year of supervision."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 6, 19),
            supervision_type=StateSupervisionType.PAROLE,
        )

        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2018, 6, 2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_date=date(2018, 9, 3),
        )

        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            admission_date=date(2018, 11, 17),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=123,
            external_id="is1",
            start_date=date(2018, 1, 1),
            incarceration_periods=[
                first_incarceration_period,
                second_incarceration_period,
            ],
            supervision_periods=[supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PAROLE
        )

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=6,
                event_date=first_incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=11,
                event_date=second_incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(
                supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                first_incarceration_period.admission_date,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_supervision_time_buckets_violation_history(self):
        state_code = "US_XX"

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code=state_code,
            start_date=date(2018, 1, 1),
            termination_date=date(2020, 1, 1),
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_violation_1 = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code=state_code,
            violation_date=date(2018, 4, 20),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=state_code,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=state_code,
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        violation_report_1 = StateSupervisionViolationResponse.new_with_defaults(
            state_code=state_code,
            supervision_violation_response_id=888,
            response_date=date(2018, 4, 21),
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            supervision_violation=supervision_violation_1,
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code=state_code,
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
            ],
        )

        supervision_violation_2 = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=32663,
            state_code=state_code,
            violation_date=date(2019, 1, 20),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=state_code,
                    violation_type=StateSupervisionViolationType.MISDEMEANOR,
                ),
            ],
        )

        violation_report_2 = StateSupervisionViolationResponse.new_with_defaults(
            state_code=state_code,
            supervision_violation_response_id=999,
            response_date=date(2019, 1, 20),
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            supervision_violation=supervision_violation_2,
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code=state_code,
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
            ],
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code=state_code,
            supervision_sentence_id=111,
            start_date=date(2018, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period],
        )

        violation_reports = [violation_report_1, violation_report_2]
        supervision_contacts = []

        supervision_time_buckets = identifier.find_supervision_time_buckets(
            [supervision_sentence],
            [],
            [supervision_period],
            [],
            [],
            violation_reports,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                most_severe_violation_type=StateSupervisionViolationType.MISDEMEANOR,
                most_severe_violation_type_subtype=StateSupervisionViolationType.MISDEMEANOR.value,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
            ),
            create_start_bucket_from_period(
                supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                end_date=violation_report_1.response_date,
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(
                    supervision_period,
                    start_date=violation_report_1.response_date,
                ),
                supervision_period_supervision_type,
                end_date=violation_report_2.response_date,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(
                    supervision_period,
                    start_date=violation_report_2.response_date,
                ),
                supervision_period_supervision_type,
                end_date=date(2019, 4, 21),
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=2,
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(supervision_period, start_date=date(2019, 4, 21)),
                supervision_period_supervision_type,
                most_severe_violation_type=StateSupervisionViolationType.MISDEMEANOR,
                most_severe_violation_type_subtype=StateSupervisionViolationType.MISDEMEANOR.value,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    #
    @mock.patch(
        "recidiviz.calculator.pipeline.supervision.identifier.assessment_utils._assessment_types_of_class_for_state"
    )
    def test_supervision_time_buckets_revocation_details_us_mo(
        self, mock_assessment_types
    ):
        """Tests the supervision_time_buckets function when there is an incarceration period with
        a revocation admission in the same month as the supervision period's termination_date. Ensures that the
        correct revocation_type, violation_count_type, supervising_officer_external_id, and
        supervising_district_external_id are set on the RevocationReturnSupervisionTimeBucket. Also tests that only
        the relevant assessment_type for the given state_code and pipeline is included in output."""
        mock_assessment_types.return_value = [
            StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
        ]

        supervision_period_agent_association = [
            {
                "agent_id": 000,
                "agent_external_id": "AGENTX",
                "supervision_period_id": 111,
            }
        ]

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_MO",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="OFFICE_1",
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_MO",
            violation_date=date(2018, 4, 20),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_MO",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_MO",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        violation_report = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_MO",
            supervision_violation_response_id=888,
            response_date=date(2018, 4, 21),
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype="INI",
            supervision_violation=supervision_violation,
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
            ],
        )

        permanent_decision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_MO",
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_date=date(2018, 4, 23),
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                supervision_violation=supervision_violation,
            )
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_MO",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        relevant_assessment = StateAssessment.new_with_defaults(
            state_code="US_MO",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2018, 3, 1),
        )

        irrelevant_assessment = StateAssessment.new_with_defaults(
            state_code="US_MO",
            assessment_type=StateAssessmentType.ORAS_PRISON_INTAKE,
            assessment_score=39,
            assessment_date=date(2018, 5, 25),
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code="US_MO",
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id="ss1",
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.REVOKED,
                completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period],
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=supervision_period.start_date,
                    end_date=supervision_period.termination_date,
                    supervision_type=StateSupervisionType.PROBATION,
                ),
                SupervisionTypeSpan(
                    start_date=supervision_period.termination_date,
                    end_date=None,
                    supervision_type=None,
                ),
            ],
        )

        incarceration_sentence = (
            FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
                StateIncarcerationSentence.new_with_defaults(
                    state_code="US_MO",
                    incarceration_sentence_id=123,
                    external_id="is1",
                    start_date=date(2018, 5, 25),
                    incarceration_periods=[incarceration_period],
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=incarceration_period.admission_date,
                        end_date=None,
                        supervision_type=None,
                    )
                ],
            )
        )

        assessments = [relevant_assessment, irrelevant_assessment]
        violation_reports = [violation_report, permanent_decision_violation_response]
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
            supervision_period_agent_association,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=5,
                event_date=incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=relevant_assessment.assessment_score,
                assessment_level=relevant_assessment.assessment_level,
                assessment_type=relevant_assessment.assessment_type,
                revocation_type=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                most_recent_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
                violation_history_description="1fel",
                violation_type_frequency_counter=[["FELONY", "TECHNICAL"]],
                supervising_officer_external_id="AGENTX",
                supervising_district_external_id="OFFICE_1",
                level_1_supervision_location_external_id="OFFICE_1",
                level_2_supervision_location_external_id=None,
                is_on_supervision_last_day_of_month=False,
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                response_count=1,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                supervising_officer_external_id="AGENTX",
                supervising_district_external_id="OFFICE_1",
                level_1_supervision_location_external_id="OFFICE_1",
            ),
            create_start_bucket_from_period(
                supervision_period,
                supervising_officer_external_id="AGENTX",
                supervising_district_external_id="OFFICE_1",
                level_1_supervision_location_external_id="OFFICE_1",
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                end_date=violation_report.response_date,
                assessment_score=relevant_assessment.assessment_score,
                assessment_level=relevant_assessment.assessment_level,
                assessment_type=relevant_assessment.assessment_type,
                supervising_officer_external_id="AGENTX",
                level_1_supervision_location_external_id="OFFICE_1",
                level_2_supervision_location_external_id=None,
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(
                    supervision_period, start_date=violation_report.response_date
                ),
                supervision_period_supervision_type,
                assessment_score=relevant_assessment.assessment_score,
                assessment_level=relevant_assessment.assessment_level,
                assessment_type=relevant_assessment.assessment_type,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
                supervising_officer_external_id="AGENTX",
                level_1_supervision_location_external_id="OFFICE_1",
                level_2_supervision_location_external_id=None,
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    #
    @mock.patch(
        "recidiviz.calculator.pipeline.supervision.identifier.assessment_utils._assessment_types_of_class_for_state"
    )
    def test_supervision_time_buckets_revocation_details_us_mo_shock_spfi(
        self, mock_assessment_types
    ):
        mock_assessment_types.return_value = [
            StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            StateAssessmentType.ORAS_COMMUNITY_SUPERVISION_SCREENING,
        ]

        supervision_period_agent_association = [
            {
                "agent_id": 000,
                "agent_external_id": "AGENTX",
                "supervision_period_id": 111,
            }
        ]

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_MO",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="OFFICE_1",
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_MO",
            violation_date=date(2018, 4, 20),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_MO",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_MO",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        violation_report = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_MO",
            supervision_violation_response_id=888,
            response_date=date(2018, 4, 21),
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype="INI",
            supervision_violation=supervision_violation,
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
            ],
        )

        permanent_decision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_MO",
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_date=date(2018, 4, 23),
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                supervision_violation_response_decisions=[
                    StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                        state_code="US_MO",
                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    )
                ],
                supervision_violation=supervision_violation,
            )
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_MO",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        relevant_assessment = StateAssessment.new_with_defaults(
            state_code="US_MO",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2018, 3, 1),
        )

        irrelevant_assessment = StateAssessment.new_with_defaults(
            state_code="US_MO",
            assessment_type=StateAssessmentType.ORAS_PRISON_INTAKE,
            assessment_score=39,
            assessment_date=date(2018, 5, 25),
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code="US_XX",
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id="ss1",
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.REVOKED,
                completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period],
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=supervision_period.start_date,
                    end_date=supervision_period.termination_date,
                    supervision_type=StateSupervisionType.PROBATION,
                ),
                SupervisionTypeSpan(
                    start_date=supervision_period.termination_date,
                    end_date=None,
                    supervision_type=None,
                ),
            ],
        )

        incarceration_sentence = (
            FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
                StateIncarcerationSentence.new_with_defaults(
                    state_code="US_XX",
                    incarceration_sentence_id=123,
                    external_id="is1",
                    start_date=date(2018, 5, 25),
                    incarceration_periods=[incarceration_period],
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=incarceration_period.admission_date,
                        end_date=None,
                        supervision_type=None,
                    )
                ],
            )
        )

        assessments = [relevant_assessment, irrelevant_assessment]
        violation_reports = [violation_report, permanent_decision_violation_response]
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
            supervision_period_agent_association,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=5,
                event_date=incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=relevant_assessment.assessment_score,
                assessment_level=relevant_assessment.assessment_level,
                assessment_type=relevant_assessment.assessment_type,
                revocation_type=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                most_recent_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
                violation_history_description="1fel",
                violation_type_frequency_counter=[["FELONY", "TECHNICAL"]],
                supervising_officer_external_id="AGENTX",
                supervising_district_external_id="OFFICE_1",
                level_1_supervision_location_external_id="OFFICE_1",
                level_2_supervision_location_external_id=None,
                is_on_supervision_last_day_of_month=False,
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                response_count=1,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                supervising_officer_external_id="AGENTX",
                supervising_district_external_id="OFFICE_1",
                level_1_supervision_location_external_id="OFFICE_1",
                level_2_supervision_location_external_id=None,
            ),
            create_start_bucket_from_period(
                supervision_period,
                supervising_officer_external_id="AGENTX",
                supervising_district_external_id="OFFICE_1",
                level_1_supervision_location_external_id="OFFICE_1",
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                end_date=violation_report.response_date,
                assessment_score=relevant_assessment.assessment_score,
                assessment_level=relevant_assessment.assessment_level,
                assessment_type=relevant_assessment.assessment_type,
                supervising_officer_external_id="AGENTX",
                level_1_supervision_location_external_id="OFFICE_1",
                level_2_supervision_location_external_id=None,
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(
                    supervision_period, start_date=violation_report.response_date
                ),
                supervision_period_supervision_type,
                assessment_score=relevant_assessment.assessment_score,
                assessment_level=relevant_assessment.assessment_level,
                assessment_type=relevant_assessment.assessment_type,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
                supervising_officer_external_id="AGENTX",
                level_1_supervision_location_external_id="OFFICE_1",
                level_2_supervision_location_external_id=None,
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_supervision_time_buckets_technical_revocation_with_subtype_us_mo(self):
        """Tests the find_supervision_time_buckets function when there is an incarceration period with a
        revocation admission in the same month as the supervision period's termination_date. Also ensures that the
        correct revocation_type, violation_count_type, violation_subtype, supervising_officer_external_id, and
        supervising_district_external_id are set on the RevocationReturnSupervisionTimeBucket."""

        supervision_period_agent_association = [
            {
                "agent_id": 000,
                "agent_external_id": "AGENTX",
                "supervision_period_id": 111,
            }
        ]

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_MO",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="OFFICE_1",
        )

        citation_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=1234,
            state_code="US_MO",
            violation_date=date(2009, 1, 7),
        )

        citation = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_MO",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.CITATION,
            response_date=date(2018, 2, 20),
            is_draft=False,
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                ),
            ],
            supervision_violation=citation_violation,
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            violation_date=date(2018, 2, 20),
            state_code="US_MO",
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_MO",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                )
            ],
            supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code="US_MO", condition="DRG"
                )
            ],
        )

        violation_report = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_MO",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_date=date(2018, 2, 21),
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype="INI",
            supervision_violation=supervision_violation,
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
            ],
        )

        permanent_decision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_MO",
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_date=date(2018, 2, 23),
                supervision_violation=supervision_violation,
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            )
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_MO",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_MO",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2018, 3, 1),
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code="US_XX",
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id="ss1",
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.COMPLETED,
                completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period],
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=supervision_period.start_date,
                    end_date=supervision_period.termination_date,
                    supervision_type=StateSupervisionType.PROBATION,
                ),
                SupervisionTypeSpan(
                    start_date=supervision_period.termination_date,
                    end_date=None,
                    supervision_type=None,
                ),
            ],
        )

        incarceration_sentence = (
            FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
                StateIncarcerationSentence.new_with_defaults(
                    state_code="US_XX",
                    incarceration_sentence_id=123,
                    external_id="is1",
                    start_date=date(2018, 5, 25),
                    incarceration_periods=[incarceration_period],
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=incarceration_period.admission_date,
                        end_date=None,
                        supervision_type=None,
                    )
                ],
            )
        )

        assessments = [assessment]
        violation_responses = [
            violation_report,
            citation,
            permanent_decision_violation_response,
        ]
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
            supervision_period_agent_association,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=5,
                event_date=incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                revocation_type=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
                most_severe_violation_type_subtype="SUBSTANCE_ABUSE",
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                most_recent_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=2,
                violation_history_description="1subs;1tech",
                violation_type_frequency_counter=[
                    [StateSupervisionViolationType.TECHNICAL.value],
                    ["SUBSTANCE_ABUSE"],
                ],
                supervising_officer_external_id="AGENTX",
                supervising_district_external_id="OFFICE_1",
                level_1_supervision_location_external_id="OFFICE_1",
                is_on_supervision_last_day_of_month=False,
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                response_count=2,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
                most_severe_violation_type_subtype="SUBSTANCE_ABUSE",
                supervising_officer_external_id="AGENTX",
                supervising_district_external_id="OFFICE_1",
                level_1_supervision_location_external_id="OFFICE_1",
            ),
            create_start_bucket_from_period(
                supervision_period,
                supervising_officer_external_id="AGENTX",
                supervising_district_external_id="OFFICE_1",
                level_1_supervision_location_external_id="OFFICE_1",
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
                most_severe_violation_type_subtype="SUBSTANCE_ABUSE",
                response_count=2,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                supervising_officer_external_id="AGENTX",
                level_1_supervision_location_external_id="OFFICE_1",
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_supervision_time_buckets_technical_revocation_with_law_subtype_us_mo(self):
        """Tests the find_supervision_time_buckets function when there is an incarceration period with a
        revocation admission in the same month as the supervision period's termination_date. Also ensures that the
        correct revocation_type, violation_count_type, violation_subtype, supervising_officer_external_id, and
        supervising_district_external_id are set on the RevocationReturnSupervisionTimeBucket."""

        supervision_period_agent_association = [
            {
                "agent_id": 000,
                "agent_external_id": "AGENTX",
                "supervision_period_id": 111,
            }
        ]

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_MO",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="OFFICE_1",
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_MO",
            violation_date=date(2018, 2, 20),
            supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code="US_MO", condition="LAW"
                ),
            ],
        )

        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_MO",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.CITATION,
            response_date=date(2018, 2, 20),
            is_draft=False,
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        permanent_decision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_MO",
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_date=date(2018, 2, 23),
                supervision_violation=supervision_violation,
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            )
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_MO",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_MO",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2018, 3, 1),
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code="US_MO",
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id="ss1",
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.COMPLETED,
                completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period],
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=supervision_period.start_date,
                    end_date=supervision_period.termination_date,
                    supervision_type=StateSupervisionType.PROBATION,
                ),
                SupervisionTypeSpan(
                    start_date=supervision_period.termination_date,
                    end_date=None,
                    supervision_type=None,
                ),
            ],
        )

        incarceration_sentence = (
            FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
                StateIncarcerationSentence.new_with_defaults(
                    state_code="US_MO",
                    incarceration_sentence_id=123,
                    external_id="is1",
                    start_date=date(2018, 5, 25),
                    incarceration_periods=[incarceration_period],
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=incarceration_period.admission_date,
                        end_date=None,
                        supervision_type=None,
                    )
                ],
            )
        )

        assessments = [assessment]
        violation_responses = [
            supervision_violation_response,
            permanent_decision_violation_response,
        ]
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
            supervision_period_agent_association,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=5,
                event_date=incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                revocation_type=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
                most_severe_violation_type_subtype="LAW_CITATION",
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                most_recent_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
                violation_history_description="1law_cit",
                violation_type_frequency_counter=[["LAW_CITATION"]],
                supervising_officer_external_id="AGENTX",
                supervising_district_external_id="OFFICE_1",
                level_1_supervision_location_external_id="OFFICE_1",
                is_on_supervision_last_day_of_month=False,
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                response_count=1,
                most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
                most_severe_violation_type_subtype="LAW_CITATION",
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                supervising_officer_external_id="AGENTX",
                supervising_district_external_id="OFFICE_1",
                level_1_supervision_location_external_id="OFFICE_1",
            ),
            create_start_bucket_from_period(
                supervision_period,
                supervising_officer_external_id="AGENTX",
                supervising_district_external_id="OFFICE_1",
                level_1_supervision_location_external_id="OFFICE_1",
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
                most_severe_violation_type_subtype="LAW_CITATION",
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
                supervising_officer_external_id="AGENTX",
                level_1_supervision_location_external_id="OFFICE_1",
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_supervision_time_buckets_technical_revocation_with_other_conditions_us_mo(
        self,
    ):
        """Tests the find_supervision_time_buckets function when there is an incarceration period with a
        revocation admission in the same month as the supervision period's termination_date. Also ensures that the
        correct revocation_type, violation_count_type, violation_subtype, supervising_officer_external_id, and
        supervising_district_external_id are set on the RevocationReturnSupervisionTimeBucket."""

        supervision_period_agent_association = [
            {
                "agent_id": 000,
                "agent_external_id": "AGENTX",
                "supervision_period_id": 111,
            }
        ]

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_MO",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="OFFICE_1",
        )

        citation_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=1234,
            state_code="US_MO",
            violation_date=date(2009, 1, 7),
        )

        citation = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_MO",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.CITATION,
            response_date=date(2018, 2, 20),
            is_draft=False,
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                ),
            ],
            supervision_violation=citation_violation,
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            violation_date=date(2018, 2, 20),
            state_code="US_MO",
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_MO",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                )
            ],
            supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code="US_MO", condition="EMP"
                ),
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code="US_MO", condition="ASC"
                ),
            ],
        )

        violation_report = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_MO",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_date=date(2018, 2, 21),
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype="INI",
            supervision_violation=supervision_violation,
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_MO",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
            ],
        )

        permanent_decision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_MO",
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_date=date(2018, 2, 23),
                supervision_violation=supervision_violation,
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            )
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_MO",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_MO",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2018, 3, 1),
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code="US_XX",
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                external_id="ss1",
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.COMPLETED,
                completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period],
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=supervision_period.start_date,
                    end_date=supervision_period.termination_date,
                    supervision_type=StateSupervisionType.PROBATION,
                ),
                SupervisionTypeSpan(
                    start_date=supervision_period.termination_date,
                    end_date=None,
                    supervision_type=None,
                ),
            ],
        )

        incarceration_sentence = (
            FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
                StateIncarcerationSentence.new_with_defaults(
                    state_code="US_XX",
                    incarceration_sentence_id=123,
                    external_id="is1",
                    start_date=date(2018, 5, 25),
                    incarceration_periods=[incarceration_period],
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=incarceration_period.admission_date,
                        end_date=None,
                        supervision_type=None,
                    )
                ],
            )
        )

        assessments = [assessment]
        violation_responses = [
            violation_report,
            citation,
            permanent_decision_violation_response,
        ]
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
            supervision_period_agent_association,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2018,
                month=5,
                event_date=incarceration_period.admission_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                revocation_type=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
                most_severe_violation_type_subtype=StateSupervisionViolationType.TECHNICAL.value,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                most_recent_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=2,
                violation_history_description="2tech",
                violation_type_frequency_counter=[
                    ["TECHNICAL"],
                    ["EMP", "ASC", "TECHNICAL"],
                ],
                supervising_officer_external_id="AGENTX",
                supervising_district_external_id="OFFICE_1",
                level_1_supervision_location_external_id="OFFICE_1",
                is_on_supervision_last_day_of_month=False,
            ),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                response_count=2,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
                most_severe_violation_type_subtype=StateSupervisionViolationType.TECHNICAL.value,
                supervising_officer_external_id="AGENTX",
                supervising_district_external_id="OFFICE_1",
                level_1_supervision_location_external_id="OFFICE_1",
            ),
            create_start_bucket_from_period(
                supervision_period,
                supervising_officer_external_id="AGENTX",
                supervising_district_external_id="OFFICE_1",
                level_1_supervision_location_external_id="OFFICE_1",
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
                most_severe_violation_type_subtype=StateSupervisionViolationType.TECHNICAL.value,
                response_count=2,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                supervising_officer_external_id="AGENTX",
                level_1_supervision_location_external_id="OFFICE_1",
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_supervision_time_buckets_no_supervision_period_end_of_month_us_mo_supervision_span_shows_supervision(
        self,
    ):
        """Tests that we do not mark someone as under supervision at the end of the month if there is no supervision
        period overlapping with the end of the month, even if the US_MO sentence supervision spans indicate that they
        are on supervision at a given time.
        """

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_MO",
            supervision_site="DISTRICTX",
            supervising_officer="AGENTX",
            start_date=date(2019, 10, 3),
            termination_date=date(2019, 10, 9),
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=supervision_period.start_date,
                external_id="ss1",
                state_code="US_MO",
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.SERVING,
                supervision_periods=[supervision_period],
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=supervision_period.start_date,
                    end_date=None,
                    supervision_type=StateSupervisionType.PROBATION,
                )
            ],
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_district_external_id=supervision_period.supervision_site,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                termination_reason=supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(supervision_period),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                StateSupervisionPeriodSupervisionType.PROBATION,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_supervision_time_buckets_period_eom_us_mo_supervision_span_shows_no_supervision_eom(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_MO",
            supervision_site="DISTRICTX",
            supervising_officer="AGENTX",
            start_date=date(2019, 10, 3),
            termination_date=date(2019, 11, 9),
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=supervision_period.start_date,
                external_id="ss1",
                state_code="US_MO",
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.SERVING,
                supervision_periods=[supervision_period],
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=supervision_period.start_date,
                    end_date=date(2019, 10, 6),
                    supervision_type=StateSupervisionType.PROBATION,
                ),
                SupervisionTypeSpan(
                    start_date=date(2019, 10, 6),
                    end_date=date(2019, 11, 6),
                    supervision_type=None,
                ),
                SupervisionTypeSpan(
                    start_date=date(2019, 11, 6),
                    end_date=None,
                    supervision_type=StateSupervisionType.PROBATION,
                ),
            ],
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_buckets = [
            create_start_bucket_from_period(supervision_period),
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_district_external_id=supervision_period.supervision_site,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                termination_reason=supervision_period.termination_reason,
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                StateSupervisionPeriodSupervisionType.PROBATION,
                end_date=date(2019, 10, 6),
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
            )
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(supervision_period, start_date=date(2019, 11, 6)),
                StateSupervisionPeriodSupervisionType.PROBATION,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_supervision_period_end_of_month_us_mo_supervision_span_shows_no_supervision_all_month(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_MO",
            supervision_site="DISTRICTX",
            supervising_officer="AGENTX",
            start_date=date(2019, 10, 3),
            termination_date=date(2019, 11, 9),
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=supervision_period.start_date,
                external_id="ss1",
                state_code="US_MO",
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.SERVING,
                supervision_periods=[supervision_period],
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=date(2019, 9, 6),
                    end_date=date(2019, 10, 3),
                    supervision_type=StateSupervisionType.PROBATION,
                ),
                SupervisionTypeSpan(
                    start_date=date(2019, 10, 3),
                    end_date=date(2019, 11, 6),
                    supervision_type=None,
                ),
                SupervisionTypeSpan(
                    start_date=date(2019, 11, 6),
                    end_date=None,
                    supervision_type=StateSupervisionType.PROBATION,
                ),
            ],
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_district_external_id=supervision_period.supervision_site,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                termination_reason=supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(supervision_period),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(supervision_period, start_date=date(2019, 11, 6)),
                StateSupervisionPeriodSupervisionType.PROBATION,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_supervision_period_us_mo_supervision_spans_do_not_overlap(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_MO",
            supervision_site="DISTRICTX",
            supervising_officer="AGENTX",
            start_date=date(2019, 10, 3),
            termination_date=date(2019, 11, 9),
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=supervision_period.start_date,
                external_id="ss1",
                state_code="US_MO",
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.SERVING,
                supervision_periods=[supervision_period],
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=date(2019, 9, 6),
                    end_date=date(2019, 10, 3),
                    supervision_type=StateSupervisionType.PROBATION,
                ),
                SupervisionTypeSpan(
                    start_date=date(2019, 10, 3),
                    end_date=date(2019, 11, 9),
                    supervision_type=None,
                ),
                SupervisionTypeSpan(
                    start_date=date(2019, 11, 9),
                    end_date=None,
                    supervision_type=StateSupervisionType.PROBATION,
                ),
            ],
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        self.assertCountEqual(supervision_time_buckets, [])

    def test_supervision_period_mid_month_us_mo_supervision_span_shows_supervision_eom(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_MO",
            supervision_site="DISTRICTX",
            supervising_officer="AGENTX",
            start_date=date(2019, 10, 3),
            termination_date=date(2019, 10, 20),
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=supervision_period.start_date,
                external_id="ss1",
                state_code="US_MO",
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.SERVING,
                supervision_periods=[supervision_period],
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=date(2019, 9, 6),
                    end_date=date(2019, 10, 15),
                    supervision_type=None,
                ),
                SupervisionTypeSpan(
                    start_date=date(2019, 10, 15),
                    end_date=None,
                    supervision_type=StateSupervisionType.PROBATION,
                ),
            ],
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        expected_buckets = [
            SupervisionTerminationBucket(
                state_code=supervision_period.state_code,
                year=supervision_period.termination_date.year,
                month=supervision_period.termination_date.month,
                event_date=supervision_period.termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_district_external_id=supervision_period.supervision_site,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                termination_reason=supervision_period.termination_reason,
            ),
            create_start_bucket_from_period(supervision_period),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(supervision_period, start_date=date(2019, 10, 15)),
                StateSupervisionPeriodSupervisionType.PROBATION,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    @freeze_time("2000-01-01")
    def test_find_supervision_time_buckets_dates_in_future(self):
        """Tests the find_supervision_time_buckets function for a supervision period where the termination date is
        in the future."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            start_date=date(1990, 1, 1),
            # An erroneous date in the future
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        supervision_period_future = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            # Erroneous dates in the future, period should be dropped entirely
            start_date=date(2020, 1, 1),
            termination_date=date(2020, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            start_date=date(1990, 1, 1),
            external_id="ss1",
            state_code="US_XX",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            completion_date=date(2018, 5, 19),
            supervision_periods=[supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period, supervision_period_future]
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
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = [
            create_start_bucket_from_period(
                supervision_period, case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE
            ),
        ]

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                supervision_period,
                end_date=date.today() + relativedelta(days=1),
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)


class TestFindRevocationReturnBuckets(unittest.TestCase):
    """Tests the find_revocation_return_buckets."""

    @staticmethod
    def _find_revocation_return_buckets(
        supervision_periods: List[StateSupervisionPeriod],
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_sentences: Optional[List[StateSupervisionSentence]] = None,
        incarceration_sentences: Optional[List[StateIncarcerationSentence]] = None,
        assessments: Optional[List[StateAssessment]] = None,
        violation_responses: Optional[List[StateSupervisionViolationResponse]] = None,
    ) -> List[SupervisionTimeBucket]:
        """Helper function for testing the find_revocation_return_buckets function."""
        supervision_sentences = supervision_sentences if supervision_sentences else []
        incarceration_sentences = (
            incarceration_sentences if incarceration_sentences else []
        )
        assessments = assessments if assessments else []
        sorted_violation_responses = (
            sorted(violation_responses, key=lambda b: b.response_date or date.min)
            if violation_responses
            else []
        )

        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods)

        return identifier.find_revocation_return_buckets(
            supervision_sentences=supervision_sentences,
            incarceration_sentences=incarceration_sentences,
            supervision_periods=supervision_periods,
            assessments=assessments,
            sorted_violation_responses=sorted_violation_responses,
            supervision_period_to_agent_associations=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            supervision_period_to_judicial_district_associations=DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATIONS,
            incarceration_period_index=incarceration_period_index,
        )

    def test_revocation_return_buckets_violation_history_cutoff(self):
        """Tests the find_supervision_time_buckets function, specifically the logic that includes the violation
        reports within the violation window. The `old` response and violation fall within a year of the last violation
        response before the revocation, but not within a year of the revocation date. Test that the `old` response is
        included in the response history."""

        supervision_violation_1 = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_XX",
            violation_date=date(2008, 12, 7),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                )
            ],
        )

        supervision_violation_response_1 = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_XX",
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2008, 12, 7),
                supervision_violation_response_decisions=[
                    StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                        state_code="US_XX",
                        decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    )
                ],
                supervision_violation=supervision_violation_1,
            )
        )

        supervision_violation_2 = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_XX",
            violation_date=date(2009, 11, 13),
        )

        supervision_violation_response_2 = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_XX",
            response_date=date(2009, 11, 13),
            supervision_violation_response_decisions=[
                # This REVOCATION decision is the most severe, but this is not the most recent response
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                )
            ],
            supervision_violation=supervision_violation_2,
        )

        supervision_violation_3 = StateSupervisionViolation.new_with_defaults(
            state_code="US_XX",
            supervision_violation_id=6789,
            violation_date=date(2009, 12, 1),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                )
            ],
        )

        supervision_violation_response_3 = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_XX",
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2009, 12, 1),
                supervision_violation_response_decisions=[
                    StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                        state_code="US_XX",
                        decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    )
                ],
                supervision_violation=supervision_violation_3,
            )
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2009, 12, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="OFFICE_1",
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=date(2009, 12, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        )

        violation_responses = [
            supervision_violation_response_1,
            supervision_violation_response_2,
            supervision_violation_response_3,
        ]

        revocation_buckets = self._find_revocation_return_buckets(
            supervision_periods=[supervision_period],
            incarceration_periods=[incarceration_period],
            violation_responses=violation_responses,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_revocation_bucket = RevocationReturnSupervisionTimeBucket(
            state_code=supervision_period.state_code,
            year=2009,
            month=12,
            event_date=incarceration_period.admission_date,
            supervision_type=supervision_period_supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            most_recent_response_decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
            response_count=3,
            violation_history_description="1felony;1technical",
            violation_type_frequency_counter=[["FELONY"], ["TECHNICAL"]],
            supervising_officer_external_id="XXX",
            supervising_district_external_id="OFFICE_1",
            judicial_district_code="XXX",
            level_1_supervision_location_external_id="OFFICE_1",
            is_on_supervision_last_day_of_month=False,
        )

        self.assertEqual([expected_revocation_bucket], revocation_buckets)

    def test_revocation_return_buckets_before_violation_history_cutoff(self):
        """Tests the find_supervision_time_buckets function, specifically the logic that includes the violation
        reports within the violation window. The `old` response and violation falls before the violation history
        window. Test that the `old` response is not included in the response history."""

        supervision_violation_old = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_XX",
            violation_date=date(2007, 12, 7),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                )
            ],
        )

        supervision_violation_response_old = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_XX",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2007, 12, 7),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                )
            ],
            supervision_violation=supervision_violation_old,
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            state_code="US_XX",
            supervision_violation_id=6789,
            violation_date=date(2009, 12, 1),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                )
            ],
        )

        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_XX",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2009, 12, 1),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2009, 12, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="OFFICE_1",
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=date(2009, 12, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        )

        violation_responses = [
            supervision_violation_response,
            supervision_violation_response_old,
        ]

        revocation_buckets = self._find_revocation_return_buckets(
            supervision_periods=[supervision_period],
            incarceration_periods=[incarceration_period],
            violation_responses=violation_responses,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_revocation_bucket = RevocationReturnSupervisionTimeBucket(
            state_code=supervision_period.state_code,
            year=2009,
            month=12,
            event_date=incarceration_period.admission_date,
            supervision_type=supervision_period_supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
            most_severe_violation_type_subtype=StateSupervisionViolationType.TECHNICAL.value,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            most_recent_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1technical",
            violation_type_frequency_counter=[["TECHNICAL"]],
            supervising_officer_external_id="XXX",
            supervising_district_external_id="OFFICE_1",
            judicial_district_code="XXX",
            level_1_supervision_location_external_id="OFFICE_1",
            is_on_supervision_last_day_of_month=False,
        )

        self.assertEqual([expected_revocation_bucket], revocation_buckets)

    def test_revocation_return_buckets_us_mo_ignore_supplemental_for_lookback_date(
        self,
    ):
        """Tests the find_supervision_time_buckets function, specifically the logic that includes the violation
        reports within the violation window. The most recent response prior to the revocation is a supplemental report,
        which should not be included when determining the date of the most recent response for the violation history
        window. Tests that the date on the most recent non-supplemental report is used for the violation history, but
        that the response decision on the supplemental report is counted for the most_severe_response_decision.
        """
        state_code = "US_MO"

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code=state_code,
            violation_date=date(2008, 12, 7),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=state_code,
                    violation_type=StateSupervisionViolationType.FELONY,
                )
            ],
        )

        supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=state_code,
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype="INI",
                response_date=date(2008, 12, 7),
                supervision_violation_response_decisions=[
                    StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                        state_code=state_code,
                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    )
                ],
                supervision_violation=supervision_violation,
            )
        )

        supervision_violation_sup = StateSupervisionViolation.new_with_defaults(
            state_code=state_code,
            supervision_violation_id=6789,
            violation_date=date(2012, 12, 1),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=state_code,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                )
            ],
        )

        supervision_violation_response_sup = StateSupervisionViolationResponse.new_with_defaults(
            state_code=state_code,
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype="SUP",
            response_date=date(2012, 12, 1),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code=state_code,
                    decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
                )
            ],
            supervision_violation=supervision_violation_sup,
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code=state_code,
            start_date=date(2008, 3, 5),
            termination_date=date(2012, 12, 19),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="OFFICE_1",
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2012, 12, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code="US_MO",
                supervision_sentence_id=111,
                start_date=date(2008, 3, 5),
                external_id="ss1",
                supervision_type=StateSupervisionType.PROBATION,
                status=StateSentenceStatus.REVOKED,
                completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period],
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=supervision_period.start_date,
                    end_date=supervision_period.termination_date,
                    supervision_type=StateSupervisionType.PROBATION,
                ),
                SupervisionTypeSpan(
                    start_date=supervision_period.termination_date,
                    end_date=None,
                    supervision_type=None,
                ),
            ],
        )

        incarceration_sentence = (
            FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
                StateIncarcerationSentence.new_with_defaults(
                    state_code="US_MO",
                    incarceration_sentence_id=123,
                    external_id="is1",
                    start_date=date(2018, 5, 25),
                    incarceration_periods=[incarceration_period],
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=incarceration_period.admission_date,
                        end_date=None,
                        supervision_type=None,
                    )
                ],
            )
        )

        violation_responses = [
            supervision_violation_response,
            supervision_violation_response_sup,
        ]

        revocation_buckets = self._find_revocation_return_buckets(
            supervision_periods=[supervision_period],
            incarceration_periods=[incarceration_period],
            violation_responses=violation_responses,
            supervision_sentences=[supervision_sentence],
            incarceration_sentences=[incarceration_sentence],
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_revocation_bucket = RevocationReturnSupervisionTimeBucket(
            state_code=supervision_period.state_code,
            year=2012,
            month=12,
            event_date=incarceration_period.admission_date,
            supervision_type=supervision_period_supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            most_recent_response_decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            response_count=1,
            violation_history_description="1fel",
            violation_type_frequency_counter=[["FELONY"]],
            supervising_officer_external_id="XXX",
            supervising_district_external_id="OFFICE_1",
            judicial_district_code="XXX",
            level_1_supervision_location_external_id="OFFICE_1",
            is_on_supervision_last_day_of_month=False,
        )

        self.assertEqual([expected_revocation_bucket], revocation_buckets)

    def test_revocation_return_buckets_us_nd_new_admission_after_probation_revocation(
        self,
    ):
        """Tests that the find_revocation_return_buckets when run for a US_ND
        incarceration period with a NEW_ADMISSION admission reason following a
        supervision period with a REVOCATION termination reason and a PROBATION
        supervision type will correctly return a RevocationReturnSupervisionTimeBucket
        for that commitment from supervision."""

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_ND",
            violation_date=date(2008, 12, 7),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_ND",
                    violation_type=StateSupervisionViolationType.FELONY,
                )
            ],
        )

        supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_ND",
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                response_date=date(2009, 12, 7),
                supervision_violation=supervision_violation,
            )
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2008, 3, 5),
            termination_date=date(2009, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="OFFICE_1",
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_ND",
            admission_date=date(2009, 12, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        violation_responses = [
            supervision_violation_response,
        ]

        revocation_buckets = self._find_revocation_return_buckets(
            supervision_periods=[supervision_period],
            incarceration_periods=[incarceration_period],
            violation_responses=violation_responses,
        )

        expected_revocation_bucket = RevocationReturnSupervisionTimeBucket(
            state_code=supervision_period.state_code,
            year=2009,
            month=12,
            event_date=incarceration_period.admission_date,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            case_type=StateSupervisionCaseType.GENERAL,
            revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
            response_count=1,
            violation_history_description="1felony",
            violation_type_frequency_counter=[["FELONY"]],
            supervising_officer_external_id="XXX",
            supervising_district_external_id="OFFICE_1",
            judicial_district_code="XXX",
            level_1_supervision_location_external_id="OFFICE_1",
            is_on_supervision_last_day_of_month=False,
        )

        self.assertEqual([expected_revocation_bucket], revocation_buckets)

    def test_revocation_return_buckets_us_nd_new_admission_after_temporary_hold_and_probation_revocation(
        self,
    ):
        """Tests that the find_revocation_return_buckets when run for a US_ND
        incarceration period with a NEW_ADMISSION admission reason following a
        TEMPORARY_CUSTODY incarceration period, which itself follows a supervision
        period with a REVOCATION termination reason and a PROBATION supervision type
        will correctly return a RevocationReturnSupervisionTimeBucket corresponding to
        that NEW_ADMISSION commitment from supervision, and not the intermediate
        TEMPORARY_CUSTODY period."""
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_ND",
            violation_date=date(2009, 11, 13),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_ND",
                    violation_type=StateSupervisionViolationType.FELONY,
                )
            ],
        )

        supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                state_code="US_ND",
                response_date=date(2009, 11, 13),
                supervision_violation=supervision_violation,
            )
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2008, 3, 5),
            termination_date=date(2009, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="OFFICE_1",
        )

        temporary_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            state_code="US_ND",
            admission_date=date(2009, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2009, 12, 31),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_ND",
            admission_date=date(2010, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        violation_responses = [supervision_violation_response]

        revocation_buckets = self._find_revocation_return_buckets(
            supervision_periods=[supervision_period],
            incarceration_periods=[
                temporary_incarceration_period,
                incarceration_period,
            ],
            violation_responses=violation_responses,
        )

        expected_revocation_bucket = RevocationReturnSupervisionTimeBucket(
            state_code=supervision_period.state_code,
            year=2010,
            month=1,
            event_date=incarceration_period.admission_date,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            case_type=StateSupervisionCaseType.GENERAL,
            revocation_type=StateSpecializedPurposeForIncarceration.GENERAL,
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
            response_count=1,
            violation_history_description="1felony",
            violation_type_frequency_counter=[["FELONY"]],
            supervising_officer_external_id="XXX",
            supervising_district_external_id="OFFICE_1",
            judicial_district_code="XXX",
            level_1_supervision_location_external_id="OFFICE_1",
            is_on_supervision_last_day_of_month=False,
        )

        self.assertEqual([expected_revocation_bucket], revocation_buckets)

    def test_revocation_return_buckets_us_nd_new_admission_after_probation_non_revocation(
        self,
    ):
        """Tests that the find_revocation_return_buckets when run for a US_ND
        incarceration period with a NEW_ADMISSION admission reason following a
        supervision period with a NON-REVOCATION termination reason and a PROBATION
        supervision type will correctly return no RevocationReturnSupervisionTimeBuckets
        in accordance with US_ND logic."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2008, 3, 5),
            termination_date=date(2009, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            supervision_type=StateSupervisionType.PAROLE,
            supervision_site="OFFICE_1",
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_ND",
            admission_date=date(2009, 12, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        revocation_buckets = self._find_revocation_return_buckets(
            supervision_periods=[supervision_period],
            incarceration_periods=[incarceration_period],
            violation_responses=[],
        )

        self.assertEqual([], revocation_buckets)

    def test_revocation_return_buckets_us_nd_new_admission_after_parole_revocation(
        self,
    ):
        """Tests that the find_revocation_return_buckets when run for a US_ND
        incarceration period with a NEW_ADMISSION admission reason following a
        supervision period with a REVOCATION termination reason and a PAROLE
        supervision type will correctly return no RevocationReturnSupervisionTimeBuckets
        in accordance with US_ND logic."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_ND",
            start_date=date(2008, 3, 5),
            termination_date=date(2009, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
            supervision_site="OFFICE_1",
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_ND",
            admission_date=date(2009, 12, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        revocation_buckets = self._find_revocation_return_buckets(
            supervision_periods=[supervision_period],
            incarceration_periods=[incarceration_period],
            violation_responses=[],
        )

        self.assertEqual([], revocation_buckets)


class TestFindTimeBucketsForSupervisionPeriod(unittest.TestCase):
    """Tests for the find_time_buckets_for_supervision_period function."""

    def setUp(self):
        self.maxDiff = None

        self.assessment_types_patcher = mock.patch(
            "recidiviz.calculator.pipeline.supervision.identifier.assessment_utils."
            "_assessment_types_of_class_for_state"
        )
        self.mock_assessment_types = self.assessment_types_patcher.start()
        self.mock_assessment_types.return_value = [
            StateAssessmentType.ORAS,
            StateAssessmentType.LSIR,
        ]

    def tearDown(self):
        self.assessment_types_patcher.stop()

    def test_find_time_buckets_for_supervision_period_revocation_no_termination(self):
        """Tests the find_time_buckets_for_supervision_period function
        when there is an incarceration period with a revocation admission,
        where there is no termination_date on the supervision_period, and
        no release_date on the incarceration_period."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2003, 7, 5),
            supervision_type=StateSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2003, 10, 10),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2003, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSentenceStatus.SERVING,
            supervision_periods=[supervision_period],
        )

        incarceration_period_index = IncarcerationPeriodIndex(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = SupervisionPeriodIndex(
            supervision_periods=[supervision_period]
        )

        assessments = []

        violation_reports = []
        supervision_contacts = []
        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]

        supervision_time_buckets = identifier.find_time_buckets_for_supervision_period(
            supervision_sentences,
            incarceration_sentences,
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_reports,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        self.assertCountEqual(
            supervision_time_buckets,
            expected_non_revocation_return_time_buckets(
                supervision_period,
                supervision_period_supervision_type,
                incarceration_period.admission_date,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    end_date_override=date(2003, 10, 10),
                ),
            ),
        )

    def test_find_time_buckets_for_supervision_period_incarceration_ends_same_month(
        self,
    ):
        """Tests the find_time_buckets_for_supervision_period function when there is an incarceration period with a
        revocation admission before the supervision period's termination_date, and the supervision_period and the
        incarceration_period end in the same month."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 4, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2018, 5, 22),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            completion_date=date(2018, 5, 19),
            supervision_periods=[supervision_period],
        )

        incarceration_period_index = IncarcerationPeriodIndex(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = SupervisionPeriodIndex(
            supervision_periods=[supervision_period]
        )

        incarceration_sentences = []
        assessments = []
        violation_reports = []
        supervision_contacts = []

        supervision_time_buckets = identifier.find_time_buckets_for_supervision_period(
            [supervision_sentence],
            incarceration_sentences,
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_reports,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            incarceration_period.admission_date,
            case_compliances=_generate_case_compliances(
                start_date=supervision_period.start_date,
                supervision_period=supervision_period,
            ),
        )
        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    @freeze_time("2019-11-03")
    def test_find_time_buckets_for_supervision_period_nested_revocation_no_termination(
        self,
    ):
        """Tests the find_time_buckets_for_supervision_period function when there is an incarceration period with
        a revocation admission, a stay in prison, and a continued supervision period after release from incarceration
        that has still not terminated."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2019, 3, 5),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.GENERAL
                ),
            ],
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2019, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2019, 10, 17),
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSentenceStatus.SERVING,
            supervision_periods=[supervision_period],
        )

        incarceration_period_index = IncarcerationPeriodIndex(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = SupervisionPeriodIndex(
            supervision_periods=[supervision_period]
        )
        assessments = []

        violation_reports = []
        supervision_contacts = []
        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]

        supervision_time_buckets = identifier.find_time_buckets_for_supervision_period(
            supervision_sentences,
            incarceration_sentences,
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_reports,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            incarceration_period.admission_date,
            case_compliances=_generate_case_compliances(
                start_date=supervision_period.start_date,
                supervision_period=supervision_period,
                end_date_override=date(2019, 11, 2),
            ),
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(
                    supervision_period, start_date=incarceration_period.release_date
                ),
                supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    end_date_override=date(2019, 11, 3),
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_time_buckets_for_supervision_period_admission_no_revocation(self):
        """Tests the find_time_buckets_for_supervision_period function when there is an incarceration period with a
        non-revocation admission before the supervision period's termination_date."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 6, 19),
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 6, 2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            completion_date=date(2018, 7, 19),
            supervision_periods=[supervision_period],
        )

        incarceration_period_index = IncarcerationPeriodIndex(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = SupervisionPeriodIndex(
            supervision_periods=[supervision_period]
        )

        assessments = []

        violation_reports = []
        supervision_contacts = []
        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]

        supervision_time_buckets = identifier.find_time_buckets_for_supervision_period(
            supervision_sentences,
            incarceration_sentences,
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_reports,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            incarceration_period.admission_date,
            case_compliances=_generate_case_compliances(
                start_date=supervision_period.start_date,
                supervision_period=supervision_period,
            ),
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_time_buckets_for_supervision_period_multiple_years(self):
        """Tests the find_time_buckets_for_supervision_period function when the supervision period overlaps
        multiple years, and there is an incarceration period during this time that also overlaps multiple years."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 3, 19),
            supervision_type=StateSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2008, 6, 2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 12, 3),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2007, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            completion_date=date(2010, 3, 19),
            supervision_periods=[supervision_period],
        )

        incarceration_period_index = IncarcerationPeriodIndex(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = SupervisionPeriodIndex(
            supervision_periods=[supervision_period]
        )

        assessments = []

        violation_reports = []
        supervision_contacts = []
        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]

        supervision_time_buckets = identifier.find_time_buckets_for_supervision_period(
            supervision_sentences,
            incarceration_sentences,
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_reports,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            incarceration_period.admission_date,
            case_compliances=_generate_case_compliances(
                start_date=supervision_period.start_date,
                supervision_period=supervision_period,
            ),
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(
                    supervision_period, start_date=incarceration_period.release_date
                ),
                supervision_period_supervision_type,
                case_compliances=_generate_case_compliances(
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_time_buckets_for_supervision_period_ends_on_first(self):
        """Tests the find_time_buckets_for_supervision_period function for a
        supervision period with no incarceration periods, where the supervision
        period ends on the 1st day of a month."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2001, 1, 5),
            termination_date=date(2001, 7, 1),
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2000, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            completion_date=date(2001, 7, 1),
            supervision_periods=[supervision_period],
        )

        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=[])
        supervision_period_index = SupervisionPeriodIndex(
            supervision_periods=[supervision_period]
        )

        assessments = []

        violation_reports = []
        supervision_contacts = []
        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]

        supervision_time_buckets = identifier.find_time_buckets_for_supervision_period(
            supervision_sentences,
            incarceration_sentences,
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_reports,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
        )

        self.assertEqual(
            expected_buckets[-1],
            NonRevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2001,
                month=6,
                event_date=date(2001, 6, 30),
                supervision_type=supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=True,
            ),
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_time_buckets_for_supervision_period_ends_on_last(self):
        """Tests the find_time_buckets_for_supervision_period function for a
        supervision period with no incarceration periods, where the supervision
        period ends on the last day of a month."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2001, 1, 5),
            termination_date=date(2001, 6, 30),
            supervision_type=StateSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2000, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            completion_date=date(2001, 6, 30),
            supervision_periods=[supervision_period],
        )

        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=[])
        supervision_period_index = SupervisionPeriodIndex(
            supervision_periods=[supervision_period]
        )

        assessments = []

        violation_reports = []
        supervision_contacts = []
        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]

        supervision_time_buckets = identifier.find_time_buckets_for_supervision_period(
            supervision_sentences,
            incarceration_sentences,
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_reports,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
        )

        self.assertEqual(
            expected_buckets[-1],
            NonRevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=2001,
                month=6,
                event_date=date(2001, 6, 29),
                supervision_type=supervision_period_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                is_on_supervision_last_day_of_month=False,
                supervision_level=supervision_period.supervision_level,
            ),
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_time_buckets_for_supervision_period_start_end_same_day(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2001, 3, 3),
            termination_date=date(2001, 3, 3),
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2000, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            completion_date=date(2001, 3, 3),
            supervision_periods=[supervision_period],
        )

        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=[])
        supervision_period_index = SupervisionPeriodIndex(
            supervision_periods=[supervision_period]
        )

        assessments = []

        violation_reports = []
        supervision_contacts = []
        incarceration_sentences = []
        supervision_sentences = [supervision_sentence]

        supervision_time_buckets = identifier.find_time_buckets_for_supervision_period(
            supervision_sentences,
            incarceration_sentences,
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_reports,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        self.assertCountEqual(supervision_time_buckets, [])

    def test_find_time_buckets_for_supervision_period_multiple_assessments(self):
        """Tests the find_time_buckets_for_supervision_period function
        when there is an incarceration period with a revocation admission,
        a stay in prison, a continued supervision period after release
        from incarceration, and multiple assessments over this time period."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 11),
            termination_date=date(2018, 12, 10),
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2018, 10, 27),
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=24,
            assessment_level=StateAssessmentLevel.MEDIUM,
            assessment_date=date(2018, 10, 27),
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            completion_date=date(2018, 12, 10),
            supervision_periods=[supervision_period],
        )

        incarceration_period_index = IncarcerationPeriodIndex(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = SupervisionPeriodIndex(
            supervision_periods=[supervision_period]
        )

        assessments = [assessment_1, assessment_2]

        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_time_buckets_for_supervision_period(
            [supervision_sentence],
            incarceration_sentences,
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            end_date=incarceration_period.admission_date,
            assessment_score=assessment_1.assessment_score,
            assessment_level=assessment_1.assessment_level,
            assessment_type=assessment_1.assessment_type,
        )

        expected_buckets.extend(
            expected_non_revocation_return_time_buckets(
                attr.evolve(
                    supervision_period, start_date=incarceration_period.release_date
                ),
                supervision_period_supervision_type,
                assessment_score=assessment_2.assessment_score,
                assessment_level=assessment_2.assessment_level,
                assessment_type=assessment_2.assessment_type,
            )
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_time_buckets_for_supervision_period_assessment_year_before(self):
        """Tests the find_time_buckets_for_supervision_period function
        when there is no revocation and the assessment date is the year before
        the start of the supervision."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 3, 19),
            supervision_type=StateSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_CA",
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=24,
            assessment_date=date(2017, 12, 17),
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionType.PROBATION,
            completion_date=date(2018, 3, 19),
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period],
        )

        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=[])
        supervision_period_index = SupervisionPeriodIndex(
            supervision_periods=[supervision_period]
        )

        assessments = [assessment]
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_time_buckets_for_supervision_period(
            [supervision_sentence],
            incarceration_sentences,
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = expected_non_revocation_return_time_buckets(
            supervision_period,
            supervision_period_supervision_type,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type,
            case_compliances=_generate_case_compliances(
                start_date=supervision_period.start_date,
                supervision_period=supervision_period,
                assessments=assessments,
            ),
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_time_buckets_for_supervision_period_with_a_supervision_downgrade(
        self,
    ):
        """Tests the find_time_buckets_for_supervision_period function
        when a supervision level downgrade has taken place."""

        supervision_periods = [
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 3, 19),
                supervision_type=StateSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.HIGH,
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            ),
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=123,
                external_id="sp2",
                state_code="US_XX",
                start_date=date(2018, 3, 19),
                termination_date=date(2018, 4, 30),
                supervision_type=StateSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MEDIUM,
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            ),
        ]

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionType.PROBATION,
            completion_date=date(2018, 4, 30),
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=supervision_periods,
        )

        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=[])
        supervision_period_index = SupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_time_buckets_for_supervision_period(
            [supervision_sentence],
            incarceration_sentences,
            supervision_periods[1],
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = expected_non_revocation_return_time_buckets(
            supervision_periods[1],
            supervision_period_supervision_type,
            supervision_downgrade_date=date(2018, 3, 19),
            supervision_downgrade_occurred=True,
            previous_supervision_level=StateSupervisionLevel.HIGH,
            case_compliances=_generate_case_compliances(
                start_date=supervision_periods[0].start_date,
                supervision_period=supervision_periods[1],
                num_days_assessment_overdue_increment=0,
            ),
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)

    def test_find_time_buckets_for_supervision_period_without_a_supervision_downgrade_out_of_time_frame(
        self,
    ):
        """Tests the find_time_buckets_for_supervision_period function
        when a supervision level downgrade has taken place."""

        supervision_periods = [
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                start_date=date(2010, 1, 5),
                termination_date=date(2010, 3, 19),
                supervision_type=StateSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MAXIMUM,
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            ),
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=123,
                external_id="sp2",
                state_code="US_XX",
                start_date=date(2018, 3, 19),
                termination_date=date(2018, 4, 30),
                supervision_type=StateSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MEDIUM,
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            ),
        ]

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionType.PROBATION,
            completion_date=date(2018, 4, 30),
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=supervision_periods,
        )

        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods=[])
        supervision_period_index = SupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        assessments = []
        violation_responses = []
        supervision_contacts = []
        incarceration_sentences = []

        supervision_time_buckets = identifier.find_time_buckets_for_supervision_period(
            [supervision_sentence],
            incarceration_sentences,
            supervision_periods[1],
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_buckets = expected_non_revocation_return_time_buckets(
            supervision_periods[1],
            supervision_period_supervision_type,
            case_compliances=_generate_case_compliances(
                start_date=supervision_periods[0].start_date,
                supervision_period=supervision_periods[1],
                num_days_assessment_overdue_increment=0,
            ),
        )

        self.assertCountEqual(expected_buckets, supervision_time_buckets)


class TestClassifySupervisionSuccess(unittest.TestCase):
    """Tests the classify_supervision_success function."""

    def setUp(self):
        self.default_supervision_period_to_judicial_district_associations = {
            _DEFAULT_SUPERVISION_PERIOD_ID: DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST[
                0
            ]
        }

    def test_classify_supervision_success(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PAROLE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            projected_completion_date=date(2018, 12, 25),
            completion_date=date(2018, 12, 25),
            supervision_periods=[supervision_period],
        )

        supervision_sentences = [supervision_sentence]

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex([]),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(1, len(projected_completion_buckets))

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PAROLE
        )

        self.assertEqual(
            [
                ProjectedSupervisionCompletionBucket(
                    state_code=supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        supervision_sentence.projected_completion_date
                    ),
                    supervision_type=supervision_period_supervision_type,
                    successful_completion=True,
                    incarcerated_during_sentence=False,
                    case_type=StateSupervisionCaseType.GENERAL,
                    sentence_days_served=(
                        supervision_sentence.completion_date
                        - supervision_sentence.start_date
                    ).days,
                )
            ],
            projected_completion_buckets,
        )

    def test_classify_supervision_success_unsuccessful(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 6, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_date=date(2018, 6, 21),
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            projected_completion_date=date(2018, 12, 25),
            completion_date=date(2018, 12, 25),
            supervision_periods=[supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_periods = [incarceration_period]

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex(incarceration_periods),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(1, len(projected_completion_buckets))

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PAROLE
        )

        self.assertEqual(
            [
                ProjectedSupervisionCompletionBucket(
                    state_code=supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        supervision_sentence.projected_completion_date
                    ),
                    supervision_type=supervision_period_supervision_type,
                    successful_completion=False,
                    incarcerated_during_sentence=True,
                    case_type=StateSupervisionCaseType.GENERAL,
                    sentence_days_served=(
                        supervision_sentence.completion_date
                        - supervision_sentence.start_date
                    ).days,
                )
            ],
            projected_completion_buckets,
        )

    def test_classify_supervision_success_multiple_periods(self):
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 8, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PAROLE,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 9, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            projected_completion_date=date(2018, 12, 25),
            completion_date=date(2018, 12, 25),
            supervision_periods=[first_supervision_period, second_supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_periods = []

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex(incarceration_periods),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(1, len(projected_completion_buckets))

        second_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PAROLE
        )

        self.assertEqual(
            [
                ProjectedSupervisionCompletionBucket(
                    state_code=second_supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        supervision_sentence.projected_completion_date
                    ),
                    supervision_type=second_supervision_period_supervision_type,
                    case_type=StateSupervisionCaseType.GENERAL,
                    successful_completion=False,
                    incarcerated_during_sentence=False,
                    sentence_days_served=(
                        supervision_sentence.completion_date
                        - supervision_sentence.start_date
                    ).days,
                )
            ],
            projected_completion_buckets,
        )

    def test_classify_supervision_success_multiple_sentences(self):
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 8, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PAROLE,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 9, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        first_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            projected_completion_date=date(2018, 12, 25),
            completion_date=date(2018, 12, 25),
            supervision_periods=[first_supervision_period],
        )

        second_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss2",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            projected_completion_date=date(2018, 12, 25),
            completion_date=date(2018, 12, 25),
            supervision_periods=[second_supervision_period],
        )

        supervision_sentences = [
            first_supervision_sentence,
            second_supervision_sentence,
        ]
        incarceration_periods = []

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex(incarceration_periods),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(2, len(projected_completion_buckets))

        first_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PAROLE
        )
        second_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PAROLE
        )

        self.assertEqual(
            [
                ProjectedSupervisionCompletionBucket(
                    state_code=first_supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        first_supervision_sentence.projected_completion_date
                    ),
                    supervision_type=first_supervision_period_supervision_type,
                    successful_completion=True,
                    incarcerated_during_sentence=False,
                    case_type=StateSupervisionCaseType.GENERAL,
                    sentence_days_served=(
                        first_supervision_sentence.completion_date
                        - first_supervision_sentence.start_date
                    ).days,
                ),
                ProjectedSupervisionCompletionBucket(
                    state_code=second_supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        second_supervision_sentence.projected_completion_date
                    ),
                    supervision_type=second_supervision_period_supervision_type,
                    case_type=StateSupervisionCaseType.GENERAL,
                    successful_completion=False,
                    incarcerated_during_sentence=False,
                    sentence_days_served=(
                        second_supervision_sentence.completion_date
                        - second_supervision_sentence.start_date
                    ).days,
                ),
            ],
            projected_completion_buckets,
        )

    def test_classify_supervision_success_multiple_sentence_types(self):
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 8, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PAROLE,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 9, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
        )

        first_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            projected_completion_date=date(2018, 12, 25),
            completion_date=date(2018, 12, 25),
            supervision_periods=[first_supervision_period],
        )

        second_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss2",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            projected_completion_date=date(2018, 12, 25),
            completion_date=date(2018, 12, 25),
            supervision_periods=[second_supervision_period],
        )

        supervision_sentences = [
            first_supervision_sentence,
            second_supervision_sentence,
        ]
        incarceration_periods = []

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex(incarceration_periods),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(2, len(projected_completion_buckets))

        first_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PAROLE
        )
        second_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        self.assertEqual(
            [
                ProjectedSupervisionCompletionBucket(
                    state_code=first_supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        first_supervision_sentence.projected_completion_date
                    ),
                    supervision_type=first_supervision_period_supervision_type,
                    case_type=StateSupervisionCaseType.GENERAL,
                    successful_completion=True,
                    incarcerated_during_sentence=False,
                    sentence_days_served=(
                        first_supervision_sentence.completion_date
                        - first_supervision_sentence.start_date
                    ).days,
                ),
                ProjectedSupervisionCompletionBucket(
                    state_code=second_supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        second_supervision_sentence.projected_completion_date
                    ),
                    supervision_type=second_supervision_period_supervision_type,
                    case_type=StateSupervisionCaseType.GENERAL,
                    successful_completion=False,
                    incarcerated_during_sentence=False,
                    sentence_days_served=(
                        second_supervision_sentence.completion_date
                        - second_supervision_sentence.start_date
                    ).days,
                ),
            ],
            projected_completion_buckets,
        )

    def test_classify_supervision_success_officer_district(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PAROLE,
            supervision_site="DISTRICTX",
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            projected_completion_date=date(2018, 12, 25),
            completion_date=date(2018, 12, 25),
            supervision_periods=[supervision_period],
        )

        supervision_period_agent_association = {
            111: {
                "agent_id": 000,
                "agent_external_id": "AGENTX",
                "supervision_period_id": 111,
            }
        }

        supervision_period_to_judicial_district_associations = {
            111: {"supervision_period_id": 111, "judicial_district_code": "NORTHWEST"}
        }

        supervision_sentences = [supervision_sentence]
        incarceration_periods = []

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex(incarceration_periods),
            supervision_period_agent_association,
            supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(1, len(projected_completion_buckets))

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PAROLE
        )

        self.assertEqual(
            [
                ProjectedSupervisionCompletionBucket(
                    state_code=supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        supervision_sentence.projected_completion_date
                    ),
                    supervision_type=supervision_period_supervision_type,
                    case_type=StateSupervisionCaseType.GENERAL,
                    successful_completion=True,
                    incarcerated_during_sentence=False,
                    supervising_officer_external_id="AGENTX",
                    supervising_district_external_id="DISTRICTX",
                    level_1_supervision_location_external_id="DISTRICTX",
                    judicial_district_code="NORTHWEST",
                    sentence_days_served=(
                        supervision_sentence.completion_date
                        - supervision_sentence.start_date
                    ).days,
                )
            ],
            projected_completion_buckets,
        )

    def test_classify_supervision_success_empty_officer_district(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PAROLE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            projected_completion_date=date(2018, 12, 25),
            completion_date=date(2018, 12, 25),
            supervision_periods=[supervision_period],
        )

        supervision_period_agent_association = {
            111: {
                "agent_id": None,
                "agent_external_id": None,
                "supervision_period_id": 111,
            }
        }

        supervision_period_to_judicial_district_associations = {
            111: {"supervision_period_id": 111, "judicial_district_code": None}
        }

        supervision_sentences = [supervision_sentence]
        incarceration_periods = []

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex(incarceration_periods),
            supervision_period_agent_association,
            supervision_period_to_judicial_district_associations,
        )
        self.assertEqual(1, len(projected_completion_buckets))

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PAROLE
        )

        self.assertEqual(
            [
                ProjectedSupervisionCompletionBucket(
                    state_code=supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        supervision_sentence.projected_completion_date
                    ),
                    supervision_type=supervision_period_supervision_type,
                    case_type=StateSupervisionCaseType.GENERAL,
                    successful_completion=True,
                    incarcerated_during_sentence=False,
                    sentence_days_served=(
                        supervision_sentence.completion_date
                        - supervision_sentence.start_date
                    ).days,
                    supervising_officer_external_id=None,
                    supervising_district_external_id=None,
                    judicial_district_code=None,
                )
            ],
            projected_completion_buckets,
        )

    def test_classify_supervision_success_period_ends_after_projected_completion(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PAROLE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            projected_completion_date=date(2018, 12, 25),
            completion_date=date(2018, 10, 1),
            supervision_periods=[supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_periods = []

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex(incarceration_periods),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(1, len(projected_completion_buckets))

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PAROLE
        )

        self.assertEqual(
            [
                ProjectedSupervisionCompletionBucket(
                    state_code=supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        supervision_sentence.projected_completion_date
                    ),
                    supervision_type=supervision_period_supervision_type,
                    successful_completion=True,
                    incarcerated_during_sentence=False,
                    case_type=StateSupervisionCaseType.GENERAL,
                    sentence_days_served=(
                        supervision_sentence.completion_date
                        - supervision_sentence.start_date
                    ).days,
                )
            ],
            projected_completion_buckets,
        )

    def test_classify_supervision_success_exclude_termination_reason_death(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DEATH,
            supervision_type=StateSupervisionType.PAROLE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            projected_completion_date=date(2018, 12, 25),
            completion_date=date(2018, 12, 25),
            supervision_periods=[supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_periods = []
        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods)

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            incarceration_period_index,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(0, len(projected_completion_buckets))

    def test_classify_supervision_success_exclude_no_completion_date(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.SUSPENSION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            projected_completion_date=date(2018, 12, 25),
            supervision_periods=[supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_periods = []
        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods)

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            incarceration_period_index,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(0, len(projected_completion_buckets))

    def test_classify_supervision_success_exclude_completion_before_start(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.SUSPENSION,
            supervision_type=StateSupervisionType.PAROLE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            completion_date=date(2016, 10, 3),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            projected_completion_date=date(2018, 12, 25),
            supervision_periods=[supervision_period],
        )

        supervision_sentences = [supervision_sentence]
        incarceration_periods = []
        incarceration_period_index = IncarcerationPeriodIndex(incarceration_periods)

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            incarceration_period_index,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(0, len(projected_completion_buckets))

    def test_classify_supervision_success_was_incarcerated_during_sentence(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PAROLE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PAROLE,
            completion_date=date(2018, 12, 20),
            projected_completion_date=date(2018, 12, 25),
            supervision_periods=[supervision_period],
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=111,
            admission_date=date(2017, 6, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_date=date(2017, 10, 5),
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentences = [supervision_sentence]
        incarceration_periods = [incarceration_period]

        projected_completion_buckets = identifier.classify_supervision_success(
            supervision_sentences,
            IncarcerationPeriodIndex(incarceration_periods),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(1, len(projected_completion_buckets))

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PAROLE
        )

        sentence_length = (
            supervision_sentence.completion_date - supervision_sentence.start_date
        ).days

        self.assertEqual(
            [
                ProjectedSupervisionCompletionBucket(
                    state_code=supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        supervision_sentence.projected_completion_date
                    ),
                    supervision_type=supervision_period_supervision_type,
                    successful_completion=True,
                    incarcerated_during_sentence=True,
                    case_type=StateSupervisionCaseType.GENERAL,
                    sentence_days_served=sentence_length,
                )
            ],
            projected_completion_buckets,
        )


class TestFindSupervisionTerminationBucket(unittest.TestCase):
    """Tests the find_supervision_termination_bucket function."""

    def test_find_supervision_termination_bucket(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2019, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION,
        )

        first_assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2018, 3, 10),
        )

        first_reassessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2018, 5, 18),
        )

        last_assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=19,
            assessment_date=date(2019, 5, 10),
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            completion_date=date(2019, 5, 19),
            supervision_periods=[supervision_period],
        )

        assessments = [first_assessment, first_reassessment, last_assessment]

        supervision_period_index = SupervisionPeriodIndex(
            supervision_periods=[supervision_period]
        )

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
        )

        assessment_score_change = (
            last_assessment.assessment_score - first_reassessment.assessment_score
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_termination_bucket = SupervisionTerminationBucket(
            state_code=supervision_period.state_code,
            year=supervision_period.termination_date.year,
            month=supervision_period.termination_date.month,
            event_date=supervision_period.termination_date,
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
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2019, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            completion_date=date(2019, 5, 19),
            supervision_periods=[supervision_period],
        )

        assessments = []

        supervision_period_index = SupervisionPeriodIndex(
            supervision_periods=[supervision_period]
        )

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
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_termination_bucket = SupervisionTerminationBucket(
            state_code=supervision_period.state_code,
            year=supervision_period.termination_date.year,
            month=supervision_period.termination_date.month,
            event_date=supervision_period.termination_date,
            supervision_type=supervision_period_supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            assessment_score=None,
            assessment_type=None,
            termination_reason=supervision_period.termination_reason,
            assessment_score_change=None,
        )

        self.assertEqual(expected_termination_bucket, termination_bucket)

    def test_find_supervision_termination_bucket_insufficient_assessments(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2019, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION,
        )

        first_assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2018, 3, 10),
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            completion_date=date(2019, 5, 19),
            supervision_periods=[supervision_period],
        )

        assessments = [first_assessment]

        supervision_period_index = SupervisionPeriodIndex(
            supervision_periods=[supervision_period]
        )

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
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_termination_bucket = SupervisionTerminationBucket(
            state_code=supervision_period.state_code,
            year=supervision_period.termination_date.year,
            month=supervision_period.termination_date.month,
            event_date=supervision_period.termination_date,
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
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_periods=[supervision_period],
        )

        assessments = []

        supervision_period_index = SupervisionPeriodIndex(
            supervision_periods=[supervision_period]
        )

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
        )

        self.assertEqual(None, termination_bucket)

    def test_find_supervision_termination_bucket_multiple_in_month(self):
        """Tests that when multiple supervision periods end in the same month, the earliest start_date and the latest
        termination_date are used as the date boundaries for the assessments, but the termination_date on the
        supervision_period is still on the SupervisionTerminationBucket."""
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2019, 11, 17),
            supervision_type=StateSupervisionType.PROBATION,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp2",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 1, 1),
            termination_date=date(2019, 11, 23),
            supervision_type=StateSupervisionType.PROBATION,
        )

        too_early_assessment = StateAssessment.new_with_defaults(
            state_code="US_CA",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=38,
            assessment_date=date(2017, 12, 10),
        )

        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_CA",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2018, 1, 10),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_CA",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2018, 5, 18),
        )

        assessment_3 = StateAssessment.new_with_defaults(
            state_code="US_CA",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=19,
            assessment_date=date(2019, 11, 21),
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            completion_date=date(2019, 12, 23),
            supervision_periods=[first_supervision_period, second_supervision_period],
        )

        assessments = [too_early_assessment, assessment_1, assessment_2, assessment_3]

        supervision_period_index = SupervisionPeriodIndex(
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
        )

        first_supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_termination_bucket = SupervisionTerminationBucket(
            state_code=first_supervision_period.state_code,
            year=first_supervision_period.termination_date.year,
            month=first_supervision_period.termination_date.month,
            event_date=first_supervision_period.termination_date,
            supervision_type=first_supervision_period_supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            assessment_score=assessment_3.assessment_score,
            assessment_type=assessment_3.assessment_type,
            termination_reason=first_supervision_period.termination_reason,
            assessment_score_change=(
                assessment_3.assessment_score - assessment_2.assessment_score
            ),
        )

        self.assertEqual(expected_termination_bucket, termination_bucket)

    def test_find_supervision_termination_bucket_incarceration_overlaps_full_supervision_period(
        self,
    ):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            completion_date=date(2019, 12, 23),
            supervision_periods=[supervision_period],
        )

        supervision_period_index = SupervisionPeriodIndex(
            supervision_periods=[supervision_period]
        )

        termination_bucket = identifier.find_supervision_termination_bucket(
            supervision_sentences=[supervision_sentence],
            incarceration_sentences=[],
            supervision_period=supervision_period,
            supervision_period_index=supervision_period_index,
            assessments=[],
            violation_responses_for_history=[],
            supervision_period_to_agent_associations=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        expected_termination_bucket = SupervisionTerminationBucket(
            state_code=supervision_period.state_code,
            year=supervision_period.termination_date.year,
            month=supervision_period.termination_date.month,
            event_date=supervision_period.termination_date,
            supervision_type=supervision_period_supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            termination_reason=supervision_period.termination_reason,
        )

        self.assertEqual(expected_termination_bucket, termination_bucket)


class TestGetMostSevereResponseDecision(unittest.TestCase):
    """Tests the _get_most_severe_response_decision function."""

    def test_get_most_severe_response_decision(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_XX",
            violation_date=date(2009, 1, 3),
        )

        supervision_violation_response = StateSupervisionViolationResponse.new_with_defaults(
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_XX",
            response_date=date(2009, 1, 7),
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    revocation_type=StateSupervisionViolationResponseRevocationType.REINCARCERATION,
                ),
                StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    revocation_type=StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        most_severe_response_decision = recidiviz.calculator.pipeline.utils.violation_response_utils.get_most_severe_response_decision(
            [supervision_violation_response]
        )

        self.assertEqual(
            StateSupervisionViolationResponseDecision.REVOCATION,
            most_severe_response_decision,
        )

    def test_get_most_severe_response_decision_no_responses(self):
        most_severe_response_decision = recidiviz.calculator.pipeline.utils.violation_response_utils.get_most_severe_response_decision(
            []
        )

        self.assertIsNone(most_severe_response_decision)


class TestIdentifyMostSevereCaseType(unittest.TestCase):
    """Tests the _identify_most_severe_case_type function."""

    def test_identify_most_severe_case_type(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                ),
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX", case_type=StateSupervisionCaseType.SEX_OFFENSE
                ),
            ],
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        most_severe_case_type = recidiviz.calculator.pipeline.utils.supervision_period_utils.identify_most_severe_case_type(
            supervision_period
        )

        self.assertEqual(most_severe_case_type, StateSupervisionCaseType.SEX_OFFENSE)

    def test_identify_most_severe_case_type_test_all_types(self):
        for case_type in StateSupervisionCaseType:
            supervision_period = StateSupervisionPeriod.new_with_defaults(
                state_code="US_XX",
                case_type_entries=[
                    StateSupervisionCaseTypeEntry.new_with_defaults(
                        state_code="US_XX", case_type=case_type
                    ),
                ],
                status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
            )

            most_severe_case_type = recidiviz.calculator.pipeline.utils.supervision_period_utils.identify_most_severe_case_type(
                supervision_period
            )

            self.assertEqual(most_severe_case_type, case_type)

    def test_identify_most_severe_case_type_no_type_entries(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            case_type_entries=[],
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        most_severe_case_type = recidiviz.calculator.pipeline.utils.supervision_period_utils.identify_most_severe_case_type(
            supervision_period
        )

        self.assertEqual(most_severe_case_type, StateSupervisionCaseType.GENERAL)


class TestConvertBucketsToDual(unittest.TestCase):
    """Tests the _convert_buckets_to_dual function."""

    def test_convert_buckets_to_dual_us_mo(self):
        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False,
            ),
        ]

        updated_buckets = identifier._convert_buckets_to_dual(
            supervision_time_buckets=supervision_time_buckets
        )

        expected_output = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
            ),
        ]

        self.assertCountEqual(updated_buckets, expected_output)

    def test_convert_buckets_to_dual_us_mo_one_dual(self):
        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False,
            ),
        ]

        updated_buckets = identifier._convert_buckets_to_dual(
            supervision_time_buckets=supervision_time_buckets
        )

        expected_output = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
            ),
        ]

        self.assertCountEqual(updated_buckets, expected_output)

    def test_convert_buckets_to_dual_us_mo_one_other_day(self):
        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 4),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
        ]

        updated_buckets = identifier._convert_buckets_to_dual(
            supervision_time_buckets=supervision_time_buckets
        )

        expected_output = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 4),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
        ]

        self.assertCountEqual(updated_buckets, expected_output)

    def test_convert_buckets_to_dual_us_mo_one_different_type(self):
        supervision_time_buckets = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=True,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=True,
                projected_end_date=None,
            ),
            ProjectedSupervisionCompletionBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=True,
            ),
        ]

        updated_buckets = identifier._convert_buckets_to_dual(
            supervision_time_buckets=supervision_time_buckets
        )

        expected_output = [
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=True,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=True,
                projected_end_date=None,
            ),
            ProjectedSupervisionCompletionBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=True,
            ),
        ]

        self.assertCountEqual(updated_buckets, expected_output)

    def test_convert_buckets_to_dual_us_mo_different_bucket_types_same_metric(self):
        supervision_time_buckets = [
            RevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
        ]

        updated_buckets = identifier._convert_buckets_to_dual(
            supervision_time_buckets=supervision_time_buckets
        )

        expected_output = [
            RevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
            NonRevocationReturnSupervisionTimeBucket(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                is_on_supervision_last_day_of_month=False,
                projected_end_date=None,
            ),
        ]

        self.assertCountEqual(updated_buckets, expected_output)

    def test_convert_buckets_to_dual_empty_input(self):
        supervision_time_buckets = []

        updated_buckets = identifier._convert_buckets_to_dual(
            supervision_time_buckets=supervision_time_buckets
        )

        expected_output = []

        self.assertCountEqual(updated_buckets, expected_output)

    def test_convert_buckets_to_dual_metric_type_coverage(self):
        """Asserts all SupervisionMetricTypes are handled in the function."""
        for metric_type in SupervisionMetricType:
            self.assertTrue(metric_type in identifier.BUCKET_TYPES_FOR_METRIC.keys())


class TestIncludeTerminationInSuccessMetric(unittest.TestCase):
    """Tests for _include_termination_in_success_metric function."""

    def test_include_termination_in_success_metric(self):
        for termination_reason in StateSupervisionPeriodTerminationReason:
            _ = identifier._include_termination_in_success_metric(termination_reason)


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
    most_severe_response_decision: Optional[
        StateSupervisionViolationResponseDecision
    ] = None,
    response_count: Optional[int] = 0,
    supervising_officer_external_id: Optional[str] = None,
    level_1_supervision_location_external_id: Optional[str] = None,
    level_2_supervision_location_external_id: Optional[str] = None,
    case_compliances: Dict[date, SupervisionCaseCompliance] = None,
    judicial_district_code: Optional[str] = None,
    supervision_downgrade_date: Optional[date] = None,
    supervision_downgrade_occurred: Optional[bool] = False,
    previous_supervision_level: Optional[StateSupervisionLevel] = None,
    projected_supervision_completion_date: Optional[date] = None,
) -> List[NonRevocationReturnSupervisionTimeBucket]:
    """Returns the expected NonRevocationReturnSupervisionTimeBuckets based on the provided |supervision_period|
    and when the buckets should end."""

    expected_buckets = []

    if not case_compliances:
        case_compliances = defaultdict()

    if not end_date:
        end_date = (
            supervision_period.termination_date
            if supervision_period.termination_date
            else date.today() + relativedelta(days=1)
        )

    start_date = supervision_period.start_date

    if start_date:
        days_on_supervision = [
            start_date + relativedelta(days=x)
            for x in range((end_date - start_date).days)
        ]

        if days_on_supervision:
            # Ensuring we're not counting the end date
            assert max(days_on_supervision) < end_date

        for day_on_supervision in days_on_supervision:
            is_on_supervision_last_day_of_month = (
                day_on_supervision == last_day_of_month(day_on_supervision)
            )

            case_compliance = case_compliances.get(day_on_supervision)

            downgrade_occurred = (
                supervision_downgrade_occurred
                if (
                    supervision_downgrade_occurred is not None
                    and supervision_downgrade_date
                    and supervision_downgrade_date == day_on_supervision
                )
                else False
            )
            previous_level = previous_supervision_level if downgrade_occurred else None

            bucket = NonRevocationReturnSupervisionTimeBucket(
                state_code=supervision_period.state_code,
                year=day_on_supervision.year,
                month=day_on_supervision.month,
                event_date=day_on_supervision,
                supervision_type=supervision_type,
                case_type=case_type,
                assessment_score=assessment_score,
                assessment_level=assessment_level,
                assessment_type=assessment_type,
                most_severe_violation_type=most_severe_violation_type,
                most_severe_violation_type_subtype=most_severe_violation_type_subtype,
                most_severe_response_decision=most_severe_response_decision,
                response_count=response_count,
                supervising_officer_external_id=supervising_officer_external_id,
                supervising_district_external_id=(
                    level_2_supervision_location_external_id
                    or level_1_supervision_location_external_id
                ),
                level_1_supervision_location_external_id=level_1_supervision_location_external_id,
                level_2_supervision_location_external_id=level_2_supervision_location_external_id,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                is_on_supervision_last_day_of_month=is_on_supervision_last_day_of_month,
                case_compliance=case_compliance,
                judicial_district_code=judicial_district_code,
                supervision_level_downgrade_occurred=downgrade_occurred,
                previous_supervision_level=previous_level,
                projected_end_date=projected_supervision_completion_date,
            )

            expected_buckets.append(bucket)

    return expected_buckets


class TestSupervisionLevelDowngradeOccurred(unittest.TestCase):
    """Tests the _supervision_level_downgrade_occurred function."""

    def test_supervision_level_downgrade_occurred_covers_all_supervision_levels(self):
        """Ensures that all values in StateSupervisionLevel are covered by _supervision_level_downgrade_occurred."""

        for level in StateSupervisionLevel:
            identifier._supervision_level_downgrade_occurred(
                level, StateSupervisionLevel.MAXIMUM
            )


class TestFindAssessmentScoreChange(unittest.TestCase):
    """Tests the find_assessment_score_change function."""

    def test_find_assessment_score_change(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_score=33,
            assessment_date=date(2015, 3, 23),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2015, 11, 2),
        )

        assessment_3 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=23,
            assessment_level=StateAssessmentLevel.MEDIUM,
            assessment_date=date(2016, 1, 13),
        )

        assessments = [assessment_1, assessment_2, assessment_3]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        (
            assessment_score_change,
            end_assessment_score,
            end_assessment_level,
            end_assessment_type,
        ) = identifier.find_assessment_score_change(
            assessment_1.state_code, start_date, termination_date, assessments
        )

        self.assertEqual(-6, assessment_score_change)
        self.assertEqual(assessment_3.assessment_score, end_assessment_score)
        self.assertEqual(assessment_3.assessment_level, end_assessment_level)
        self.assertEqual(assessment_3.assessment_type, end_assessment_type)

    def test_find_assessment_score_change_insufficient_assessments(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2015, 3, 23),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2015, 11, 2),
        )

        assessments = [assessment_1, assessment_2]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        (
            assessment_score_change,
            end_assessment_score,
            end_assessment_level,
            end_assessment_type,
        ) = identifier.find_assessment_score_change(
            assessment_1.state_code, start_date, termination_date, assessments
        )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)

    #
    @mock.patch(
        "recidiviz.calculator.pipeline.supervision.identifier.second_assessment_on_supervision_is_more_reliable"
    )
    def test_find_assessment_score_change_first_reliable_assessment_is_first_assessment(
        self, mock_reliable_assessment
    ):
        mock_reliable_assessment.return_value = False

        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2015, 3, 23),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2015, 11, 2),
        )

        assessments = [assessment_1, assessment_2]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        (
            assessment_score_change,
            end_assessment_score,
            end_assessment_level,
            end_assessment_type,
        ) = identifier.find_assessment_score_change(
            assessment_1.state_code, start_date, termination_date, assessments
        )

        self.assertEqual(-4, assessment_score_change)
        self.assertEqual(assessment_2.assessment_score, end_assessment_score)
        self.assertEqual(assessment_2.assessment_level, end_assessment_level)
        self.assertEqual(assessment_2.assessment_type, end_assessment_type)

    def test_find_assessment_score_change_different_type(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2015, 3, 23),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2015, 11, 2),
        )

        assessment_3 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=23,
            assessment_date=date(2016, 1, 13),
        )

        assessments = [assessment_1, assessment_2, assessment_3]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        (
            assessment_score_change,
            end_assessment_score,
            end_assessment_level,
            end_assessment_type,
        ) = identifier.find_assessment_score_change(
            assessment_1.state_code, start_date, termination_date, assessments
        )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)

    def test_find_assessment_score_change_same_date(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2015, 3, 23),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2015, 11, 2),
        )

        assessment_3 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=23,
            assessment_date=date(2016, 11, 2),
        )

        assessments = [assessment_1, assessment_2, assessment_3]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        (
            assessment_score_change,
            end_assessment_score,
            end_assessment_level,
            end_assessment_type,
        ) = identifier.find_assessment_score_change(
            assessment_1.state_code, start_date, termination_date, assessments
        )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)

    def test_find_assessment_score_change_no_assessments(self):
        assessments = []

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        (
            assessment_score_change,
            end_assessment_score,
            end_assessment_level,
            end_assessment_type,
        ) = identifier.find_assessment_score_change(
            "US_XX", start_date, termination_date, assessments
        )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)

    def test_find_assessment_score_change_outside_boundary(self):
        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=33,
            assessment_date=date(2011, 3, 23),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=29,
            assessment_date=date(2015, 11, 2),
        )

        assessment_3 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS,
            assessment_score=23,
            assessment_date=date(2016, 1, 13),
        )

        assessments = [assessment_1, assessment_2, assessment_3]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        (
            assessment_score_change,
            end_assessment_score,
            end_assessment_level,
            end_assessment_type,
        ) = identifier.find_assessment_score_change(
            assessment_1.state_code, start_date, termination_date, assessments
        )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)


class TestProjectedCompletionDate(unittest.TestCase):
    """Tests for _get_projected_end_date function."""

    def test_get_projected_end_date_supervision_period_not_in_sentence(self):
        """Tests the _get_projected_completion function. The supervision periods extends past
        the supervision sentence projected completion date, but is not associated with the supervision
         sentence. However, it should still be considered past the completion date,"""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            start_date=date(2018, 5, 1),
            # Termination date is after sentence's projected completion date
            termination_date=date(2018, 5, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            start_date=date(2018, 5, 1),
            external_id="ss1",
            state_code="US_XX",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            projected_completion_date=date(2018, 5, 10),
            supervision_periods=[],
        )

        self.assertEqual(
            _get_projected_end_date(
                period=supervision_period,
                supervision_sentences=[supervision_sentence],
                incarceration_sentences=[],
            ),
            supervision_sentence.projected_completion_date,
        )

    def test_get_projected_end_date_no_supervision_sentence(self):
        """Tests the _get_projected_completion function when there is a supervision period and no
        supervision sentence."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            start_date=date(2018, 5, 1),
            termination_date=date(2018, 5, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        self.assertIsNone(
            _get_projected_end_date(
                period=supervision_period,
                supervision_sentences=[],
                incarceration_sentences=[],
            )
        )

    def test_get_projected_end_date_supervision_period_in_supervision_sentence(self):
        """Tests the _get_projected_end_date function. The supervision periods extends past
        the supervision sentence projected completion date, and is associated with the supervision
         sentence."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            start_date=date(2018, 5, 1),
            # Termination date is after sentence's projected completion date
            termination_date=date(2018, 5, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            start_date=date(2018, 5, 1),
            external_id="ss1",
            state_code="US_XX",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            projected_completion_date=date(2018, 5, 10),
            supervision_periods=[supervision_period],
        )

        self.assertEqual(
            _get_projected_end_date(
                period=supervision_period,
                supervision_sentences=[supervision_sentence],
                incarceration_sentences=[],
            ),
            supervision_sentence.projected_completion_date,
        )

    def test_get_projected_end_date_supervision_period_in_incarceration_sentence(self):
        """Tests the _get_projected_end_date function. The supervision periods extends past
        the supervision sentence projected completion date, and is associated with the incarceration
        sentence."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            start_date=date(2018, 5, 1),
            # Termination date is after sentence's projected completion date
            termination_date=date(2018, 5, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=123,
            external_id="is1",
            start_date=date(2018, 5, 1),
            supervision_periods=[supervision_period],
            projected_max_release_date=date(2018, 5, 10),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(
            _get_projected_end_date(
                period=supervision_period,
                incarceration_sentences=[incarceration_sentence],
                supervision_sentences=[],
            ),
            incarceration_sentence.projected_max_release_date,
        )

    def test_get_projected_end_date_supervision_period_in_incarceration_and_supervision_sentence(
        self,
    ):
        """Tests the _get_projected_end_date function. The supervision period is within the bounds of both a
        supervision and incarceration sentence, but it extends past the supervision sentence projected completion date,
        and does not extend past the incarceration sentence's projected release date."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            status=StateSupervisionPeriodStatus.TERMINATED,
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            start_date=date(2018, 5, 1),
            # Termination date is after sentence's projected completion date
            termination_date=date(2018, 5, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            start_date=date(2018, 5, 1),
            external_id="ss1",
            state_code="US_XX",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            projected_completion_date=date(2018, 5, 10),
            supervision_periods=[supervision_period],
        )

        # The supervision period is within the bounds of the incarceration sentence, and because its projected max
        # release date is after the supervision sentence's, it will be used to determine whether the event date is past
        # the projected end date.
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=123,
            external_id="is1",
            start_date=date(2018, 5, 1),
            supervision_periods=[supervision_period],
            projected_max_release_date=date(2018, 5, 20),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(
            _get_projected_end_date(
                period=supervision_period,
                incarceration_sentences=[incarceration_sentence],
                supervision_sentences=[supervision_sentence],
            ),
            incarceration_sentence.projected_max_release_date,
        )


def create_start_bucket_from_period(period, **kwargs):
    (
        _supervising_officer_external_id,
        level_1_supervision_location_external_id,
        level_2_supervision_location_external_id,
    ) = get_supervising_officer_and_location_info_from_supervision_period(
        period, supervision_period_to_agent_associations={}
    )

    deprecated_supervising_district_external_id = (
        level_2_supervision_location_external_id
        or level_1_supervision_location_external_id
    )

    bucket = SupervisionStartBucket(
        state_code=period.state_code,
        year=period.start_date.year,
        month=period.start_date.month,
        event_date=period.start_date,
        supervision_type=period.supervision_period_supervision_type,
        supervision_level_raw_text=period.supervision_level_raw_text,
        supervising_district_external_id=deprecated_supervising_district_external_id,
        level_1_supervision_location_external_id=level_1_supervision_location_external_id,
        level_2_supervision_location_external_id=level_2_supervision_location_external_id,
        supervision_level=period.supervision_level,
        case_type=StateSupervisionCaseType.GENERAL,
        admission_reason=period.admission_reason,
    )
    bucket = attr.evolve(bucket, **kwargs)
    return bucket


def create_termination_bucket_from_period(period, **kwargs):
    (
        _supervising_officer_external_id,
        level_1_supervision_location_external_id,
        level_2_supervision_location_external_id,
    ) = get_supervising_officer_and_location_info_from_supervision_period(
        period, supervision_period_to_agent_associations={}
    )

    deprecated_supervising_district_external_id = (
        level_2_supervision_location_external_id
        or level_1_supervision_location_external_id
    )

    bucket = SupervisionTerminationBucket(
        state_code=period.state_code,
        year=period.termination_date.year,
        month=period.termination_date.month,
        event_date=period.termination_date,
        supervision_type=period.supervision_period_supervision_type,
        supervision_level=period.supervision_level,
        supervision_level_raw_text=period.supervision_level_raw_text,
        supervising_district_external_id=deprecated_supervising_district_external_id,
        level_1_supervision_location_external_id=level_1_supervision_location_external_id,
        level_2_supervision_location_external_id=level_2_supervision_location_external_id,
        case_type=StateSupervisionCaseType.GENERAL,
        termination_reason=period.termination_reason,
    )
    bucket = attr.evolve(bucket, **kwargs)
    return bucket


def _generate_case_compliances(
    start_date: date,
    supervision_period: StateSupervisionPeriod,
    assessments: Optional[List[StateAssessment]] = None,
    face_to_face_contacts: Optional[List[StateSupervisionContact]] = None,
    end_date_override: Optional[date] = None,
    num_days_assessment_overdue_increment: Optional[int] = None,
) -> Dict[date, SupervisionCaseCompliance]:
    """Generates the expected list of supervision case compliances. Because case compliance logic is tested in
    supervision_case_compliance_manager_test and state-specific compliance tests, this function generates expected case
    compliances using the compliance manager."""
    end_date: Optional[date] = (
        supervision_period.termination_date
        if not end_date_override
        else end_date_override
    )

    # If there is no end date for generating a list of case compliances, raise an error.
    if not end_date:
        raise ValueError(
            "Generation of case compliances requires an end date, which is either the supervision period's"
            "termination date or a passed in end date override."
        )

    state_specific_case_compliance_manager: Optional[
        StateSupervisionCaseComplianceManager
    ] = get_state_specific_case_compliance_manager(
        supervision_period,
        StateSupervisionCaseType.GENERAL,
        start_date,
        assessments or [],
        face_to_face_contacts or [],
    )
    # There will only be case compliance output if there's a case compliance manager implemented for the given state.
    if not state_specific_case_compliance_manager:
        return {}

    current_date: date = start_date
    case_compliances: Dict[date, SupervisionCaseCompliance] = {}
    while start_date <= current_date <= end_date:
        case_compliance: Optional[
            SupervisionCaseCompliance
        ] = state_specific_case_compliance_manager.get_case_compliance_on_date(
            current_date
        )

        if num_days_assessment_overdue_increment:
            # If there is value for `num_days_assessment_overdue_increment`, update the case compliance's value to it,
            # and increment it.
            num_days_assessment_overdue: int = num_days_assessment_overdue_increment
            case_compliance = attr.evolve(
                case_compliance, num_days_assessment_overdue=num_days_assessment_overdue
            )
            num_days_assessment_overdue_increment += 1

        if case_compliance:
            case_compliances[current_date] = case_compliance
        current_date += relativedelta(days=1)

    return case_compliances
