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
# pylint: disable=protected-access
"""Tests for supervision/identifier.py."""

import datetime
import unittest
from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Optional
from unittest import mock

import attr
from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

import recidiviz.calculator.pipeline.utils.supervision_period_utils
import recidiviz.calculator.pipeline.utils.violation_response_utils
import recidiviz.calculator.pipeline.utils.violation_utils
from recidiviz.calculator.pipeline.metrics.supervision import identifier
from recidiviz.calculator.pipeline.metrics.supervision.events import (
    ProjectedSupervisionCompletionEvent,
    SupervisionEvent,
    SupervisionPopulationEvent,
    SupervisionStartEvent,
    SupervisionTerminationEvent,
)
from recidiviz.calculator.pipeline.metrics.supervision.metrics import (
    SupervisionMetricType,
)
from recidiviz.calculator.pipeline.metrics.supervision.pipeline import (
    SupervisionMetricsPipelineRunDelegate,
)
from recidiviz.calculator.pipeline.metrics.supervision.supervision_case_compliance import (
    SupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.assessment_utils import (
    DEFAULT_ASSESSMENT_SCORE_BUCKET,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_incarceration_period_index import (
    NormalizedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_required_state_specific_delegates,
    get_state_specific_case_compliance_manager,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_violations_delegate import (
    UsXxViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_delegate import (
    UsIdSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_sentence_classification import (
    SupervisionTypeSpan,
    UsMoSentenceStatus,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_delegate import (
    UsMoSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_delegate import (
    UsNdSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_delegate import (
    UsPaSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.supervision_case_compliance_manager import (
    StateSupervisionCaseComplianceManager,
)
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    supervising_officer_and_location_info,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_agent import StateAgentType
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodReleaseReason as ReleaseReason,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
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
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import last_day_of_month
from recidiviz.persistence.entity.state.entities import (
    StateAgent,
    StateAssessment,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StateSupervisionCaseTypeEntry,
    StateSupervisionContact,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.tests.calculator.pipeline.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_ip_index_for_tests,
    default_normalized_sp_index_for_tests,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.state_calculation_config_manager_test import (
    STATE_DELEGATES_FOR_TESTS,
)
from recidiviz.tests.calculator.pipeline.utils.us_mo_fakes import (
    FakeUsMoIncarcerationSentence,
    FakeUsMoSupervisionSentence,
)

_STATE_CODE = "US_XX"
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
class TestClassifySupervisionEvents(unittest.TestCase):
    """Tests for the find_supervision_events function."""

    def setUp(self) -> None:
        self.identifier = identifier.SupervisionIdentifier()
        self.person = StatePerson.new_with_defaults(
            state_code="US_XX", person_id=99000123
        )

    def _test_find_supervision_events(
        self,
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_periods: List[StateSupervisionPeriod],
        incarceration_periods: List[StateIncarcerationPeriod],
        assessments: List[StateAssessment],
        violation_responses: List[StateSupervisionViolationResponse],
        supervision_contacts: List[StateSupervisionContact],
        supervision_period_to_agent_association: Optional[List[Dict[str, Any]]] = None,
        supervision_period_judicial_district_association: Optional[
            List[Dict[str, Any]]
        ] = None,
        state_code_override: Optional[str] = None,
    ) -> List[SupervisionEvent]:
        """Helper for testing the find_events function on the identifier."""
        state_specific_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.state_utils"
            ".state_calculation_config_manager.get_all_state_specific_delegates",
            return_value=STATE_DELEGATES_FOR_TESTS,
        )
        if not state_code_override:
            state_specific_delegate_patcher.start()
        else:
            self.person.person_id = (
                int(StateCode(state_code_override).get_state().fips) * 1000 + 123
            )

        required_delegates = get_required_state_specific_delegates(
            state_code=(state_code_override or _STATE_CODE),
            required_delegates=SupervisionMetricsPipelineRunDelegate.pipeline_config().state_specific_required_delegates,
        )

        if not state_code_override:
            state_specific_delegate_patcher.stop()

        all_kwargs = {
            **required_delegates,
            StateIncarcerationPeriod.__name__: incarceration_periods,
            StateIncarcerationSentence.__name__: incarceration_sentences,
            StateSupervisionSentence.__name__: supervision_sentences,
            StateSupervisionPeriod.__name__: supervision_periods,
            StateAssessment.__name__: assessments,
            StateSupervisionContact.__name__: supervision_contacts,
            StateSupervisionViolationResponse.__name__: violation_responses,
            "supervision_period_to_agent_association": supervision_period_to_agent_association
            or DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            "supervision_period_judicial_district_association": supervision_period_judicial_district_association
            or DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
        }

        return self.identifier.find_events(self.person, all_kwargs)

    def test_find_supervision_events(self) -> None:
        """Tests the find_supervision_population_events function for a single
        supervision period with no incarceration periods."""

        termination_date = date(2018, 5, 19)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        start_date = date(2017, 1, 1)
        completion_date = date(2018, 5, 19)
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            start_date=start_date,
            external_id="ss1",
            state_code="US_XX",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            projected_completion_date=completion_date,
            completion_date=completion_date,
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 1),
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods: List[StateIncarcerationPeriod] = []
        assessments = [assessment]
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            ProjectedSupervisionCompletionEvent(
                state_code=supervision_period.state_code,
                year=2018,
                month=5,
                event_date=last_day_of_month(completion_date),
                supervision_type=supervision_type,
                sentence_days_served=(completion_date - start_date).days,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                successful_completion=True,
                incarcerated_during_sentence=False,
            ),
            SupervisionTerminationEvent(
                state_code=supervision_period.state_code,
                year=termination_date.year,
                month=termination_date.month,
                event_date=termination_date,
                supervision_type=supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason,
                in_supervision_population_on_date=True,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            create_start_event_from_period(
                supervision_period,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                supervision_type,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                assessment_score_bucket=StateAssessmentLevel.HIGH.value,
                projected_supervision_completion_date=supervision_sentence.projected_completion_date,
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_overlaps_year(self) -> None:
        """Tests the find_supervision_events function for a single
        supervision period with no incarceration periods, where the supervision
        period overlaps two calendar years."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2019, 1, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods: List[StateIncarcerationPeriod] = []
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_start_event_from_period(
                supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            create_termination_event_from_period(
                supervision_period,
                supervision_type=supervision_type,
                in_supervision_population_on_date=True,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_two_supervision_periods(self) -> None:
        """Tests the find_supervision_events function for two supervision
        periods with no incarceration periods."""

        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_XX",
            start_date=date(2019, 8, 5),
            termination_date=date(2019, 12, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period, second_supervision_period]
        incarceration_periods: List[StateIncarcerationPeriod] = []
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        first_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        second_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_termination_event_from_period(
                first_supervision_period,
                supervision_type=first_supervision_type,
                in_supervision_population_on_date=True,
            ),
            create_termination_event_from_period(
                second_supervision_period,
                supervision_type=second_supervision_type,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            create_start_event_from_period(
                second_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                first_supervision_period,
                first_supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        expected_events.extend(
            expected_population_events(
                second_supervision_period,
                second_supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_overlapping_supervision_periods(
        self,
    ) -> None:
        """Tests the find_supervision_events function for two supervision
        periods with no incarceration periods, where the supervision
        periods are of the same type and have overlapping months."""

        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_XX",
            start_date=date(2018, 4, 15),
            termination_date=date(2018, 7, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period, second_supervision_period]
        incarceration_periods: List[StateIncarcerationPeriod] = []
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        first_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        second_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_termination_event_from_period(
                first_supervision_period,
                in_supervision_population_on_date=True,
                supervision_type=first_supervision_type,
            ),
            create_termination_event_from_period(
                second_supervision_period,
                supervision_type=second_supervision_type,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            create_start_event_from_period(
                second_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                first_supervision_period,
                first_supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        expected_events.extend(
            expected_population_events(
                second_supervision_period,
                second_supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_overlapping_periods_different_types(
        self,
    ) -> None:
        """Tests the find_supervision_events function for two supervision
        periods with no incarceration periods, where the supervision
        periods are of different types and have overlapping months."""

        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_XX",
            start_date=date(2018, 4, 15),
            termination_date=date(2018, 7, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            completion_date=date(2018, 7, 19),
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period, second_supervision_period]
        incarceration_periods: List[StateIncarcerationPeriod] = []
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        first_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        second_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        expected_events = [
            create_termination_event_from_period(
                first_supervision_period,
                in_supervision_population_on_date=True,
                supervision_type=first_supervision_type,
            ),
            create_termination_event_from_period(
                second_supervision_period,
                supervision_type=second_supervision_type,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            create_start_event_from_period(
                second_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                first_supervision_period,
                first_supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        expected_events.extend(
            expected_population_events(
                second_supervision_period,
                second_supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_findSupervisionEvents_usNd_ignoreTemporaryCustodyPeriod(self) -> None:
        """Tests the find_supervision_events function for state code US_ND to ensure that temporary
        custody periods are ignored."""

        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 9),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        temporary_custody_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            facility="CJ",
            custodial_authority=StateCustodialAuthority.COURT,
            state_code="US_ND",
            admission_date=date(2018, 4, 11),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2018, 5, 17),
            release_reason=ReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        admission_date = date(2018, 5, 17)
        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            admission_date=admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2018, 1, 1),
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_periods = [first_supervision_period]
        incarceration_periods = [temporary_custody_period, revocation_period]
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []

        first_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_start_event_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            ),
            create_termination_event_from_period(
                first_supervision_period,
                supervision_type=first_supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                in_supervision_population_on_date=True,
                in_incarceration_population_on_date=False,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                first_supervision_period,
                first_supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                    supervision_delegate=UsNdSupervisionDelegate(),
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        supervision_events = self._test_find_supervision_events(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
            state_code_override="US_ND",
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_findSupervisionEvents_usId_supervisionAfterInvestigation(
        self,
    ) -> None:
        """Tests the find_supervision_events function for state code US_ID where the person starts supervision
        after being on INVESTIGATION. Investigative periods should produce no SupervisionEvents, and the start
        of probation should be marked as the beginning of supervision.
        """

        supervision_period_investigation = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )

        supervision_period_start_date = date(2017, 5, 9)
        supervision_period_termination_date = date(2017, 5, 13)
        supervision_period_type = StateSupervisionPeriodSupervisionType.PROBATION
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=supervision_period_start_date,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=supervision_period_type,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            status=StateSentenceStatus.COMPLETED,
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_periods = [supervision_period_investigation, supervision_period]
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_periods: List[StateIncarcerationPeriod] = []

        expected_events = [
            create_start_event_from_period(
                supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            ),
            SupervisionTerminationEvent(
                state_code=supervision_period.state_code,
                year=supervision_period_termination_date.year,
                month=supervision_period_termination_date.month,
                event_date=supervision_period_termination_date,
                supervision_type=supervision_period.supervision_type,
                supervision_level=supervision_period.supervision_level,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                in_supervision_population_on_date=True,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
        ]
        expected_events.extend(
            expected_population_events(
                supervision_period,
                supervision_period_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period_start_date,
                    supervision_period=supervision_period,
                    supervision_delegate=UsIdSupervisionDelegate(),
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )
        supervision_events = self._test_find_supervision_events(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
            state_code_override="US_ID",
        )
        self.assertCountEqual(expected_events, supervision_events)

    def test_findSupervisionEvents_usId_admittedAfterInvestigation(self) -> None:
        """Tests the find_supervision_events function for state code US_ID where the person is admitted after
        being on INVESTIGATION supervision. These periods should produce no
        SupervisionEvents."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ID",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
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
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []

        expected_events: List[SupervisionEvent] = []
        supervision_events = self._test_find_supervision_events(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
            state_code_override="US_ID",
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_findSupervisionEvents_usNd_admittedAfterPreConfinement(self) -> None:
        """Tests the find_supervision_events function for state code US_ND where the
        person is admitted after being on INVESTIGATION supervision. These periods
        should produce no SupervisionEvents.
        """

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ND",
            custodial_authority_raw_text="US_ND_DOCR",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_ND",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
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
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []

        expected_events: List[SupervisionEvent] = []
        supervision_events = self._test_find_supervision_events(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
            state_code_override="US_ND",
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_multiple_periods(self) -> None:
        """Tests the find_supervision_events function for two supervision
        periods with two incarceration periods."""

        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        first_admission_date = date(2017, 5, 25)
        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=first_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2017, 8, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_XX",
            start_date=date(2018, 8, 5),
            termination_date=date(2018, 12, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        second_admission_date = date(2018, 12, 25)
        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_XX",
            admission_date=second_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period, second_supervision_period]
        incarceration_periods = [
            first_incarceration_period,
            second_incarceration_period,
        ]
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        first_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        second_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_termination_event_from_period(
                first_supervision_period,
                supervision_type=first_supervision_type,
                in_supervision_population_on_date=True,
            ),
            create_termination_event_from_period(
                second_supervision_period,
                supervision_type=second_supervision_type,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            create_start_event_from_period(
                second_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                first_supervision_period,
                first_supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        expected_events.extend(
            expected_population_events(
                second_supervision_period,
                second_supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_multiple_admissions_in_month(self) -> None:
        """Tests the find_supervision_events function for a supervision period with two incarceration periods
        with admission dates in the same month."""
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        first_admission_date = date(2017, 5, 11)
        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=first_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2017, 5, 15),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE,
        )

        second_admission_date = date(2017, 5, 17)
        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_XX",
            admission_date=second_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period]
        incarceration_periods = [
            first_incarceration_period,
            second_incarceration_period,
        ]
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        first_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_termination_event_from_period(
                first_supervision_period,
                supervision_type=first_supervision_type,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                first_supervision_period,
                first_supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_incarceration_overlaps_periods(self) -> None:
        """Tests the find_supervision_events function for two supervision
        periods with an incarceration period that overlaps the end of one
        supervision period and the start of another."""

        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        admission_date = date(2017, 5, 15)
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2017, 9, 20),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_XX",
            start_date=date(2017, 8, 5),
            termination_date=date(2017, 12, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            completion_date=date(2017, 12, 19),
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period, second_supervision_period]
        incarceration_periods = [incarceration_period]
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        first_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        second_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_termination_event_from_period(
                first_supervision_period,
                in_incarceration_population_on_date=True,
                supervision_type=first_supervision_type,
            ),
            create_termination_event_from_period(
                second_supervision_period,
                supervision_type=second_supervision_type,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            create_start_event_from_period(
                second_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
                in_incarceration_population_on_date=True,
                in_supervision_population_on_date=False,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                first_supervision_period,
                first_supervision_type,
                incarceration_period.admission_date,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        expected_events.extend(
            expected_population_events(
                attr.evolve(
                    second_supervision_period,
                    start_date=incarceration_period.release_date,
                ),
                second_supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_supervision_authority_incarceration_overlaps_periods(
        self,
    ) -> None:
        """Tests the find_supervision_events function for two supervision periods with an incarceration period
        that overlaps the end of one supervision period and the start of another, where the incarceration period is
        under the custodial authority of a supervision entity so the period does not exclude the supervision periods
        from being counted towards the supervision population."""

        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
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
            state_code="US_XX",
            start_date=date(2017, 8, 5),
            termination_date=date(2017, 12, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            completion_date=date(2017, 12, 19),
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period, second_supervision_period]
        incarceration_periods = [incarceration_period]
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        first_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE
        second_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_termination_event_from_period(
                first_supervision_period,
                supervision_type=first_supervision_type,
                in_incarceration_population_on_date=False,
                in_supervision_population_on_date=True,
            ),
            create_termination_event_from_period(
                second_supervision_period,
                supervision_type=second_supervision_type,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(first_supervision_period),
            create_start_event_from_period(
                second_supervision_period,
                in_incarceration_population_on_date=False,
            ),
        ]

        # The entirety of both supervision periods should be counted as SupervisionPopulationEvents
        expected_events.extend(
            expected_population_events(
                first_supervision_period,
                first_supervision_type,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        expected_events.extend(
            expected_population_events(
                second_supervision_period,
                second_supervision_type,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_transition_from_parole_to_probation_in_month(self) -> None:
        """Tests the find_supervision_events function for transition between two supervision periods in a
        month."""
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_XX",
            start_date=date(2018, 5, 19),
            termination_date=date(2018, 6, 20),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            state_code="US_XX",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [first_supervision_period, second_supervision_period]
        incarceration_periods: List[StateIncarcerationPeriod] = []
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        first_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE
        second_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_termination_event_from_period(
                first_supervision_period,
                in_supervision_population_on_date=True,
                supervision_type=first_supervision_type,
            ),
            create_termination_event_from_period(
                second_supervision_period,
                supervision_type=second_supervision_type,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            create_start_event_from_period(
                second_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                first_supervision_period,
                first_supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        expected_events.extend(
            expected_population_events(
                second_supervision_period,
                second_supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_return_next_month(self) -> None:
        """Tests the find_supervision_events function
        when there is an incarceration period with a revocation admission
        the month after the supervision period's termination_date."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 26),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_site="X",
        )

        admission_date = date(2018, 6, 25)
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_termination_event_from_period(
                supervision_period,
                supervision_type=supervision_type,
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
                supervising_district_external_id="X",
                level_1_supervision_location_external_id="X",
                level_2_supervision_location_external_id=None,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                supervision_period,
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
                supervising_district_external_id="X",
                level_1_supervision_location_external_id="X",
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                supervision_type,
                supervising_officer_external_id="XXX",
                level_1_supervision_location_external_id="X",
                level_2_supervision_location_external_id=None,
                judicial_district_code="XXX",
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_multiple_periods_revocations(self) -> None:
        """Tests the find_supervision_events function
        when the person is revoked and returned to supervision twice in one
        year."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MINIMUM,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
        )

        first_admission_date = date(2018, 3, 25)
        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=first_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2018, 8, 2),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE,
        )

        second_admission_date = date(2018, 9, 25)
        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip2",
            state_code="US_XX",
            admission_date=second_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2018, 12, 2),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            completion_date=date(2018, 12, 19),
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = [
            first_incarceration_period,
            second_incarceration_period,
        ]
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        expected_events = [
            create_termination_event_from_period(
                supervision_period,
                supervision_type=supervision_type,
                supervision_level=StateSupervisionLevel.MINIMUM,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                supervision_type,
                first_incarceration_period.admission_date,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        expected_events.extend(
            expected_population_events(
                attr.evolve(
                    supervision_period,
                    start_date=first_incarceration_period.release_date,
                ),
                supervision_type,
                second_incarceration_period.admission_date,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        expected_events.extend(
            expected_population_events(
                attr.evolve(
                    supervision_period,
                    start_date=second_incarceration_period.release_date,
                ),
                supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_multiple_sentences_revocations(self) -> None:
        """Tests the find_supervision_events function
        when the person is revoked and returned to supervision twice in one
        year, and they have multiple supervision sentences."""

        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        first_admission_date = date(2018, 3, 25)
        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=first_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2018, 8, 2),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE,
        )

        second_admission_date = date(2018, 9, 25)
        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip2",
            state_code="US_XX",
            admission_date=second_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2018, 12, 2),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=234,
            external_id="sp2",
            state_code="US_XX",
            start_date=date(2019, 1, 1),
            termination_date=date(2019, 1, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        first_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            completion_date=date(2018, 12, 19),
        )

        second_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=222,
            start_date=date(2017, 1, 1),
            external_id="ss2",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            completion_date=date(2019, 1, 19),
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
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        first_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE
        second_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        expected_events = [
            create_termination_event_from_period(
                first_supervision_period,
                supervision_type=first_supervision_type,
                in_supervision_population_on_date=True,
            ),
            create_termination_event_from_period(
                second_supervision_period,
                supervision_type=second_supervision_type,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            create_start_event_from_period(
                second_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                first_supervision_period,
                first_supervision_type,
                first_incarceration_period.admission_date,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        expected_events.extend(
            expected_population_events(
                attr.evolve(
                    first_supervision_period,
                    start_date=first_incarceration_period.release_date,
                ),
                first_supervision_type,
                second_incarceration_period.admission_date,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_incarceration_period.release_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        expected_events.extend(
            expected_population_events(
                attr.evolve(
                    first_supervision_period,
                    start_date=second_incarceration_period.release_date,
                ),
                first_supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=second_incarceration_period.release_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        expected_events.extend(
            expected_population_events(
                second_supervision_period,
                second_supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=second_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_single_sentence_with_past_completion_date(
        self,
    ) -> None:
        """Tests the find_supervision_events function for a single
        supervision period with no incarceration periods. The supervision period extends past
        the supervision sentence."""

        supervision_period_termination_date = date(2018, 5, 15)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_XX",
            start_date=date(2018, 5, 1),
            # Termination date is after sentence's projected completion date
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            start_date=date(2018, 5, 1),
            external_id="ss1",
            state_code="US_XX",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            projected_completion_date=date(2018, 5, 10),
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 1),
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods: List[StateIncarcerationPeriod] = []
        assessments = [assessment]
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            SupervisionTerminationEvent(
                state_code=supervision_period.state_code,
                year=supervision_period_termination_date.year,
                month=supervision_period_termination_date.month,
                event_date=supervision_period_termination_date,
                supervision_type=supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason,
                in_supervision_population_on_date=True,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            create_start_event_from_period(
                supervision_period,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                supervision_type,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                assessment_score_bucket=StateAssessmentLevel.HIGH.value,
                projected_supervision_completion_date=supervision_sentence.projected_completion_date,
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_multiple_sentences_past_sentence_completion_dates(
        self,
    ) -> None:
        """Tests the find_supervision_events function when there are multiple supervision periods and
        multiple supervision sentences. Both supervision periods extend past their corresponding supervision
        sentences."""

        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 1),
            # termination date is after first supervision sentence's projected completion date
            termination_date=date(2018, 1, 3),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=234,
            external_id="sp2",
            state_code="US_XX",
            start_date=date(2019, 2, 1),
            # termination date is after second supervision sentence's projected completion date
            termination_date=date(2019, 2, 3),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        first_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2018, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            completion_date=date(2018, 1, 2),
        )

        second_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=222,
            start_date=date(2019, 2, 1),
            external_id="ss2",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            completion_date=date(2019, 2, 2),
        )

        supervision_sentences = [
            first_supervision_sentence,
            second_supervision_sentence,
        ]
        supervision_periods = [first_supervision_period, second_supervision_period]
        incarceration_periods: List[StateIncarcerationPeriod] = []
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        first_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE
        second_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        expected_events = [
            create_termination_event_from_period(
                first_supervision_period,
                supervision_type=first_supervision_type,
                in_supervision_population_on_date=True,
            ),
            create_termination_event_from_period(
                second_supervision_period,
                supervision_type=second_supervision_type,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                first_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            create_start_event_from_period(
                second_supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                first_supervision_period,
                first_supervision_type,
                projected_supervision_completion_date=first_supervision_sentence.projected_completion_date,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_supervision_period.start_date,
                    supervision_period=first_supervision_period,
                ),
            )
        )

        expected_events.extend(
            expected_population_events(
                second_supervision_period,
                second_supervision_type,
                projected_supervision_completion_date=second_supervision_sentence.projected_completion_date,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=first_supervision_period.start_date,
                    supervision_period=second_supervision_period,
                ),
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_placeholders(self) -> None:
        """Tests the find_supervision_events function
        when there are placeholder supervision periods that should be dropped
        from the calculations."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        admission_date = date(2018, 5, 25)
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_termination_event_from_period(
                supervision_period,
                supervision_type=supervision_type,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_us_id(self) -> None:
        """Tests the find_supervision_events function where the supervision type should be taken from the
        supervision_type off of the supervision_period."""

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        supervision_period_termination_date = date(2018, 5, 19)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
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
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=supervision_type,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=123,
            external_id="is1",
            start_date=date(2017, 1, 1),
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
        incarceration_periods: List[StateIncarcerationPeriod] = []
        assessments = [assessment]
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = [
            incarceration_sentence
        ]

        expected_events = [
            create_start_event_from_period(
                supervision_period,
                UsIdSupervisionDelegate(),
                supervising_district_external_id="DISTRICT_1",
            ),
            SupervisionTerminationEvent(
                state_code=supervision_period.state_code,
                year=supervision_period_termination_date.year,
                month=supervision_period_termination_date.month,
                event_date=supervision_period_termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                supervising_district_external_id="DISTRICT_1",
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                in_supervision_population_on_date=True,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                supervision_type,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                assessment_score_bucket="30-38",
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    assessments=assessments,
                ),
            )
        )

        supervision_events = self._test_find_supervision_events(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
            state_code_override="US_ID",
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_us_pa(self) -> None:
        """Tests the find_supervision_events function for periods in US_PA."""

        supervision_period_termination_date = date(2018, 5, 19)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_PA", case_type=StateSupervisionCaseType.GENERAL
                )
            ],
            state_code="US_PA",
            supervision_site="DISTRICT_1|OFFICE_2|1345",
            start_date=date(2018, 3, 5),
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
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

        supervision_sentences: List[StateSupervisionSentence] = []
        supervision_periods = [supervision_period]
        incarceration_periods: List[StateIncarcerationPeriod] = []
        assessments = [assessment]
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        supervision_events = self._test_find_supervision_events(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
            state_code_override="US_PA",
        )

        expected_events = [
            create_start_event_from_period(
                supervision_period,
                UsPaSupervisionDelegate(),
                supervising_district_external_id="DISTRICT_1",
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
            ),
            SupervisionTerminationEvent(
                state_code=supervision_period.state_code,
                year=supervision_period_termination_date.year,
                month=supervision_period_termination_date.month,
                event_date=supervision_period_termination_date,
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
                in_supervision_population_on_date=True,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score=assessment.assessment_score,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=assessment.assessment_type,
                assessment_score_bucket=StateAssessmentLevel.HIGH.value,
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                supervising_officer_external_id="XXX",
                judicial_district_code="XXX",
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    assessments=assessments,
                ),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_infer_supervision_type_dual_us_mo(
        self,
    ) -> None:
        """Tests the find_supervision_events function where the supervision type needs to be inferred, the
        but the supervision period is attached to both a supervision sentence of type PROBATION and an incarceration
        sentence, so the inferred type should be DUAL."""

        supervision_period_start_date = date(2018, 3, 5)
        supervision_period_termination_date = date(2018, 5, 19)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_MO",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_MO",
            start_date=supervision_period_start_date,
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervising_officer=StateAgent.new_with_defaults(
                state_code="US_MO",
                agent_type=StateAgentType.SUPERVISION_OFFICER,
                full_name="Ted Jones",
            ),
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code="US_MO",
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                completion_date=date(2018, 5, 19),
                external_id="ss1",
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=supervision_period_start_date,
                    end_date=supervision_period_termination_date,
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=supervision_period_start_date,
                            status_code="15I1000",
                            status_description="New Court Probation",
                        )
                    ],
                    end_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=supervision_period_termination_date,
                            status_code="99O1000",
                            status_description="Court Probation Discharge",
                        )
                    ],
                ),
                SupervisionTypeSpan(
                    start_date=supervision_period_termination_date,
                    end_date=None,
                    supervision_type=None,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=supervision_period_termination_date,
                            status_code="99O1000",
                            status_description="Court Probation Discharge",
                        )
                    ],
                    end_critical_statuses=None,
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
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=supervision_period_start_date,
                        end_date=supervision_period_termination_date,
                        supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
                        start_critical_statuses=[
                            UsMoSentenceStatus(
                                sentence_external_id="123",
                                sentence_status_external_id="test-status-2",
                                status_date=supervision_period_start_date,
                                status_code="40O1010",
                                status_description="Parole Release",
                            )
                        ],
                        end_critical_statuses=[
                            UsMoSentenceStatus(
                                sentence_external_id="123",
                                sentence_status_external_id="test-status-2",
                                status_date=supervision_period_termination_date,
                                status_code="99O2010",
                                status_description="Parole Discharge",
                            )
                        ],
                    ),
                    SupervisionTypeSpan(
                        start_date=supervision_period_termination_date,
                        end_date=None,
                        supervision_type=None,
                        start_critical_statuses=[
                            UsMoSentenceStatus(
                                sentence_external_id="123",
                                sentence_status_external_id="test-status-2",
                                status_date=supervision_period_termination_date,
                                status_code="99O2010",
                                status_description="Parole Discharge",
                            )
                        ],
                        end_critical_statuses=None,
                    ),
                ],
            )
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_MO",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 2, 10),
        )

        supervision_sentences: List[StateSupervisionSentence] = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods: List[StateIncarcerationPeriod] = []
        assessments = [assessment]
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = [
            incarceration_sentence
        ]

        expected_events = [
            SupervisionTerminationEvent(
                state_code=supervision_period.state_code,
                year=supervision_period_termination_date.year,
                month=supervision_period_termination_date.month,
                event_date=supervision_period_termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                termination_reason=supervision_period.termination_reason,
                in_supervision_population_on_date=True,
                supervising_officer_external_id="XXX",
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            create_start_event_from_period(
                supervision_period,
                UsMoSupervisionDelegate(),
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervising_officer_external_id="XXX",
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                StateSupervisionPeriodSupervisionType.DUAL,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                assessment_score_bucket=StateAssessmentLevel.HIGH.value,
                supervising_officer_external_id="XXX",
            )
        )

        supervision_events = self._test_find_supervision_events(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            [
                {
                    "agent_id": 000,
                    "agent_external_id": "XXX",
                    "supervision_period_id": supervision_period.supervision_period_id,
                    "agent_start_date": supervision_period_start_date,
                    "agent_end_date": supervision_period_termination_date,
                }
            ],
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
            state_code_override="US_MO",
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_not_in_population_if_no_active_po_us_mo(
        self,
    ) -> None:
        """Tests the find_supervision_events function where a SupervisionPopulationEvent
        will not be created if there is no supervising officer attached to the period."""

        supervision_period_start_date = date(2018, 3, 5)
        supervision_period_termination_date = date(2018, 5, 19)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            case_type_entries=[
                StateSupervisionCaseTypeEntry.new_with_defaults(
                    state_code="US_MO",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_MO",
            start_date=supervision_period_start_date,
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code="US_MO",
                supervision_sentence_id=111,
                start_date=date(2017, 1, 1),
                completion_date=date(2018, 5, 19),
                external_id="ss1",
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=supervision_period_start_date,
                    end_date=supervision_period_termination_date,
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=supervision_period_start_date,
                            status_code="15I1000",
                            status_description="New Court Probation",
                        )
                    ],
                    end_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=supervision_period_termination_date,
                            status_code="99O1000",
                            status_description="Court Probation Discharge",
                        )
                    ],
                ),
                SupervisionTypeSpan(
                    start_date=supervision_period_termination_date,
                    end_date=None,
                    supervision_type=None,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=supervision_period_termination_date,
                            status_code="99O1000",
                            status_description="Court Probation Discharge",
                        )
                    ],
                    end_critical_statuses=None,
                ),
            ],
        )

        incarceration_sentence = FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
            StateIncarcerationSentence.new_with_defaults(
                state_code="US_MO",
                incarceration_sentence_id=123,
                external_id="is1",
                start_date=date(2017, 1, 1),
                completion_date=date(2018, 5, 19),
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=supervision_period_start_date,
                    end_date=supervision_period_termination_date,
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=supervision_period_start_date,
                            status_code="15I1000",
                            status_description="New Court Probation",
                        )
                    ],
                    end_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=supervision_period_termination_date,
                            status_code="99O1000",
                            status_description="Court Probation Discharge",
                        )
                    ],
                ),
                SupervisionTypeSpan(
                    start_date=supervision_period_termination_date,
                    end_date=None,
                    supervision_type=None,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=supervision_period_termination_date,
                            status_code="99O1000",
                            status_description="Court Probation Discharge",
                        )
                    ],
                    end_critical_statuses=None,
                ),
            ],
        )

        supervision_sentences: List[StateSupervisionSentence] = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods: List[StateIncarcerationPeriod] = []
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = [
            incarceration_sentence
        ]

        supervision_events = self._test_find_supervision_events(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            [
                {
                    "agent_id": 000,
                    "agent_external_id": None,
                    "supervision_period_id": supervision_period.supervision_period_id,
                    "agent_start_date": supervision_period_start_date,
                    "agent_end_date": supervision_period_termination_date,
                }
            ],
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
            state_code_override="US_MO",
        )
        for event in supervision_events:
            if isinstance(event, SupervisionPopulationEvent):
                self.fail("No supervision population event should be generated.")

    def test_find_supervision_events_infer_supervision_type_dual_us_id(
        self,
    ) -> None:
        """Tests the find_supervision_events function where the supervision type is taken from a `DUAL`
        supervision period. Asserts that the DUAL events are NOT expanded into separate PROBATION and PAROLE events.
        """

        supervision_period_termination_date = date(2018, 5, 19)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2018, 3, 5),
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
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

        supervision_sentences: List[StateSupervisionSentence] = []
        supervision_periods = [supervision_period]
        incarceration_periods: List[StateIncarcerationPeriod] = []
        assessments = [assessment]
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        expected_events = [
            create_start_event_from_period(
                supervision_period, UsIdSupervisionDelegate()
            ),
            SupervisionTerminationEvent(
                state_code=supervision_period.state_code,
                year=supervision_period_termination_date.year,
                case_type=StateSupervisionCaseType.GENERAL,
                month=supervision_period_termination_date.month,
                event_date=supervision_period_termination_date,
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                termination_reason=supervision_period.termination_reason,
                in_supervision_population_on_date=True,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                StateSupervisionPeriodSupervisionType.DUAL,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                assessment_score_bucket="30-38",
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    assessments=assessments,
                ),
            )
        )

        supervision_events = self._test_find_supervision_events(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
            state_code_override="US_ID",
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_no_supervision_when_no_sentences_supervision_spans_us_mo(
        self,
    ) -> None:

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
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
        )

        assessment = StateAssessment.new_with_defaults(
            state_code="US_MO",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10),
        )

        supervision_sentences: List[StateSupervisionSentence] = []
        supervision_periods = [supervision_period]
        incarceration_periods: List[StateIncarcerationPeriod] = []
        assessments = [assessment]
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        supervision_events = self._test_find_supervision_events(
            supervision_sentences,
            incarceration_sentences,
            supervision_periods,
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
            state_code_override="US_MO",
        )

        self.assertCountEqual([], supervision_events)

    @freeze_time("2019-09-04")
    def test_find_supervision_events_admission_today(self) -> None:
        """Tests the find_population_events_for_supervision_period function when there
        is an incarceration period with a revocation admission today, where there is
        no termination_date on the supervision_period, and no release_date
        on the incarceration_period."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2019, 6, 2),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        admission_date = date.today()
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
        )

        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]

        expected_events: List[SupervisionEvent] = [
            create_start_event_from_period(
                supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                StateSupervisionPeriodSupervisionType.PROBATION,
                date.today(),
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    end_date_override=date(2019, 9, 3),
                ),
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_multiple_incarcerations_in_year(
        self,
    ) -> None:
        """Tests the find_population_events_for_supervision_period function when there are multiple
        incarceration periods in the year of supervision."""
        supervision_period_termination_date = date(2018, 6, 9)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=supervision_period_termination_date,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
        )

        first_admission_date = date(2018, 6, 2)
        first_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=first_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2018, 9, 3),
        )

        second_admission_date = date(2018, 11, 17)
        second_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=222,
            external_id="ip2",
            state_code="US_XX",
            admission_date=second_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=123,
            external_id="is1",
            start_date=date(2018, 1, 1),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        assessments: List[StateAssessment] = []
        violation_reports: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        expected_events = [
            SupervisionTerminationEvent(
                state_code=supervision_period.state_code,
                year=supervision_period_termination_date.year,
                month=supervision_period_termination_date.month,
                event_date=supervision_period_termination_date,
                in_incarceration_population_on_date=True,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                case_type=StateSupervisionCaseType.GENERAL,
                termination_reason=supervision_period.termination_reason,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            create_start_event_from_period(
                supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                supervision_type,
                first_incarceration_period.admission_date,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_supervision_events_violation_history(self) -> None:
        state_code = "US_XX"

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code=state_code,
            start_date=date(2018, 1, 1),
            termination_date=date(2020, 1, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
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
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
        )

        violation_reports = [violation_report_1, violation_report_2]
        supervision_contacts: List[StateSupervisionContact] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_termination_event_from_period(
                supervision_period,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                most_severe_violation_type=StateSupervisionViolationType.MISDEMEANOR,
                most_severe_violation_type_subtype=StateSupervisionViolationType.MISDEMEANOR.value,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                supervision_period,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                supervision_type,
                end_date=violation_report_1.response_date,
            )
        )

        expected_events.extend(
            expected_population_events(
                attr.evolve(
                    supervision_period,
                    start_date=violation_report_1.response_date,
                ),
                supervision_type,
                end_date=violation_report_2.response_date,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
            )
        )

        expected_events.extend(
            expected_population_events(
                attr.evolve(
                    supervision_period,
                    start_date=violation_report_2.response_date,
                ),
                supervision_type,
                end_date=date(2019, 4, 21),
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=2,
            )
        )

        expected_events.extend(
            expected_population_events(
                attr.evolve(supervision_period, start_date=date(2019, 4, 21)),
                supervision_type,
                most_severe_violation_type=StateSupervisionViolationType.MISDEMEANOR,
                most_severe_violation_type_subtype=StateSupervisionViolationType.MISDEMEANOR.value,
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)

    def test_supervision_events_no_supervision_period_end_of_month_us_mo_supervision_span_shows_supervision(
        self,
    ) -> None:
        """Tests that we do not mark someone as under supervision at the end of the month if there is no supervision
        period overlapping with the end of the month, even if the US_MO sentence supervision spans indicate that they
        are on supervision at a given time.
        """

        start_date = date(2019, 10, 3)
        end_date = date(2019, 10, 9)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp2",
            state_code="US_MO",
            supervision_site="DISTRICTX",
            supervising_officer="AGENTX",
            start_date=start_date,
            termination_date=end_date,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=supervision_period.start_date,
                external_id="ss1",
                state_code="US_MO",
                supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                status=StateSentenceStatus.SERVING,
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=start_date,
                    end_date=None,
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=start_date,
                            status_code="40O9010",
                            status_description="Release to Probation",
                        )
                    ],
                    end_critical_statuses=None,
                )
            ],
        )

        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []
        incarceration_periods: List[StateIncarcerationPeriod] = []

        expected_events = [
            create_termination_event_from_period(
                supervision_period,
                UsMoSupervisionDelegate(),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_district_external_id=supervision_period.supervision_site,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                in_supervision_population_on_date=True,
                termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
                supervising_officer_external_id="XXX",
            ),
            create_start_event_from_period(
                supervision_period,
                UsMoSupervisionDelegate(),
                admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
                supervising_officer_external_id="XXX",
            ),
            SupervisionStartEvent(
                state_code="US_MO",
                year=2019,
                month=10,
                event_date=end_date,
                admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
                in_incarceration_population_on_date=False,
                in_supervision_population_on_date=False,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                StateSupervisionPeriodSupervisionType.PROBATION,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id="XXX",
            )
        )

        supervision_events = self._test_find_supervision_events(
            [supervision_sentence],
            incarceration_sentences,
            [supervision_period],
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            [
                {
                    "agent_id": 000,
                    "agent_external_id": "XXX",
                    "supervision_period_id": supervision_period.supervision_period_id,
                    "agent_start_date": start_date,
                    "agent_end_date": end_date,
                }
            ],
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
            state_code_override="US_MO",
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_supervision_events_period_eom_us_mo_supervision_span_shows_no_supervision_eom(
        self,
    ) -> None:

        start_date = date(2019, 10, 3)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp2",
            state_code="US_MO",
            supervision_site="DISTRICTX",
            supervising_officer="AGENTX",
            start_date=start_date,
            termination_date=date(2019, 11, 9),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=supervision_period.start_date,
                external_id="ss1",
                state_code="US_MO",
                supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                status=StateSentenceStatus.SERVING,
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=start_date,
                    end_date=date(2019, 10, 6),
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=start_date,
                            status_code="15I1000",
                            status_description="New Court Probation",
                        )
                    ],
                    end_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=date(2019, 10, 6),
                            status_code="65L9999",
                            status_description="DOC Warrant/Detainer Issued",
                        )
                    ],
                ),
                SupervisionTypeSpan(
                    start_date=date(2019, 10, 6),
                    end_date=date(2019, 11, 6),
                    supervision_type=None,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=date(2019, 10, 6),
                            status_code="65L9999",
                            status_description="DOC Warrant/Detainer Issued",
                        )
                    ],
                    end_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=date(2019, 11, 6),
                            status_code="65N9999",
                            status_description="DOC Warrant/Detainer Cancelled",
                        )
                    ],
                ),
                SupervisionTypeSpan(
                    start_date=date(2019, 11, 6),
                    end_date=None,
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=date(2019, 11, 6),
                            status_code="65N9999",
                            status_description="DOC Warrant/Detainer Cancelled",
                        )
                    ],
                    end_critical_statuses=None,
                ),
            ],
        )

        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []
        incarceration_periods: List[StateIncarcerationPeriod] = []

        expected_events = [
            create_start_event_from_period(
                supervision_period,
                UsMoSupervisionDelegate(),
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                supervising_officer_external_id="XXX",
            ),
            create_termination_event_from_period(
                supervision_period,
                UsMoSupervisionDelegate(),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_district_external_id=supervision_period.supervision_site,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                in_supervision_population_on_date=True,
                termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
                supervising_officer_external_id="XXX",
            ),
            create_termination_event_from_period(
                attr.evolve(supervision_period, termination_date=date(2019, 10, 6)),
                UsMoSupervisionDelegate(),
                in_supervision_population_on_date=True,
                supervising_officer_external_id="XXX",
            ),
            create_start_event_from_period(
                attr.evolve(
                    supervision_period,
                    start_date=date(2019, 11, 6),
                    termination_date=date(2019, 11, 9),
                ),
                UsMoSupervisionDelegate(),
                supervising_officer_external_id="XXX",
            ),
            create_start_event_from_period(
                attr.evolve(
                    supervision_period,
                    start_date=date(2019, 11, 9),
                    supervision_site=None,
                ),
                UsMoSupervisionDelegate(),
                in_supervision_population_on_date=False,
                admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                StateSupervisionPeriodSupervisionType.PROBATION,
                end_date=date(2019, 10, 6),
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id="XXX",
            )
        )

        expected_events.extend(
            expected_population_events(
                attr.evolve(supervision_period, start_date=date(2019, 11, 6)),
                StateSupervisionPeriodSupervisionType.PROBATION,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id="XXX",
            )
        )

        supervision_events = self._test_find_supervision_events(
            [supervision_sentence],
            incarceration_sentences,
            [supervision_period],
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            [
                {
                    "agent_id": 000,
                    "agent_external_id": "XXX",
                    "supervision_period_id": supervision_period.supervision_period_id,
                    "agent_start_date": start_date,
                    "agent_end_date": date(2019, 11, 9),
                }
            ],
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
            state_code_override="US_MO",
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_supervision_period_end_of_month_us_mo_supervision_span_shows_no_supervision_all_month(
        self,
    ) -> None:

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp2",
            state_code="US_MO",
            supervision_site="DISTRICTX",
            supervising_officer="AGENTX",
            start_date=date(2019, 10, 3),
            termination_date=date(2019, 11, 9),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=supervision_period.start_date,
                external_id="ss1",
                state_code="US_MO",
                supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                status=StateSentenceStatus.SERVING,
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=date(2019, 9, 6),
                    end_date=date(2019, 10, 3),
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=date(2019, 9, 6),
                            status_code="15I1000",
                            status_description="New Court Probation",
                        )
                    ],
                    end_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=date(2019, 10, 3),
                            status_code="65L9999",
                            status_description="DOC Warrant/Detainer Issued",
                        )
                    ],
                ),
                SupervisionTypeSpan(
                    start_date=date(2019, 10, 3),
                    end_date=date(2019, 11, 6),
                    supervision_type=None,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=date(2019, 10, 3),
                            status_code="65L9999",
                            status_description="DOC Warrant/Detainer Issued",
                        )
                    ],
                    end_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=date(2019, 11, 6),
                            status_code="65N9999",
                            status_description="DOC Warrant/Detainer Cancelled",
                        )
                    ],
                ),
                SupervisionTypeSpan(
                    start_date=date(2019, 11, 6),
                    end_date=None,
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=date(2019, 11, 6),
                            status_code="65N9999",
                            status_description="DOC Warrant/Detainer Cancelled",
                        )
                    ],
                    end_critical_statuses=[],
                ),
            ],
        )

        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []
        incarceration_periods: List[StateIncarcerationPeriod] = []

        expected_events = [
            create_termination_event_from_period(
                supervision_period,
                UsMoSupervisionDelegate(),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_district_external_id=supervision_period.supervision_site,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                in_supervision_population_on_date=True,
                termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
                supervising_officer_external_id="XXX",
            ),
            create_start_event_from_period(
                attr.evolve(
                    supervision_period,
                    start_date=date(2019, 9, 6),
                    supervision_site=None,
                ),
                UsMoSupervisionDelegate(),
                in_supervision_population_on_date=False,
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            ),
            create_termination_event_from_period(
                attr.evolve(
                    supervision_period,
                    start_date=date(2019, 9, 6),
                    termination_date=date(2019, 10, 3),
                    supervision_site=None,
                ),
                UsMoSupervisionDelegate(),
            ),
            create_start_event_from_period(
                attr.evolve(
                    supervision_period,
                    start_date=date(2019, 11, 6),
                    termination_date=date(2019, 11, 9),
                ),
                UsMoSupervisionDelegate(),
                supervising_officer_external_id="XXX",
            ),
            create_start_event_from_period(
                attr.evolve(
                    supervision_period,
                    start_date=date(2019, 11, 9),
                    supervision_site=None,
                ),
                UsMoSupervisionDelegate(),
                in_supervision_population_on_date=False,
                admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                attr.evolve(supervision_period, start_date=date(2019, 11, 6)),
                StateSupervisionPeriodSupervisionType.PROBATION,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id="XXX",
            )
        )

        supervision_events = self._test_find_supervision_events(
            [supervision_sentence],
            incarceration_sentences,
            [supervision_period],
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            [
                {
                    "agent_id": 000,
                    "agent_external_id": "XXX",
                    "supervision_period_id": supervision_period.supervision_period_id,
                    "agent_start_date": date(2019, 10, 3),
                    "agent_end_date": date(2019, 11, 9),
                }
            ],
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
            state_code_override="US_MO",
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_supervision_period_us_mo_supervision_spans_do_not_overlap(self) -> None:

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp2",
            state_code="US_MO",
            supervision_site="DISTRICTX",
            supervising_officer="AGENTX",
            start_date=date(2019, 10, 3),
            termination_date=date(2019, 11, 9),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=supervision_period.start_date,
                external_id="ss1",
                state_code="US_MO",
                supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                status=StateSentenceStatus.SERVING,
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=date(2019, 9, 6),
                    end_date=date(2019, 10, 3),
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=date(2019, 9, 6),
                            status_code="99O1000",
                            status_description="Court Probation Discharge",
                        )
                    ],
                    end_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=date(2019, 10, 3),
                            status_code="60L6999",
                            status_description="DOC Warrant/Detainer Issued",
                        )
                    ],
                ),
                SupervisionTypeSpan(
                    start_date=date(2019, 10, 3),
                    end_date=date(2019, 11, 9),
                    supervision_type=None,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=date(2019, 10, 3),
                            status_code="60L6999",
                            status_description="DOC Warrant/Detainer Issued",
                        )
                    ],
                    end_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=date(2019, 11, 9),
                            status_code="60N6999",
                            status_description="DOC Warrant/Detainer Cancelled",
                        )
                    ],
                ),
                SupervisionTypeSpan(
                    start_date=date(2019, 11, 9),
                    end_date=None,
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=date(2019, 11, 9),
                            status_code="60N6999",
                            status_description="DOC Warrant/Detainer Cancelled",
                        )
                    ],
                    end_critical_statuses=None,
                ),
            ],
        )

        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []
        incarceration_periods: List[StateIncarcerationPeriod] = []

        expected_events = [
            create_start_event_from_period(
                attr.evolve(
                    supervision_period,
                    start_date=date(2019, 9, 6),
                    termination_date=date(2019, 10, 3),
                    supervision_site=None,
                ),
                in_supervision_population_on_date=False,
            ),
            create_termination_event_from_period(
                attr.evolve(
                    supervision_period,
                    start_date=date(2019, 9, 6),
                    termination_date=date(2019, 10, 3),
                    supervision_site=None,
                ),
            ),
            create_start_event_from_period(
                attr.evolve(
                    supervision_period,
                    start_date=date(2019, 11, 9),
                    supervision_site=None,
                ),
                in_supervision_population_on_date=False,
            ),
        ]

        supervision_events = self._test_find_supervision_events(
            [supervision_sentence],
            incarceration_sentences,
            [supervision_period],
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            [
                {
                    "agent_id": 000,
                    "agent_external_id": "XXX",
                    "supervision_period_id": supervision_period.supervision_period_id,
                    "agent_start_date": date(2019, 10, 3),
                    "agent_end_date": date(2019, 11, 9),
                }
            ],
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
            state_code_override="US_MO",
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_supervision_period_mid_month_us_mo_supervision_span_shows_supervision_eom(
        self,
    ) -> None:

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp2",
            state_code="US_MO",
            supervision_site="DISTRICTX",
            supervising_officer="AGENTX",
            start_date=date(2019, 10, 3),
            termination_date=date(2019, 10, 20),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=111,
                start_date=supervision_period.start_date,
                external_id="ss1",
                state_code="US_MO",
                supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                status=StateSentenceStatus.SERVING,
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=date(2019, 9, 6),
                    end_date=date(2019, 10, 15),
                    supervision_type=None,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=date(2019, 9, 6),
                            status_code="15I1000",
                            status_description="New Court Probation",
                        )
                    ],
                    end_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=date(2019, 10, 15),
                            status_code="99O1000",
                            status_description="Court Probation Discharge",
                        )
                    ],
                ),
                SupervisionTypeSpan(
                    start_date=date(2019, 10, 15),
                    end_date=None,
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    start_critical_statuses=[
                        UsMoSentenceStatus(
                            sentence_external_id="123",
                            sentence_status_external_id="test-status-2",
                            status_date=date(2019, 10, 15),
                            status_code="99O1000",
                            status_description="Court Probation Discharge",
                        )
                    ],
                    end_critical_statuses=None,
                ),
            ],
        )

        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []
        incarceration_periods: List[StateIncarcerationPeriod] = []

        expected_events = [
            create_termination_event_from_period(
                supervision_period,
                UsMoSupervisionDelegate(),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                supervising_district_external_id=supervision_period.supervision_site,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                in_supervision_population_on_date=True,
                termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
                supervising_officer_external_id="XXX",
            ),
            create_start_event_from_period(
                attr.evolve(supervision_period, start_date=date(2019, 10, 15)),
                UsMoSupervisionDelegate(),
                supervising_officer_external_id="XXX",
            ),
            create_start_event_from_period(
                attr.evolve(
                    supervision_period,
                    start_date=date(2019, 10, 20),
                    supervision_site=None,
                    supervising_officer=None,
                ),
                UsMoSupervisionDelegate(),
                in_supervision_population_on_date=False,
                admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                attr.evolve(supervision_period, start_date=date(2019, 10, 15)),
                StateSupervisionPeriodSupervisionType.PROBATION,
                level_1_supervision_location_external_id=supervision_period.supervision_site,
                level_2_supervision_location_external_id=None,
                supervising_officer_external_id="XXX",
            )
        )

        supervision_events = self._test_find_supervision_events(
            [supervision_sentence],
            incarceration_sentences,
            [supervision_period],
            incarceration_periods,
            assessments,
            violation_responses,
            supervision_contacts,
            [
                {
                    "agent_id": 000,
                    "agent_external_id": "XXX",
                    "supervision_period_id": supervision_period.supervision_period_id,
                    "agent_start_date": date(2019, 10, 3),
                    "agent_end_date": date(2019, 10, 20),
                }
            ],
            DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST,
            state_code_override="US_MO",
        )

        self.assertCountEqual(expected_events, supervision_events)

    @freeze_time("2000-01-01")
    def test_find_supervision_events_dates_in_future(self) -> None:
        """Tests the find_supervision_events function for a supervision period where the termination date is
        in the future."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
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
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        supervision_period_future = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
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
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            start_date=date(1990, 1, 1),
            external_id="ss1",
            state_code="US_XX",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            completion_date=date(2018, 5, 19),
        )

        supervision_sentences = [supervision_sentence]
        supervision_periods = [supervision_period, supervision_period_future]
        incarceration_periods: List[StateIncarcerationPeriod] = []
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events: List[SupervisionEvent] = [
            create_start_event_from_period(
                supervision_period,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                end_date=date.today() + relativedelta(days=1),
                supervision_type=supervision_type,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            )
        )

        supervision_events = self._test_find_supervision_events(
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

        self.assertCountEqual(expected_events, supervision_events)


class TestFindPopulationEventsForSupervisionPeriod(unittest.TestCase):
    """Tests for the find_population_events_for_supervision_period function."""

    def setUp(self) -> None:
        self.maxDiff = None

        self.identifier = identifier.SupervisionIdentifier()

        self.person = StatePerson.new_with_defaults(state_code="US_XX")

    def test_find_population_events_for_supervision_period_revocation_no_termination(
        self,
    ) -> None:
        """Tests the find_population_events_for_supervision_period function
        when there is an incarceration period with a revocation admission,
        where there is no termination_date on the supervision_period, and
        no release_date on the incarceration_period."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2003, 7, 5),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2003, 10, 10),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2003, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            status=StateSentenceStatus.SERVING,
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        assessments: List[StateAssessment] = []

        violation_reports: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_sentences = [supervision_sentence]

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_reports,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
                violation_delegate=UsXxViolationDelegate(),
                supervision_delegate=UsXxSupervisionDelegate(),
            )
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        self.assertCountEqual(
            expected_population_events(
                supervision_period,
                supervision_type,
                incarceration_period.admission_date,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    end_date_override=date(2003, 10, 10),
                ),
            ),
            supervision_events,
        )

    def test_find_population_events_for_supervision_period_incarceration_ends_same_month(
        self,
    ) -> None:
        """Tests the find_population_events_for_supervision_period function when there is an incarceration period with a
        revocation admission before the supervision period's termination_date, and the supervision_period and the
        incarceration_period end in the same month."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 4, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2018, 5, 22),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            completion_date=date(2018, 5, 19),
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        incarceration_sentences: List[StateIncarcerationSentence] = []
        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        supervision_sentences = [supervision_sentence]

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = expected_population_events(
            supervision_period,
            supervision_type,
            incarceration_period.admission_date,
            case_compliances=_generate_case_compliances(
                person=self.person,
                start_date=supervision_period.start_date,
                supervision_period=supervision_period,
            ),
        )

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_responses,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
                violation_delegate=UsXxViolationDelegate(),
                supervision_delegate=UsXxSupervisionDelegate(),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    @freeze_time("2019-11-03")
    def test_find_population_events_for_supervision_period_nested_revocation_no_termination(
        self,
    ) -> None:
        """Tests the find_population_events_for_supervision_period function when there is an incarceration period with
        a revocation admission, a stay in prison, and a continued supervision period after release from incarceration
        that has still not terminated."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2019, 3, 5),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
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
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 10, 17),
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            status=StateSentenceStatus.SERVING,
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )
        assessments: List[StateAssessment] = []

        violation_reports: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_sentences = [supervision_sentence]

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = expected_population_events(
            supervision_period,
            supervision_type,
            incarceration_period.admission_date,
            case_compliances=_generate_case_compliances(
                person=self.person,
                start_date=supervision_period.start_date,
                supervision_period=supervision_period,
                end_date_override=date(2019, 11, 2),
            ),
        )

        expected_events.extend(
            expected_population_events(
                attr.evolve(
                    supervision_period, start_date=incarceration_period.release_date
                ),
                supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    end_date_override=date(2019, 11, 3),
                ),
            )
        )

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_reports,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
                violation_delegate=UsXxViolationDelegate(),
                supervision_delegate=UsXxSupervisionDelegate(),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_population_events_for_supervision_period_admission_no_revocation(
        self,
    ) -> None:
        """Tests the find_population_events_for_supervision_period function when there is an incarceration period with a
        non-revocation admission before the supervision period's termination_date."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 6, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 6, 2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            completion_date=date(2018, 7, 19),
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        assessments: List[StateAssessment] = []

        violation_reports: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_sentences = [supervision_sentence]

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = expected_population_events(
            supervision_period,
            supervision_type,
            incarceration_period.admission_date,
            case_compliances=_generate_case_compliances(
                person=self.person,
                start_date=supervision_period.start_date,
                supervision_period=supervision_period,
            ),
        )

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_reports,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
                violation_delegate=UsXxViolationDelegate(),
                supervision_delegate=UsXxSupervisionDelegate(),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_population_events_for_supervision_period_multiple_years(self) -> None:
        """Tests the find_population_events_for_supervision_period function when the supervision period overlaps
        multiple years, and there is an incarceration period during this time that also overlaps multiple years."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 3, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2008, 6, 2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2009, 12, 3),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2007, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            completion_date=date(2010, 3, 19),
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        assessments: List[StateAssessment] = []

        violation_reports: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_sentences = [supervision_sentence]

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = expected_population_events(
            supervision_period,
            supervision_type,
            incarceration_period.admission_date,
            case_compliances=_generate_case_compliances(
                person=self.person,
                start_date=supervision_period.start_date,
                supervision_period=supervision_period,
            ),
        )

        expected_events.extend(
            expected_population_events(
                attr.evolve(
                    supervision_period, start_date=incarceration_period.release_date
                ),
                supervision_type,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_reports,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
                violation_delegate=UsXxViolationDelegate(),
                supervision_delegate=UsXxSupervisionDelegate(),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_population_events_for_supervision_period_ends_on_first(self) -> None:
        """Tests the find_population_events_for_supervision_period function for a
        supervision period with no incarceration periods, where the supervision
        period ends on the 1st day of a month."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2001, 1, 5),
            termination_date=date(2001, 7, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2000, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            completion_date=date(2001, 7, 1),
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        assessments: List[StateAssessment] = []

        violation_reports: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_sentences = [supervision_sentence]

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = expected_population_events(
            supervision_period,
            supervision_type,
        )

        self.assertEqual(
            expected_events[-1],
            SupervisionPopulationEvent(
                state_code=supervision_period.state_code,
                year=2001,
                month=6,
                event_date=date(2001, 6, 30),
                supervision_type=supervision_type,
                supervision_level=supervision_period.supervision_level,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
        )

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_reports,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
                violation_delegate=UsXxViolationDelegate(),
                supervision_delegate=UsXxSupervisionDelegate(),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_population_events_for_supervision_period_ends_on_last(self) -> None:
        """Tests the find_population_events_for_supervision_period function for a
        supervision period with no incarceration periods, where the supervision
        period ends on the last day of a month."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2001, 1, 5),
            termination_date=date(2001, 6, 30),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2000, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            completion_date=date(2001, 6, 30),
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        assessments: List[StateAssessment] = []

        violation_reports: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_sentences = [supervision_sentence]

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = expected_population_events(
            supervision_period,
            supervision_type,
        )

        self.assertEqual(
            expected_events[-1],
            SupervisionPopulationEvent(
                state_code=supervision_period.state_code,
                year=2001,
                month=6,
                event_date=date(2001, 6, 29),
                supervision_type=supervision_type,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_level=supervision_period.supervision_level,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
        )

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_reports,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
                violation_delegate=UsXxViolationDelegate(),
                supervision_delegate=UsXxSupervisionDelegate(),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_population_events_for_supervision_period_start_end_same_day(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2001, 3, 3),
            termination_date=date(2001, 3, 3),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2000, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            completion_date=date(2001, 3, 3),
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        assessments: List[StateAssessment] = []

        violation_reports: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_sentences: List[StateSupervisionSentence] = [supervision_sentence]

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_reports,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
                violation_delegate=UsXxViolationDelegate(),
                supervision_delegate=UsXxSupervisionDelegate(),
            )
        )

        self.assertCountEqual([], supervision_events)

    def test_find_population_events_for_supervision_period_multiple_assessments(
        self,
    ) -> None:
        """Tests the find_population_events_for_supervision_period function
        when there is an incarceration period with a revocation admission,
        a stay in prison, a continued supervision period after release
        from incarceration, and multiple assessments over this time period."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 11),
            termination_date=date(2018, 12, 10),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2018, 10, 27),
        )

        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=24,
            assessment_level=StateAssessmentLevel.MEDIUM,
            assessment_date=date(2018, 10, 27),
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            status=StateSentenceStatus.COMPLETED,
            completion_date=date(2018, 12, 10),
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        assessments = [assessment_1, assessment_2]

        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_sentences = [supervision_sentence]

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = expected_population_events(
            supervision_period,
            supervision_type,
            end_date=incarceration_period.admission_date,
            assessment_score=assessment_1.assessment_score,
            assessment_level=assessment_1.assessment_level,
            assessment_type=assessment_1.assessment_type,
            assessment_score_bucket=StateAssessmentLevel.HIGH.value,
        )

        expected_events.extend(
            expected_population_events(
                attr.evolve(
                    supervision_period, start_date=incarceration_period.release_date
                ),
                supervision_type,
                assessment_score=assessment_2.assessment_score,
                assessment_level=assessment_2.assessment_level,
                assessment_type=assessment_2.assessment_type,
                assessment_score_bucket=StateAssessmentLevel.MEDIUM.value,
            )
        )

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_responses,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
                violation_delegate=UsXxViolationDelegate(),
                supervision_delegate=UsXxSupervisionDelegate(),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_population_events_for_supervision_period_assessment_year_before(
        self,
    ) -> None:
        """Tests the find_population_events_for_supervision_period function
        when there is no revocation and the assessment date is the year before
        the start of the supervision."""

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 3, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
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
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            completion_date=date(2018, 3, 19),
            status=StateSentenceStatus.COMPLETED,
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        assessments = [assessment]
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_sentences = [supervision_sentence]

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = expected_population_events(
            supervision_period,
            supervision_type,
            assessment_score=assessment.assessment_score,
            assessment_level=assessment.assessment_level,
            assessment_type=assessment.assessment_type,
            assessment_score_bucket="24-29",
            case_compliances=_generate_case_compliances(
                person=self.person,
                start_date=supervision_period.start_date,
                supervision_period=supervision_period,
                assessments=assessments,
            ),
        )

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_sentences,
                incarceration_sentences,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_responses,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
                violation_delegate=UsXxViolationDelegate(),
                supervision_delegate=UsXxSupervisionDelegate(),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_population_events_for_supervision_period_with_a_supervision_downgrade(
        self,
    ) -> None:
        """Tests the find_population_events_for_supervision_period function
        when a supervision level downgrade has taken place."""

        supervision_periods = [
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 3, 19),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.HIGH,
            ),
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=123,
                external_id="sp2",
                state_code="US_XX",
                start_date=date(2018, 3, 19),
                termination_date=date(2018, 4, 30),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MEDIUM,
            ),
        ]

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            completion_date=date(2018, 4, 30),
            status=StateSentenceStatus.COMPLETED,
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=supervision_periods
        )

        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_sentences = [supervision_sentence]

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = expected_population_events(
            supervision_periods[1],
            supervision_type,
            supervision_downgrade_date=date(2018, 3, 19),
            supervision_downgrade_occurred=True,
            previous_supervision_level=StateSupervisionLevel.HIGH,
            case_compliances=_generate_case_compliances(
                person=self.person,
                start_date=supervision_periods[0].start_date,
                supervision_period=supervision_periods[1],
                next_recommended_assessment_date=supervision_periods[0].start_date,
            ),
        )

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_sentences,
                incarceration_sentences,
                supervision_periods[1],
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_responses,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
                violation_delegate=UsXxViolationDelegate(),
                supervision_delegate=UsXxSupervisionDelegate(),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_population_events_for_supervision_period_without_a_supervision_downgrade_out_of_time_frame(
        self,
    ) -> None:
        """Tests the find_population_events_for_supervision_period function
        when a supervision level downgrade has taken place."""

        supervision_periods = [
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id="sp1",
                state_code="US_XX",
                start_date=date(2010, 1, 5),
                termination_date=date(2010, 3, 19),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MAXIMUM,
            ),
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=123,
                external_id="sp2",
                state_code="US_XX",
                start_date=date(2018, 3, 19),
                termination_date=date(2018, 4, 30),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MEDIUM,
            ),
        ]

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            completion_date=date(2018, 4, 30),
            status=StateSentenceStatus.COMPLETED,
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=supervision_periods
        )

        assessments: List[StateAssessment] = []
        violation_responses: List[StateSupervisionViolationResponse] = []
        supervision_contacts: List[StateSupervisionContact] = []
        incarceration_sentences: List[StateIncarcerationSentence] = []

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                [supervision_sentence],
                incarceration_sentences,
                supervision_periods[1],
                supervision_period_index,
                incarceration_period_index,
                assessments,
                violation_responses,
                supervision_contacts,
                DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
                violation_delegate=UsXxViolationDelegate(),
                supervision_delegate=UsXxSupervisionDelegate(),
            )
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = expected_population_events(
            supervision_periods[1],
            supervision_type,
            case_compliances=_generate_case_compliances(
                person=self.person,
                start_date=supervision_periods[0].start_date,
                supervision_period=supervision_periods[1],
                next_recommended_assessment_date=supervision_periods[0].start_date,
            ),
        )

        self.assertCountEqual(expected_events, supervision_events)


class TestClassifySupervisionSuccess(unittest.TestCase):
    """Tests the classify_supervision_success function."""

    def setUp(self) -> None:
        self.default_supervision_period_to_judicial_district_associations = {
            _DEFAULT_SUPERVISION_PERIOD_ID: DEFAULT_SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_LIST[
                0
            ]
        }
        self.identifier = identifier.SupervisionIdentifier()

    def test_classify_supervision_success(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_sentence_projected_completion_date = date(2018, 12, 25)
        supervision_sentence_completion_date = date(2018, 12, 25)
        supervision_sentence_start_date = date(2017, 1, 1)
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=supervision_sentence_start_date,
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            projected_completion_date=supervision_sentence_projected_completion_date,
            completion_date=supervision_sentence_completion_date,
        )

        supervision_sentences = [supervision_sentence]

        projected_completion_events = self.identifier._classify_supervision_success(
            supervision_sentences,
            [],
            [supervision_period],
            default_normalized_ip_index_for_tests(),
            UsXxSupervisionDelegate(),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(1, len(projected_completion_events))

        supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertEqual(
            [
                ProjectedSupervisionCompletionEvent(
                    state_code=supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        supervision_sentence_projected_completion_date
                    ),
                    supervision_type=supervision_type,
                    successful_completion=True,
                    incarcerated_during_sentence=False,
                    case_type=StateSupervisionCaseType.GENERAL,
                    sentence_days_served=(
                        supervision_sentence_completion_date
                        - supervision_sentence_start_date
                    ).days,
                )
            ],
            projected_completion_events,
        )

    def test_classify_supervision_success_unsuccessful(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=date(2018, 6, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2018, 6, 21),
        )

        supervision_sentence_projected_completion_date = date(2018, 12, 25)
        supervision_sentence_completion_date = date(2018, 12, 25)
        supervision_sentence_start_date = date(2017, 1, 1)
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=supervision_sentence_start_date,
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            projected_completion_date=supervision_sentence_projected_completion_date,
            completion_date=supervision_sentence_completion_date,
        )

        supervision_sentences = [supervision_sentence]
        incarceration_periods = [incarceration_period]

        projected_completion_events = self.identifier._classify_supervision_success(
            supervision_sentences,
            [],
            [supervision_period],
            default_normalized_ip_index_for_tests(
                incarceration_periods=incarceration_periods
            ),
            UsXxSupervisionDelegate(),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(1, len(projected_completion_events))

        supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertEqual(
            [
                ProjectedSupervisionCompletionEvent(
                    state_code=supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        supervision_sentence_projected_completion_date
                    ),
                    supervision_type=supervision_type,
                    successful_completion=False,
                    incarcerated_during_sentence=True,
                    case_type=StateSupervisionCaseType.GENERAL,
                    sentence_days_served=(
                        supervision_sentence_completion_date
                        - supervision_sentence_start_date
                    ).days,
                )
            ],
            projected_completion_events,
        )

    def test_classify_supervision_success_multiple_periods(self) -> None:
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 8, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_XX",
            start_date=date(2018, 9, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_sentence_projected_completion_date = date(2018, 12, 25)
        supervision_sentence_completion_date = date(2018, 12, 25)
        supervision_sentence_start_date = date(2017, 1, 1)
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=supervision_sentence_start_date,
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            projected_completion_date=supervision_sentence_projected_completion_date,
            completion_date=supervision_sentence_completion_date,
        )

        supervision_sentences = [supervision_sentence]

        projected_completion_events = self.identifier._classify_supervision_success(
            supervision_sentences,
            [],
            [first_supervision_period, second_supervision_period],
            default_normalized_ip_index_for_tests(),
            UsXxSupervisionDelegate(),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(1, len(projected_completion_events))

        second_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertEqual(
            [
                ProjectedSupervisionCompletionEvent(
                    state_code=second_supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        supervision_sentence_projected_completion_date
                    ),
                    supervision_type=second_supervision_type,
                    case_type=StateSupervisionCaseType.GENERAL,
                    successful_completion=False,
                    incarcerated_during_sentence=False,
                    sentence_days_served=(
                        supervision_sentence_completion_date
                        - supervision_sentence_start_date
                    ).days,
                )
            ],
            projected_completion_events,
        )

    def test_classify_supervision_success_multiple_sentences(self) -> None:
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 8, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_XX",
            start_date=date(2018, 9, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        first_supervision_sentence_projected_completion_date = date(2018, 8, 19)
        first_supervision_sentence_completion_date = date(2018, 8, 19)
        first_supervision_sentence_start_date = date(2017, 1, 1)
        first_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=first_supervision_sentence_start_date,
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            projected_completion_date=first_supervision_sentence_projected_completion_date,
            completion_date=first_supervision_sentence_completion_date,
        )

        second_supervision_sentence_projected_completion_date = date(2018, 12, 25)
        second_supervision_sentence_completion_date = date(2018, 12, 25)
        second_supervision_sentence_start_date = date(2017, 1, 1)
        second_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=second_supervision_sentence_start_date,
            external_id="ss2",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            projected_completion_date=second_supervision_sentence_projected_completion_date,
            completion_date=second_supervision_sentence_completion_date,
        )

        supervision_sentences = [
            first_supervision_sentence,
            second_supervision_sentence,
        ]

        projected_completion_events = self.identifier._classify_supervision_success(
            supervision_sentences,
            [],
            [first_supervision_period, second_supervision_period],
            default_normalized_ip_index_for_tests(),
            UsXxSupervisionDelegate(),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(2, len(projected_completion_events))

        first_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE
        second_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertEqual(
            [
                ProjectedSupervisionCompletionEvent(
                    state_code=first_supervision_period.state_code,
                    year=2018,
                    month=8,
                    event_date=last_day_of_month(
                        first_supervision_sentence_projected_completion_date
                    ),
                    supervision_type=first_supervision_type,
                    successful_completion=True,
                    incarcerated_during_sentence=False,
                    case_type=StateSupervisionCaseType.GENERAL,
                    sentence_days_served=(
                        first_supervision_sentence_completion_date
                        - first_supervision_sentence_start_date
                    ).days,
                ),
                ProjectedSupervisionCompletionEvent(
                    state_code=second_supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        second_supervision_sentence_projected_completion_date
                    ),
                    supervision_type=second_supervision_type,
                    case_type=StateSupervisionCaseType.GENERAL,
                    successful_completion=False,
                    incarcerated_during_sentence=False,
                    sentence_days_served=(
                        second_supervision_sentence_completion_date
                        - second_supervision_sentence_start_date
                    ).days,
                ),
            ],
            projected_completion_events,
        )

    def test_classify_supervision_success_multiple_sentence_types(self) -> None:
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 8, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=222,
            external_id="sp2",
            state_code="US_XX",
            start_date=date(2018, 9, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        first_supervision_sentence_projected_completion_date = date(2018, 12, 25)
        first_supervision_sentence_completion_date = date(2018, 12, 25)
        first_supervision_sentence_start_date = date(2017, 1, 1)
        first_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=first_supervision_sentence_start_date,
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            projected_completion_date=first_supervision_sentence_projected_completion_date,
            completion_date=first_supervision_sentence_completion_date,
        )

        second_supervision_sentence_projected_completion_date = date(2018, 12, 25)
        second_supervision_sentence_completion_date = date(2018, 12, 25)
        second_supervision_sentence_start_date = date(2017, 1, 1)
        second_supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=second_supervision_sentence_start_date,
            external_id="ss2",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
            projected_completion_date=second_supervision_sentence_projected_completion_date,
            completion_date=second_supervision_sentence_completion_date,
        )

        supervision_sentences = [
            first_supervision_sentence,
            second_supervision_sentence,
        ]

        projected_completion_events = self.identifier._classify_supervision_success(
            supervision_sentences,
            [],
            [first_supervision_period, second_supervision_period],
            default_normalized_ip_index_for_tests(),
            UsXxSupervisionDelegate(),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(2, len(projected_completion_events))

        first_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE
        second_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        self.assertEqual(
            [
                ProjectedSupervisionCompletionEvent(
                    state_code=first_supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        first_supervision_sentence_projected_completion_date
                    ),
                    supervision_type=first_supervision_type,
                    case_type=StateSupervisionCaseType.GENERAL,
                    successful_completion=True,
                    incarcerated_during_sentence=False,
                    sentence_days_served=(
                        first_supervision_sentence_completion_date
                        - first_supervision_sentence_start_date
                    ).days,
                ),
                ProjectedSupervisionCompletionEvent(
                    state_code=second_supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        second_supervision_sentence_projected_completion_date
                    ),
                    supervision_type=second_supervision_type,
                    case_type=StateSupervisionCaseType.GENERAL,
                    successful_completion=False,
                    incarcerated_during_sentence=False,
                    sentence_days_served=(
                        second_supervision_sentence_completion_date
                        - second_supervision_sentence_start_date
                    ).days,
                ),
            ],
            projected_completion_events,
        )

    def test_classify_supervision_success_officer_district(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_site="DISTRICTX",
        )

        supervision_sentence_projected_completion_date = date(2018, 12, 25)
        supervision_sentence_completion_date = date(2018, 12, 25)
        supervision_sentence_start_date = date(2017, 1, 1)
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=supervision_sentence_start_date,
            external_id="ss",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            projected_completion_date=supervision_sentence_projected_completion_date,
            completion_date=supervision_sentence_completion_date,
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

        projected_completion_events = self.identifier._classify_supervision_success(
            supervision_sentences,
            [],
            [supervision_period],
            default_normalized_ip_index_for_tests(),
            UsXxSupervisionDelegate(),
            supervision_period_agent_association,
            supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(1, len(projected_completion_events))

        supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertEqual(
            [
                ProjectedSupervisionCompletionEvent(
                    state_code=supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        supervision_sentence_projected_completion_date
                    ),
                    supervision_type=supervision_type,
                    case_type=StateSupervisionCaseType.GENERAL,
                    successful_completion=True,
                    incarcerated_during_sentence=False,
                    supervising_officer_external_id="AGENTX",
                    supervising_district_external_id="DISTRICTX",
                    level_1_supervision_location_external_id="DISTRICTX",
                    judicial_district_code="NORTHWEST",
                    sentence_days_served=(
                        supervision_sentence_completion_date
                        - supervision_sentence_start_date
                    ).days,
                )
            ],
            projected_completion_events,
        )

    def test_classify_supervision_success_empty_officer_district(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_sentence_projected_completion_date = date(2018, 12, 25)
        supervision_sentence_completion_date = date(2018, 12, 25)
        supervision_sentence_start_date = date(2017, 1, 1)
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=supervision_sentence_start_date,
            external_id="ss",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            projected_completion_date=supervision_sentence_projected_completion_date,
            completion_date=supervision_sentence_completion_date,
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

        projected_completion_events = self.identifier._classify_supervision_success(
            supervision_sentences,
            [],
            [supervision_period],
            default_normalized_ip_index_for_tests(),
            UsXxSupervisionDelegate(),
            supervision_period_agent_association,
            supervision_period_to_judicial_district_associations,
        )
        self.assertEqual(1, len(projected_completion_events))

        supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertEqual(
            [
                ProjectedSupervisionCompletionEvent(
                    state_code=supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        supervision_sentence_projected_completion_date
                    ),
                    supervision_type=supervision_type,
                    case_type=StateSupervisionCaseType.GENERAL,
                    successful_completion=True,
                    incarcerated_during_sentence=False,
                    sentence_days_served=(
                        supervision_sentence_completion_date
                        - supervision_sentence_start_date
                    ).days,
                    supervising_officer_external_id=None,
                    supervising_district_external_id=None,
                    judicial_district_code=None,
                )
            ],
            projected_completion_events,
        )

    def test_classify_supervision_success_period_ends_after_projected_completion(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        start_date = date(2017, 1, 1)
        projected_completion_date = date(2018, 12, 25)
        completion_date = date(2018, 12, 19)
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=start_date,
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            projected_completion_date=projected_completion_date,
            completion_date=completion_date,
        )

        supervision_sentences = [supervision_sentence]

        projected_completion_events = self.identifier._classify_supervision_success(
            supervision_sentences,
            [],
            [supervision_period],
            default_normalized_ip_index_for_tests(),
            UsXxSupervisionDelegate(),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(1, len(projected_completion_events))

        supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertEqual(
            [
                ProjectedSupervisionCompletionEvent(
                    state_code=supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(projected_completion_date),
                    supervision_type=supervision_type,
                    successful_completion=True,
                    incarcerated_during_sentence=False,
                    case_type=StateSupervisionCaseType.GENERAL,
                    sentence_days_served=(completion_date - start_date).days,
                )
            ],
            projected_completion_events,
        )

    def test_classify_supervision_success_exclude_termination_reason_death(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DEATH,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            projected_completion_date=date(2018, 12, 25),
            completion_date=date(2018, 12, 25),
        )

        supervision_sentences = [supervision_sentence]
        incarceration_periods: List[StateIncarcerationPeriod] = []
        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=incarceration_periods
        )

        projected_completion_events = self.identifier._classify_supervision_success(
            supervision_sentences,
            [],
            [supervision_period],
            incarceration_period_index,
            UsXxSupervisionDelegate(),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(0, len(projected_completion_events))

    def test_classify_supervision_success_exclude_no_completion_date(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.SUSPENSION,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            projected_completion_date=date(2018, 12, 25),
        )

        supervision_sentences = [supervision_sentence]
        incarceration_periods: List[StateIncarcerationPeriod] = []
        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=incarceration_periods
        )

        projected_completion_events = self.identifier._classify_supervision_success(
            supervision_sentences,
            [],
            [supervision_period],
            incarceration_period_index,
            UsXxSupervisionDelegate(),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(0, len(projected_completion_events))

    def test_classify_supervision_success_exclude_completion_before_start(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.SUSPENSION,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            completion_date=date(2016, 10, 3),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            projected_completion_date=date(2018, 12, 25),
        )

        supervision_sentences = [supervision_sentence]
        incarceration_periods: List[StateIncarcerationPeriod] = []
        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=incarceration_periods
        )

        projected_completion_events = self.identifier._classify_supervision_success(
            supervision_sentences,
            [],
            [supervision_period],
            incarceration_period_index,
            UsXxSupervisionDelegate(),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(0, len(projected_completion_events))

    def test_classify_supervision_success_was_incarcerated_during_sentence(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        projected_completion_date = date(2018, 12, 25)
        start_date = date(2017, 1, 1)
        completion_date = date(2018, 12, 20)
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=start_date,
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            completion_date=completion_date,
            projected_completion_date=projected_completion_date,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_XX",
            incarceration_period_id=111,
            admission_date=date(2017, 6, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2017, 10, 5),
        )

        supervision_sentences = [supervision_sentence]
        incarceration_periods = [incarceration_period]

        projected_completion_events = self.identifier._classify_supervision_success(
            supervision_sentences,
            [],
            [supervision_period],
            default_normalized_ip_index_for_tests(
                incarceration_periods=incarceration_periods
            ),
            UsXxSupervisionDelegate(),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(1, len(projected_completion_events))

        supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        sentence_length = (completion_date - start_date).days

        self.assertEqual(
            [
                ProjectedSupervisionCompletionEvent(
                    state_code=supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(projected_completion_date),
                    supervision_type=supervision_type,
                    successful_completion=True,
                    incarcerated_during_sentence=True,
                    case_type=StateSupervisionCaseType.GENERAL,
                    sentence_days_served=sentence_length,
                )
            ],
            projected_completion_events,
        )

    def test_classify_supervision_success_with_incarceration_sentences(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        start_date = date(2017, 1, 1)
        completion_date = date(2018, 12, 25)
        max_length_days = (completion_date - start_date).days
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=111,
            start_date=start_date,
            external_id="is1",
            status=StateSentenceStatus.COMPLETED,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            max_length_days=max_length_days,
            completion_date=completion_date,
        )

        incarceration_sentences: List[StateIncarcerationSentence] = [
            incarceration_sentence
        ]

        projected_completion_events = self.identifier._classify_supervision_success(
            [],
            incarceration_sentences,
            [supervision_period],
            default_normalized_ip_index_for_tests(),
            UsXxSupervisionDelegate(),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(1, len(projected_completion_events))

        supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertEqual(
            [
                ProjectedSupervisionCompletionEvent(
                    state_code=supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(
                        start_date + datetime.timedelta(days=max_length_days)
                    ),
                    supervision_type=supervision_type,
                    successful_completion=True,
                    incarcerated_during_sentence=False,
                    case_type=StateSupervisionCaseType.GENERAL,
                    sentence_days_served=(completion_date - start_date).days,
                )
            ],
            projected_completion_events,
        )

    def test_classify_supervision_success_with_incarceration_sentence_future_start_date(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date.today() + datetime.timedelta(days=10),
            termination_date=date.today() + datetime.timedelta(days=1000),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=111,
            start_date=date.today() + datetime.timedelta(days=10),
            external_id="is1",
            status=StateSentenceStatus.COMPLETED,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            max_length_days=1000,
            completion_date=date.today() + datetime.timedelta(days=1000),
        )

        incarceration_sentences: List[StateIncarcerationSentence] = [
            incarceration_sentence
        ]

        projected_completion_events = self.identifier._classify_supervision_success(
            [],
            incarceration_sentences,
            [supervision_period],
            default_normalized_ip_index_for_tests(),
            UsXxSupervisionDelegate(),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(0, len(projected_completion_events))

    def test_classify_supervision_success_with_incarceration_sentence_without_max_length_day(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="is1",
            status=StateSentenceStatus.COMPLETED,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            completion_date=date(2018, 12, 25),
        )

        incarceration_sentences: List[StateIncarcerationSentence] = [
            incarceration_sentence
        ]

        projected_completion_events = self.identifier._classify_supervision_success(
            [],
            incarceration_sentences,
            [supervision_period],
            default_normalized_ip_index_for_tests(),
            UsXxSupervisionDelegate(),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(0, len(projected_completion_events))

    def test_classify_supervision_success_with_large_max_length_date_incarceration_sentence(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="is1",
            status=StateSentenceStatus.COMPLETED,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            max_length_days=100000000,
            completion_date=date(2018, 12, 25),
        )

        incarceration_sentences: List[StateIncarcerationSentence] = [
            incarceration_sentence
        ]

        projected_completion_events = self.identifier._classify_supervision_success(
            [],
            incarceration_sentences,
            [supervision_period],
            default_normalized_ip_index_for_tests(),
            UsXxSupervisionDelegate(),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(0, len(projected_completion_events))

    def test_classify_supervision_success_with_incarceration_and_supervision_sentences(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_sentence_completion_date = date(2018, 12, 25)
        supervision_sentence_start_date = date(2017, 1, 1)
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            supervision_sentence_id=111,
            start_date=supervision_sentence_start_date,
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            projected_completion_date=supervision_sentence_completion_date,
            completion_date=supervision_sentence_completion_date,
        )

        supervision_sentences = [supervision_sentence]

        incarceration_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2007, 6, 3),
            termination_date=date(2007, 12, 3),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        start_date = date(2006, 2, 21)
        completion_date = date(2007, 12, 3)
        max_length_days = 730
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=111,
            start_date=start_date,
            external_id="is1",
            status=StateSentenceStatus.COMPLETED,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            max_length_days=max_length_days,
            completion_date=completion_date,
        )

        incarceration_sentences: List[StateIncarcerationSentence] = [
            incarceration_sentence
        ]

        projected_completion_events = self.identifier._classify_supervision_success(
            supervision_sentences,
            incarceration_sentences,
            [supervision_period, incarceration_supervision_period],
            default_normalized_ip_index_for_tests(),
            UsXxSupervisionDelegate(),
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            self.default_supervision_period_to_judicial_district_associations,
        )

        self.assertEqual(2, len(projected_completion_events))

        supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertEqual(
            [
                ProjectedSupervisionCompletionEvent(
                    state_code=supervision_period.state_code,
                    year=2018,
                    month=12,
                    event_date=last_day_of_month(supervision_sentence_completion_date),
                    supervision_type=supervision_type,
                    successful_completion=True,
                    incarcerated_during_sentence=False,
                    case_type=StateSupervisionCaseType.GENERAL,
                    sentence_days_served=(
                        supervision_sentence_completion_date
                        - supervision_sentence_start_date
                    ).days,
                ),
                ProjectedSupervisionCompletionEvent(
                    state_code=supervision_period.state_code,
                    year=2008,
                    month=2,
                    event_date=last_day_of_month(
                        start_date + datetime.timedelta(days=max_length_days)
                    ),
                    supervision_type=supervision_type,
                    successful_completion=True,
                    incarcerated_during_sentence=False,
                    case_type=StateSupervisionCaseType.GENERAL,
                    sentence_days_served=(completion_date - start_date).days,
                ),
            ],
            projected_completion_events,
        )


class TestFindSupervisionTerminationEvent(unittest.TestCase):
    """Tests the find_supervision_termination_event function."""

    def setUp(self) -> None:
        self.identifier = identifier.SupervisionIdentifier()

    def test_find_supervision_termination_event(self) -> None:
        supervision_period_termination_date = date(2019, 5, 19)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        first_assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2018, 3, 10),
        )

        first_reassessment_score = 29
        first_reassessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=first_reassessment_score,
            assessment_date=date(2018, 5, 18),
        )

        last_assessment_score = 19
        last_assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=last_assessment_score,
            assessment_date=date(2019, 5, 10),
        )

        assessments = [first_assessment, first_reassessment, last_assessment]

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()

        violation_responses: List[StateSupervisionViolationResponse] = []

        termination_event = self.identifier._find_supervision_termination_event(
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            violation_delegate=UsXxViolationDelegate(),
            supervision_delegate=UsXxSupervisionDelegate(),
        )

        assessment_score_change = last_assessment_score - first_reassessment_score

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_termination_event = SupervisionTerminationEvent(
            state_code=supervision_period.state_code,
            year=supervision_period_termination_date.year,
            month=supervision_period_termination_date.month,
            event_date=supervision_period_termination_date,
            supervision_type=supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            assessment_score=last_assessment.assessment_score,
            assessment_type=last_assessment.assessment_type,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            termination_reason=supervision_period.termination_reason,
            assessment_score_change=assessment_score_change,
            in_supervision_population_on_date=True,
        )

        self.assertEqual(expected_termination_event, termination_event)

    def test_find_supervision_termination_event_no_assessments(self) -> None:
        supervision_period_termination_date = date(2019, 5, 19)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        assessments: List[StateAssessment] = []

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()

        violation_responses: List[StateSupervisionViolationResponse] = []

        termination_event = self.identifier._find_supervision_termination_event(
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            violation_delegate=UsXxViolationDelegate(),
            supervision_delegate=UsXxSupervisionDelegate(),
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_termination_event = SupervisionTerminationEvent(
            state_code=supervision_period.state_code,
            year=supervision_period_termination_date.year,
            month=supervision_period_termination_date.month,
            event_date=supervision_period_termination_date,
            supervision_type=supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            assessment_score=None,
            assessment_type=None,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            termination_reason=supervision_period.termination_reason,
            assessment_score_change=None,
            in_supervision_population_on_date=True,
        )

        self.assertEqual(expected_termination_event, termination_event)

    def test_find_supervision_termination_event_insufficient_assessments(self) -> None:
        supervision_period_termination_date = date(2019, 5, 19)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        first_assessment = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2018, 3, 10),
        )

        assessments = [first_assessment]

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()

        violation_responses: List[StateSupervisionViolationResponse] = []

        termination_event = self.identifier._find_supervision_termination_event(
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            violation_delegate=UsXxViolationDelegate(),
            supervision_delegate=UsXxSupervisionDelegate(),
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_termination_event = SupervisionTerminationEvent(
            state_code=supervision_period.state_code,
            year=supervision_period_termination_date.year,
            month=supervision_period_termination_date.month,
            event_date=supervision_period_termination_date,
            supervision_type=supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            assessment_score=None,
            assessment_type=None,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            termination_reason=supervision_period.termination_reason,
            assessment_score_change=None,
            in_supervision_population_on_date=True,
        )

        self.assertEqual(expected_termination_event, termination_event)

    def test_find_supervision_termination_event_no_termination(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        assessments: List[StateAssessment] = []

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()

        violation_responses: List[StateSupervisionViolationResponse] = []

        termination_event = self.identifier._find_supervision_termination_event(
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            violation_delegate=UsXxViolationDelegate(),
            supervision_delegate=UsXxSupervisionDelegate(),
        )

        self.assertEqual(None, termination_event)

    def test_find_supervision_termination_event_multiple_in_month(self) -> None:
        """Tests that when multiple supervision periods end in the same month, the earliest start_date and the latest
        termination_date are used as the date boundaries for the assessments, but the termination_date on the
        supervision_period is still on the SupervisionTerminationEvent."""
        first_supervision_period_termination_date = date(2019, 11, 17)
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=first_supervision_period_termination_date,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp2",
            state_code="US_XX",
            start_date=date(2018, 1, 1),
            termination_date=date(2019, 11, 23),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        too_early_assessment = StateAssessment.new_with_defaults(
            state_code="US_CA",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=38,
            assessment_date=date(2017, 12, 10),
        )

        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_CA",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2018, 1, 10),
        )

        assessment_2_score = 29
        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_CA",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=assessment_2_score,
            assessment_date=date(2018, 5, 18),
        )

        assessment_3_score = 19
        assessment_3 = StateAssessment.new_with_defaults(
            state_code="US_CA",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=assessment_3_score,
            assessment_date=date(2019, 11, 21),
        )

        assessments = [too_early_assessment, assessment_1, assessment_2, assessment_3]

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[first_supervision_period, second_supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()

        violation_responses: List[StateSupervisionViolationResponse] = []

        termination_event = self.identifier._find_supervision_termination_event(
            first_supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            violation_delegate=UsXxViolationDelegate(),
            supervision_delegate=UsXxSupervisionDelegate(),
        )

        first_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        # in_supervision_population_on_date=True because another SP was in progress the same day as this termination
        expected_termination_event = SupervisionTerminationEvent(
            state_code=first_supervision_period.state_code,
            year=first_supervision_period_termination_date.year,
            month=first_supervision_period_termination_date.month,
            event_date=first_supervision_period_termination_date,
            in_supervision_population_on_date=True,
            supervision_type=first_supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            assessment_score=assessment_3_score,
            assessment_type=assessment_3.assessment_type,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            termination_reason=first_supervision_period.termination_reason,
            assessment_score_change=(assessment_3_score - assessment_2_score),
        )

        self.assertEqual(expected_termination_event, termination_event)

    def test_find_supervision_termination_event_incarceration_overlaps_full_supervision_period(
        self,
    ) -> None:
        supervision_period_termination_date = date(2018, 5, 19)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()

        termination_event = self.identifier._find_supervision_termination_event(
            supervision_period=supervision_period,
            supervision_period_index=supervision_period_index,
            incarceration_period_index=incarceration_period_index,
            assessments=[],
            violation_responses_for_history=[],
            supervision_period_to_agent_associations=DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            violation_delegate=UsXxViolationDelegate(),
            supervision_delegate=UsXxSupervisionDelegate(),
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_termination_event = SupervisionTerminationEvent(
            state_code=supervision_period.state_code,
            year=supervision_period_termination_date.year,
            month=supervision_period_termination_date.month,
            event_date=supervision_period_termination_date,
            supervision_type=supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            termination_reason=supervision_period.termination_reason,
            in_supervision_population_on_date=True,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
        )

        self.assertEqual(expected_termination_event, termination_event)

    def test_find_supervision_termination_event_overlapping_incarceration(
        self,
    ) -> None:
        """Tests that the SupervisionTerminationEvent has in_incarceration_population_on_date=True
        and in_supervision_population_on_date=False if the termination date occurs
        during an incarceration period for that person."""
        self.run_overlapping_periods_test(
            date(2019, 5, 19), date(2019, 5, 15), True, False
        )

    def test_find_supervision_termination_event_overlapping_incarceration_incarceration_ends_first(
        self,
    ) -> None:
        """Tests that the SupervisionTerminationEvent has in_incarceration_population_on_date=False
        and in_supervision_population_on_date=True if the termination date occurs after
        an incarceration period for that person, even if the supervision and
        incarceration periods were overlapping previously."""
        self.run_overlapping_periods_test(
            date(2019, 5, 19), date(2018, 5, 15), False, True, date(2019, 5, 15)
        )

    def test_find_supervision_termination_event_termination_admission_same_date(
        self,
    ) -> None:
        """Tests that the SupervisionTerminationEvent has in_incarceration_population_on_date=True
        and in_supervision_population_on_date=True if the termination date is on the
        same day as the admission date of an incarceration period for that person."""
        self.run_overlapping_periods_test(
            date(2019, 5, 19), date(2019, 5, 19), True, True
        )

    def test_find_supervision_termination_event_termination_day_before_admission(
        self,
    ) -> None:
        """Tests that the SupervisionTerminationEvent has in_incarceration_population_on_date=False
        and in_supervision_population_on_date=True if the termination date is the day
        before the admission date of an incarceration period for that person."""
        self.run_overlapping_periods_test(
            date(2019, 5, 19), date(2019, 5, 20), False, True
        )

    def test_find_supervision_termination_event_single_day_termination_non_overlapping(
        self,
    ) -> None:
        """Tests that the SupervisionTerminationEvent has in_supervision_population_on_date=True
        and in_incarceration_population_on_date=False if the supervision period is 1 full day and
        non-overlapping with other periods."""
        self.run_overlapping_periods_test(
            date(2018, 3, 6), date(2019, 5, 20), False, True
        )

    def test_find_supervision_termination_event_single_day_termination_overlapping(
        self,
    ) -> None:
        """Tests that the SupervisionTerminationEvent has in_supervision_population_on_date=True
        if the supervision period is 1 full day and non-overlapping with other
        supervision periods, but in_incarceration_population_on_date=True because it's overlapping
        with an incarceration period."""
        self.run_overlapping_periods_test(
            date(2018, 3, 6), date(2018, 3, 6), True, True
        )

    def test_find_supervision_termination_event_single_day_termination_overlapping_with_supervision(
        self,
    ) -> None:
        """Tests that the SupervisionTerminationEvent has in_supervision_population_on_date=True
        if the supervision period is 1 full day and overlapping with another
        supervision period, but in_incarceration_population_on_date=False because it's overlapping
        with no incarceration periods."""
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 3, 6),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 6),
            termination_date=date(2020, 5, 18),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        assessments: List[StateAssessment] = []

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[first_supervision_period, second_supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()

        violation_responses: List[StateSupervisionViolationResponse] = []

        termination_event = self.identifier._find_supervision_termination_event(
            first_supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            violation_delegate=UsXxViolationDelegate(),
            supervision_delegate=UsXxSupervisionDelegate(),
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        if first_supervision_period.termination_date is None:
            raise ValueError(
                "Expected supervision_period.termination_date to be filled"
            )

        expected_termination_event = SupervisionTerminationEvent(
            state_code=first_supervision_period.state_code,
            year=first_supervision_period.termination_date.year,
            month=first_supervision_period.termination_date.month,
            event_date=first_supervision_period.termination_date,
            in_incarceration_population_on_date=False,
            in_supervision_population_on_date=True,
            supervision_type=supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            assessment_score=None,
            assessment_type=None,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            termination_reason=first_supervision_period.termination_reason,
            assessment_score_change=None,
        )

        self.assertEqual(expected_termination_event, termination_event)

    def test_find_supervision_termination_event_zero_day_termination_non_overlapping(
        self,
    ) -> None:
        """Tests that the SupervisionTerminationEvent has in_supervision_population_on_date=False
        if the supervision period is 0 full days and non-overlapping with other periods."""
        self.run_overlapping_periods_test(
            date(2018, 3, 5), date(2019, 5, 20), False, False
        )

    def test_find_supervision_termination_event_zero_day_termination_overlapping(
        self,
    ) -> None:
        """Tests that the SupervisionTerminationEvent has in_supervision_population_on_date=False
        if the supervision period is 0 full days and non-overlapping with other
        supervision periods, but in_incarceration_population_on_date=True because it's overlapping
        with an incarceration period."""
        self.run_overlapping_periods_test(
            date(2018, 3, 5), date(2018, 3, 5), True, False
        )

    def test_find_supervision_termination_event_zero_day_termination_overlapping_with_supervision(
        self,
    ) -> None:
        """Tests that the SupervisionTerminationEvent has in_supervision_population_on_date=False
        if the supervision period is 0 full days and overlapping with another
        supervision period, but in_incarceration_population_on_date=False because it's overlapping
        with no incarceration periods."""
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 3, 5),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2020, 5, 18),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        assessments: List[StateAssessment] = []

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[first_supervision_period, second_supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()

        violation_responses: List[StateSupervisionViolationResponse] = []

        termination_event = self.identifier._find_supervision_termination_event(
            first_supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            violation_delegate=UsXxViolationDelegate(),
            supervision_delegate=UsXxSupervisionDelegate(),
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        if first_supervision_period.termination_date is None:
            raise ValueError(
                "Expected supervision_period.termination_date to be filled"
            )

        expected_termination_event = SupervisionTerminationEvent(
            state_code=first_supervision_period.state_code,
            year=first_supervision_period.termination_date.year,
            month=first_supervision_period.termination_date.month,
            event_date=first_supervision_period.termination_date,
            in_incarceration_population_on_date=False,
            in_supervision_population_on_date=False,
            supervision_type=supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            assessment_score=None,
            assessment_type=None,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            termination_reason=first_supervision_period.termination_reason,
            assessment_score_change=None,
        )

        self.assertEqual(expected_termination_event, termination_event)

    def run_overlapping_periods_test(
        self,
        supervision_termination_date: date,
        incarceration_admission_date: date,
        expected_in_incarceration_population_on_date: bool,
        expected_in_supervision_population_on_date: bool,
        incarceration_release_date: Optional[date] = date(2020, 9, 20),
    ) -> None:
        """Runs a test for find_supervision_termination_event where there are
        overlapping supervision and incarceration periods."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=supervision_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=incarceration_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=incarceration_release_date,
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        assessments: List[StateAssessment] = []

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )

        violation_responses: List[StateSupervisionViolationResponse] = []

        termination_event = self.identifier._find_supervision_termination_event(
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
            DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            violation_delegate=UsXxViolationDelegate(),
            supervision_delegate=UsXxSupervisionDelegate(),
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        if supervision_period.termination_date is None:
            raise ValueError(
                "Expected supervision_period.termination_date to be filled"
            )

        expected_termination_event = SupervisionTerminationEvent(
            state_code=supervision_period.state_code,
            year=supervision_period.termination_date.year,
            month=supervision_period.termination_date.month,
            event_date=supervision_period.termination_date,
            in_incarceration_population_on_date=expected_in_incarceration_population_on_date,
            in_supervision_population_on_date=expected_in_supervision_population_on_date,
            supervision_type=supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            assessment_score=None,
            assessment_type=None,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            termination_reason=supervision_period.termination_reason,
            assessment_score_change=None,
        )

        self.assertEqual(expected_termination_event, termination_event)


class TestFindSupervisionStartEvent(unittest.TestCase):
    """Tests the find_supervision_start_event function."""

    def setUp(self) -> None:
        self.identifier = identifier.SupervisionIdentifier()

    def test_find_supervision_start_event_overlapping_incarceration(self) -> None:
        """Tests that the SupervisionStartEvent has in_incarceration_population_on_date=True
        if the start date occurs during an incarceration period for that person.
        in_supervision_population_on_date=False because the IP overlapping with the
        start date means they are generally not counted in the supervision population
        on that date."""
        self.run_overlapping_periods_test(
            date(2018, 3, 5), date(2018, 4, 1), True, False
        )

    def test_find_supervision_start_event_overlapping_incarceration_supervision_authority(
        self,
    ) -> None:
        """Tests that the SupervisionStartEvent has in_incarceration_population_on_date=True
        if the start date occurs during an incarceration period for that person.
        in_supervision_population_on_date=True because the IP overlapping with the
        start date has custodial_authority=SUPERVISION_AUTHORITY which means the person
        is counted in the supervision population on that date."""
        self.run_overlapping_periods_test(
            supervision_start_date=date(2018, 3, 5),
            incarceration_release_date=date(2018, 4, 1),
            expected_in_incarceration_population_on_date=False,
            expected_in_supervision_population_on_date=True,
            incarceration_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
        )

    def test_find_supervision_start_event_overlapping_incarceration_supervision_starts_first(
        self,
    ) -> None:
        """Tests that the SupervisionStartEvent has in_incarceration_population_on_date=False
        if the start date occurs before an incarceration period for that person,
        even if the supervision and incarceration periods are overlapping thereafter.
        in_supervision_population_on_date=True because it's not a 0-day supervision period."""
        self.run_overlapping_periods_test(
            date(2018, 3, 5), date(2019, 5, 20), False, True, date(2018, 4, 1)
        )

    def test_find_supervision_start_event_start_release_same_date(self) -> None:
        """Tests that the SupervisionStartEvent has in_incarceration_population_on_date=False
        and in_supervision_population_on_date=True if the start date is on the same day
        as the release date of an incarceration period for that person."""
        self.run_overlapping_periods_test(
            date(2018, 3, 5), date(2018, 3, 5), False, True
        )

    def test_find_supervision_start_event_start_day_before_release(self) -> None:
        """Tests that the SupervisionStartEvent has in_incarceration_population_on_date=True
        and in_supervision_population_on_date=False if the start date is the day before
        the release date of an incarceration period for that person."""
        self.run_overlapping_periods_test(
            date(2018, 3, 5), date(2018, 3, 6), True, False
        )

    def test_find_supervision_start_event_single_day_termination_non_overlapping(
        self,
    ) -> None:
        """Tests that the SupervisionStartEvent has in_supervision_population_on_date=True
        and in_incarceration_population_on_date=False if the supervision period is 1 full day and
        non-overlapping with other periods."""
        self.run_overlapping_periods_test(
            date(2019, 5, 18), date(2019, 2, 20), False, True
        )

    def test_find_supervision_start_event_single_day_termination_overlapping(
        self,
    ) -> None:
        """Tests that the SupervisionStartEvent has in_supervision_population_on_date=False
        and in_incarceration_population_on_date=True if the the supervision period
        is 1 full day and that day is overlapping with an incarceration period."""
        self.run_overlapping_periods_test(
            date(2019, 5, 18), date(2020, 5, 18), True, False, date(2019, 5, 18)
        )

    def test_find_supervision_start_event_single_day_termination_overlapping_with_supervision(
        self,
    ) -> None:
        """Tests that the SupervisionStartEvent has in_supervision_population_on_date=True
        and in_incarceration_population_on_date=False if the supervision period is 1
        full day and overlapping with another supervision period, but with no
        incarceration periods."""
        first_supervision_period_start_date = date(2018, 3, 5)
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=first_supervision_period_start_date,
            termination_date=date(2018, 3, 6),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 6),
            termination_date=date(2020, 5, 18),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[first_supervision_period, second_supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()

        start_event = self.identifier._find_supervision_start_event(
            first_supervision_period,
            supervision_period_index,
            incarceration_period_index,
            UsXxSupervisionDelegate(),
            {},
            None,
        )

        if first_supervision_period.termination_date is None:
            raise ValueError(
                "Expected supervision_period.termination_date to be filled"
            )

        expected_start_event = SupervisionStartEvent(
            state_code=first_supervision_period.state_code,
            year=first_supervision_period_start_date.year,
            month=first_supervision_period_start_date.month,
            event_date=first_supervision_period_start_date,
            in_incarceration_population_on_date=False,
            in_supervision_population_on_date=True,
            supervision_type=first_supervision_period.supervision_type,
            supervision_level=first_supervision_period.supervision_level,
            case_type=StateSupervisionCaseType.GENERAL,
            admission_reason=first_supervision_period.admission_reason,
        )

        self.assertEqual(expected_start_event, start_event)

    def test_find_supervision_start_event_zero_day_termination_non_overlapping(
        self,
    ) -> None:
        """Tests that the SupervisionStartEvent has in_supervision_population_on_date=False
        if the supervision period is 0 full days and non-overlapping with other periods."""
        self.run_overlapping_periods_test(
            date(2019, 5, 19), date(2019, 2, 20), False, False
        )

    def test_find_supervision_start_event_zero_day_termination_overlapping(
        self,
    ) -> None:
        """Tests that the SupervisionStartEvent has in_supervision_population_on_date=False
        and in_incarceration_population_on_date=True if the supervision period is 0 full
        days and it's overlapping with an incarceration period."""
        self.run_overlapping_periods_test(
            date(2019, 5, 19), date(2020, 5, 19), True, False, date(2019, 5, 19)
        )

    def test_find_supervision_start_event_zero_day_termination_overlapping_with_supervision(
        self,
    ) -> None:
        """Tests that the SupervisionStartEvent has in_supervision_population_on_date=True
        and in_incarceration_population_on_date=False if the supervision period is 0
        full days and overlapping with another supervision period but with no
        incarceration periods."""
        start_date = date(2018, 3, 5)
        first_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=start_date,
            termination_date=date(2018, 3, 5),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        second_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 18),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[first_supervision_period, second_supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()

        start_event = self.identifier._find_supervision_start_event(
            first_supervision_period,
            supervision_period_index,
            incarceration_period_index,
            UsXxSupervisionDelegate(),
            {},
            None,
        )

        if first_supervision_period.termination_date is None:
            raise ValueError(
                "Expected supervision_period.termination_date to be filled"
            )

        expected_start_event = SupervisionStartEvent(
            state_code=first_supervision_period.state_code,
            year=start_date.year,
            month=start_date.month,
            event_date=start_date,
            in_incarceration_population_on_date=False,
            in_supervision_population_on_date=True,
            supervision_type=first_supervision_period.supervision_type,
            supervision_level=first_supervision_period.supervision_level,
            case_type=StateSupervisionCaseType.GENERAL,
            admission_reason=first_supervision_period.admission_reason,
        )

        self.assertEqual(expected_start_event, start_event)

    def run_overlapping_periods_test(
        self,
        supervision_start_date: date,
        incarceration_release_date: date,
        expected_in_incarceration_population_on_date: bool,
        expected_in_supervision_population_on_date: bool,
        incarceration_admission_date: Optional[date] = date(2018, 2, 20),
        incarceration_authority: Optional[StateCustodialAuthority] = None,
    ) -> None:
        """Runs a test for find_supervision_start_event where there are overlapping
        supervision and incarceration periods."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_XX",
            start_date=supervision_start_date,
            termination_date=date(2019, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            state_code="US_XX",
            admission_date=incarceration_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=incarceration_release_date,
            release_reason=ReleaseReason.SENTENCE_SERVED,
            custodial_authority=incarceration_authority,
        )

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )

        start_event = self.identifier._find_supervision_start_event(
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            UsXxSupervisionDelegate(),
            {},
            None,
        )

        if supervision_period.start_date is None:
            raise ValueError("Expected supervision_period.start_date to be filled")

        expected_start_event = SupervisionStartEvent(
            state_code=supervision_period.state_code,
            year=supervision_period.start_date.year,
            month=supervision_period.start_date.month,
            event_date=supervision_period.start_date,
            in_incarceration_population_on_date=expected_in_incarceration_population_on_date,
            in_supervision_population_on_date=expected_in_supervision_population_on_date,
            supervision_type=supervision_period.supervision_type,
            supervision_level=supervision_period.supervision_level,
            case_type=StateSupervisionCaseType.GENERAL,
            admission_reason=supervision_period.admission_reason,
        )

        self.assertEqual(expected_start_event, start_event)


class TestGetMostSevereResponseDecision(unittest.TestCase):
    """Tests the _get_most_severe_response_decision function."""

    def setUp(self) -> None:
        self.identifier = identifier.SupervisionIdentifier()

    def test_get_most_severe_response_decision(self) -> None:
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_XX",
            violation_date=date(2009, 1, 3),
        )

        supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                state_code="US_XX",
                response_date=date(2009, 1, 7),
                supervision_violation_response_decisions=[
                    StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                        state_code="US_XX",
                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    ),
                    StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                        state_code="US_XX",
                        decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                    ),
                ],
                supervision_violation=supervision_violation,
            )
        )

        most_severe_response_decision = recidiviz.calculator.pipeline.utils.violation_response_utils.get_most_severe_response_decision(
            [supervision_violation_response]
        )

        self.assertEqual(
            StateSupervisionViolationResponseDecision.REVOCATION,
            most_severe_response_decision,
        )

    def test_get_most_severe_response_decision_no_responses(self) -> None:
        most_severe_response_decision = recidiviz.calculator.pipeline.utils.violation_response_utils.get_most_severe_response_decision(
            []
        )

        self.assertIsNone(most_severe_response_decision)


class TestConvertEventsToDual(unittest.TestCase):
    """Tests the _convert_events_to_dual function."""

    def setUp(self) -> None:
        self.identifier = identifier.SupervisionIdentifier()

    def test_convert_events_to_dual_us_mo(self) -> None:
        supervision_events: List[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
        ]

        updated_events = self.identifier._convert_events_to_dual(
            supervision_events=supervision_events
        )

        expected_output = [
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
        ]

        self.assertCountEqual(updated_events, expected_output)

    def test_convert_events_to_dual_us_mo_one_dual(self) -> None:
        supervision_events: List[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
        ]

        updated_events = self.identifier._convert_events_to_dual(
            supervision_events=supervision_events
        )

        expected_output = [
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
        ]

        self.assertCountEqual(updated_events, expected_output)

    def test_convert_events_to_dual_us_mo_one_other_day(self) -> None:
        supervision_events: List[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                projected_end_date=None,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                projected_end_date=None,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 4),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
        ]

        updated_events = self.identifier._convert_events_to_dual(
            supervision_events=supervision_events
        )

        expected_output = [
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                projected_end_date=None,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 3),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                projected_end_date=None,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 4),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
        ]

        self.assertCountEqual(updated_events, expected_output)

    def test_convert_events_to_dual_us_mo_one_different_type(self) -> None:
        supervision_events = [
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                projected_end_date=None,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                projected_end_date=None,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            ProjectedSupervisionCompletionEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=True,
            ),
        ]

        updated_events = self.identifier._convert_events_to_dual(
            supervision_events=supervision_events
        )

        expected_output = [
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                projected_end_date=None,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                projected_end_date=None,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            ProjectedSupervisionCompletionEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                successful_completion=True,
            ),
        ]

        self.assertCountEqual(updated_events, expected_output)

    def test_convert_events_to_dual_empty_input(self) -> None:
        supervision_events: List[SupervisionEvent] = []

        updated_events = self.identifier._convert_events_to_dual(
            supervision_events=supervision_events
        )

        expected_output: List[SupervisionEvent] = []

        self.assertCountEqual(updated_events, expected_output)

    def test_convert_events_to_dual_metric_type_coverage(self) -> None:
        """Asserts all SupervisionMetricTypes are handled in the function."""
        for metric_type in SupervisionMetricType:
            self.assertTrue(metric_type in self.identifier.EVENT_TYPES_FOR_METRIC)


def expected_population_events(
    supervision_period: StateSupervisionPeriod,
    supervision_type: StateSupervisionPeriodSupervisionType,
    end_date: Optional[date] = None,
    case_type: Optional[StateSupervisionCaseType] = StateSupervisionCaseType.GENERAL,
    assessment_score: Optional[int] = None,
    assessment_level: Optional[StateAssessmentLevel] = None,
    assessment_type: Optional[StateAssessmentType] = None,
    assessment_score_bucket: Optional[str] = None,
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
) -> List[SupervisionPopulationEvent]:
    """Returns the expected SupervisionPopulationEvents based on the provided
    |supervision_period| and when the events should end."""

    expected_events = []

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

            event = SupervisionPopulationEvent(
                state_code=supervision_period.state_code,
                year=day_on_supervision.year,
                month=day_on_supervision.month,
                event_date=day_on_supervision,
                supervision_type=supervision_type,
                case_type=case_type,
                assessment_score=assessment_score,
                assessment_level=assessment_level,
                assessment_type=assessment_type,
                assessment_score_bucket=assessment_score_bucket
                or DEFAULT_ASSESSMENT_SCORE_BUCKET,
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
                case_compliance=case_compliance,
                judicial_district_code=judicial_district_code,
                supervision_level_downgrade_occurred=downgrade_occurred,
                previous_supervision_level=previous_level,
                projected_end_date=projected_supervision_completion_date,
            )

            expected_events.append(event)

    return expected_events


class TestSupervisionLevelDowngradeOccurred(unittest.TestCase):
    """Tests the _supervision_level_downgrade_occurred function."""

    def setUp(self) -> None:
        self.identifier = identifier.SupervisionIdentifier()

    def test_supervision_level_downgrade_occurred_covers_all_supervision_levels(
        self,
    ) -> None:
        """Ensures that all values in StateSupervisionLevel are covered by _supervision_level_downgrade_occurred."""

        for level in StateSupervisionLevel:
            self.identifier._supervision_level_downgrade_occurred(
                level, StateSupervisionLevel.MAXIMUM
            )


class TestFindAssessmentScoreChange(unittest.TestCase):
    """Tests the find_assessment_score_change function."""

    def setUp(self) -> None:
        self.identifier = identifier.SupervisionIdentifier()

    def test_find_assessment_score_change(self) -> None:
        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_score=33,
            assessment_date=date(2015, 3, 23),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=29,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2015, 11, 2),
        )

        assessment_3 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
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
        ) = self.identifier._find_assessment_score_change(
            start_date,
            termination_date,
            assessments,
            UsXxSupervisionDelegate(),
        )

        self.assertEqual(-6, assessment_score_change)
        self.assertEqual(assessment_3.assessment_score, end_assessment_score)
        self.assertEqual(assessment_3.assessment_level, end_assessment_level)
        self.assertEqual(assessment_3.assessment_type, end_assessment_type)

    def test_find_assessment_score_change_insufficient_assessments(self) -> None:
        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2015, 3, 23),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
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
        ) = self.identifier._find_assessment_score_change(
            start_date,
            termination_date,
            assessments,
            UsXxSupervisionDelegate(),
        )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)

    class SecondAssessmentDelegate(UsXxSupervisionDelegate):
        def get_index_of_first_reliable_supervision_assessment(self) -> int:
            return 0

    def test_find_assessment_score_change_first_reliable_assessment_is_first_assessment(
        self,
    ) -> None:

        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2015, 3, 23),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
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
        ) = self.identifier._find_assessment_score_change(
            start_date,
            termination_date,
            assessments,
            self.SecondAssessmentDelegate(),
        )

        self.assertEqual(-4, assessment_score_change)
        self.assertEqual(assessment_2.assessment_score, end_assessment_score)
        self.assertEqual(assessment_2.assessment_level, end_assessment_level)
        self.assertEqual(assessment_2.assessment_type, end_assessment_type)

    def test_find_assessment_score_change_different_type(self) -> None:
        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2015, 3, 23),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
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
        ) = self.identifier._find_assessment_score_change(
            start_date,
            termination_date,
            assessments,
            UsXxSupervisionDelegate(),
        )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)

    def test_find_assessment_score_change_same_date(self) -> None:
        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2015, 3, 23),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=29,
            assessment_date=date(2015, 11, 2),
        )

        assessment_3 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
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
        ) = self.identifier._find_assessment_score_change(
            start_date,
            termination_date,
            assessments,
            UsXxSupervisionDelegate(),
        )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)

    def test_find_assessment_score_change_no_assessments(self) -> None:
        assessments: List[StateAssessment] = []

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        (
            assessment_score_change,
            end_assessment_score,
            end_assessment_level,
            end_assessment_type,
        ) = self.identifier._find_assessment_score_change(
            start_date,
            termination_date,
            assessments,
            UsXxSupervisionDelegate(),
        )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)

    def test_find_assessment_score_change_outside_boundary(self) -> None:
        assessment_1 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2011, 3, 23),
        )

        assessment_2 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=29,
            assessment_date=date(2015, 11, 2),
        )

        assessment_3 = StateAssessment.new_with_defaults(
            state_code="US_XX",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
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
        ) = self.identifier._find_assessment_score_change(
            start_date,
            termination_date,
            assessments,
            UsXxSupervisionDelegate(),
        )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)


class TestTerminationReasonFunctionCoverageCompleteness(unittest.TestCase):
    """Tests that functions that classify all possible values of the
    StateSupervisionPeriodTerminationReason enum have complete coverage."""

    def setUp(self) -> None:
        self.identifier = identifier.SupervisionIdentifier()

    def test__termination_is_successful_if_should_include_in_success_metric(
        self,
    ) -> None:
        for termination_reason in StateSupervisionPeriodTerminationReason:
            (
                include_in_metric,
                is_successful_completion,
            ) = self.identifier._termination_is_successful_if_should_include_in_success_metric(
                termination_reason
            )

            if not include_in_metric:
                self.assertIsNone(is_successful_completion)
            else:
                self.assertIsNotNone(include_in_metric)


def create_start_event_from_period(
    period: StateSupervisionPeriod,
    supervision_delegate: StateSpecificSupervisionDelegate = UsXxSupervisionDelegate(),
    **kwargs: Any,
) -> SupervisionStartEvent:
    """Creates the SupervisionStartEvent we expect to be created from the given
    period."""
    (
        _supervising_officer_external_id,
        level_1_supervision_location_external_id,
        level_2_supervision_location_external_id,
    ) = supervising_officer_and_location_info(period, {}, supervision_delegate)

    deprecated_supervising_district_external_id = (
        level_2_supervision_location_external_id
        or level_1_supervision_location_external_id
    )

    assert period.start_date is not None

    event = SupervisionStartEvent(
        state_code=period.state_code,
        year=period.start_date.year,
        month=period.start_date.month,
        event_date=period.start_date,
        supervision_type=period.supervision_type,
        supervision_level_raw_text=period.supervision_level_raw_text,
        supervising_district_external_id=deprecated_supervising_district_external_id,
        level_1_supervision_location_external_id=level_1_supervision_location_external_id,
        level_2_supervision_location_external_id=level_2_supervision_location_external_id,
        supervision_level=period.supervision_level,
        case_type=StateSupervisionCaseType.GENERAL,
        in_supervision_population_on_date=True,
        admission_reason=period.admission_reason
        or StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
    )
    event = attr.evolve(event, **kwargs)
    return event


def create_termination_event_from_period(
    period: StateSupervisionPeriod,
    supervision_delegate: StateSpecificSupervisionDelegate = UsXxSupervisionDelegate(),
    **kwargs: Any,
) -> SupervisionTerminationEvent:
    """Creates the SupervisionTerminationEvent we expect to be created from the given
    period."""
    (
        _supervising_officer_external_id,
        level_1_supervision_location_external_id,
        level_2_supervision_location_external_id,
    ) = supervising_officer_and_location_info(period, {}, supervision_delegate)

    deprecated_supervising_district_external_id = (
        level_2_supervision_location_external_id
        or level_1_supervision_location_external_id
    )

    termination_reason = period.termination_reason

    if period.termination_date and not termination_reason:
        # Unset termination reasons will be set to INTERNAL_UNKNOWN in pre-processing
        termination_reason = StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN

    assert period.termination_date is not None

    event = SupervisionTerminationEvent(
        state_code=period.state_code,
        year=period.termination_date.year,
        month=period.termination_date.month,
        event_date=period.termination_date,
        supervision_type=period.supervision_type,
        supervision_level=period.supervision_level,
        supervision_level_raw_text=period.supervision_level_raw_text,
        supervising_district_external_id=deprecated_supervising_district_external_id,
        level_1_supervision_location_external_id=level_1_supervision_location_external_id,
        level_2_supervision_location_external_id=level_2_supervision_location_external_id,
        case_type=StateSupervisionCaseType.GENERAL,
        termination_reason=termination_reason,
        assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
    )
    event = attr.evolve(event, **kwargs)
    return event


def _generate_case_compliances(
    person: StatePerson,
    start_date: Optional[date],
    supervision_period: StateSupervisionPeriod,
    assessments: Optional[List[StateAssessment]] = None,
    face_to_face_contacts: Optional[List[StateSupervisionContact]] = None,
    end_date_override: Optional[date] = None,
    next_recommended_assessment_date: Optional[date] = None,
    violation_responses: Optional[List[StateSupervisionViolationResponse]] = None,
    incarceration_sentences: Optional[List[StateIncarcerationSentence]] = None,
    incarceration_period_index: Optional[NormalizedIncarcerationPeriodIndex] = None,
    supervision_delegate: Optional[StateSpecificSupervisionDelegate] = None,
) -> Dict[date, SupervisionCaseCompliance]:
    """Generates the expected list of supervision case compliances. Because case compliance logic is tested in
    supervision_case_compliance_manager_test and state-specific compliance tests, this function generates expected case
    compliances using the compliance manager."""

    # This was simpler than trying to refactor much of the above code to satisfy mypy
    assert start_date is not None

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
        person,
        supervision_period,
        StateSupervisionCaseType.GENERAL,
        start_date,
        assessments or [],
        face_to_face_contacts or [],
        violation_responses or [],
        incarceration_sentences or [],
        incarceration_period_index=(
            incarceration_period_index or default_normalized_ip_index_for_tests()
        ),
        supervision_delegate=(supervision_delegate or UsXxSupervisionDelegate()),
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

        if next_recommended_assessment_date:
            case_compliance = attr.evolve(
                case_compliance,
                next_recommended_assessment_date=next_recommended_assessment_date,
            )

        if case_compliance:
            case_compliances[current_date] = case_compliance
        current_date += relativedelta(days=1)

    return case_compliances
