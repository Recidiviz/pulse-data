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
from typing import Any, Dict, List, Optional, Sequence, Set, Type, Union
from unittest.mock import patch

import attr
from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

import recidiviz.pipelines.metrics.utils.violation_utils
import recidiviz.pipelines.utils.supervision_period_utils
import recidiviz.pipelines.utils.violation_response_utils
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
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateAssessment,
    NormalizedStateIncarcerationPeriod,
    NormalizedStatePerson,
    NormalizedStateSupervisionCaseTypeEntry,
    NormalizedStateSupervisionContact,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
    NormalizedStateSupervisionViolationResponseDecisionEntry,
    NormalizedStateSupervisionViolationTypeEntry,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.assessment_normalization_manager import (
    DEFAULT_ASSESSMENT_SCORE_BUCKET,
)
from recidiviz.pipelines.metrics.supervision import identifier as supervision_identifier
from recidiviz.pipelines.metrics.supervision.events import (
    SupervisionEvent,
    SupervisionPopulationEvent,
    SupervisionStartEvent,
    SupervisionTerminationEvent,
)
from recidiviz.pipelines.metrics.supervision.identifier import SupervisionIdentifier
from recidiviz.pipelines.metrics.supervision.metrics import SupervisionMetricType
from recidiviz.pipelines.metrics.supervision.supervision_case_compliance import (
    SupervisionCaseCompliance,
)
from recidiviz.pipelines.metrics.utils.supervision_case_compliance_manager import (
    StateSupervisionCaseComplianceManager,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_incarceration_period_index import (
    NormalizedIncarcerationPeriodIndex,
)
from recidiviz.pipelines.utils.execution_utils import TableRow
from recidiviz.pipelines.utils.identifier_models import IdentifierResult
from recidiviz.pipelines.utils.state_utils.state_calculation_config_manager import (
    get_state_specific_case_compliance_manager,
)
from recidiviz.pipelines.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_ix.us_ix_supervision_delegate import (
    UsIxSupervisionDelegate,
)
from recidiviz.pipelines.utils.state_utils.us_pa.us_pa_supervision_delegate import (
    UsPaSupervisionDelegate,
)
from recidiviz.pipelines.utils.supervision_period_utils import supervising_location_info
from recidiviz.tests.pipelines.fake_state_calculation_config_manager import (
    start_pipeline_delegate_getter_patchers,
)
from recidiviz.tests.pipelines.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_ip_index_for_tests,
    default_normalized_sp_index_for_tests,
)
from recidiviz.utils.range_querier import RangeQuerier
from recidiviz.utils.types import assert_type

_STATE_CODE = "US_XX"
_DEFAULT_SUPERVISION_PERIOD_ID = 999
_DEFAULT_SSVR_ID = 999


class TestClassifySupervisionEvents(unittest.TestCase):
    """Tests for the find_supervision_events function."""

    def setUp(self) -> None:
        self.maxDiff = None
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            supervision_identifier
        )
        self.state_code = StateCode.US_XX
        self.identifier = SupervisionIdentifier(self.state_code)
        self.person = NormalizedStatePerson(
            state_code=self.state_code.value, person_id=99000123
        )

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def _test_find_supervision_events(
        self,
        *,
        identifier: SupervisionIdentifier,
        supervision_periods: List[NormalizedStateSupervisionPeriod],
        incarceration_periods: List[NormalizedStateIncarcerationPeriod],
        assessments: List[NormalizedStateAssessment],
        violation_responses: List[NormalizedStateSupervisionViolationResponse],
        supervision_contacts: List[NormalizedStateSupervisionContact],
        included_result_classes: Optional[Set[Type[IdentifierResult]]] = None,
    ) -> List[SupervisionEvent]:
        """Helper for testing the find_events function on the identifier."""
        self._set_expected_sp_fields(supervision_periods)

        all_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {
            NormalizedStateIncarcerationPeriod.__name__: incarceration_periods,
            NormalizedStateSupervisionPeriod.__name__: supervision_periods,
            NormalizedStateAssessment.__name__: assessments,
            NormalizedStateSupervisionContact.__name__: supervision_contacts,
            NormalizedStateSupervisionViolationResponse.__name__: violation_responses,
        }

        return identifier.identify(
            self.person,
            all_kwargs,
            included_result_classes=included_result_classes
            or {
                SupervisionStartEvent,
                SupervisionPopulationEvent,
                SupervisionTerminationEvent,
            },
        )

    def _set_expected_sp_fields(
        self, supervision_periods: List[NormalizedStateSupervisionPeriod]
    ) -> None:
        """Sets the fields that we expect to be non-null on all
        NormalizedStateSupervisionPeriods."""
        for sp in supervision_periods:
            sp.admission_reason = (
                sp.admission_reason
                or StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN
            )

            if sp.termination_date is not None:
                sp.termination_reason = (
                    sp.termination_reason
                    or StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN
                )

    def test_find_supervision_events(self) -> None:
        """Tests the find_supervision_population_events function for a single
        supervision period with no incarceration periods."""

        termination_date = date(2018, 5, 19)
        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code=self.state_code.value,
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            sequence_num=0,
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        assessment = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 1),
            assessment_score_bucket="HIGH",
            sequence_num=0,
        )

        supervision_periods = [supervision_period]
        incarceration_periods: List[NormalizedStateIncarcerationPeriod] = []
        assessments = [assessment]
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_termination_event_from_period(
                supervision_period,
                supervision_type=supervision_type,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                in_supervision_population_on_date=True,
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
            )
        )

        supervision_events = self._test_find_supervision_events(
            identifier=self.identifier,
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_filtered(self) -> None:
        termination_date = date(2018, 5, 19)
        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code=self.state_code.value,
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            sequence_num=0,
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        assessment = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 1),
            assessment_score_bucket="HIGH",
            sequence_num=0,
        )

        supervision_periods = [supervision_period]
        incarceration_periods: List[NormalizedStateIncarcerationPeriod] = []
        assessments = [assessment]
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_termination_event_from_period(
                supervision_period,
                supervision_type=supervision_type,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                supervision_period,
                case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
            ),
        ]

        supervision_events = self._test_find_supervision_events(
            identifier=self.identifier,
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
            included_result_classes={
                SupervisionStartEvent,
                SupervisionTerminationEvent,
            },
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_overlaps_year(self) -> None:
        """Tests the find_supervision_events function for a single
        supervision period with no incarceration periods, where the supervision
        period overlaps two calendar years."""
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2019, 1, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_periods = [supervision_period]
        incarceration_periods: List[NormalizedStateIncarcerationPeriod] = []
        assessments: List[NormalizedStateAssessment] = []
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_start_event_from_period(
                supervision_period,
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
            identifier=self.identifier,
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_two_supervision_periods(self) -> None:
        """Tests the find_supervision_events function for two supervision
        periods with no incarceration periods."""

        first_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
        )

        second_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=222,
            external_id="sp2",
            state_code=self.state_code.value,
            start_date=date(2019, 8, 5),
            termination_date=date(2019, 12, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=1,
        )

        supervision_periods = [first_supervision_period, second_supervision_period]
        incarceration_periods: List[NormalizedStateIncarcerationPeriod] = []
        assessments: List[NormalizedStateAssessment] = []
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

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
            ),
            create_start_event_from_period(
                second_supervision_period,
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
            identifier=self.identifier,
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_overlapping_supervision_periods(
        self,
    ) -> None:
        """Tests the find_supervision_events function for two supervision
        periods with no incarceration periods, where the supervision
        periods are of the same type and have overlapping months."""

        first_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
        )

        second_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=222,
            external_id="sp2",
            state_code=self.state_code.value,
            start_date=date(2018, 4, 15),
            termination_date=date(2018, 7, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=1,
        )

        supervision_periods = [first_supervision_period, second_supervision_period]
        incarceration_periods: List[NormalizedStateIncarcerationPeriod] = []
        assessments: List[NormalizedStateAssessment] = []
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

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
            ),
            create_start_event_from_period(
                second_supervision_period,
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
            identifier=self.identifier,
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_overlapping_periods_different_types(
        self,
    ) -> None:
        """Tests the find_supervision_events function for two supervision
        periods with no incarceration periods, where the supervision
        periods are of different types and have overlapping months."""

        first_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            sequence_num=0,
        )

        second_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=222,
            external_id="sp2",
            state_code=self.state_code.value,
            start_date=date(2018, 4, 15),
            termination_date=date(2018, 7, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            sequence_num=1,
        )

        supervision_periods = [first_supervision_period, second_supervision_period]
        incarceration_periods: List[NormalizedStateIncarcerationPeriod] = []
        assessments: List[NormalizedStateAssessment] = []
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

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
            ),
            create_start_event_from_period(
                second_supervision_period,
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
            identifier=self.identifier,
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_multiple_periods(self) -> None:
        """Tests the find_supervision_events function for two supervision
        periods with two incarceration periods."""

        first_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
        )

        first_admission_date = date(2017, 5, 25)
        first_incarceration_period = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=111,
            external_id="ip1",
            state_code=self.state_code.value,
            admission_date=first_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2017, 8, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            sequence_num=0,
        )

        second_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=222,
            external_id="sp2",
            state_code=self.state_code.value,
            start_date=date(2018, 8, 5),
            termination_date=date(2018, 12, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=1,
        )

        second_admission_date = date(2018, 12, 25)
        second_incarceration_period = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=self.state_code.value,
            admission_date=second_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            sequence_num=1,
        )

        supervision_periods = [first_supervision_period, second_supervision_period]
        incarceration_periods = [
            first_incarceration_period,
            second_incarceration_period,
        ]
        assessments: List[NormalizedStateAssessment] = []
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

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
            ),
            create_start_event_from_period(
                second_supervision_period,
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
            identifier=self.identifier,
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_multiple_admissions_in_month(self) -> None:
        """Tests the find_supervision_events function for a supervision period with two incarceration periods
        with admission dates in the same month."""
        first_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
        )

        first_admission_date = date(2017, 5, 11)
        first_incarceration_period = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=111,
            external_id="ip1",
            state_code=self.state_code.value,
            admission_date=first_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2017, 5, 15),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE,
            sequence_num=0,
        )

        second_admission_date = date(2017, 5, 17)
        second_incarceration_period = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=self.state_code.value,
            admission_date=second_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2019, 3, 3),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            sequence_num=1,
        )

        supervision_periods = [first_supervision_period]
        incarceration_periods = [
            first_incarceration_period,
            second_incarceration_period,
        ]
        assessments: List[NormalizedStateAssessment] = []
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        first_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_termination_event_from_period(
                first_supervision_period,
                supervision_type=first_supervision_type,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                first_supervision_period,
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
            identifier=self.identifier,
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_incarceration_overlaps_periods(self) -> None:
        """Tests the find_supervision_events function for two supervision
        periods with an incarceration period that overlaps the end of one
        supervision period and the start of another."""

        first_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
        )

        admission_date = date(2017, 5, 15)
        incarceration_period = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=111,
            external_id="ip1",
            state_code=self.state_code.value,
            admission_date=admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2017, 9, 20),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            sequence_num=0,
        )

        second_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=222,
            external_id="sp2",
            state_code=self.state_code.value,
            start_date=date(2017, 8, 5),
            termination_date=date(2017, 12, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=1,
        )

        supervision_periods = [first_supervision_period, second_supervision_period]
        incarceration_periods = [incarceration_period]
        assessments: List[NormalizedStateAssessment] = []
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

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
            ),
            create_start_event_from_period(
                second_supervision_period,
                in_incarceration_population_on_date=True,
                in_supervision_population_on_date=False,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                first_supervision_period,
                first_supervision_type,
                end_date=incarceration_period.admission_date,
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
                    start_date=assert_type(
                        incarceration_period.release_date, datetime.date
                    ),
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
            identifier=self.identifier,
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_supervision_authority_incarceration_overlaps_periods(
        self,
    ) -> None:
        """Tests the find_supervision_events function for two supervision periods with an incarceration period
        that overlaps the end of one supervision period and the start of another, where the incarceration period is
        under the custodial authority of a supervision entity so the period does not exclude the supervision periods
        from being counted towards the supervision population."""

        first_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            sequence_num=0,
        )

        incarceration_period = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=111,
            external_id="ip1",
            state_code=self.state_code.value,
            admission_date=date(2017, 5, 15),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2017, 9, 20),
            release_reason=ReleaseReason.SENTENCE_SERVED,
            custodial_authority=StateCustodialAuthority.SUPERVISION_AUTHORITY,
            sequence_num=0,
        )

        second_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=222,
            external_id="sp2",
            state_code=self.state_code.value,
            start_date=date(2017, 8, 5),
            termination_date=date(2017, 12, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=1,
        )

        supervision_periods = [first_supervision_period, second_supervision_period]
        incarceration_periods = [incarceration_period]
        assessments: List[NormalizedStateAssessment] = []
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

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
            create_start_event_from_period(
                first_supervision_period,
            ),
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
            identifier=self.identifier,
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_transition_from_parole_to_probation_in_month(self) -> None:
        """Tests the find_supervision_events function for transition between two supervision periods in a
        month."""
        first_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            sequence_num=0,
        )

        second_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=222,
            external_id="sp2",
            state_code=self.state_code.value,
            start_date=date(2018, 5, 19),
            termination_date=date(2018, 6, 20),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=1,
        )

        supervision_periods = [first_supervision_period, second_supervision_period]
        incarceration_periods: List[NormalizedStateIncarcerationPeriod] = []
        assessments: List[NormalizedStateAssessment] = []
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

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
            ),
            create_start_event_from_period(
                second_supervision_period,
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
            identifier=self.identifier,
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_return_next_month(self) -> None:
        """Tests the find_supervision_events function
        when there is an incarceration period with a revocation admission
        the month after the supervision period's termination_date."""

        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 26),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_site="X",
        )

        admission_date = date(2018, 6, 25)
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=self.state_code.value,
            admission_date=admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
        )

        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments: List[NormalizedStateAssessment] = []
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_termination_event_from_period(
                supervision_period,
                supervision_type=supervision_type,
                level_1_supervision_location_external_id="X",
                level_2_supervision_location_external_id=None,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                supervision_period,
                level_1_supervision_location_external_id="X",
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                supervision_type,
                level_1_supervision_location_external_id="X",
                level_2_supervision_location_external_id=None,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            )
        )

        supervision_events = self._test_find_supervision_events(
            identifier=self.identifier,
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_multiple_periods_revocations(self) -> None:
        """Tests the find_supervision_events function
        when the person is revoked and returned to supervision twice in one
        year."""

        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 12, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_level=StateSupervisionLevel.MINIMUM,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            sequence_num=0,
        )

        first_admission_date = date(2018, 3, 25)
        first_incarceration_period = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=111,
            external_id="ip1",
            state_code=self.state_code.value,
            admission_date=first_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2018, 8, 2),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE,
            sequence_num=0,
        )

        second_admission_date = date(2018, 9, 25)
        second_incarceration_period = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=111,
            external_id="ip2",
            state_code=self.state_code.value,
            admission_date=second_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2018, 12, 2),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE,
            sequence_num=1,
        )

        supervision_periods = [supervision_period]
        incarceration_periods = [
            first_incarceration_period,
            second_incarceration_period,
        ]
        assessments: List[NormalizedStateAssessment] = []
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

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
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                supervision_type,
                end_date=first_incarceration_period.admission_date,
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
                    start_date=assert_type(
                        first_incarceration_period.release_date, datetime.date
                    ),
                ),
                supervision_type,
                end_date=second_incarceration_period.admission_date,
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
                    start_date=assert_type(
                        second_incarceration_period.release_date, datetime.date
                    ),
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
            identifier=self.identifier,
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_drops_periods_with_no_external_ids(self) -> None:
        """Tests the find_supervision_events function
        when there are supervision periods that should be dropped
        from the calculations due to no external ID."""

        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        admission_date = date(2018, 5, 25)
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=111,
            external_id="ip1",
            state_code=self.state_code.value,
            admission_date=admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
        )

        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]
        assessments: List[NormalizedStateAssessment] = []
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_termination_event_from_period(
                supervision_period,
                supervision_type=supervision_type,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                supervision_period,
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
            identifier=self.identifier,
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_us_ix(self) -> None:
        """Tests the find_supervision_events function where the supervision type should be taken from the
        supervision_type off of the supervision_period."""

        self._stop_state_specific_delegate_patchers()

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION
        supervision_period_termination_date = date(2018, 5, 19)
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_IX",
                    case_type=StateSupervisionCaseType.GENERAL,
                )
            ],
            state_code="US_IX",
            supervision_site="DISTRICT_1",
            custodial_authority_raw_text="US_ID_DOC",
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            start_date=date(2018, 3, 5),
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=supervision_type,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
        )

        assessment = NormalizedStateAssessment(
            assessment_id=1,
            state_code="US_IX",
            external_id="a1",
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 1),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        supervision_periods = [supervision_period]
        incarceration_periods: List[NormalizedStateIncarcerationPeriod] = []
        assessments = [assessment]
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        expected_events = [
            create_start_event_from_period(
                supervision_period,
                UsIxSupervisionDelegate(),
            ),
            create_termination_event_from_period(
                supervision_period,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                level_1_supervision_location_external_id="DISTRICT_1",
                level_2_supervision_location_external_id=None,
                in_supervision_population_on_date=True,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                supervision_type,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
                level_1_supervision_location_external_id="DISTRICT_1",
                level_2_supervision_location_external_id=None,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    assessments=assessments,
                ),
            )
        )

        supervision_events = self._test_find_supervision_events(
            identifier=SupervisionIdentifier(StateCode.US_IX),
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_us_pa(self) -> None:
        """Tests the find_supervision_events function for periods in US_PA."""

        self._stop_state_specific_delegate_patchers()

        supervision_period_termination_date = date(2018, 5, 19)
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=_DEFAULT_SUPERVISION_PERIOD_ID,
            external_id="sp1",
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_PA",
                    case_type=StateSupervisionCaseType.GENERAL,
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

        assessment = NormalizedStateAssessment(
            assessment_id=1,
            state_code="US_PA",
            external_id="a1",
            assessment_type=StateAssessmentType.LSIR,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_score=33,
            assessment_date=date(2018, 3, 1),
            assessment_score_bucket=StateAssessmentLevel.HIGH.value,
            sequence_num=0,
        )

        supervision_periods = [supervision_period]
        incarceration_periods: List[NormalizedStateIncarcerationPeriod] = []
        assessments = [assessment]
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        supervision_events = self._test_find_supervision_events(
            identifier=SupervisionIdentifier(StateCode.US_PA),
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )

        expected_events = [
            create_start_event_from_period(
                supervision_period,
                UsPaSupervisionDelegate(),
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
            ),
            create_termination_event_from_period(
                supervision_period,
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                in_supervision_population_on_date=True,
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
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    assessments=assessments,
                ),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_not_in_population_if_no_active_po_us_mo(
        self,
    ) -> None:
        """Tests the find_supervision_events function where a SupervisionPopulationEvent
        will not be created if there is no supervising officer attached to the period.
        """

        self._stop_state_specific_delegate_patchers()

        supervision_period_start_date = date(2018, 3, 5)
        supervision_period_termination_date = date(2018, 5, 19)
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code="US_MO",
                    case_type=StateSupervisionCaseType.DOMESTIC_VIOLENCE,
                )
            ],
            state_code="US_MO",
            start_date=supervision_period_start_date,
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
        )

        supervision_periods = [supervision_period]
        incarceration_periods: List[NormalizedStateIncarcerationPeriod] = []
        assessments: List[NormalizedStateAssessment] = []
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []
        supervision_events = self._test_find_supervision_events(
            identifier=SupervisionIdentifier(StateCode.US_MO),
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )
        for event in supervision_events:
            if isinstance(event, SupervisionPopulationEvent):
                self.fail("No supervision population event should be generated.")

    def test_find_supervision_events_infer_supervision_type_dual_us_ix(
        self,
    ) -> None:
        """Tests the find_supervision_events function where the supervision type is taken from a `DUAL`
        supervision period. Asserts that the DUAL events are NOT expanded into separate PROBATION and PAROLE events.
        """

        self._stop_state_specific_delegate_patchers()

        supervision_period_termination_date = date(2018, 5, 19)
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            state_code="US_IX",
            custodial_authority_raw_text="US_IX_DOC",
            start_date=date(2018, 3, 5),
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_level=StateSupervisionLevel.MINIMUM,
            supervision_level_raw_text="LOW",
        )

        assessment = NormalizedStateAssessment(
            assessment_id=1,
            state_code="US_IX",
            external_id="a1",
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 1),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        supervision_periods = [supervision_period]
        incarceration_periods: List[NormalizedStateIncarcerationPeriod] = []
        assessments = [assessment]
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        expected_events = [
            create_start_event_from_period(
                supervision_period,
                UsIxSupervisionDelegate(),
            ),
            create_termination_event_from_period(
                supervision_period,
                case_type=StateSupervisionCaseType.GENERAL,
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                in_supervision_population_on_date=True,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                StateSupervisionPeriodSupervisionType.DUAL,
                assessment_score=assessment.assessment_score,
                assessment_level=assessment.assessment_level,
                assessment_type=assessment.assessment_type,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    assessments=assessments,
                ),
            )
        )

        supervision_events = self._test_find_supervision_events(
            identifier=SupervisionIdentifier(StateCode.US_IX),
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)

    @freeze_time("2019-09-04 00:00:00-05:00")
    def test_find_supervision_events_admission_today(self) -> None:
        """Tests the find_population_events_for_supervision_period function when there
        is an incarceration period with a revocation admission today, where there is
        no termination_date on the supervision_period, and no release_date
        on the incarceration_period."""

        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2019, 6, 2),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
        )

        admission_date = date.today()
        incarceration_period = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=111,
            external_id="ip1",
            state_code=self.state_code.value,
            admission_date=admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            sequence_num=0,
        )

        assessments: List[NormalizedStateAssessment] = []
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        supervision_periods = [supervision_period]
        incarceration_periods = [incarceration_period]

        expected_events: List[SupervisionEvent] = [
            create_start_event_from_period(
                supervision_period,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                StateSupervisionPeriodSupervisionType.PROBATION,
                end_date=date.today(),
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                    end_date_override=date(2019, 9, 3),
                ),
            )
        )

        supervision_events = self._test_find_supervision_events(
            identifier=self.identifier,
            supervision_periods=supervision_periods,
            incarceration_periods=incarceration_periods,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_supervision_events_multiple_incarcerations_in_year(
        self,
    ) -> None:
        """Tests the find_population_events_for_supervision_period function when there are multiple
        incarceration periods in the year of supervision."""
        supervision_period_termination_date = date(2018, 6, 9)
        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=supervision_period_termination_date,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            sequence_num=0,
        )

        first_admission_date = date(2018, 6, 2)
        first_incarceration_period = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=111,
            external_id="ip1",
            state_code=self.state_code.value,
            admission_date=first_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2018, 9, 3),
            sequence_num=0,
        )

        second_admission_date = date(2018, 11, 17)
        second_incarceration_period = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=222,
            external_id="ip2",
            state_code=self.state_code.value,
            admission_date=second_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            sequence_num=1,
        )

        assessments: List[NormalizedStateAssessment] = []
        violation_reports: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        expected_events = [
            create_termination_event_from_period(
                supervision_period,
                in_incarceration_population_on_date=True,
            ),
            create_start_event_from_period(
                supervision_period,
            ),
        ]

        expected_events.extend(
            expected_population_events(
                supervision_period,
                supervision_type,
                end_date=first_incarceration_period.admission_date,
                case_compliances=_generate_case_compliances(
                    person=self.person,
                    start_date=supervision_period.start_date,
                    supervision_period=supervision_period,
                ),
            )
        )

        supervision_events = self._test_find_supervision_events(
            identifier=self.identifier,
            supervision_periods=[supervision_period],
            incarceration_periods=[
                first_incarceration_period,
                second_incarceration_period,
            ],
            assessments=assessments,
            violation_responses=violation_reports,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_supervision_events_violation_history(self) -> None:
        state_code = "US_XX"

        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            external_id="sp1",
            state_code=state_code,
            start_date=date(2018, 1, 1),
            termination_date=date(2020, 1, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            sequence_num=0,
        )

        supervision_violation_1 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code=state_code,
            violation_date=date(2018, 4, 20),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    supervision_violation_type_entry_id=1,
                    state_code=state_code,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
                NormalizedStateSupervisionViolationTypeEntry(
                    supervision_violation_type_entry_id=1,
                    state_code=state_code,
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        violation_report_1 = NormalizedStateSupervisionViolationResponse(
            state_code=state_code,
            supervision_violation_response_id=888,
            external_id="svr1",
            response_date=date(2018, 4, 21),
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            supervision_violation=supervision_violation_1,
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    supervision_violation_response_decision_entry_id=1,
                    state_code=state_code,
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
            ],
            sequence_num=0,
        )

        supervision_violation_2 = NormalizedStateSupervisionViolation(
            supervision_violation_id=32663,
            external_id="sv2",
            state_code=state_code,
            violation_date=date(2019, 1, 20),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    supervision_violation_type_entry_id=1,
                    state_code=state_code,
                    violation_type=StateSupervisionViolationType.MISDEMEANOR,
                ),
            ],
        )

        violation_report_2 = NormalizedStateSupervisionViolationResponse(
            state_code=state_code,
            supervision_violation_response_id=999,
            external_id="svr2",
            response_date=date(2019, 1, 20),
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            supervision_violation=supervision_violation_2,
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    supervision_violation_response_decision_entry_id=1,
                    state_code=state_code,
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
            ],
            sequence_num=1,
        )

        violation_reports = [violation_report_1, violation_report_2]
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = [
            create_termination_event_from_period(
                supervision_period,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                case_type=StateSupervisionCaseType.GENERAL,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
                most_severe_violation_id=123455,
                violation_history_id_array="32663,123455",
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=2,
                in_supervision_population_on_date=True,
            ),
            create_start_event_from_period(
                supervision_period,
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
                    start_date=assert_type(
                        violation_report_1.response_date, datetime.date
                    ),
                ),
                supervision_type,
                end_date=violation_report_2.response_date,
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
                most_severe_violation_id=123455,
                violation_history_id_array="123455",
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
            )
        )

        expected_events.extend(
            expected_population_events(
                attr.evolve(
                    supervision_period,
                    start_date=assert_type(
                        violation_report_2.response_date, datetime.date
                    ),
                ),
                supervision_type,
                end_date=date(2019, 4, 21),
                most_severe_violation_type=StateSupervisionViolationType.FELONY,
                most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
                most_severe_violation_id=123455,
                violation_history_id_array="32663,123455",
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
                most_severe_violation_id=32663,
                violation_history_id_array="32663",
                most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
                response_count=1,
            )
        )

        supervision_events = self._test_find_supervision_events(
            identifier=self.identifier,
            supervision_periods=[supervision_period],
            incarceration_periods=[],
            assessments=[],
            violation_responses=violation_reports,
            supervision_contacts=supervision_contacts,
        )

        self.assertCountEqual(expected_events, supervision_events)


class TestFindPopulationEventsForSupervisionPeriod(unittest.TestCase):
    """Tests for the find_population_events_for_supervision_period function."""

    def setUp(self) -> None:
        self.maxDiff = None

        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            supervision_identifier
        )
        self.state_code = StateCode.US_XX
        self.identifier = SupervisionIdentifier(self.state_code)

        self.person = NormalizedStatePerson(
            state_code=self.state_code.value, person_id=12345
        )

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def test_find_population_events_for_supervision_period_revocation_no_termination(
        self,
    ) -> None:
        """Tests the find_population_events_for_supervision_period function
        when there is an incarceration period with a revocation admission,
        where there is no termination_date on the supervision_period, and
        no release_date on the incarceration_period."""

        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2003, 7, 5),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=111,
            external_id="ip1",
            state_code=self.state_code.value,
            admission_date=date(2003, 10, 10),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        assessments: List[NormalizedStateAssessment] = []

        violation_reports: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                RangeQuerier(
                    assessments, lambda assessment: assessment.assessment_date
                ),
                violation_reports,
                RangeQuerier(
                    supervision_contacts, lambda contact: contact.contact_date
                ),
            )
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        self.assertCountEqual(
            expected_population_events(
                supervision_period,
                supervision_type,
                end_date=incarceration_period.admission_date,
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

        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=111,
            external_id="ip1",
            state_code=self.state_code.value,
            admission_date=date(2018, 4, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2018, 5, 22),
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        assessments: List[NormalizedStateAssessment] = []
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = expected_population_events(
            supervision_period,
            supervision_type,
            end_date=incarceration_period.admission_date,
            case_compliances=_generate_case_compliances(
                person=self.person,
                start_date=supervision_period.start_date,
                supervision_period=supervision_period,
            ),
        )

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                RangeQuerier(
                    assessments, lambda assessment: assessment.assessment_date
                ),
                violation_responses,
                RangeQuerier(
                    supervision_contacts, lambda contact: contact.contact_date
                ),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    @freeze_time("2019-11-03 00:00:00-05:00")
    def test_find_population_events_for_supervision_period_nested_revocation_no_termination(
        self,
    ) -> None:
        """Tests the find_population_events_for_supervision_period function when there is an incarceration period with
        a revocation admission, a stay in prison, and a continued supervision period after release from incarceration
        that has still not terminated."""
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2019, 3, 5),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            case_type_entries=[
                NormalizedStateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=1,
                    state_code=self.state_code.value,
                    case_type=StateSupervisionCaseType.GENERAL,
                ),
            ],
        )

        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=111,
            external_id="ip1",
            state_code=self.state_code.value,
            admission_date=date(2019, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 10, 17),
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )
        assessments: List[NormalizedStateAssessment] = []

        violation_reports: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = expected_population_events(
            supervision_period,
            supervision_type,
            end_date=incarceration_period.admission_date,
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
                    supervision_period,
                    start_date=assert_type(
                        incarceration_period.release_date, datetime.date
                    ),
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
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                RangeQuerier(
                    assessments, lambda assessment: assessment.assessment_date
                ),
                violation_reports,
                RangeQuerier(
                    supervision_contacts, lambda contact: contact.contact_date
                ),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_population_events_for_supervision_period_admission_no_revocation(
        self,
    ) -> None:
        """Tests the find_population_events_for_supervision_period function when there is an incarceration period with a
        non-revocation admission before the supervision period's termination_date."""
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 6, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=111,
            external_id="ip1",
            state_code=self.state_code.value,
            admission_date=date(2018, 6, 2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        assessments: List[NormalizedStateAssessment] = []

        violation_reports: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = expected_population_events(
            supervision_period,
            supervision_type,
            end_date=incarceration_period.admission_date,
            case_compliances=_generate_case_compliances(
                person=self.person,
                start_date=supervision_period.start_date,
                supervision_period=supervision_period,
            ),
        )

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                RangeQuerier(
                    assessments, lambda assessment: assessment.assessment_date
                ),
                violation_reports,
                RangeQuerier(
                    supervision_contacts, lambda contact: contact.contact_date
                ),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_population_events_for_supervision_period_multiple_years(self) -> None:
        """Tests the find_population_events_for_supervision_period function when the supervision period overlaps
        multiple years, and there is an incarceration period during this time that also overlaps multiple years.
        """

        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2008, 3, 5),
            termination_date=date(2010, 3, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=111,
            external_id="ip1",
            state_code=self.state_code.value,
            admission_date=date(2008, 6, 2),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2009, 12, 3),
            release_reason=ReleaseReason.CONDITIONAL_RELEASE,
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        assessments: List[NormalizedStateAssessment] = []

        violation_reports: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = expected_population_events(
            supervision_period,
            supervision_type,
            end_date=incarceration_period.admission_date,
            case_compliances=_generate_case_compliances(
                person=self.person,
                start_date=supervision_period.start_date,
                supervision_period=supervision_period,
            ),
        )

        expected_events.extend(
            expected_population_events(
                attr.evolve(
                    supervision_period,
                    start_date=assert_type(
                        incarceration_period.release_date, datetime.date
                    ),
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
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                RangeQuerier(
                    assessments, lambda assessment: assessment.assessment_date
                ),
                violation_reports,
                RangeQuerier(
                    supervision_contacts, lambda contact: contact.contact_date
                ),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_population_events_for_supervision_period_ends_on_first(self) -> None:
        """Tests the find_population_events_for_supervision_period function for a
        supervision period with no incarceration periods, where the supervision
        period ends on the 1st day of a month."""

        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2001, 1, 5),
            termination_date=date(2001, 7, 1),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        assessments: List[NormalizedStateAssessment] = []

        violation_reports: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = expected_population_events(
            supervision_period,
            supervision_type,
        )

        event = SupervisionPopulationEvent(
            state_code=supervision_period.state_code,
            year=2001,
            month=6,
            event_date=date(2001, 6, 30),
            supervision_type=supervision_type,
            supervision_level=supervision_period.supervision_level,
            case_type=StateSupervisionCaseType.GENERAL,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            supervision_out_of_state=False,
        )

        self.assertEqual(
            expected_events[-1],
            event,
        )

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                RangeQuerier(
                    assessments, lambda assessment: assessment.assessment_date
                ),
                violation_reports,
                RangeQuerier(
                    supervision_contacts, lambda contact: contact.contact_date
                ),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_population_events_for_supervision_period_ends_on_last(self) -> None:
        """Tests the find_population_events_for_supervision_period function for a
        supervision period with no incarceration periods, where the supervision
        period ends on the last day of a month."""

        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2001, 1, 5),
            termination_date=date(2001, 6, 30),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        assessments: List[NormalizedStateAssessment] = []

        violation_reports: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_events = expected_population_events(
            supervision_period,
            supervision_type,
        )

        event = SupervisionPopulationEvent(
            state_code=supervision_period.state_code,
            year=2001,
            month=6,
            event_date=date(2001, 6, 29),
            supervision_type=supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            supervision_level=supervision_period.supervision_level,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            supervision_out_of_state=False,
        )

        self.assertEqual(
            expected_events[-1],
            event,
        )

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                RangeQuerier(
                    assessments, lambda assessment: assessment.assessment_date
                ),
                violation_reports,
                RangeQuerier(
                    supervision_contacts, lambda contact: contact.contact_date
                ),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_population_events_for_supervision_period_start_end_same_day(
        self,
    ) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2001, 3, 3),
            termination_date=date(2001, 3, 3),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        assessments: List[NormalizedStateAssessment] = []

        violation_reports: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                RangeQuerier(
                    assessments, lambda assessment: assessment.assessment_date
                ),
                violation_reports,
                RangeQuerier(
                    supervision_contacts, lambda contact: contact.contact_date
                ),
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

        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 11),
            termination_date=date(2018, 12, 10),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=111,
            external_id="ip1",
            state_code=self.state_code.value,
            admission_date=date(2018, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2018, 10, 27),
        )

        assessment_1 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2018, 3, 10),
            assessment_score_bucket=StateAssessmentLevel.HIGH.value,
            sequence_num=0,
        )

        assessment_2 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a2",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=24,
            assessment_level=StateAssessmentLevel.MEDIUM,
            assessment_date=date(2018, 10, 27),
            assessment_score_bucket=StateAssessmentLevel.MEDIUM.value,
            sequence_num=1,
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        assessments = [assessment_1, assessment_2]

        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

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
                    supervision_period,
                    start_date=assert_type(
                        incarceration_period.release_date, datetime.date
                    ),
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
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                RangeQuerier(
                    assessments, lambda assessment: assessment.assessment_date
                ),
                violation_responses,
                RangeQuerier(
                    supervision_contacts, lambda contact: contact.contact_date
                ),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_population_events_for_supervision_period_assessment_year_before(
        self,
    ) -> None:
        """Tests the find_population_events_for_supervision_period function
        when there is no revocation and the assessment date is the year before
        the start of the supervision."""

        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 1, 5),
            termination_date=date(2018, 3, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        assessment = NormalizedStateAssessment(
            assessment_id=1,
            state_code="US_CA",
            external_id="a1",
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=24,
            assessment_date=date(2017, 12, 17),
            assessment_score_bucket="24-29",
            sequence_num=0,
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        assessments = [assessment]
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

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
                supervision_period,
                supervision_period_index,
                incarceration_period_index,
                RangeQuerier(
                    assessments, lambda assessment: assessment.assessment_date
                ),
                violation_responses,
                RangeQuerier(
                    supervision_contacts, lambda contact: contact.contact_date
                ),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_population_events_for_supervision_period_with_a_supervision_downgrade(
        self,
    ) -> None:
        """Tests the find_population_events_for_supervision_period function
        when a supervision level downgrade has taken place."""

        supervision_periods = [
            NormalizedStateSupervisionPeriod(
                supervision_period_id=111,
                sequence_num=0,
                external_id="sp1",
                state_code=self.state_code.value,
                start_date=date(2018, 1, 5),
                termination_date=date(2018, 3, 19),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.HIGH,
            ),
            NormalizedStateSupervisionPeriod(
                supervision_period_id=123,
                sequence_num=1,
                external_id="sp2",
                state_code=self.state_code.value,
                start_date=date(2018, 3, 19),
                termination_date=date(2018, 4, 30),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MEDIUM,
            ),
        ]

        incarceration_period_index = default_normalized_ip_index_for_tests()
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=supervision_periods
        )

        assessments: List[NormalizedStateAssessment] = []
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

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

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_periods[1],
                supervision_period_index,
                incarceration_period_index,
                RangeQuerier(
                    assessments, lambda assessment: assessment.assessment_date
                ),
                violation_responses,
                RangeQuerier(
                    supervision_contacts, lambda contact: contact.contact_date
                ),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)

    def test_find_population_events_for_supervision_period_without_a_supervision_downgrade_out_of_time_frame(
        self,
    ) -> None:
        """Tests the find_population_events_for_supervision_period function
        when a supervision level downgrade has taken place."""

        supervision_periods = [
            NormalizedStateSupervisionPeriod(
                supervision_period_id=111,
                sequence_num=0,
                external_id="sp1",
                state_code=self.state_code.value,
                start_date=date(2010, 1, 5),
                termination_date=date(2010, 3, 19),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MAXIMUM,
            ),
            NormalizedStateSupervisionPeriod(
                supervision_period_id=123,
                sequence_num=1,
                external_id="sp2",
                state_code=self.state_code.value,
                start_date=date(2018, 3, 19),
                termination_date=date(2018, 4, 30),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                supervision_level=StateSupervisionLevel.MEDIUM,
            ),
        ]

        incarceration_period_index = default_normalized_ip_index_for_tests()
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=supervision_periods
        )

        assessments: List[NormalizedStateAssessment] = []
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []
        supervision_contacts: List[NormalizedStateSupervisionContact] = []

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

        supervision_events = (
            self.identifier._find_population_events_for_supervision_period(
                self.person,
                supervision_periods[1],
                supervision_period_index,
                incarceration_period_index,
                RangeQuerier(
                    assessments, lambda assessment: assessment.assessment_date
                ),
                violation_responses,
                RangeQuerier(
                    supervision_contacts, lambda contact: contact.contact_date
                ),
            )
        )

        self.assertCountEqual(expected_events, supervision_events)


class TestFindSupervisionTerminationEvent(unittest.TestCase):
    """Tests the find_supervision_termination_event function."""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            supervision_identifier
        )
        self.state_code = StateCode.US_XX
        self.identifier = SupervisionIdentifier(self.state_code)

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def test_find_supervision_termination_event(self) -> None:
        supervision_period_termination_date = date(2019, 5, 19)
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        first_assessment = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2018, 3, 10),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        first_reassessment_score = 29
        first_reassessment = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a2",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=first_reassessment_score,
            assessment_date=date(2018, 5, 18),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=1,
        )

        last_assessment_score = 19
        last_assessment = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a3",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=last_assessment_score,
            assessment_date=date(2019, 5, 10),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=2,
        )

        assessments = [first_assessment, first_reassessment, last_assessment]

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()

        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []

        termination_event = self.identifier._find_supervision_termination_event(
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
        )

        assessment_score_change = last_assessment_score - first_reassessment_score

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_termination_event = create_termination_event_from_period(
            supervision_period,
            supervision_type=supervision_type,
            assessment_score=last_assessment.assessment_score,
            assessment_type=last_assessment.assessment_type,
            assessment_score_change=assessment_score_change,
            in_supervision_population_on_date=True,
        )

        self.assertEqual(expected_termination_event, termination_event)

    def test_find_supervision_termination_event_no_assessments(self) -> None:
        supervision_period_termination_date = date(2019, 5, 19)
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        assessments: List[NormalizedStateAssessment] = []

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()

        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []

        termination_event = self.identifier._find_supervision_termination_event(
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_termination_event = create_termination_event_from_period(
            supervision_period,
            supervision_type=supervision_type,
            assessment_score=None,
            assessment_type=None,
            assessment_score_change=None,
            in_supervision_population_on_date=True,
        )

        self.assertEqual(expected_termination_event, termination_event)

    def test_find_supervision_termination_event_insufficient_assessments(self) -> None:
        supervision_period_termination_date = date(2019, 5, 19)
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=supervision_period_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        first_assessment = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2018, 3, 10),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        assessments = [first_assessment]

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()

        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []

        termination_event = self.identifier._find_supervision_termination_event(
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_termination_event = create_termination_event_from_period(
            supervision_period,
            supervision_type=supervision_type,
            assessment_score=None,
            assessment_type=None,
            assessment_score_change=None,
            in_supervision_population_on_date=True,
        )

        self.assertEqual(expected_termination_event, termination_event)

    def test_find_supervision_termination_event_no_termination(self) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        assessments: List[NormalizedStateAssessment] = []

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()

        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []

        termination_event = self.identifier._find_supervision_termination_event(
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
        )

        self.assertEqual(None, termination_event)

    def test_find_supervision_termination_event_multiple_in_month(self) -> None:
        """Tests that when multiple supervision periods end in the same month, the earliest start_date and the latest
        termination_date are used as the date boundaries for the assessments, but the termination_date on the
        supervision_period is still on the SupervisionTerminationEvent."""
        second_supervision_period_termination_date = date(2019, 11, 17)

        first_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            sequence_num=0,
            external_id="sp2",
            state_code=self.state_code.value,
            start_date=date(2018, 1, 1),
            termination_date=date(2019, 11, 23),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        second_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=222,
            sequence_num=1,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=second_supervision_period_termination_date,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        too_early_assessment = NormalizedStateAssessment(
            assessment_id=1,
            state_code="US_CA",
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=38,
            assessment_date=date(2017, 12, 10),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        assessment_1 = NormalizedStateAssessment(
            assessment_id=1,
            state_code="US_CA",
            external_id="a2",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2018, 1, 10),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=1,
        )

        assessment_2_score = 29
        assessment_2 = NormalizedStateAssessment(
            assessment_id=1,
            state_code="US_CA",
            external_id="a2",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=assessment_2_score,
            assessment_date=date(2018, 5, 18),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=2,
        )

        assessment_3_score = 19
        assessment_3 = NormalizedStateAssessment(
            assessment_id=1,
            state_code="US_CA",
            external_id="a3",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=assessment_3_score,
            assessment_date=date(2019, 11, 21),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=3,
        )

        assessments = [too_early_assessment, assessment_1, assessment_2, assessment_3]

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[first_supervision_period, second_supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()

        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []

        termination_event = self.identifier._find_supervision_termination_event(
            second_supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
        )

        first_supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        # in_supervision_population_on_date=True because another SP was in progress the same day as this termination
        expected_termination_event = create_termination_event_from_period(
            second_supervision_period,
            in_supervision_population_on_date=True,
            supervision_type=first_supervision_type,
            assessment_score=assessment_3_score,
            assessment_type=assessment_3.assessment_type,
            termination_reason=first_supervision_period.termination_reason,
            assessment_score_change=(assessment_3_score - assessment_2_score),
        )

        self.assertEqual(expected_termination_event, termination_event)

    def test_find_supervision_termination_event_incarceration_overlaps_full_supervision_period(
        self,
    ) -> None:
        supervision_period_termination_date = date(2018, 5, 19)
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
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
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        expected_termination_event = create_termination_event_from_period(
            supervision_period,
            supervision_type=supervision_type,
            in_supervision_population_on_date=True,
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
        first_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            sequence_num=0,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 3, 6),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        second_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=222,
            sequence_num=1,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 6),
            termination_date=date(2020, 5, 18),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        assessments: List[NormalizedStateAssessment] = []

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[first_supervision_period, second_supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()

        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []

        termination_event = self.identifier._find_supervision_termination_event(
            first_supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        if first_supervision_period.termination_date is None:
            raise ValueError(
                "Expected supervision_period.termination_date to be filled"
            )

        expected_termination_event = create_termination_event_from_period(
            first_supervision_period,
            in_incarceration_population_on_date=False,
            in_supervision_population_on_date=True,
            supervision_type=supervision_type,
            assessment_score=None,
            assessment_type=None,
            assessment_score_change=None,
        )

        self.assertEqual(expected_termination_event, termination_event)

    def test_find_supervision_termination_event_zero_day_termination_non_overlapping(
        self,
    ) -> None:
        """Tests that the SupervisionTerminationEvent has in_supervision_population_on_date=False
        if the supervision period is 0 full days and non-overlapping with other periods.
        """
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
        first_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            sequence_num=0,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 3, 5),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        second_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=222,
            sequence_num=1,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=date(2020, 5, 18),
            termination_reason=StateSupervisionPeriodTerminationReason.EXPIRATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        assessments: List[NormalizedStateAssessment] = []

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[first_supervision_period, second_supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests()

        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []

        termination_event = self.identifier._find_supervision_termination_event(
            first_supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        if first_supervision_period.termination_date is None:
            raise ValueError(
                "Expected supervision_period.termination_date to be filled"
            )

        expected_termination_event = create_termination_event_from_period(
            first_supervision_period,
            in_incarceration_population_on_date=False,
            in_supervision_population_on_date=False,
            supervision_type=supervision_type,
            assessment_score=None,
            assessment_type=None,
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
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=date(2018, 3, 5),
            termination_date=supervision_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=111,
            external_id="ip1",
            state_code=self.state_code.value,
            admission_date=incarceration_admission_date,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=incarceration_release_date,
            release_reason=ReleaseReason.SENTENCE_SERVED,
        )

        assessments: List[NormalizedStateAssessment] = []

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )

        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []

        termination_event = self.identifier._find_supervision_termination_event(
            supervision_period,
            supervision_period_index,
            incarceration_period_index,
            assessments,
            violation_responses,
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        if supervision_period.termination_date is None:
            raise ValueError(
                "Expected supervision_period.termination_date to be filled"
            )

        expected_termination_event = create_termination_event_from_period(
            supervision_period,
            in_incarceration_population_on_date=expected_in_incarceration_population_on_date,
            in_supervision_population_on_date=expected_in_supervision_population_on_date,
            supervision_type=supervision_type,
            assessment_score=None,
            assessment_type=None,
            assessment_score_change=None,
        )

        self.assertEqual(expected_termination_event, termination_event)


class TestFindSupervisionStartEvent(unittest.TestCase):
    """Tests the find_supervision_start_event function."""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            supervision_identifier
        )
        self.state_code = StateCode.US_XX
        self.identifier = SupervisionIdentifier(self.state_code)

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

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
        in_supervision_population_on_date=True because it's not a 0-day supervision period.
        """
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
        first_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            sequence_num=0,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=first_supervision_period_start_date,
            termination_date=date(2018, 3, 6),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        second_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=222,
            sequence_num=1,
            external_id="sp1",
            state_code=self.state_code.value,
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
        )

        if first_supervision_period.termination_date is None:
            raise ValueError(
                "Expected supervision_period.termination_date to be filled"
            )

        expected_start_event = create_start_event_from_period(
            first_supervision_period,
            in_incarceration_population_on_date=False,
            in_supervision_population_on_date=True,
            admission_reason=first_supervision_period.admission_reason,
        )

        self.assertEqual(expected_start_event, start_event)

    def test_find_supervision_start_event_zero_day_termination_non_overlapping(
        self,
    ) -> None:
        """Tests that the SupervisionStartEvent has in_supervision_population_on_date=False
        if the supervision period is 0 full days and non-overlapping with other periods.
        """
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
        first_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=111,
            sequence_num=0,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=start_date,
            termination_date=date(2018, 3, 5),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        second_supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=222,
            sequence_num=1,
            external_id="sp1",
            state_code=self.state_code.value,
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
        )

        if first_supervision_period.termination_date is None:
            raise ValueError(
                "Expected supervision_period.termination_date to be filled"
            )

        expected_start_event = create_start_event_from_period(
            first_supervision_period,
            in_incarceration_population_on_date=False,
            in_supervision_population_on_date=True,
            admission_reason=first_supervision_period.admission_reason,
        )

        self.assertEqual(expected_start_event, start_event)

    def run_overlapping_periods_test(
        self,
        supervision_start_date: date,
        incarceration_release_date: date,
        expected_in_incarceration_population_on_date: bool,
        expected_in_supervision_population_on_date: bool,
        incarceration_admission_date: date = date(2018, 2, 20),
        incarceration_authority: Optional[StateCustodialAuthority] = None,
    ) -> None:
        """Runs a test for find_supervision_start_event where there are overlapping
        supervision and incarceration periods."""
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=111,
            external_id="sp1",
            state_code=self.state_code.value,
            start_date=supervision_start_date,
            termination_date=date(2019, 5, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=111,
            external_id="ip1",
            state_code=self.state_code.value,
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
        )

        if supervision_period.start_date is None:
            raise ValueError("Expected supervision_period.start_date to be filled")

        expected_start_event = create_start_event_from_period(
            supervision_period,
            in_incarceration_population_on_date=expected_in_incarceration_population_on_date,
            in_supervision_population_on_date=expected_in_supervision_population_on_date,
            supervision_type=supervision_period.supervision_type,
            admission_reason=supervision_period.admission_reason,
        )

        self.assertEqual(expected_start_event, start_event)


class TestGetMostSevereResponseDecision(unittest.TestCase):
    """Tests the _get_most_severe_response_decision function."""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            supervision_identifier
        )
        self.state_code = StateCode.US_XX
        self.identifier = SupervisionIdentifier(self.state_code)

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def test_get_most_severe_response_decision(self) -> None:
        supervision_violation = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code=self.state_code.value,
            violation_date=date(2009, 1, 3),
        )

        supervision_violation_response = NormalizedStateSupervisionViolationResponse(
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code=self.state_code.value,
            response_date=date(2009, 1, 7),
            sequence_num=0,
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    supervision_violation_response_decision_entry_id=1,
                    state_code=self.state_code.value,
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    supervision_violation_response_decision_entry_id=1,
                    state_code=self.state_code.value,
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        most_severe_response_decision = recidiviz.pipelines.utils.violation_response_utils.get_most_severe_response_decision(
            [supervision_violation_response]
        )

        self.assertEqual(
            StateSupervisionViolationResponseDecision.REVOCATION,
            most_severe_response_decision,
        )

    def test_get_most_severe_response_decision_no_responses(self) -> None:
        most_severe_response_decision = recidiviz.pipelines.utils.violation_response_utils.get_most_severe_response_decision(
            []
        )

        self.assertIsNone(most_severe_response_decision)


class TestConvertEventsToDual(unittest.TestCase):
    """Tests the _convert_events_to_dual function."""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            supervision_identifier
        )
        self.state_code = StateCode.US_XX
        self.identifier = SupervisionIdentifier(self.state_code)

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

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
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 4),
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
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 4),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
        ]

        self.assertCountEqual(updated_events, expected_output)

    def test_convert_events_to_dual_us_mo_one_different_type(self) -> None:
        supervision_events: list[SupervisionEvent] = [
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
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
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            SupervisionPopulationEvent(
                state_code="US_MO",
                year=1900,
                month=1,
                event_date=date(1900, 1, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
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
    supervision_period: NormalizedStateSupervisionPeriod,
    supervision_type: StateSupervisionPeriodSupervisionType,
    *,
    end_date: Optional[date] = None,
    case_type: Optional[StateSupervisionCaseType] = StateSupervisionCaseType.GENERAL,
    assessment_score: Optional[int] = None,
    assessment_level: Optional[StateAssessmentLevel] = None,
    assessment_type: Optional[StateAssessmentType] = None,
    assessment_score_bucket: Optional[str] = None,
    most_severe_violation_type: Optional[StateSupervisionViolationType] = None,
    most_severe_violation_type_subtype: Optional[str] = None,
    most_severe_violation_id: Optional[int] = None,
    violation_history_id_array: Optional[str] = None,
    most_severe_response_decision: Optional[
        StateSupervisionViolationResponseDecision
    ] = None,
    response_count: Optional[int] = 0,
    level_1_supervision_location_external_id: Optional[str] = None,
    level_2_supervision_location_external_id: Optional[str] = None,
    case_compliances: Optional[Dict[date, SupervisionCaseCompliance]] = None,
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
                most_severe_violation_id=most_severe_violation_id,
                violation_history_id_array=violation_history_id_array,
                most_severe_response_decision=most_severe_response_decision,
                response_count=response_count,
                supervising_officer_staff_id=supervision_period.supervising_officer_staff_id,
                level_1_supervision_location_external_id=level_1_supervision_location_external_id,
                level_2_supervision_location_external_id=level_2_supervision_location_external_id,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_compliance=case_compliance,
                supervision_out_of_state=False,
            )

            expected_events.append(event)

    return expected_events


class TestFindAssessmentScoreChange(unittest.TestCase):
    """Tests the find_assessment_score_change function."""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            supervision_identifier
        )
        self.state_code = StateCode.US_XX
        self.identifier = SupervisionIdentifier(self.state_code)

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def test_find_assessment_score_change(self) -> None:
        assessment_1 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_score=33,
            assessment_date=date(2015, 3, 23),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        assessment_2 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a2",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=29,
            assessment_level=StateAssessmentLevel.HIGH,
            assessment_date=date(2015, 11, 2),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=1,
        )

        assessment_3 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a3",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=23,
            assessment_level=StateAssessmentLevel.MEDIUM,
            assessment_date=date(2016, 1, 13),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=2,
        )

        assessments = [assessment_1, assessment_2, assessment_3]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        (
            assessment_score_change,
            end_assessment_score,
            end_assessment_level,
            end_assessment_type,
            end_assessment_score_bucket,
        ) = self.identifier._find_assessment_score_change(
            start_date,
            termination_date,
            assessments,
        )

        self.assertEqual(-6, assessment_score_change)
        self.assertEqual(assessment_3.assessment_score, end_assessment_score)
        self.assertEqual(assessment_3.assessment_level, end_assessment_level)
        self.assertEqual(assessment_3.assessment_type, end_assessment_type)
        self.assertEqual(
            assessment_3.assessment_score_bucket, end_assessment_score_bucket
        )

    def test_find_assessment_score_change_insufficient_assessments(self) -> None:
        assessment_1 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2015, 3, 23),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        assessment_2 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a2",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=29,
            assessment_date=date(2015, 11, 2),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=1,
        )

        assessments = [assessment_1, assessment_2]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        (
            assessment_score_change,
            end_assessment_score,
            end_assessment_level,
            end_assessment_type,
            end_assessment_score_bucket,
        ) = self.identifier._find_assessment_score_change(
            start_date,
            termination_date,
            assessments,
        )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)
        self.assertEqual(end_assessment_score_bucket, DEFAULT_ASSESSMENT_SCORE_BUCKET)

    def test_find_assessment_score_change_first_reliable_assessment_is_first_assessment(
        self,
    ) -> None:
        assessment_1 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2015, 3, 23),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        assessment_2 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a2",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=29,
            assessment_date=date(2015, 11, 2),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=1,
        )

        assessments = [assessment_1, assessment_2]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        with patch.object(
            StateSpecificSupervisionDelegate,
            "get_index_of_first_reliable_supervision_assessment",
            return_value=0,
        ):
            (
                assessment_score_change,
                end_assessment_score,
                end_assessment_level,
                end_assessment_type,
                end_assessment_score_bucket,
            ) = self.identifier._find_assessment_score_change(
                start_date,
                termination_date,
                assessments,
            )

        self.assertEqual(-4, assessment_score_change)
        self.assertEqual(assessment_2.assessment_score, end_assessment_score)
        self.assertEqual(assessment_2.assessment_level, end_assessment_level)
        self.assertEqual(assessment_2.assessment_type, end_assessment_type)
        self.assertEqual(
            assessment_2.assessment_score_bucket, end_assessment_score_bucket
        )

    def test_find_assessment_score_change_different_type(self) -> None:
        assessment_1 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2015, 3, 23),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        assessment_2 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a2",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=29,
            assessment_date=date(2015, 11, 2),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=1,
        )

        assessment_3 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a3",
            assessment_type=StateAssessmentType.LSIR,
            assessment_score=23,
            assessment_date=date(2016, 1, 13),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=2,
        )

        assessments = [assessment_1, assessment_2, assessment_3]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        (
            assessment_score_change,
            end_assessment_score,
            end_assessment_level,
            end_assessment_type,
            end_assessment_score_bucket,
        ) = self.identifier._find_assessment_score_change(
            start_date,
            termination_date,
            assessments,
        )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)
        self.assertEqual(end_assessment_score_bucket, DEFAULT_ASSESSMENT_SCORE_BUCKET)

    def test_find_assessment_score_change_same_date(self) -> None:
        assessment_1 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2015, 3, 23),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        assessment_2 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a2",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=29,
            assessment_date=date(2015, 11, 2),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=1,
        )

        assessment_3 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a3",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=23,
            assessment_date=date(2016, 11, 2),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=2,
        )

        assessments = [assessment_1, assessment_2, assessment_3]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        (
            assessment_score_change,
            end_assessment_score,
            end_assessment_level,
            end_assessment_type,
            end_assessment_score_bucket,
        ) = self.identifier._find_assessment_score_change(
            start_date,
            termination_date,
            assessments,
        )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)
        self.assertEqual(end_assessment_score_bucket, DEFAULT_ASSESSMENT_SCORE_BUCKET)

    def test_find_assessment_score_change_no_assessments(self) -> None:
        assessments: List[NormalizedStateAssessment] = []

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        (
            assessment_score_change,
            end_assessment_score,
            end_assessment_level,
            end_assessment_type,
            end_assessment_score_bucket,
        ) = self.identifier._find_assessment_score_change(
            start_date,
            termination_date,
            assessments,
        )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)
        self.assertEqual(end_assessment_score_bucket, DEFAULT_ASSESSMENT_SCORE_BUCKET)

    def test_find_assessment_score_change_outside_boundary(self) -> None:
        assessment_1 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a1",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=33,
            assessment_date=date(2011, 3, 23),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=0,
        )

        assessment_2 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a2",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=29,
            assessment_date=date(2015, 11, 2),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=1,
        )

        assessment_3 = NormalizedStateAssessment(
            assessment_id=1,
            state_code=self.state_code.value,
            external_id="a3",
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
            assessment_score=23,
            assessment_date=date(2016, 1, 13),
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            sequence_num=2,
        )

        assessments = [assessment_1, assessment_2, assessment_3]

        start_date = date(2015, 2, 22)
        termination_date = date(2016, 3, 12)

        (
            assessment_score_change,
            end_assessment_score,
            end_assessment_level,
            end_assessment_type,
            end_assessment_score_bucket,
        ) = self.identifier._find_assessment_score_change(
            start_date,
            termination_date,
            assessments,
        )

        self.assertIsNone(assessment_score_change)
        self.assertIsNone(end_assessment_score)
        self.assertIsNone(end_assessment_level)
        self.assertIsNone(end_assessment_type)
        self.assertEqual(end_assessment_score_bucket, DEFAULT_ASSESSMENT_SCORE_BUCKET)


def create_start_event_from_period(
    period: NormalizedStateSupervisionPeriod,
    supervision_delegate: StateSpecificSupervisionDelegate = UsXxSupervisionDelegate(),
    **kwargs: Any,
) -> SupervisionStartEvent:
    """Creates the SupervisionStartEvent we expect to be created from the given
    period."""
    (
        level_1_supervision_location_external_id,
        level_2_supervision_location_external_id,
    ) = supervising_location_info(
        period,
        supervision_delegate,
    )

    assert period.start_date is not None

    event = SupervisionStartEvent(
        state_code=period.state_code,
        year=period.start_date.year,
        month=period.start_date.month,
        event_date=period.start_date,
        supervision_type=period.supervision_type,
        supervision_level_raw_text=period.supervision_level_raw_text,
        supervising_officer_staff_id=period.supervising_officer_staff_id,
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
    period: NormalizedStateSupervisionPeriod,
    supervision_delegate: StateSpecificSupervisionDelegate = UsXxSupervisionDelegate(),
    **kwargs: Any,
) -> SupervisionTerminationEvent:
    """Creates the SupervisionTerminationEvent we expect to be created from the given
    period."""
    (
        level_1_supervision_location_external_id,
        level_2_supervision_location_external_id,
    ) = supervising_location_info(
        period,
        supervision_delegate,
    )

    termination_reason = period.termination_reason

    if period.termination_date and not termination_reason:
        # Unset termination reasons will be set to INTERNAL_UNKNOWN in normalization
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
        supervising_officer_staff_id=period.supervising_officer_staff_id,
        level_1_supervision_location_external_id=level_1_supervision_location_external_id,
        level_2_supervision_location_external_id=level_2_supervision_location_external_id,
        case_type=StateSupervisionCaseType.GENERAL,
        termination_reason=termination_reason,
        assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
    )
    event = attr.evolve(event, **kwargs)
    return event


def _generate_case_compliances(
    *,
    person: NormalizedStatePerson,
    start_date: date,
    supervision_period: NormalizedStateSupervisionPeriod,
    assessments: Optional[List[NormalizedStateAssessment]] = None,
    face_to_face_contacts: Optional[List[NormalizedStateSupervisionContact]] = None,
    end_date_override: Optional[date] = None,
    next_recommended_assessment_date: Optional[date] = None,
    violation_responses: Optional[
        List[NormalizedStateSupervisionViolationResponse]
    ] = None,
    incarceration_period_index: Optional[NormalizedIncarcerationPeriodIndex] = None,
    supervision_delegate: Optional[StateSpecificSupervisionDelegate] = None,
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
        person,
        supervision_period,
        StateSupervisionCaseType.GENERAL,
        start_date,
        RangeQuerier(assessments or [], lambda assessment: assessment.assessment_date),
        RangeQuerier(face_to_face_contacts or [], lambda contact: contact.contact_date),
        violation_responses or [],
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

        if case_compliance:
            if next_recommended_assessment_date:
                case_compliance = attr.evolve(
                    case_compliance,
                    next_recommended_assessment_date=next_recommended_assessment_date,
                )
            case_compliances[current_date] = case_compliance
        current_date += relativedelta(days=1)

    return case_compliances
