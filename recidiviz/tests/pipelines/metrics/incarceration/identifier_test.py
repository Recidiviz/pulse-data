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
"""Tests for incarceration/identifier.py."""
import unittest
from datetime import date
from typing import Dict, List, Optional, Sequence, Union

from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_person import StateEthnicity
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
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
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
    NormalizedStateSupervisionViolationResponseDecisionEntry,
    NormalizedStateSupervisionViolationTypeEntry,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.assessment_normalization_manager import (
    DEFAULT_ASSESSMENT_SCORE_BUCKET,
)
from recidiviz.pipelines.metrics.incarceration import (
    identifier as incarceration_identifier,
)
from recidiviz.pipelines.metrics.incarceration.events import (
    IncarcerationAdmissionEvent,
    IncarcerationCommitmentFromSupervisionAdmissionEvent,
    IncarcerationEvent,
    IncarcerationReleaseEvent,
    IncarcerationStandardAdmissionEvent,
)
from recidiviz.pipelines.metrics.incarceration.identifier import IncarcerationIdentifier
from recidiviz.pipelines.utils.entity_normalization.normalized_incarceration_period_index import (
    NormalizedIncarcerationPeriodIndex,
)
from recidiviz.pipelines.utils.execution_utils import TableRow
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

_DEFAULT_IP_ID = 123
_DEFAULT_SP_ID = 999
_DEFAULT_SSVR_ID = 789


class TestFindIncarcerationEvents(unittest.TestCase):
    """Tests the find_incarceration_events function."""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            incarceration_identifier
        )
        self.identifier = IncarcerationIdentifier(StateCode.US_XX)
        self.person = NormalizedStatePerson(
            state_code="US_XX",
            person_id=99000123,
            ethnicity=StateEthnicity.PRESENT_WITHOUT_INFO,
        )

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def _run_find_incarceration_events(
        self,
        incarceration_periods: Optional[
            List[NormalizedStateIncarcerationPeriod]
        ] = None,
        supervision_periods: Optional[List[NormalizedStateSupervisionPeriod]] = None,
        assessments: Optional[List[NormalizedStateAssessment]] = None,
        violation_responses: Optional[
            List[NormalizedStateSupervisionViolationResponse]
        ] = None,
    ) -> List[IncarcerationEvent]:
        """Helper for testing the find_events function on the identifier."""
        all_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {
            NormalizedStateIncarcerationPeriod.__name__: incarceration_periods or [],
            NormalizedStateSupervisionPeriod.__name__: supervision_periods or [],
            NormalizedStateAssessment.__name__: assessments or [],
            NormalizedStateSupervisionViolationResponse.__name__: violation_responses
            or [],
        }

        return self.identifier.identify(
            self.person,
            all_kwargs,
            included_result_classes={
                IncarcerationStandardAdmissionEvent,
                IncarcerationCommitmentFromSupervisionAdmissionEvent,
                IncarcerationReleaseEvent,
            },
        )

    def test_find_incarceration_events(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=_DEFAULT_IP_ID,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="INCARCERATION_ADMISSION",
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_events = self._run_find_incarceration_events(
            incarceration_periods=[incarceration_period],
        )

        assert incarceration_period.admission_date is not None
        assert incarceration_period.release_date is not None
        expected_events = [
            IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility=incarceration_period.facility,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                admission_reason_raw_text="INCARCERATION_ADMISSION",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.release_date,
                facility=incarceration_period.facility,
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
                admission_reason=incarceration_period.admission_reason,
                total_days_incarcerated=(
                    incarceration_period.release_date
                    - incarceration_period.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
        ]

        self.assertCountEqual(expected_events, incarceration_events)

    def test_find_incarceration_events_transfer(self) -> None:
        incarceration_period_1 = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=_DEFAULT_IP_ID,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2009, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="NA",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2009, 12, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            sequence_num=0,
        )

        incarceration_period_2 = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON 10",
            admission_date=date(2009, 12, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2010, 2, 4),
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            sequence_num=1,
        )

        incarceration_events = self._run_find_incarceration_events(
            incarceration_periods=[incarceration_period_1, incarceration_period_2],
        )

        assert incarceration_period_1.admission_date is not None
        assert incarceration_period_2.release_date is not None
        expected_events = [
            IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period_1.state_code,
                event_date=incarceration_period_1.admission_date,
                facility=incarceration_period_1.facility,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                admission_reason_raw_text="NA",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period_2.state_code,
                event_date=incarceration_period_2.release_date,
                facility=incarceration_period_2.facility,
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
                admission_reason=incarceration_period_1.admission_reason,
                total_days_incarcerated=(
                    incarceration_period_2.release_date
                    - incarceration_period_1.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
        ]

        self.assertCountEqual(expected_events, incarceration_events)

    def test_find_incarceration_events_revocation_then_escape(self) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="XY2",
            state_code="US_XX",
            start_date=date(2001, 3, 13),
            termination_date=date(2008, 12, 20),
            supervision_site="X",
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period_1 = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=_DEFAULT_IP_ID,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2009, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2009, 12, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.ESCAPE,
            sequence_num=0,
        )

        incarceration_period_2 = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=2222,
            external_id="ip2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON 10",
            admission_date=date(2009, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2010, 2, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            sequence_num=1,
        )

        incarceration_events = self._run_find_incarceration_events(
            incarceration_periods=[incarceration_period_1, incarceration_period_2],
            supervision_periods=[supervision_period],
        )

        assert incarceration_period_1.admission_date is not None
        assert incarceration_period_1.release_date is not None
        assert incarceration_period_2.admission_date is not None
        assert incarceration_period_2.release_date is not None
        expected_events = [
            IncarcerationCommitmentFromSupervisionAdmissionEvent(
                state_code=incarceration_period_1.state_code,
                event_date=incarceration_period_1.admission_date,
                facility=incarceration_period_1.facility,
                admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                level_1_supervision_location_external_id="X",
                supervising_officer_staff_id=None,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period_2.state_code,
                event_date=incarceration_period_2.admission_date,
                facility=incarceration_period_2.facility,
                admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period_1.state_code,
                event_date=incarceration_period_1.release_date,
                facility=incarceration_period_1.facility,
                release_reason=StateIncarcerationPeriodReleaseReason.ESCAPE,
                admission_reason=incarceration_period_1.admission_reason,
                total_days_incarcerated=(
                    incarceration_period_1.release_date
                    - incarceration_period_1.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                commitment_from_supervision_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period_2.state_code,
                event_date=incarceration_period_2.release_date,
                facility=incarceration_period_2.facility,
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
                admission_reason=incarceration_period_1.admission_reason,
                total_days_incarcerated=(
                    incarceration_period_2.release_date
                    - incarceration_period_1.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                commitment_from_supervision_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            ),
        ]
        self.assertCountEqual(expected_events, incarceration_events)

    def test_find_incarceration_events_multiple_sentences(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=_DEFAULT_IP_ID,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_events = self._run_find_incarceration_events(
            incarceration_periods=[incarceration_period],
        )

        assert incarceration_period.admission_date is not None
        assert incarceration_period.release_date is not None
        expected_events: List[IncarcerationEvent] = [
            IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility=incarceration_period.facility,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.release_date,
                facility=incarceration_period.facility,
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
                admission_reason=incarceration_period.admission_reason,
                total_days_incarcerated=(
                    incarceration_period.release_date
                    - incarceration_period.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
        ]

        self.assertCountEqual(expected_events, incarceration_events)

    def test_find_incarceration_events_wrong_result_classes(self) -> None:
        with self.assertRaisesRegex(NotImplementedError, "Filtering of events"):
            self.identifier.identify(
                self.person,
                identifier_context={},
                included_result_classes={IncarcerationStandardAdmissionEvent},
            )


class TestAdmissionEventForPeriod(unittest.TestCase):
    """Tests the admission_event_for_period function."""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            incarceration_identifier
        )

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def _run_admission_event_for_period(
        self,
        state_code: StateCode,
        incarceration_period: NormalizedStateIncarcerationPeriod,
        incarceration_period_index: Optional[NormalizedIncarcerationPeriodIndex] = None,
        supervision_periods: Optional[List[NormalizedStateSupervisionPeriod]] = None,
        assessments: Optional[List[NormalizedStateAssessment]] = None,
        violation_responses: Optional[
            List[NormalizedStateSupervisionViolationResponse]
        ] = None,
    ) -> Optional[IncarcerationAdmissionEvent]:
        """Runs `_admission_event_for_period`."""
        if state_code != StateCode.US_XX:
            self._stop_state_specific_delegate_patchers()

        identifier = IncarcerationIdentifier(state_code)
        incarceration_period_index = (
            incarceration_period_index
            or default_normalized_ip_index_for_tests(
                incarceration_periods=[incarceration_period]
            )
        )
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=supervision_periods
        )
        assessments = assessments or []
        sorted_violation_responses = (
            sorted(violation_responses, key=lambda b: b.response_date or date.min)
            if violation_responses
            else []
        )

        # pylint: disable=protected-access
        return identifier._admission_event_for_period(
            incarceration_period=incarceration_period,
            incarceration_period_index=incarceration_period_index,
            supervision_period_index=supervision_period_index,
            assessments_by_date=RangeQuerier(
                assessments, lambda assessment: assessment.assessment_date
            ),
            sorted_violation_responses=sorted_violation_responses,
        )

    def test_admission_event_for_period_us_mo(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_MO",
            facility="PRISON3",
            admission_date=date(2010, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=1111,
            external_id="sp1",
            state_code="US_MO",
            start_date=date(2010, 1, 1),
            termination_date=date(2010, 2, 15),
        )

        admission_event = self._run_admission_event_for_period(
            state_code=StateCode.US_MO,
            incarceration_period=incarceration_period,
            supervision_periods=[supervision_period],
        )

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        self.assertEqual(
            IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility="PRISON3",
                admission_reason=incarceration_period.admission_reason,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            admission_event,
        )

    def test_admission_event_for_period(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        admission_event = self._run_admission_event_for_period(
            state_code=StateCode.US_XX,
            incarceration_period=incarceration_period,
        )

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        self.assertEqual(
            IncarcerationCommitmentFromSupervisionAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility="PRISON3",
                admission_reason=incarceration_period.admission_reason,
                supervision_type=StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            admission_event,
        )

    def test_admission_event_for_temporary_custody_period(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        admission_event = self._run_admission_event_for_period(
            state_code=StateCode.US_XX,
            incarceration_period=incarceration_period,
        )

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        self.assertEqual(
            IncarcerationCommitmentFromSupervisionAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility="PRISON3",
                admission_reason=incarceration_period.admission_reason,
                supervision_type=StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
                case_type=StateSupervisionCaseType.GENERAL,
                assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
            ),
            admission_event,
        )

    def test_admission_event_for_temporary_custody_period_returns_commitment_from_supervision(
        self,
    ) -> None:
        incarceration_period_tc = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=1,
            external_id="ip1",
            sequence_num=0,
            state_code="US_XX",
            admission_date=date(2020, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )
        incarceration_period_same_date_not_rev = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=2,
            external_id="ip2",
            sequence_num=1,
            state_code="US_XX",
            admission_date=date(2020, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_period_diff_date_rev = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=3,
            external_id="ip3",
            sequence_num=2,
            state_code="US_XX",
            admission_date=date(2023, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )
        incarceration_period_same_date_rev = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=4,
            external_id="ip4",
            sequence_num=3,
            state_code="US_XX",
            admission_date=date(2020, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
        )
        self.assertIsInstance(
            self._run_admission_event_for_period(
                state_code=StateCode.US_XX,
                incarceration_period=incarceration_period_tc,
            ),
            IncarcerationCommitmentFromSupervisionAdmissionEvent,
        )
        self.assertIsInstance(
            self._run_admission_event_for_period(
                state_code=StateCode.US_XX,
                incarceration_period=incarceration_period_same_date_not_rev,
            ),
            IncarcerationStandardAdmissionEvent,
        )
        incarceration_period_index_no_revocation = (
            default_normalized_ip_index_for_tests(
                incarceration_periods=[
                    incarceration_period_tc,
                    incarceration_period_same_date_not_rev,
                ]
            )
        )
        incarceration_period_index_revocation_different_date = (
            default_normalized_ip_index_for_tests(
                incarceration_periods=[
                    incarceration_period_tc,
                    incarceration_period_same_date_not_rev,
                    incarceration_period_diff_date_rev,
                ]
            )
        )
        incarceration_period_index_revocation_same_date = (
            default_normalized_ip_index_for_tests(
                incarceration_periods=[
                    incarceration_period_tc,
                    incarceration_period_same_date_not_rev,
                    incarceration_period_diff_date_rev,
                    incarceration_period_same_date_rev,
                ]
            )
        )
        self.assertIsInstance(
            self._run_admission_event_for_period(
                state_code=StateCode.US_XX,
                incarceration_period=incarceration_period_tc,
                incarceration_period_index=incarceration_period_index_no_revocation,
            ),
            IncarcerationCommitmentFromSupervisionAdmissionEvent,
        )
        self.assertIsInstance(
            self._run_admission_event_for_period(
                state_code=StateCode.US_XX,
                incarceration_period=incarceration_period_tc,
                incarceration_period_index=incarceration_period_index_revocation_different_date,
            ),
            IncarcerationCommitmentFromSupervisionAdmissionEvent,
        )
        self.assertIsInstance(
            self._run_admission_event_for_period(
                state_code=StateCode.US_XX,
                incarceration_period=incarceration_period_tc,
                incarceration_period_index=incarceration_period_index_revocation_same_date,
            ),
            IncarcerationStandardAdmissionEvent,
        )

    def test_admission_event_for_period_all_admission_reasons(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        for admission_reason in StateIncarcerationPeriodAdmissionReason:
            incarceration_period.admission_reason = admission_reason

            if admission_reason not in (
                # ADMITTED_FROM_SUPERVISION is an ingest-only enum
                StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
                # We don't produce admission events for transfers
                StateIncarcerationPeriodAdmissionReason.TRANSFER,
            ):
                admission_event = self._run_admission_event_for_period(
                    state_code=StateCode.US_XX,
                    incarceration_period=incarceration_period,
                )

                self.assertIsNotNone(admission_event)

    def test_admission_event_for_period_specialized_pfi(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        admission_event = self._run_admission_event_for_period(
            state_code=StateCode.US_XX,
            incarceration_period=incarceration_period,
        )

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        self.assertEqual(
            IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility="PRISON3",
                admission_reason=incarceration_period.admission_reason,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            admission_event,
        )

    def test_admission_event_for_period_county_jail(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            state_code="US_XX",
            facility="CJ10",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        admission_event = self._run_admission_event_for_period(
            state_code=StateCode.US_XX,
            incarceration_period=incarceration_period,
        )

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        self.assertEqual(
            IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility="CJ10",
                admission_reason=incarceration_period.admission_reason,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            admission_event,
        )

    def test_cannot_instantiate_IncarcerationAdmissionEvent(self) -> None:
        """Test to confirm that an exception will be raised if an
        IncarcerationAdmissionEvent is instantiated directly."""
        with self.assertRaises(Exception):
            _ = IncarcerationAdmissionEvent(
                state_code="US_XX",
                event_date=date(2000, 12, 1),
                facility="CJ10",
            )


class TestCommitmentFromSupervisionEventForPeriod(unittest.TestCase):
    """Tests the _commitment_from_supervision_event_for_period function."""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            incarceration_identifier
        )

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def _run_commitment_from_supervision_event_for_period(
        self,
        incarceration_period: NormalizedStateIncarcerationPeriod,
        pre_commitment_supervision_period: Optional[NormalizedStateSupervisionPeriod],
        incarceration_period_index: Optional[NormalizedIncarcerationPeriodIndex] = None,
        assessments: Optional[List[NormalizedStateAssessment]] = None,
        violation_responses: Optional[
            List[NormalizedStateSupervisionViolationResponse]
        ] = None,
    ) -> IncarcerationCommitmentFromSupervisionAdmissionEvent:
        """Helper function for testing the
        _commitment_from_supervision_event_for_period function."""

        state_code = StateCode(incarceration_period.state_code)
        if state_code != StateCode.US_XX:
            self._stop_state_specific_delegate_patchers()

        assessments = assessments or []
        sorted_violation_responses = (
            sorted(violation_responses, key=lambda b: b.response_date or date.min)
            if violation_responses
            else []
        )

        incarceration_period_index = (
            incarceration_period_index
            or default_normalized_ip_index_for_tests(
                incarceration_periods=[incarceration_period]
            )
        )
        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=(
                [pre_commitment_supervision_period]
                if pre_commitment_supervision_period
                else []
            )
        )

        # pylint: disable=protected-access
        return IncarcerationIdentifier(
            StateCode(incarceration_period.state_code)
        )._commitment_from_supervision_event_for_period(
            incarceration_period=incarceration_period,
            incarceration_period_index=incarceration_period_index,
            supervision_period_index=supervision_period_index,
            assessments_by_date=RangeQuerier(
                assessments, lambda assessment: assessment.assessment_date
            ),
            sorted_violation_responses=sorted_violation_responses,
        )

    def test_commitment_from_supervision_event_violation_history_cutoff(self) -> None:
        """Tests the _commitment_from_supervision_event_for_period function,
        specifically the logic that includes the violation reports within the violation
        window. The `old` response and violation fall within a year of the last
        violation response before the revocation admission, but not within a year of the
        revocation date. Test that the `old` response is included in the response
        history."""

        supervision_violation_1 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_XX",
            violation_date=date(2008, 12, 7),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    supervision_violation_type_entry_id=1,
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                )
            ],
        )

        supervision_violation_response_1 = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            state_code="US_XX",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2008, 12, 7),
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    supervision_violation_response_decision_entry_id=1,
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                )
            ],
            supervision_violation=supervision_violation_1,
        )

        supervision_violation_2 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv2",
            state_code="US_XX",
            violation_date=date(2009, 11, 13),
        )

        supervision_violation_response_2 = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr2",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_XX",
            response_date=date(2009, 11, 13),
            supervision_violation_response_decisions=[
                # This REVOCATION decision is the most severe, but this is not the most recent response
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    supervision_violation_response_decision_entry_id=1,
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                )
            ],
            supervision_violation=supervision_violation_2,
        )

        supervision_violation_3 = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            supervision_violation_id=6789,
            external_id="sv3",
            violation_date=date(2009, 12, 1),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    supervision_violation_type_entry_id=1,
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                )
            ],
        )

        supervision_violation_response_3 = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            state_code="US_XX",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr3",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2009, 12, 1),
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    supervision_violation_response_decision_entry_id=1,
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                )
            ],
            supervision_violation=supervision_violation_3,
        )

        supervision_period = NormalizedStateSupervisionPeriod(
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2009, 12, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_site="OFFICE_1",
            sequence_num=0,
        )

        incarceration_period = NormalizedStateIncarcerationPeriod(
            incarceration_period_id=111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=date(2009, 12, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            sequence_num=0,
        )

        violation_responses = [
            supervision_violation_response_1,
            supervision_violation_response_2,
            supervision_violation_response_3,
        ]

        commitment_from_supervision_event = (
            self._run_commitment_from_supervision_event_for_period(
                pre_commitment_supervision_period=supervision_period,
                incarceration_period=incarceration_period,
                violation_responses=violation_responses,
            )
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        expected_commitment_from_supervision_event = IncarcerationCommitmentFromSupervisionAdmissionEvent(
            state_code=supervision_period.state_code,
            event_date=incarceration_period.admission_date,
            admission_reason=incarceration_period.admission_reason,
            supervision_type=supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
            most_severe_violation_id=123455,
            violation_history_id_array="6789,123455",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            most_recent_response_decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
            response_count=3,
            violation_history_description="1felony;1technical",
            violation_type_frequency_counter=[["FELONY"], ["TECHNICAL"]],
            level_1_supervision_location_external_id="OFFICE_1",
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
        )

        self.assertEqual(
            expected_commitment_from_supervision_event,
            commitment_from_supervision_event,
        )

    def test_commitment_from_supervision_event_before_violation_history_cutoff(
        self,
    ) -> None:
        """Tests the _commitment_from_supervision_event_for_period function, specifically the logic that includes the violation
        reports within the violation window. The `old` response and violation falls before the violation history
        window. Test that the `old` response is not included in the response history."""

        supervision_violation_old = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_XX",
            violation_date=date(2007, 12, 7),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    supervision_violation_type_entry_id=1,
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                )
            ],
        )

        supervision_violation_response_old = (
            NormalizedStateSupervisionViolationResponse(
                sequence_num=0,
                state_code="US_XX",
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                external_id="svr1",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2007, 12, 7),
                supervision_violation_response_decisions=[
                    NormalizedStateSupervisionViolationResponseDecisionEntry(
                        supervision_violation_response_decision_entry_id=1,
                        state_code="US_XX",
                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    )
                ],
                supervision_violation=supervision_violation_old,
            )
        )

        supervision_violation = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            external_id="sv2",
            supervision_violation_id=6789,
            violation_date=date(2009, 12, 1),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    supervision_violation_type_entry_id=1,
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                )
            ],
        )

        supervision_violation_response = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            state_code="US_XX",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr2",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2009, 12, 1),
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    supervision_violation_response_decision_entry_id=1,
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                ),
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    supervision_violation_response_decision_entry_id=1,
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                ),
            ],
            supervision_violation=supervision_violation,
        )

        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2009, 12, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_site="OFFICE_1",
        )

        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=date(2009, 12, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        violation_responses = [
            supervision_violation_response,
            supervision_violation_response_old,
        ]

        commitment_from_supervision_event = (
            self._run_commitment_from_supervision_event_for_period(
                pre_commitment_supervision_period=supervision_period,
                incarceration_period=incarceration_period,
                violation_responses=violation_responses,
            )
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        expected_commitment_from_supervision_event = IncarcerationCommitmentFromSupervisionAdmissionEvent(
            state_code=supervision_period.state_code,
            event_date=incarceration_period.admission_date,
            admission_reason=incarceration_period.admission_reason,
            supervision_type=supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
            most_severe_violation_type_subtype=StateSupervisionViolationType.TECHNICAL.value,
            most_severe_violation_id=6789,
            violation_history_id_array="6789",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            most_recent_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1technical",
            violation_type_frequency_counter=[["TECHNICAL"]],
            level_1_supervision_location_external_id="OFFICE_1",
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
        )

        self.assertEqual(
            expected_commitment_from_supervision_event,
            commitment_from_supervision_event,
        )

    def test_commitment_from_supervision_event_us_mo_ignore_supplemental_for_lookback_date(
        self,
    ) -> None:
        """Tests the _commitment_from_supervision_event_for_period function, specifically the logic that includes the violation
        reports within the violation window. The most recent response prior to the revocation is a supplemental report,
        which should not be included when determining the date of the most recent response for the violation history
        window. Tests that the date on the most recent non-supplemental report is used for the violation history, but
        that the response decision on the supplemental report is counted for the most_severe_response_decision.
        """
        state_code = "US_MO"

        supervision_violation = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code=state_code,
            violation_date=date(2008, 12, 7),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    supervision_violation_type_entry_id=1,
                    state_code=state_code,
                    violation_type=StateSupervisionViolationType.FELONY,
                )
            ],
        )

        supervision_violation_response = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            state_code=state_code,
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype="INI",
            response_date=date(2008, 12, 7),
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    supervision_violation_response_decision_entry_id=1,
                    state_code=state_code,
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                )
            ],
            supervision_violation=supervision_violation,
        )

        supervision_violation_sup = NormalizedStateSupervisionViolation(
            state_code=state_code,
            supervision_violation_id=6789,
            external_id="sv2",
            violation_date=date(2012, 12, 1),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    supervision_violation_type_entry_id=1,
                    state_code=state_code,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                )
            ],
        )

        supervision_violation_response_sup = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            state_code=state_code,
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr2",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_subtype="SUP",
            response_date=date(2012, 12, 1),
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    supervision_violation_response_decision_entry_id=1,
                    state_code=state_code,
                    decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
                )
            ],
            supervision_violation=supervision_violation_sup,
        )

        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            state_code=state_code,
            start_date=date(2008, 3, 5),
            termination_date=date(2012, 12, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_site="OFFICE_1",
        )

        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            admission_date=date(2012, 12, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        assert supervision_period.start_date is not None
        assert supervision_period.termination_date is not None
        assert incarceration_period.admission_date is not None

        violation_responses = [
            supervision_violation_response,
            supervision_violation_response_sup,
        ]

        commitment_from_supervision_event = (
            self._run_commitment_from_supervision_event_for_period(
                pre_commitment_supervision_period=supervision_period,
                incarceration_period=incarceration_period,
                violation_responses=violation_responses,
            )
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        expected_commitment_from_supervision_event = IncarcerationCommitmentFromSupervisionAdmissionEvent(
            state_code=supervision_period.state_code,
            event_date=incarceration_period.admission_date,
            admission_reason=incarceration_period.admission_reason,
            supervision_type=supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
            most_severe_violation_id=123455,
            violation_history_id_array="123455",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            most_recent_response_decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            response_count=1,
            violation_history_description="1fel",
            violation_type_frequency_counter=[["FELONY"]],
            level_1_supervision_location_external_id="OFFICE_1",
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
        )

        self.assertEqual(
            expected_commitment_from_supervision_event,
            commitment_from_supervision_event,
        )

    def test_commitment_from_supervision_event_us_nd(self) -> None:
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_site="4",
        )

        supervision_violation = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_ND",
            violation_date=date(2018, 4, 20),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    supervision_violation_type_entry_id=1,
                    state_code="US_ND",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        ssvr = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            state_code="US_ND",
            external_id="svr1",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            supervision_violation=supervision_violation,
            response_date=date(2018, 5, 25),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_ND",
            admission_date=date(2018, 6, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="PARL",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
        )

        commitment_from_supervision_event = (
            self._run_commitment_from_supervision_event_for_period(
                pre_commitment_supervision_period=supervision_period,
                incarceration_period=incarceration_period,
                violation_responses=[ssvr],
            )
        )

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        expected_commitment_from_supervision_event = IncarcerationCommitmentFromSupervisionAdmissionEvent(
            state_code=supervision_period.state_code,
            event_date=incarceration_period.admission_date,
            admission_reason=incarceration_period.admission_reason,
            admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
            most_severe_violation_id=123455,
            violation_history_id_array="123455",
            response_count=1,
            violation_history_description="1felony",
            violation_type_frequency_counter=[["FELONY"]],
            level_1_supervision_location_external_id="4",
            level_2_supervision_location_external_id=None,
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
        )

        self.assertEqual(
            expected_commitment_from_supervision_event,
            commitment_from_supervision_event,
        )

    def test_most_severe_violation_and_incarceration_admission_violation_type(
        self,
    ) -> None:
        """Tests that most_severe_violation_type, most_severe_violation_subtype, and
        most_severe_violation_id are all set correctly when incarceration_admission_violation_type
        is valued.  That is, most_severe_violation_type should be set according to
        incarceration_admission_violation_type while the most_severe_violation_subtype and
        most_severe_violation_id should set to null."""

        supervision_violation_1 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_XX",
            violation_date=date(2008, 12, 7),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    supervision_violation_type_entry_id=1,
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.FELONY,
                )
            ],
        )

        supervision_violation_response_1 = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            state_code="US_XX",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr1",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2008, 12, 7),
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    supervision_violation_response_decision_entry_id=1,
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                )
            ],
            supervision_violation=supervision_violation_1,
        )

        supervision_violation_2 = NormalizedStateSupervisionViolation(
            supervision_violation_id=123456,
            external_id="sv2",
            state_code="US_XX",
            violation_date=date(2009, 11, 13),
        )

        supervision_violation_response_2 = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr2",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            state_code="US_XX",
            response_date=date(2009, 11, 13),
            supervision_violation_response_decisions=[
                # This REVOCATION decision is the most severe, but this is not the most recent response
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    supervision_violation_response_decision_entry_id=1,
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.REVOCATION,
                )
            ],
            supervision_violation=supervision_violation_2,
        )

        supervision_violation_3 = NormalizedStateSupervisionViolation(
            state_code="US_XX",
            supervision_violation_id=6789,
            external_id="sv3",
            violation_date=date(2009, 12, 1),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    supervision_violation_type_entry_id=1,
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                )
            ],
        )

        supervision_violation_response_3 = NormalizedStateSupervisionViolationResponse(
            sequence_num=0,
            state_code="US_XX",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            external_id="svr3",
            response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            response_date=date(2009, 12, 1),
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    supervision_violation_response_decision_entry_id=1,
                    state_code="US_XX",
                    decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
                )
            ],
            supervision_violation=supervision_violation_3,
        )

        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2009, 12, 19),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_site="OFFICE_1",
        )

        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            admission_date=date(2009, 12, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            incarceration_admission_violation_type=StateSupervisionViolationType.TECHNICAL,
        )

        violation_responses = [
            supervision_violation_response_1,
            supervision_violation_response_2,
            supervision_violation_response_3,
        ]

        commitment_from_supervision_event = (
            self._run_commitment_from_supervision_event_for_period(
                pre_commitment_supervision_period=supervision_period,
                incarceration_period=incarceration_period,
                violation_responses=violation_responses,
            )
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PROBATION

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        expected_commitment_from_supervision_event = IncarcerationCommitmentFromSupervisionAdmissionEvent(
            state_code=supervision_period.state_code,
            event_date=incarceration_period.admission_date,
            admission_reason=incarceration_period.admission_reason,
            supervision_type=supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            most_severe_violation_type_subtype="FELONY",
            most_severe_violation_id=123455,
            violation_history_id_array="4,6789,123455,123456",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            most_recent_response_decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
            response_count=3,
            violation_history_description="1felony;2technical",
            violation_type_frequency_counter=[["FELONY"], ["TECHNICAL"], ["TECHNICAL"]],
            level_1_supervision_location_external_id="OFFICE_1",
            assessment_score_bucket=DEFAULT_ASSESSMENT_SCORE_BUCKET,
        )

        self.assertEqual(
            expected_commitment_from_supervision_event,
            commitment_from_supervision_event,
        )


class TestReleaseEventForPeriod(unittest.TestCase):
    """Tests the release_event_for_period function."""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            incarceration_identifier
        )

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def _run_release_for_period(
        self, incarceration_period: NormalizedStateIncarcerationPeriod
    ) -> Optional[IncarcerationReleaseEvent]:
        """Helper for testing `_release_event_for_period`."""

        state_code = StateCode(incarceration_period.state_code)
        if state_code != StateCode.US_XX:
            self._stop_state_specific_delegate_patchers()

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )

        supervision_period_index = default_normalized_sp_index_for_tests()

        # pylint: disable=protected-access
        return IncarcerationIdentifier(
            StateCode(incarceration_period.state_code)
        )._release_event_for_period(
            incarceration_period,
            incarceration_period_index,
            supervision_period_index,
            {},  # commitments from supervision
        )

    def test_release_event_for_period(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=1111,
            external_id="sp1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            release_reason_raw_text="SS",
        )

        release_event = self._run_release_for_period(incarceration_period)

        assert incarceration_period.release_reason is not None
        assert incarceration_period.release_date is not None
        assert incarceration_period.admission_date is not None
        self.assertEqual(
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.release_date,
                facility="PRISON3",
                release_reason=incarceration_period.release_reason,
                release_reason_raw_text=incarceration_period.release_reason_raw_text,
                purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
                admission_reason=incarceration_period.admission_reason,
                total_days_incarcerated=(
                    incarceration_period.release_date
                    - incarceration_period.admission_date
                ).days,
            ),
            release_event,
        )

    def test_release_event_for_period_all_release_reasons(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        for release_reason in StateIncarcerationPeriodReleaseReason:
            if release_reason == StateIncarcerationPeriodReleaseReason.TRANSFER:
                continue

            incarceration_period.release_reason = release_reason

            release_event = self._run_release_for_period(incarceration_period)

            self.assertIsNotNone(release_event)

    def test_release_event_for_period_county_jail(self) -> None:
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            state_code="US_XX",
            facility="CJ19",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        release_event = self._run_release_for_period(incarceration_period)

        assert incarceration_period.release_reason is not None
        assert incarceration_period.release_date is not None
        assert incarceration_period.admission_date is not None
        self.assertEqual(
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.release_date,
                facility="CJ19",
                release_reason=incarceration_period.release_reason,
                admission_reason=incarceration_period.admission_reason,
                total_days_incarcerated=(
                    incarceration_period.release_date
                    - incarceration_period.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            release_event,
        )

    def test_release_event_for_period_us_ix(self) -> None:
        self._stop_state_specific_delegate_patchers()
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_IX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="SS",
        )

        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            state_code="US_IX",
            external_id="sp1",
            supervision_period_id=111,
            start_date=assert_type(incarceration_period.release_date, date),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        # pylint: disable=protected-access
        release_event = IncarcerationIdentifier(
            StateCode.US_IX
        )._release_event_for_period(
            incarceration_period=incarceration_period,
            incarceration_period_index=incarceration_period_index,
            supervision_period_index=supervision_period_index,
            commitments_from_supervision={},
        )

        assert incarceration_period.release_reason is not None
        assert incarceration_period.release_date is not None
        assert incarceration_period.admission_date is not None
        self.assertEqual(
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.release_date,
                facility="PRISON3",
                release_reason=incarceration_period.release_reason,
                release_reason_raw_text=incarceration_period.release_reason_raw_text,
                purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
                supervision_type_at_release=StateSupervisionPeriodSupervisionType.PROBATION,
                admission_reason=incarceration_period.admission_reason,
                total_days_incarcerated=(
                    incarceration_period.release_date
                    - incarceration_period.admission_date
                ).days,
            ),
            release_event,
        )

    def test_release_event_for_period_us_mo(self) -> None:
        self._stop_state_specific_delegate_patchers()
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_MO",
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=1111,
            external_id="sp1",
            state_code="US_MO",
            start_date=date(2019, 12, 4),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        # pylint: disable=protected-access
        release_event = IncarcerationIdentifier(
            StateCode.US_MO
        )._release_event_for_period(
            incarceration_period=incarceration_period,
            incarceration_period_index=incarceration_period_index,
            supervision_period_index=supervision_period_index,
            commitments_from_supervision={},
        )

        assert incarceration_period.release_date is not None
        assert incarceration_period.release_reason is not None
        assert incarceration_period.admission_date is not None
        self.assertEqual(
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.release_date,
                facility="PRISON3",
                release_reason=incarceration_period.release_reason,
                release_reason_raw_text=incarceration_period.release_reason_raw_text,
                purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
                supervision_type_at_release=StateSupervisionPeriodSupervisionType.PROBATION,
                admission_reason=incarceration_period.admission_reason,
                total_days_incarcerated=(
                    incarceration_period.release_date
                    - incarceration_period.admission_date
                ).days,
            ),
            release_event,
        )

    def test_release_event_for_period_us_nd(self) -> None:
        self._stop_state_specific_delegate_patchers()
        admission_date = date(2008, 11, 20)
        release_date = date(2010, 12, 4)
        admission_reason = StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION
        release_reason = StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE
        incarceration_period = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_ND",
            facility="NDSP",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            admission_date=admission_date,
            admission_reason=admission_reason,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=release_date,
            release_reason=release_reason,
            release_reason_raw_text="RPAR",
        )
        supervision_period = NormalizedStateSupervisionPeriod(
            sequence_num=0,
            supervision_period_id=1112,
            state_code="US_ND",
            external_id="sp1",
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            start_date=date(2010, 12, 4),
        )

        incarceration_period_index = default_normalized_ip_index_for_tests(
            incarceration_periods=[incarceration_period]
        )

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=[supervision_period]
        )

        # pylint: disable=protected-access
        release_event = IncarcerationIdentifier(
            StateCode.US_ND
        )._release_event_for_period(
            incarceration_period,
            incarceration_period_index,
            supervision_period_index,
            {},
        )

        self.assertEqual(
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=release_date,
                facility=incarceration_period.facility,
                release_reason=release_reason,
                release_reason_raw_text=incarceration_period.release_reason_raw_text,
                purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
                supervision_type_at_release=StateSupervisionPeriodSupervisionType.PAROLE,
                admission_reason=admission_reason,
                total_days_incarcerated=(release_date - admission_date).days,
            ),
            release_event,
        )
