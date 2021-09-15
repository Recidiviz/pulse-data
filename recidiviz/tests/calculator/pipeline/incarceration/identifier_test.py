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
"""Tests for incarceration/identifier.py."""
import unittest
from datetime import date
from typing import Any, Dict, List, Optional
from unittest import mock

import attr
from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

from recidiviz.calculator.pipeline.incarceration import identifier
from recidiviz.calculator.pipeline.incarceration.events import (
    IncarcerationAdmissionEvent,
    IncarcerationCommitmentFromSupervisionAdmissionEvent,
    IncarcerationEvent,
    IncarcerationReleaseEvent,
    IncarcerationStandardAdmissionEvent,
    IncarcerationStayEvent,
)
from recidiviz.calculator.pipeline.utils.pre_processed_incarceration_period_index import (
    PreProcessedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_commitment_from_supervision_delegate import (
    StateSpecificCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_sentence_classification import (
    SupervisionTypeSpan,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_delegate import (
    UsMoSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violations_delegate import (
    UsMoViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_commitment_from_supervision_delegate import (
    UsNdCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_delegate import (
    UsNdSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_violations_delegate import (
    UsNdViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_incarceration_period_pre_processing_delegate import (
    SHOCK_INCARCERATION_9_MONTHS,
)
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecidingBodyType,
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateCharge,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateSentenceGroup,
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.us_xx.us_xx_commitment_from_supervision_utils import (
    UsXxCommitmentFromSupervisionDelegate,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.us_xx.us_xx_incarceration_period_pre_processing_delegate import (
    UsXxIncarcerationPreProcessingDelegate,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.us_xx.us_xx_violation_response_preprocessing_delegate import (
    UsXxViolationResponsePreprocessingDelegate,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.us_xx.us_xx_violations_delegate import (
    UsXxViolationDelegate,
)
from recidiviz.tests.calculator.pipeline.utils.us_mo_fakes import (
    FakeUsMoIncarcerationSentence,
    FakeUsMoSupervisionSentence,
)

_COUNTY_OF_RESIDENCE = "county"
_COUNTY_OF_RESIDENCE_ROWS = [
    {
        "state_code": "US_XX",
        "person_id": 123,
        "county_of_residence": _COUNTY_OF_RESIDENCE,
    }
]

_DEFAULT_IP_ID = 123
_DEFAULT_SP_ID = 999
_DEFAULT_SSVR_ID = 789

_DEFAULT_INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION = [
    {"incarceration_period_id": _DEFAULT_IP_ID, "judicial_district_code": "NW"}
]

_DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS = {
    _DEFAULT_SP_ID: {
        "agent_id": 000,
        "agent_external_id": "XXX",
        "supervision_period_id": _DEFAULT_SP_ID,
    }
}

_DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST = list(
    _DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS.values()
)


class TestFindIncarcerationEvents(unittest.TestCase):
    """Tests the find_incarceration_events function."""

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

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        self.incarceration_pre_processing_delegate_patcher.stop()
        self.commitment_from_supervision_delegate_patcher.stop()
        self.violation_delegate_patcher.stop()
        self.violation_pre_processing_delegate_patcher.stop()
        self.supervision_delegate_patcher.stop()

    def _run_find_incarceration_events(
        self,
        sentence_groups: Optional[List[StateSentenceGroup]] = None,
        assessments: Optional[List[StateAssessment]] = None,
        violation_responses: Optional[List[StateSupervisionViolationResponse]] = None,
        supervision_period_to_agent_association: Optional[List[Dict[str, Any]]] = None,
    ) -> List[IncarcerationEvent]:
        sentence_groups = sentence_groups or []
        assessments = assessments or []
        violation_responses = violation_responses or []
        supervision_period_to_agent_association = (
            supervision_period_to_agent_association
            or _DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST
        )

        return self.identifier._find_incarceration_events(
            sentence_groups,
            assessments,
            violation_responses,
            _DEFAULT_INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION,
            _COUNTY_OF_RESIDENCE_ROWS,
            supervision_period_to_agent_association,
        )

    def test_find_incarceration_events(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="INCARCERATION_ADMISSION",
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            start_date=date(2008, 10, 11),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 11),
                    ncic_code="0901",
                    statute="9999",
                )
            ],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence],
        )

        incarceration_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = self._run_find_incarceration_events(sentence_groups)

        assert incarceration_period.admission_date is not None
        assert incarceration_period.release_date is not None
        expected_events = [
            *expected_incarceration_stay_events(
                incarceration_period,
                most_serious_offense_ncic_code="0901",
                most_serious_offense_statute="9999",
                judicial_district_code="NW",
            ),
            IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                admission_reason_raw_text="INCARCERATION_ADMISSION",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.release_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
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
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2009, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="NA",
            release_date=date(2009, 12, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON 10",
            admission_date=date(2009, 12, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2010, 2, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            start_date=date(2008, 1, 11),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[incarceration_period_1, incarceration_period_2],
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 11),
                    ncic_code="5511",
                    statute="9999",
                )
            ],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence],
        )

        incarceration_period_1.incarceration_sentences = [incarceration_sentence]
        incarceration_period_2.incarceration_sentences = [incarceration_sentence]
        incarceration_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = self._run_find_incarceration_events(sentence_groups)

        assert incarceration_period_1.admission_date is not None
        assert incarceration_period_2.release_date is not None
        expected_events = [
            *expected_incarceration_stay_events(
                incarceration_period_1,
                most_serious_offense_ncic_code="5511",
                most_serious_offense_statute="9999",
                judicial_district_code="NW",
            ),
            *expected_incarceration_stay_events(
                incarceration_period_2,
                original_admission_reason=incarceration_period_1.admission_reason,
                original_admission_reason_raw_text=incarceration_period_1.admission_reason_raw_text,
                most_serious_offense_ncic_code="5511",
                most_serious_offense_statute="9999",
            ),
            IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period_1.state_code,
                event_date=incarceration_period_1.admission_date,
                facility=incarceration_period_1.facility,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                admission_reason_raw_text="NA",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period_2.state_code,
                event_date=incarceration_period_2.release_date,
                facility=incarceration_period_2.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
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

    def test_find_incarceration_events_transfer_status_change(self) -> None:
        """Tests that adjacent IPs with TRANSFER edges are updated to have STATUS_CHANGE
        release and admission reasons when the IPs have different
        specialized_purpose_for_incarceration values.
        """
        state_code = "US_XX"
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code=state_code,
            facility="PRISON3",
            admission_date=date(2019, 12, 4),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2019, 12, 8),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=state_code,
            incarceration_sentence_id=111,
            start_date=date(2017, 1, 1),
            status=StateSentenceStatus.COMPLETED,
            incarceration_periods=[incarceration_period_1, incarceration_period_2],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=state_code,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=9797,
            incarceration_sentences=[incarceration_sentence],
        )

        incarceration_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = self._run_find_incarceration_events(sentence_groups)

        assert incarceration_period_1.admission_date is not None
        assert incarceration_period_1.release_date is not None
        assert incarceration_period_2.admission_date is not None
        assert incarceration_period_2.release_date is not None
        assert incarceration_period_2.release_reason is not None
        expected_events = [
            *expected_incarceration_stay_events(incarceration_period_1),
            *expected_incarceration_stay_events(
                incarceration_period_2,
                original_admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            ),
            IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period_1.state_code,
                event_date=incarceration_period_1.admission_date,
                facility=incarceration_period_1.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period_1.state_code,
                event_date=incarceration_period_1.release_date,
                facility=incarceration_period_1.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
                release_reason_raw_text=incarceration_period_1.release_reason_raw_text,
                admission_reason=incarceration_period_1.admission_reason,
                total_days_incarcerated=(
                    incarceration_period_1.release_date
                    - incarceration_period_1.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            ),
            IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period_2.state_code,
                event_date=incarceration_period_2.admission_date,
                facility=incarceration_period_2.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period_2.state_code,
                event_date=incarceration_period_2.release_date,
                facility=incarceration_period_2.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=incarceration_period_2.release_reason,
                release_reason_raw_text=incarceration_period_2.release_reason_raw_text,
                admission_reason=incarceration_period_1.admission_reason,
                total_days_incarcerated=(
                    incarceration_period_2.release_date
                    - incarceration_period_2.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
        ]

        self.assertCountEqual(expected_events, incarceration_events)

    def test_find_incarceration_events_multiple_sentences(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            start_date=date(2008, 10, 11),
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            start_date=date(2008, 10, 11),
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence],
            supervision_sentences=[supervision_sentence],
        )

        incarceration_sentence.sentence_group = sentence_group
        supervision_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = self._run_find_incarceration_events(sentence_groups)

        assert incarceration_period.admission_date is not None
        assert incarceration_period.release_date is not None
        expected_events: List[IncarcerationEvent] = [
            *expected_incarceration_stay_events(
                incarceration_period, judicial_district_code="NW"
            ),
            IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.release_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
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

    def test_find_incarceration_events_multiple_sentences_with_investigative_supervision_period_us_id(
        self,
    ) -> None:
        self._stop_state_specific_delegate_patchers()

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_ID",
            facility="PRISON3",
            admission_date=date(2018, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            release_date=date(2018, 11, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_ID",
            start_date=date(2018, 11, 20),
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            state_code="US_ID",
            start_date=date(2018, 11, 1),
            termination_date=date(2018, 11, 19),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_ID",
            start_date=date(2018, 11, 1),
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_ID",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence],
            supervision_sentences=[supervision_sentence],
        )

        incarceration_sentence.sentence_group = sentence_group
        supervision_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = self._run_find_incarceration_events(sentence_groups)

        assert incarceration_period.admission_date is not None
        assert incarceration_period.release_date is not None
        expected_events = [
            *expected_incarceration_stay_events(
                incarceration_period,
                judicial_district_code="NW",
                original_admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            ),
            IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.release_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                total_days_incarcerated=(
                    incarceration_period.release_date
                    - incarceration_period.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
        ]

        self.assertCountEqual(expected_events, incarceration_events)

    def test_find_incarceration_events_purpose_for_incarceration_change_us_id(
        self,
    ) -> None:
        """Tests that with state code US_ID, treatment in prison periods that are followed by a transfer to general
        result in the correct IncarcerationStayEvents, IncarcerationAdmissionEvents, and
        IncarcerationReleaseEvents with updated STATUS_CHANGE reasons"""
        self._stop_state_specific_delegate_patchers()

        treatment_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_ID",
            facility="PRISON3",
            admission_date=date(2009, 11, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="NA",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=date(2009, 12, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        general_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_ID",
            facility="PRISON 10",
            admission_date=date(2009, 12, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2009, 12, 3),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_ID",
            start_date=date(2008, 1, 11),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[
                treatment_incarceration_period,
                general_incarceration_period,
            ],
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_ID",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 11),
                    ncic_code="5511",
                    statute="9999",
                )
            ],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_ID",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence],
        )

        treatment_incarceration_period.incarceration_sentences = [
            incarceration_sentence
        ]
        general_incarceration_period.incarceration_sentences = [incarceration_sentence]
        incarceration_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = self._run_find_incarceration_events(sentence_groups)

        assert general_incarceration_period.admission_date is not None
        assert treatment_incarceration_period.release_date is not None
        assert treatment_incarceration_period.admission_date is not None
        assert general_incarceration_period.release_date is not None
        assert general_incarceration_period.admission_date is not None
        expected_events: List[IncarcerationEvent] = [
            *expected_incarceration_stay_events(
                treatment_incarceration_period,
                most_serious_offense_ncic_code="5511",
                most_serious_offense_statute="9999",
                judicial_district_code="NW",
            ),
            *expected_incarceration_stay_events(
                general_incarceration_period,
                original_admission_reason=treatment_incarceration_period.admission_reason,
                original_admission_reason_raw_text=treatment_incarceration_period.admission_reason_raw_text,
                most_serious_offense_ncic_code="5511",
                most_serious_offense_statute="9999",
            ),
            IncarcerationStandardAdmissionEvent(
                state_code=treatment_incarceration_period.state_code,
                event_date=treatment_incarceration_period.admission_date,
                facility=treatment_incarceration_period.facility,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                admission_reason_raw_text="NA",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            IncarcerationStandardAdmissionEvent(
                state_code=general_incarceration_period.state_code,
                event_date=general_incarceration_period.admission_date,
                facility=general_incarceration_period.facility,
                admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            IncarcerationReleaseEvent(
                state_code=treatment_incarceration_period.state_code,
                event_date=treatment_incarceration_period.release_date,
                facility=treatment_incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
                purpose_for_incarceration=treatment_incarceration_period.specialized_purpose_for_incarceration,
                admission_reason=treatment_incarceration_period.admission_reason,
                total_days_incarcerated=(
                    treatment_incarceration_period.release_date
                    - treatment_incarceration_period.admission_date
                ).days,
            ),
            IncarcerationReleaseEvent(
                state_code=general_incarceration_period.state_code,
                event_date=general_incarceration_period.release_date,
                facility=general_incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
                admission_reason=treatment_incarceration_period.admission_reason,
                purpose_for_incarceration=general_incarceration_period.specialized_purpose_for_incarceration,
                total_days_incarcerated=(
                    general_incarceration_period.release_date
                    - general_incarceration_period.admission_date
                ).days,
            ),
        ]

        self.assertCountEqual(expected_events, incarceration_events)

    @freeze_time("2000-01-01")
    def test_find_incarceration_events_dates_in_future(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(1990, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="INCARCERATION_ADMISSION",
            # Erroneous release_date in the future
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_period_in_future = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            # Erroneous admission_date in the future, period should be dropped entirely
            admission_date=date(2010, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_reason_raw_text="INCARCERATION_ADMISSION",
            release_date=date(2010, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            start_date=date(1989, 11, 1),
            incarceration_periods=[
                incarceration_period,
                incarceration_period_in_future,
            ],
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(1989, 11, 1),
                    ncic_code="0901",
                    statute="9999",
                )
            ],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence],
        )

        incarceration_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = self._run_find_incarceration_events(sentence_groups)

        assert incarceration_period.admission_date is not None
        expected_events = [
            *expected_incarceration_stay_events(
                incarceration_period,
                most_serious_offense_ncic_code="0901",
                most_serious_offense_statute="9999",
                judicial_district_code="NW",
            ),
            IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                admission_reason_raw_text="INCARCERATION_ADMISSION",
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
        ]

        self.assertCountEqual(expected_events, incarceration_events)

    def testFindIncarcerationEvents_only_placeholder_ips_and_sps(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SP_ID,
            state_code="US_XX",
        )
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
            state_code="US_XX",
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            start_date=date(2008, 10, 11),
            incarceration_periods=[incarceration_period],
            supervision_periods=[supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            start_date=date(2008, 10, 11),
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence],
            supervision_sentences=[supervision_sentence],
        )

        incarceration_sentence.sentence_group = sentence_group
        supervision_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = self._run_find_incarceration_events(sentence_groups)

        self.assertCountEqual([], incarceration_events)

    def testFindIncarcerationEvents_usNd_tempCustodyFollowedByRevocation(self) -> None:
        """Tests that with state code US_ND, temporary custody periods are dropped
        before finding all incarceration events.
        """
        self._stop_state_specific_delegate_patchers()

        temp_custody_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_ND",
            facility="PRISON",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="INCARCERATION_ADMISSION",
            release_date=date(2008, 12, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_ND",
            violation_date=date(2018, 4, 20),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_ND",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_ND",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            supervision_violation=supervision_violation,
            response_date=date(2008, 12, 25),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        duplicate_supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_ND",
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_ND",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        duplicate_ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_ND",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            supervision_violation=duplicate_supervision_violation,
            response_date=date(2008, 12, 25),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=_DEFAULT_IP_ID,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_ND",
            facility="PRISON",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text="Revocation",
            release_date=date(2008, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="RPRB",
        )

        revoked_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="XY2",
            state_code="US_ND",
            start_date=date(2001, 3, 13),
            termination_date=date(2008, 12, 20),
            supervision_site="X",
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_ND",
            start_date=date(2008, 12, 11),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[temp_custody_period, revocation_period],
            supervision_periods=[revoked_supervision_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_ND",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 11),
                    ncic_code="0901",
                    statute="9999",
                )
            ],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_ND",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=9797,
            incarceration_sentences=[incarceration_sentence],
        )

        incarceration_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = self._run_find_incarceration_events(
            sentence_groups, violation_responses=[ssvr, duplicate_ssvr]
        )

        assert revocation_period.release_date is not None
        assert revocation_period.release_reason is not None
        assert revocation_period.admission_date is not None
        assert revocation_period.admission_reason is not None
        self.assertCountEqual(
            [
                IncarcerationStayEvent(
                    admission_reason=revocation_period.admission_reason,
                    admission_reason_raw_text=revocation_period.admission_reason_raw_text,
                    state_code=revocation_period.state_code,
                    event_date=revocation_period.admission_date,
                    facility=revocation_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    most_serious_offense_ncic_code="0901",
                    most_serious_offense_statute="9999",
                    judicial_district_code="NW",
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                ),
                IncarcerationCommitmentFromSupervisionAdmissionEvent(
                    state_code=revoked_supervision_period.state_code,
                    event_date=revocation_period.admission_date,
                    facility=revocation_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    admission_reason=revocation_period.admission_reason,
                    admission_reason_raw_text=revocation_period.admission_reason_raw_text,
                    supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                    case_type=StateSupervisionCaseType.GENERAL,
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    most_severe_violation_type=StateSupervisionViolationType.FELONY,
                    most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
                    # Duplicate responses were merged in pre-processing
                    response_count=1,
                    violation_history_description="1felony",
                    violation_type_frequency_counter=[["FELONY"]],
                    supervising_officer_external_id="XXX",
                    supervising_district_external_id="X",
                    level_1_supervision_location_external_id="X",
                ),
                IncarcerationReleaseEvent(
                    state_code=revocation_period.state_code,
                    event_date=revocation_period.release_date,
                    facility=revocation_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    release_reason=revocation_period.release_reason,
                    release_reason_raw_text=revocation_period.release_reason_raw_text,
                    supervision_type_at_release=StateSupervisionPeriodSupervisionType.PROBATION,
                    admission_reason=revocation_period.admission_reason,
                    total_days_incarcerated=(
                        revocation_period.release_date
                        - revocation_period.admission_date
                    ).days,
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                ),
            ],
            incarceration_events,
        )

    def test_testFindIncarcerationEvents_usNd_NewAdmissionAfterProbationRevocation(
        self,
    ) -> None:
        """Tests that the find_incarceration_events when run for a US_ND
        incarceration period with a NEW_ADMISSION admission reason following a
        supervision period with a REVOCATION termination reason and a PROBATION
        supervision type will correctly return an
        IncarcerationCommitmentFromSupervisionAdmissionEvent for that commitment from
        supervision admission."""
        self._stop_state_specific_delegate_patchers()

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
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2008, 3, 5),
            termination_date=date(2009, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="X",
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_ND",
            admission_date=date(2009, 12, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 1, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        violation_responses = [
            supervision_violation_response,
        ]

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=111,
            start_date=date(1990, 1, 1),
            external_id="ss1",
            state_code="US_ND",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            completion_date=date(2018, 5, 19),
            supervision_periods=[supervision_period],
            incarceration_periods=[incarceration_period],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_ND",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=9797,
            supervision_sentences=[supervision_sentence],
        )

        supervision_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = self._run_find_incarceration_events(
            sentence_groups, violation_responses=violation_responses
        )

        assert incarceration_period.release_reason is not None
        assert incarceration_period.release_date is not None
        assert incarceration_period.admission_date is not None
        self.assertCountEqual(
            [
                IncarcerationStayEvent(
                    admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                    admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
                    state_code=incarceration_period.state_code,
                    event_date=incarceration_period.admission_date,
                    facility=incarceration_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                ),
                IncarcerationCommitmentFromSupervisionAdmissionEvent(
                    state_code=incarceration_period.state_code,
                    event_date=incarceration_period.admission_date,
                    facility=incarceration_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                    admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
                    supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                    case_type=StateSupervisionCaseType.GENERAL,
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    most_severe_violation_type=StateSupervisionViolationType.FELONY,
                    most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
                    # Duplicate responses were merged in pre-processing
                    response_count=1,
                    violation_history_description="1felony",
                    violation_type_frequency_counter=[["FELONY"]],
                    supervising_officer_external_id="XXX",
                    supervising_district_external_id="X",
                    level_1_supervision_location_external_id="X",
                ),
                IncarcerationReleaseEvent(
                    state_code=incarceration_period.state_code,
                    event_date=incarceration_period.release_date,
                    facility=incarceration_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    release_reason=incarceration_period.release_reason,
                    release_reason_raw_text=incarceration_period.release_reason_raw_text,
                    admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                    total_days_incarcerated=(
                        incarceration_period.release_date
                        - incarceration_period.admission_date
                    ).days,
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                ),
            ],
            incarceration_events,
        )

    def test_testFindIncarcerationEvents_usNd_NewAdmissionAfterProbationRevocationTempCustody(
        self,
    ) -> None:
        """Tests that the find_incarceration_events when run for a US_ND
        incarceration period with a NEW_ADMISSION admission reason following a
        TEMPORARY_CUSTODY incarceration period, which itself follows a supervision
        period with a REVOCATION termination reason and a PROBATION supervision type
        will correctly return an IncarcerationCommitmentFromSupervisionAdmissionEvent
        corresponding to that NEW_ADMISSION commitment from supervision, and not the
        intermediate TEMPORARY_CUSTODY period."""
        self._stop_state_specific_delegate_patchers()

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
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2008, 3, 5),
            termination_date=date(2009, 12, 19),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_site="X",
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
            incarceration_period_id=222,
            external_id="ip2",
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_ND",
            admission_date=date(2010, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 1, 2),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
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
            incarceration_periods=[
                temporary_incarceration_period,
                incarceration_period,
            ],
        )

        violation_responses = [supervision_violation_response]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_ND",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=9797,
            supervision_sentences=[supervision_sentence],
        )

        supervision_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = self._run_find_incarceration_events(
            sentence_groups, violation_responses=violation_responses
        )

        assert incarceration_period.release_date is not None
        assert incarceration_period.release_reason is not None
        assert incarceration_period.admission_date is not None
        self.assertCountEqual(
            [
                IncarcerationStayEvent(
                    admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                    admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
                    state_code=incarceration_period.state_code,
                    event_date=incarceration_period.admission_date,
                    facility=incarceration_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                ),
                IncarcerationCommitmentFromSupervisionAdmissionEvent(
                    state_code=incarceration_period.state_code,
                    event_date=incarceration_period.admission_date,
                    facility=incarceration_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                    admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
                    supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                    case_type=StateSupervisionCaseType.GENERAL,
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    most_severe_violation_type=StateSupervisionViolationType.FELONY,
                    most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
                    # Duplicate responses were merged in pre-processing
                    response_count=1,
                    violation_history_description="1felony",
                    violation_type_frequency_counter=[["FELONY"]],
                    supervising_officer_external_id="XXX",
                    supervising_district_external_id="X",
                    level_1_supervision_location_external_id="X",
                ),
                IncarcerationReleaseEvent(
                    state_code=incarceration_period.state_code,
                    event_date=incarceration_period.release_date,
                    facility=incarceration_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    release_reason=incarceration_period.release_reason,
                    release_reason_raw_text=incarceration_period.release_reason_raw_text,
                    admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                    total_days_incarcerated=(
                        incarceration_period.release_date
                        - incarceration_period.admission_date
                    ).days,
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                ),
            ],
            incarceration_events,
        )

    def testFindIncarcerationEvents_usMo_tempCustodyFollowedByRevocation(self) -> None:
        """Tests that when a temporary custody period is followed by a revocation
        period.
        """
        self._stop_state_specific_delegate_patchers()

        temp_custody_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_MO",
            facility="PRISON",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="40I0050",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="S",
            release_date=date(2008, 11, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )
        revocation_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_MO",
            facility="PRISON",
            admission_date=date(2008, 11, 21),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            admission_reason_raw_text="40I2000",
            release_date=date(2008, 11, 22),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_MO",
            supervision_period_id=1313,
            external_id="3",
            start_date=date(2008, 1, 1),
            supervision_level=StateSupervisionLevel.MEDIUM,
        )

        assert revocation_period.admission_date is not None
        assert supervision_period.start_date is not None
        assert temp_custody_period.admission_date is not None
        incarceration_sentence = (
            FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
                StateIncarcerationSentence.new_with_defaults(
                    state_code="US_MO",
                    incarceration_sentence_id=123,
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                    incarceration_periods=[temp_custody_period, revocation_period],
                    supervision_periods=[supervision_period],
                    start_date=date(2008, 1, 1),
                    charges=[
                        StateCharge.new_with_defaults(
                            state_code="US_MO",
                            status=ChargeStatus.PRESENT_WITHOUT_INFO,
                            offense_date=date(2007, 12, 11),
                            ncic_code="0901",
                            statute="9999",
                        )
                    ],
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=supervision_period.start_date,
                        end_date=temp_custody_period.admission_date,
                        supervision_type=StateSupervisionType.PAROLE,
                    ),
                    SupervisionTypeSpan(
                        start_date=temp_custody_period.admission_date,
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
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code="US_MO",
                supervision_sentence_id=123,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_periods=[temp_custody_period, revocation_period],
                supervision_periods=[supervision_period],
                start_date=date(2008, 1, 1),
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=supervision_period.start_date,
                    end_date=temp_custody_period.admission_date,
                    supervision_type=StateSupervisionType.PROBATION,
                ),
                SupervisionTypeSpan(
                    start_date=temp_custody_period.admission_date,
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

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_MO",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence],
            incarceration_sentences=[incarceration_sentence],
        )

        incarceration_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = self._run_find_incarceration_events(sentence_groups)

        assert temp_custody_period.admission_reason is not None
        assert temp_custody_period.release_date is not None
        assert temp_custody_period.release_reason is not None
        assert revocation_period.release_date is not None
        assert revocation_period.admission_reason is not None
        assert revocation_period.release_reason is not None
        self.assertCountEqual(
            [
                IncarcerationStayEvent(
                    admission_reason=temp_custody_period.admission_reason,
                    admission_reason_raw_text=temp_custody_period.admission_reason_raw_text,
                    state_code=temp_custody_period.state_code,
                    event_date=temp_custody_period.admission_date,
                    facility=temp_custody_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    most_serious_offense_ncic_code="0901",
                    most_serious_offense_statute="9999",
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
                ),
                IncarcerationStayEvent(
                    admission_reason=revocation_period.admission_reason,
                    admission_reason_raw_text=revocation_period.admission_reason_raw_text,
                    state_code=revocation_period.state_code,
                    event_date=revocation_period.admission_date,
                    facility=revocation_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    most_serious_offense_ncic_code="0901",
                    most_serious_offense_statute="9999",
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                ),
                IncarcerationStandardAdmissionEvent(
                    state_code=temp_custody_period.state_code,
                    event_date=temp_custody_period.admission_date,
                    facility=temp_custody_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    admission_reason=temp_custody_period.admission_reason,
                    admission_reason_raw_text=temp_custody_period.admission_reason_raw_text,
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
                ),
                IncarcerationCommitmentFromSupervisionAdmissionEvent(
                    state_code=revocation_period.state_code,
                    event_date=revocation_period.admission_date,
                    facility=revocation_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    admission_reason=revocation_period.admission_reason,
                    admission_reason_raw_text=revocation_period.admission_reason_raw_text,
                    supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    case_type=StateSupervisionCaseType.GENERAL,
                    supervision_level=supervision_period.supervision_level,
                ),
                IncarcerationReleaseEvent(
                    state_code=temp_custody_period.state_code,
                    event_date=temp_custody_period.release_date,
                    facility=temp_custody_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    release_reason=temp_custody_period.release_reason,
                    admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
                    total_days_incarcerated=(
                        temp_custody_period.release_date
                        - temp_custody_period.admission_date
                    ).days,
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
                ),
                IncarcerationReleaseEvent(
                    state_code=revocation_period.state_code,
                    event_date=revocation_period.release_date,
                    facility=revocation_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    release_reason=revocation_period.release_reason,
                    admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                    total_days_incarcerated=(
                        revocation_period.release_date
                        - revocation_period.admission_date
                    ).days,
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                ),
            ],
            incarceration_events,
        )

    def testFindIncarcerationEvents_usMo_tempCustody(self) -> None:
        """Tests that when there is only a temporary custody period."""
        self._stop_state_specific_delegate_patchers()

        temp_custody_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_MO",
            facility="PRISON",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
            admission_reason_raw_text="40I0050",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            specialized_purpose_for_incarceration_raw_text="S",
            release_date=date(2008, 11, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
        )

        assert temp_custody_period.admission_date is not None
        assert temp_custody_period.admission_reason is not None
        assert temp_custody_period.release_date is not None
        assert temp_custody_period.release_reason is not None
        incarceration_sentence = (
            FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
                StateIncarcerationSentence.new_with_defaults(
                    state_code="US_MO",
                    incarceration_sentence_id=123,
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                    incarceration_periods=[temp_custody_period],
                    supervision_periods=[],
                    start_date=date(2008, 1, 1),
                    charges=[
                        StateCharge.new_with_defaults(
                            state_code="US_MO",
                            status=ChargeStatus.PRESENT_WITHOUT_INFO,
                            offense_date=date(2007, 12, 11),
                            ncic_code="0901",
                            statute="9999",
                        )
                    ],
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=temp_custody_period.admission_date,
                        end_date=temp_custody_period.release_date,
                        supervision_type=StateSupervisionType.PROBATION,
                    ),
                    SupervisionTypeSpan(
                        start_date=temp_custody_period.release_date,
                        end_date=None,
                        supervision_type=None,
                    ),
                ],
            )
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code="US_MO",
                supervision_sentence_id=123,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_periods=[temp_custody_period],
                supervision_periods=[],
                start_date=date(2008, 1, 1),
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=temp_custody_period.admission_date,
                    end_date=temp_custody_period.release_date,
                    supervision_type=StateSupervisionType.PROBATION,
                ),
                SupervisionTypeSpan(
                    start_date=temp_custody_period.release_date,
                    end_date=None,
                    supervision_type=None,
                ),
            ],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_MO",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence],
            incarceration_sentences=[incarceration_sentence],
        )

        incarceration_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = self._run_find_incarceration_events(sentence_groups)

        self.assertCountEqual(
            [
                IncarcerationStayEvent(
                    admission_reason=temp_custody_period.admission_reason,
                    admission_reason_raw_text=temp_custody_period.admission_reason_raw_text,
                    state_code=temp_custody_period.state_code,
                    event_date=temp_custody_period.admission_date,
                    facility=temp_custody_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    most_serious_offense_ncic_code="0901",
                    most_serious_offense_statute="9999",
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
                ),
                IncarcerationStandardAdmissionEvent(
                    state_code=temp_custody_period.state_code,
                    event_date=temp_custody_period.admission_date,
                    facility=temp_custody_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    admission_reason=temp_custody_period.admission_reason,
                    admission_reason_raw_text=temp_custody_period.admission_reason_raw_text,
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
                ),
                IncarcerationReleaseEvent(
                    state_code=temp_custody_period.state_code,
                    event_date=temp_custody_period.release_date,
                    facility=temp_custody_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    release_reason=temp_custody_period.release_reason,
                    admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
                    total_days_incarcerated=(
                        temp_custody_period.release_date
                        - temp_custody_period.admission_date
                    ).days,
                ),
            ],
            incarceration_events,
        )

    def testFindIncarcerationEvents_usId_RevocationAdmission(self) -> None:
        """Tests the find_incarceration_events function for state code US_ID."""
        self._stop_state_specific_delegate_patchers()

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            state_code="US_ID",
            custodial_authority_raw_text="US_ID_DOC",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="MEDIUM",
            supervision_site="X|Y",
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
            release_date=date(2017, 5, 18),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_ID",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period],
            incarceration_periods=[revocation_period],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_ID",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=9797,
            supervision_sentences=[supervision_sentence],
        )

        supervision_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = self._run_find_incarceration_events(sentence_groups)

        assert revocation_period.admission_date is not None
        assert revocation_period.release_date is not None
        assert revocation_period.release_reason is not None
        self.assertCountEqual(
            [
                IncarcerationStayEvent(
                    admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                    admission_reason_raw_text=revocation_period.admission_reason_raw_text,
                    state_code=revocation_period.state_code,
                    event_date=revocation_period.admission_date,
                    facility=revocation_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                ),
                IncarcerationCommitmentFromSupervisionAdmissionEvent(
                    state_code=revocation_period.state_code,
                    event_date=revocation_period.admission_date,
                    facility=revocation_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                    admission_reason_raw_text=revocation_period.admission_reason_raw_text,
                    supervision_type=supervision_period.supervision_period_supervision_type,
                    supervision_level=supervision_period.supervision_level,
                    supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                    case_type=StateSupervisionCaseType.GENERAL,
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                    supervising_district_external_id="X",
                    level_1_supervision_location_external_id="Y",
                    level_2_supervision_location_external_id="X",
                    supervising_officer_external_id="XXX",
                ),
                IncarcerationReleaseEvent(
                    state_code=revocation_period.state_code,
                    event_date=revocation_period.release_date,
                    facility=revocation_period.facility,
                    county_of_residence=_COUNTY_OF_RESIDENCE,
                    release_reason=revocation_period.release_reason,
                    release_reason_raw_text=revocation_period.release_reason_raw_text,
                    admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
                    total_days_incarcerated=(
                        revocation_period.release_date
                        - revocation_period.admission_date
                    ).days,
                    purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                ),
            ],
            incarceration_events,
        )

    def testFindIncarcerationEvents_usId_FailedTreatmentStatusChange(
        self,
    ) -> None:
        """Tests that with state code US_ID, treatment in prison periods that are
        followed by a transfer to general result in an admission event for the
        general period with the admission_reason STATUS_CHANGE.
        """
        treatment_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            external_id="1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_ID",
            facility="PRISON",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=date(2008, 12, 20),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        general_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_ID",
            facility="PRISON",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            release_date=date(2008, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_ID",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[],
            incarceration_periods=[treatment_period, general_period],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_ID",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=9797,
            supervision_sentences=[supervision_sentence],
        )

        supervision_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        incarceration_events = self._run_find_incarceration_events(sentence_groups)

        assert treatment_period.admission_date is not None
        assert treatment_period.release_date is not None
        assert general_period.admission_date is not None
        assert general_period.release_date is not None
        assert general_period.release_reason is not None
        expected_events = [
            *expected_incarceration_stay_events(treatment_period),
            *expected_incarceration_stay_events(
                general_period,
                original_admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            ),
            IncarcerationStandardAdmissionEvent(
                state_code=treatment_period.state_code,
                event_date=treatment_period.admission_date,
                facility=treatment_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            IncarcerationReleaseEvent(
                state_code=treatment_period.state_code,
                event_date=treatment_period.release_date,
                facility=treatment_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
                release_reason_raw_text=treatment_period.release_reason_raw_text,
                admission_reason=treatment_period.admission_reason,
                total_days_incarcerated=(
                    treatment_period.release_date - treatment_period.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            IncarcerationStandardAdmissionEvent(
                state_code=general_period.state_code,
                event_date=general_period.admission_date,
                facility=general_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            IncarcerationReleaseEvent(
                state_code=general_period.state_code,
                event_date=general_period.release_date,
                facility=general_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=general_period.release_reason,
                release_reason_raw_text=general_period.release_reason_raw_text,
                admission_reason=treatment_period.admission_reason,
                total_days_incarcerated=(
                    general_period.release_date - general_period.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
        ]

        self.assertCountEqual(expected_events, incarceration_events)

    def testFindIncarcerationEvents_usPA_SanctionAdmission(self) -> None:
        """Tests the find_incarceration_events function for periods in US_PA, where
        there is a sanction admission for shock incarceration."""
        self._stop_state_specific_delegate_patchers()
        state_code = "US_PA"

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
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

        parole_board_decision_entry = (
            StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                state_code=state_code,
                decision_raw_text=SHOCK_INCARCERATION_9_MONTHS,
                decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            )
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
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        shock_incarceration_period = StateIncarcerationPeriod.new_with_defaults(
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
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code=state_code,
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            status=StateSentenceStatus.COMPLETED,
            supervision_periods=[supervision_period],
            incarceration_periods=[parole_board_hold, shock_incarceration_period],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=state_code,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=9797,
            supervision_sentences=[supervision_sentence],
        )

        supervision_sentence.sentence_group = sentence_group

        sentence_groups = [sentence_group]

        assessments = [assessment]
        violation_responses = [parole_board_permanent_decision]

        incarceration_events = self._run_find_incarceration_events(
            sentence_groups=sentence_groups,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_period_to_agent_association=_DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATION_LIST,
        )

        expected_board_hold_stay_events = expected_incarceration_stay_events(
            incarceration_period=parole_board_hold,
            original_admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
        )

        expected_shock_stay_events = expected_incarceration_stay_events(
            incarceration_period=shock_incarceration_period,
            original_admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

        assert parole_board_hold.admission_date is not None
        assert shock_incarceration_period.admission_date is not None
        expected_admission_events = [
            IncarcerationStandardAdmissionEvent(
                state_code=state_code,
                event_date=parole_board_hold.admission_date,
                facility=parole_board_hold.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            ),
            IncarcerationCommitmentFromSupervisionAdmissionEvent(
                state_code=state_code,
                event_date=shock_incarceration_period.admission_date,
                facility=shock_incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                admission_reason_raw_text=shock_incarceration_period.admission_reason_raw_text,
                supervision_type=supervision_period.supervision_period_supervision_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                case_type=StateSupervisionCaseType.GENERAL,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                purpose_for_incarceration_subtype=SHOCK_INCARCERATION_9_MONTHS,
                supervising_district_external_id="DISTRICT_1",
                level_1_supervision_location_external_id="OFFICE_2",
                level_2_supervision_location_external_id="DISTRICT_1",
                supervising_officer_external_id="XXX",
                assessment_score=assessment.assessment_score,
                assessment_level=StateAssessmentLevel.HIGH,
                assessment_type=assessment.assessment_type,
            ),
        ]

        assert parole_board_hold.release_date is not None
        assert shock_incarceration_period.release_date is not None
        assert shock_incarceration_period.release_reason is not None
        expected_release_events = [
            IncarcerationReleaseEvent(
                state_code=state_code,
                event_date=parole_board_hold.release_date,
                facility=parole_board_hold.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
                release_reason_raw_text=parole_board_hold.release_reason_raw_text,
                admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
                total_days_incarcerated=(
                    parole_board_hold.release_date - parole_board_hold.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            ),
            IncarcerationReleaseEvent(
                state_code=state_code,
                event_date=shock_incarceration_period.release_date,
                facility=shock_incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=shock_incarceration_period.release_reason,
                release_reason_raw_text=shock_incarceration_period.release_reason_raw_text,
                admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                total_days_incarcerated=(
                    shock_incarceration_period.release_date
                    - shock_incarceration_period.admission_date
                ).days,
                purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
            ),
        ]

        self.assertCountEqual(
            [
                *expected_board_hold_stay_events,
                *expected_shock_stay_events,
                *expected_admission_events,
                *expected_release_events,
            ],
            incarceration_events,
        )


class TestFindIncarcerationStays(unittest.TestCase):
    """Tests the find_incarceration_stays function."""

    def setUp(self) -> None:
        self.identifier = identifier.IncarcerationIdentifier()

    def _run_find_incarceration_stays_with_no_sentences(
        self,
        incarceration_period: StateIncarcerationPeriod,
        county_of_residence: Optional[str] = _COUNTY_OF_RESIDENCE,
        incarceration_period_judicial_district_association: Optional[
            Dict[int, Dict[str, Any]]
        ] = None,
    ) -> List[IncarcerationStayEvent]:
        """Runs `find_incarceration_stays` without providing sentence information.
        Sentence information is only used in `US_MO` to inform information about
        supervision types prior to admission. All tests using this method should not
        require that state specific logic.
        """
        incarceration_period_judicial_district_association = incarceration_period_judicial_district_association or {
            _DEFAULT_IP_ID: _DEFAULT_INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION[
                0
            ]
        }

        incarceration_period_index = PreProcessedIncarcerationPeriodIndex(
            incarceration_periods=[incarceration_period],
            ip_id_to_pfi_subtype=(
                {incarceration_period.incarceration_period_id: None}
                if incarceration_period.incarceration_period_id
                else {}
            ),
        )

        return self.identifier._find_incarceration_stays(
            incarceration_period,
            incarceration_period_index,
            incarceration_period_judicial_district_association,
            county_of_residence,
        )

    def test_find_incarceration_stays_type_us_mo(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_MO",
            facility="PRISON3",
            admission_date=date(2010, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            state_code="US_MO",
            start_date=date(2010, 1, 1),
            termination_date=date(2010, 2, 15),
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=1111,
                state_code="US_MO",
                supervision_type=StateSupervisionType.PROBATION,
                start_date=date(2010, 1, 1),
                supervision_periods=[supervision_period],
                incarceration_periods=[incarceration_period],
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=date(2010, 1, 1),
                    end_date=None,
                    supervision_type=StateSupervisionType.PROBATION,
                )
            ],
        )

        incarceration_period.supervision_sentences = [supervision_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_MO",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=6666,
            external_id="12345",
        )
        supervision_sentence.sentence_group = sentence_group

        assert incarceration_period.incarceration_period_id is not None
        incarceration_period_judicial_district_association = {
            incarceration_period.incarceration_period_id: {
                "incarceration_period_id": incarceration_period.incarceration_period_id,
                "judicial_district_code": "XXX",
            }
        }

        incarceration_events = self._run_find_incarceration_stays_with_no_sentences(
            incarceration_period,
            incarceration_period_judicial_district_association=incarceration_period_judicial_district_association,
        )

        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period
        )

        updated_expected_events = []
        for expected_event in expected_incarceration_events:
            updated_expected_events.append(
                attr.evolve(
                    expected_event,
                    judicial_district_code="XXX",
                )
            )

        self.assertEqual(updated_expected_events, incarceration_events)

    def test_find_incarceration_stays(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2000, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2010, 12, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        incarceration_period.incarceration_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=6666,
            external_id="12345",
        )
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = self._run_find_incarceration_stays_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE
        )

        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period
        )

        self.assertEqual(expected_incarceration_events, incarceration_events)

    @freeze_time("2019-11-01")
    def test_find_incarceration_stays_no_release(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2018, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        incarceration_period.incarceration_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=6666,
            external_id="12345",
        )
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = self._run_find_incarceration_stays_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE
        )

        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period
        )

        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_incarceration_stays_no_admission_or_release(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        with self.assertRaises(ValueError):
            _ = self._run_find_incarceration_stays_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

    def test_find_incarceration_stays_no_release_reason(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2000, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION,
            release_date=date(2010, 12, 1),
            release_reason=None,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        incarceration_period.incarceration_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=6666,
            external_id="12345",
        )
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = self._run_find_incarceration_stays_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE
        )

        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period
        )

        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_incarceration_stays_admitted_end_of_month(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2000, 1, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2000, 2, 13),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        incarceration_period.incarceration_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=6666,
            external_id="12345",
        )
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = self._run_find_incarceration_stays_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE
        )

        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period
        )

        self.assertEqual(expected_incarceration_events, incarceration_events)

    @freeze_time("2019-12-02")
    def test_find_incarceration_stays_still_in_custody(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2019, 11, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        incarceration_period.incarceration_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=6666,
            external_id="12345",
        )
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = self._run_find_incarceration_stays_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE
        )

        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period
        )

        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_incarceration_stays_released_end_of_month(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2019, 11, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2019, 11, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        incarceration_period.incarceration_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=6666,
            external_id="12345",
        )
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = self._run_find_incarceration_stays_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE
        )

        # We do not count the termination date of an incarceration period as a day the person is incarcerated.
        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period
        )

        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_incarceration_stays_transfers_end_of_month(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2019, 11, 29),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2019, 11, 30),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON4",
            admission_date=date(2019, 11, 30),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2019, 12, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period, incarceration_period_2],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        incarceration_period.incarceration_sentences = [incarceration_sentence]
        incarceration_period_2.incarceration_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=6666,
            external_id="12345",
        )
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events_period_1 = (
            self._run_find_incarceration_stays_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )
        )

        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period
        )

        self.assertEqual(expected_incarceration_events, incarceration_events_period_1)

        incarceration_events_period_2 = (
            self._run_find_incarceration_stays_with_no_sentences(
                incarceration_period_2, _COUNTY_OF_RESIDENCE
            )
        )

        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period_2
        )

        self.assertEqual(expected_incarceration_events, incarceration_events_period_2)

    def test_find_incarceration_stays_released_first_of_month(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2019, 11, 15),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2019, 12, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        incarceration_period.incarceration_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=6666,
            external_id="12345",
        )
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = self._run_find_incarceration_stays_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE
        )

        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period
        )

        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_incarceration_stays_only_one_day(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2019, 7, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2019, 7, 31),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        incarceration_period.incarceration_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=6666,
            external_id="12345",
        )
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = self._run_find_incarceration_stays_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE
        )

        # We do not count people who were released on the last day of the month as being incarcerated on that last day.
        # In normal circumstances, if this person remained incarcerated but had a quick, one-day transfer, there will
        # be another incarceration period that opens on the last day of the month with a later termination date - we
        # *will* count this one.
        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period
        )

        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_incarceration_stays_county_jail(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2000, 1, 31),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2000, 2, 13),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=9797,
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        incarceration_period.incarceration_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=6666,
            external_id="12345",
        )
        incarceration_sentence.sentence_group = sentence_group

        incarceration_events = self._run_find_incarceration_stays_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE
        )

        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period
        )

        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_incarceration_stays_original_admission_reason(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2010, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2010, 3, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2010, 3, 31),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=9797,
            incarceration_periods=incarceration_periods,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_2.incarceration_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=6666,
            external_id="12345",
        )
        incarceration_sentence.sentence_group = sentence_group

        assert incarceration_period_1.incarceration_period_id is not None
        incarceration_period_judicial_district_association = {
            incarceration_period_1.incarceration_period_id: {
                "incarceration_period_id": incarceration_period_1.incarceration_period_id,
                "judicial_district_code": "XXX",
            }
        }

        ips = [incarceration_period_1, incarceration_period_2]

        incarceration_period_index = PreProcessedIncarcerationPeriodIndex(
            incarceration_periods=ips,
            ip_id_to_pfi_subtype={
                ip.incarceration_period_id: None
                for ip in ips
                if ip.incarceration_period_id
            },
        )

        incarceration_events = self.identifier._find_incarceration_stays(
            incarceration_period_2,
            incarceration_period_index,
            incarceration_period_judicial_district_association,
            _COUNTY_OF_RESIDENCE,
        )

        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period_2,
            original_admission_reason=incarceration_period_1.admission_reason,
            original_admission_reason_raw_text=incarceration_period_1.admission_reason_raw_text,
        )

        self.assertEqual(expected_incarceration_events, incarceration_events)

    def test_find_incarceration_stays_two_official_admission_reasons(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2010, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2010, 3, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            release_date=date(2010, 3, 31),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            incarceration_sentence_id=9797,
            incarceration_periods=incarceration_periods,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        incarceration_period_2.incarceration_sentences = [incarceration_sentence]

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            sentence_group_id=6666,
            external_id="12345",
        )
        incarceration_sentence.sentence_group = sentence_group

        assert incarceration_period_1.incarceration_period_id is not None
        incarceration_period_judicial_district_association = {
            incarceration_period_1.incarceration_period_id: {
                "incarceration_period_id": incarceration_period_1.incarceration_period_id,
                "judicial_district_code": "XXX",
            }
        }

        incarceration_periods = [incarceration_period_1, incarceration_period_2]

        incarceration_period_index = PreProcessedIncarcerationPeriodIndex(
            incarceration_periods=incarceration_periods,
            ip_id_to_pfi_subtype={
                ip.incarceration_period_id: None
                for ip in incarceration_periods
                if ip.incarceration_period_id is not None
            },
        )

        incarceration_events = self.identifier._find_incarceration_stays(
            incarceration_period_2,
            incarceration_period_index,
            incarceration_period_judicial_district_association,
            _COUNTY_OF_RESIDENCE,
        )

        expected_incarceration_events = expected_incarceration_stay_events(
            incarceration_period_2
        )

        self.assertEqual(expected_incarceration_events, incarceration_events)


class TestAdmissionEventForPeriod(unittest.TestCase):
    """Tests the admission_event_for_period function."""

    def setUp(self) -> None:
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
        self.supervision_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.incarceration.identifier.get_state_specific_supervision_delegate"
        )
        self.mock_supervision_delegate = self.supervision_delegate_patcher.start()
        self.mock_supervision_delegate.return_value = UsXxSupervisionDelegate()
        self.identifier = identifier.IncarcerationIdentifier()

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        self.commitment_from_supervision_delegate_patcher.stop()
        self.violation_delegate_patcher.stop()
        self.supervision_delegate_patcher.stop()

    def _run_admission_event_for_period(
        self,
        incarceration_period: StateIncarcerationPeriod,
        incarceration_period_index: Optional[
            PreProcessedIncarcerationPeriodIndex
        ] = None,
        incarceration_sentences: Optional[List[StateIncarcerationSentence]] = None,
        supervision_sentences: Optional[List[StateSupervisionSentence]] = None,
        supervision_periods: Optional[List[StateSupervisionPeriod]] = None,
        assessments: Optional[List[StateAssessment]] = None,
        violation_responses: Optional[List[StateSupervisionViolationResponse]] = None,
        county_of_residence: Optional[str] = _COUNTY_OF_RESIDENCE,
    ) -> Optional[IncarcerationAdmissionEvent]:
        """Runs `admission_event_for_period` without providing sentence information.
        Sentence information is only used in `US_MO` to inform information about
        supervision types prior to admission. All tests using this method should not
        require that state specific logic.
        """
        incarceration_sentences = incarceration_sentences or []
        supervision_sentences = supervision_sentences or []
        incarceration_period_index = (
            incarceration_period_index
            or PreProcessedIncarcerationPeriodIndex(
                incarceration_periods=[incarceration_period],
                ip_id_to_pfi_subtype=(
                    {incarceration_period.incarceration_period_id: None}
                    if incarceration_period.incarceration_period_id
                    else {}
                ),
            )
        )
        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods or []
        )
        assessments = assessments or []
        sorted_violation_responses = (
            sorted(violation_responses, key=lambda b: b.response_date or date.min)
            if violation_responses
            else []
        )

        return self.identifier._admission_event_for_period(
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=supervision_sentences,
            incarceration_period=incarceration_period,
            incarceration_period_index=incarceration_period_index,
            supervision_period_index=supervision_period_index,
            assessments=assessments,
            sorted_violation_responses=sorted_violation_responses,
            supervision_period_to_agent_associations=_DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            county_of_residence=county_of_residence,
        )

    def test_admission_event_for_period_us_mo(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_MO",
            facility="PRISON3",
            admission_date=date(2010, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            state_code="US_MO",
            start_date=date(2010, 1, 1),
            termination_date=date(2010, 2, 15),
        )
        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=1111,
                state_code="US_MO",
                supervision_type=StateSupervisionType.PROBATION,
                start_date=date(2010, 1, 1),
                supervision_periods=[supervision_period],
                incarceration_periods=[incarceration_period],
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=date(2010, 1, 1),
                    end_date=None,
                    supervision_type=StateSupervisionType.PROBATION,
                )
            ],
        )

        admission_event = self._run_admission_event_for_period(
            incarceration_period=incarceration_period,
            supervision_sentences=[supervision_sentence],
        )

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        self.assertEqual(
            IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility="PRISON3",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=incarceration_period.admission_reason,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            ),
            admission_event,
        )

    def test_admission_event_for_period(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        admission_event = self._run_admission_event_for_period(
            incarceration_period=incarceration_period,
        )

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        self.assertEqual(
            IncarcerationCommitmentFromSupervisionAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility="PRISON3",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=incarceration_period.admission_reason,
                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                case_type=StateSupervisionCaseType.GENERAL,
            ),
            admission_event,
        )

    def test_admission_event_for_period_all_admission_reasons(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        for admission_reason in StateIncarcerationPeriodAdmissionReason:
            incarceration_period.admission_reason = admission_reason

            # ADMITTED_FROM_SUPERVISION is an ingest-only enum
            if (
                admission_reason
                != StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
            ):
                admission_event = self._run_admission_event_for_period(
                    incarceration_period=incarceration_period,
                )

                self.assertIsNotNone(admission_event)

    def test_admission_event_for_period_specialized_pfi(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        admission_event = self._run_admission_event_for_period(
            incarceration_period=incarceration_period,
        )

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        self.assertEqual(
            IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility="PRISON3",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                admission_reason=incarceration_period.admission_reason,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            ),
            admission_event,
        )

    def test_admission_event_for_period_county_jail(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="CJ10",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        admission_event = self._run_admission_event_for_period(
            incarceration_period=incarceration_period,
        )

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        self.assertEqual(
            IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.admission_date,
                facility="CJ10",
                county_of_residence=_COUNTY_OF_RESIDENCE,
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
                county_of_residence=_COUNTY_OF_RESIDENCE,
            )


class TestCommitmentFromSupervisionEventForPeriod(unittest.TestCase):
    """Tests the _commitment_from_supervision_event_for_period function."""

    def setUp(self) -> None:
        self.identifier = identifier.IncarcerationIdentifier()
        self.violation_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.incarceration.identifier.get_state_specific_violation_delegate"
        )
        self.mock_violation_delegate = self.violation_delegate_patcher.start()
        self.mock_violation_delegate.return_value = UsXxViolationDelegate()
        self.supervision_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.incarceration.identifier.get_state_specific_supervision_delegate"
        )
        self.mock_supervision_delegate = self.supervision_delegate_patcher.start()
        self.mock_supervision_delegate.return_value = UsXxSupervisionDelegate()

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        self.violation_delegate_patcher.stop()
        self.supervision_delegate_patcher.stop()

    def _run_commitment_from_supervision_event_for_period(
        self,
        incarceration_period: StateIncarcerationPeriod,
        pre_commitment_supervision_period: Optional[StateSupervisionPeriod],
        violation_delegate: StateSpecificViolationDelegate,
        supervision_delegate: StateSpecificSupervisionDelegate,
        incarceration_period_index: Optional[
            PreProcessedIncarcerationPeriodIndex
        ] = None,
        supervision_sentences: Optional[List[StateSupervisionSentence]] = None,
        incarceration_sentences: Optional[List[StateIncarcerationSentence]] = None,
        assessments: Optional[List[StateAssessment]] = None,
        violation_responses: Optional[List[StateSupervisionViolationResponse]] = None,
        commitment_from_supervision_delegate: Optional[
            StateSpecificCommitmentFromSupervisionDelegate
        ] = None,
    ) -> IncarcerationCommitmentFromSupervisionAdmissionEvent:
        """Helper function for testing the
        _commitment_from_supervision_event_for_period function."""
        supervision_sentences = supervision_sentences or []
        incarceration_sentences = incarceration_sentences or []
        assessments = assessments or []
        sorted_violation_responses = (
            sorted(violation_responses, key=lambda b: b.response_date or date.min)
            if violation_responses
            else []
        )
        commitment_from_supervision_delegate = (
            commitment_from_supervision_delegate
            or UsXxCommitmentFromSupervisionDelegate()
        )
        incarceration_period_index = (
            incarceration_period_index
            or PreProcessedIncarcerationPeriodIndex(
                incarceration_periods=[incarceration_period],
                ip_id_to_pfi_subtype=(
                    {incarceration_period.incarceration_period_id: None}
                    if incarceration_period.incarceration_period_id
                    else {}
                ),
            )
        )
        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=(
                [pre_commitment_supervision_period]
                if pre_commitment_supervision_period
                else []
            )
        )

        return self.identifier._commitment_from_supervision_event_for_period(
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=supervision_sentences,
            incarceration_period=incarceration_period,
            incarceration_period_index=incarceration_period_index,
            supervision_period_index=supervision_period_index,
            assessments=assessments,
            sorted_violation_responses=sorted_violation_responses,
            supervision_period_to_agent_associations=_DEFAULT_SUPERVISION_PERIOD_AGENT_ASSOCIATIONS,
            county_of_residence=_COUNTY_OF_RESIDENCE,
            commitment_from_supervision_delegate=commitment_from_supervision_delegate,
            violation_delegate=violation_delegate,
            supervision_delegate=supervision_delegate,
        )

    def test_commitment_from_supervision_event_violation_history_cutoff(self) -> None:
        """Tests the _commitment_from_supervision_event_for_period function,
        specifically the logic that includes the violation reports within the violation
        window. The `old` response and violation fall within a year of the last
        violation response before the revocation admission, but not within a year of the
        revocation date. Test that the `old` response is included in the response
        history."""

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
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2009, 12, 19),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
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
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
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
                violation_delegate=UsXxViolationDelegate(),
                supervision_delegate=UsXxSupervisionDelegate(),
            )
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        expected_commitment_from_supervision_event = IncarcerationCommitmentFromSupervisionAdmissionEvent(
            state_code=supervision_period.state_code,
            event_date=incarceration_period.admission_date,
            admission_reason=incarceration_period.admission_reason,
            supervision_type=supervision_period_supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            most_recent_response_decision=StateSupervisionViolationResponseDecision.CONTINUANCE,
            response_count=3,
            violation_history_description="1felony;1technical",
            violation_type_frequency_counter=[["FELONY"], ["TECHNICAL"]],
            supervising_officer_external_id="XXX",
            supervising_district_external_id="OFFICE_1",
            level_1_supervision_location_external_id="OFFICE_1",
            county_of_residence=_COUNTY_OF_RESIDENCE,
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

        supervision_violation_response_old = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_XX",
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2007, 12, 7),
                supervision_violation_response_decisions=[
                    StateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                        state_code="US_XX",
                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    )
                ],
                supervision_violation=supervision_violation_old,
            )
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

        supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_XX",
                supervision_violation_response_id=_DEFAULT_SSVR_ID,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=date(2009, 12, 1),
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

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            state_code="US_XX",
            start_date=date(2008, 3, 5),
            termination_date=date(2009, 12, 19),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
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
                violation_delegate=UsXxViolationDelegate(),
                supervision_delegate=UsXxSupervisionDelegate(),
            )
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        expected_commitment_from_supervision_event = IncarcerationCommitmentFromSupervisionAdmissionEvent(
            state_code=supervision_period.state_code,
            event_date=incarceration_period.admission_date,
            admission_reason=incarceration_period.admission_reason,
            supervision_type=supervision_period_supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            most_severe_violation_type=StateSupervisionViolationType.TECHNICAL,
            most_severe_violation_type_subtype=StateSupervisionViolationType.TECHNICAL.value,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            most_recent_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=1,
            violation_history_description="1technical",
            violation_type_frequency_counter=[["TECHNICAL"]],
            supervising_officer_external_id="XXX",
            supervising_district_external_id="OFFICE_1",
            level_1_supervision_location_external_id="OFFICE_1",
            county_of_residence=_COUNTY_OF_RESIDENCE,
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
        self._stop_state_specific_delegate_patchers()
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
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            state_code=state_code,
            start_date=date(2008, 3, 5),
            termination_date=date(2012, 12, 19),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
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
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        assert supervision_period.start_date is not None
        assert supervision_period.termination_date is not None
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

        assert incarceration_period.admission_date is not None
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

        commitment_from_supervision_event = (
            self._run_commitment_from_supervision_event_for_period(
                pre_commitment_supervision_period=supervision_period,
                incarceration_period=incarceration_period,
                violation_responses=violation_responses,
                supervision_sentences=[supervision_sentence],
                incarceration_sentences=[incarceration_sentence],
                violation_delegate=UsMoViolationDelegate(),
                supervision_delegate=UsMoSupervisionDelegate(),
            )
        )

        supervision_period_supervision_type = (
            StateSupervisionPeriodSupervisionType.PROBATION
        )

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        expected_commitment_from_supervision_event = IncarcerationCommitmentFromSupervisionAdmissionEvent(
            state_code=supervision_period.state_code,
            event_date=incarceration_period.admission_date,
            admission_reason=incarceration_period.admission_reason,
            supervision_type=supervision_period_supervision_type,
            case_type=StateSupervisionCaseType.GENERAL,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            most_recent_response_decision=StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
            response_count=1,
            violation_history_description="1fel",
            violation_type_frequency_counter=[["FELONY"]],
            supervising_officer_external_id="XXX",
            supervising_district_external_id="OFFICE_1",
            level_1_supervision_location_external_id="OFFICE_1",
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        self.assertEqual(
            expected_commitment_from_supervision_event,
            commitment_from_supervision_event,
        )

    def test_commitment_from_supervision_event_us_nd(self) -> None:
        self._stop_state_specific_delegate_patchers()
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=_DEFAULT_SP_ID,
            external_id="sp1",
            state_code="US_ND",
            start_date=date(2018, 3, 5),
            termination_date=date(2018, 5, 19),
            supervision_type=StateSupervisionType.PAROLE,
            supervision_site="X",
        )

        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            state_code="US_ND",
            violation_date=date(2018, 4, 20),
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_ND",
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code="US_ND",
            supervision_violation_response_id=_DEFAULT_SSVR_ID,
            supervision_violation=supervision_violation,
            response_date=date(2018, 5, 25),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=111,
            external_id="ip1",
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_ND",
            admission_date=date(2018, 6, 3),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_ND",
            supervision_sentence_id=111,
            start_date=date(2017, 1, 1),
            external_id="ss1",
            status=StateSentenceStatus.COMPLETED,
            supervision_type=StateSupervisionType.PROBATION,
            supervision_periods=[supervision_period],
        )

        commitment_from_supervision_event = self._run_commitment_from_supervision_event_for_period(
            pre_commitment_supervision_period=supervision_period,
            incarceration_period=incarceration_period,
            violation_responses=[ssvr],
            supervision_sentences=[supervision_sentence],
            incarceration_sentences=[],
            commitment_from_supervision_delegate=UsNdCommitmentFromSupervisionDelegate(),
            violation_delegate=UsNdViolationDelegate(),
            supervision_delegate=UsNdSupervisionDelegate(),
        )

        assert incarceration_period.admission_date is not None
        assert incarceration_period.admission_reason is not None
        expected_commitment_from_supervision_event = IncarcerationCommitmentFromSupervisionAdmissionEvent(
            state_code=supervision_period.state_code,
            event_date=incarceration_period.admission_date,
            admission_reason=incarceration_period.admission_reason,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            case_type=StateSupervisionCaseType.GENERAL,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            most_severe_violation_type=StateSupervisionViolationType.FELONY,
            most_severe_violation_type_subtype=StateSupervisionViolationType.FELONY.value,
            response_count=1,
            violation_history_description="1felony",
            violation_type_frequency_counter=[["FELONY"]],
            supervising_officer_external_id="XXX",
            supervising_district_external_id="X",
            level_1_supervision_location_external_id="X",
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        self.assertEqual(
            expected_commitment_from_supervision_event,
            commitment_from_supervision_event,
        )


class TestReleaseEventForPeriod(unittest.TestCase):
    """Tests the release_event_for_period function."""

    def setUp(self) -> None:
        self.identifier = identifier.IncarcerationIdentifier()

    def _run_release_for_period_with_no_sentences(
        self,
        incarceration_period: StateIncarcerationPeriod,
        county_of_residence: Optional[str],
    ) -> Optional[IncarcerationReleaseEvent]:
        """Runs `release_event_for_period` without providing sentence information. Sentence information
        is only used to inform supervision_type_at_release for US_MO and US_ID. All tests using this method should
        not require that state specific logic.
        """
        incarceration_sentences: List[StateIncarcerationSentence] = []
        supervision_sentences: List[StateSupervisionSentence] = []

        incarceration_period_index = PreProcessedIncarcerationPeriodIndex(
            incarceration_periods=[incarceration_period],
            ip_id_to_pfi_subtype=(
                {incarceration_period.incarceration_period_id: None}
                if incarceration_period.incarceration_period_id
                else {}
            ),
        )

        return self.identifier._release_event_for_period(
            incarceration_sentences,
            supervision_sentences,
            incarceration_period,
            incarceration_period_index,
            county_of_residence,
        )

    def test_release_event_for_period(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            release_reason_raw_text="SS",
        )

        release_event = self._run_release_for_period_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE
        )

        assert incarceration_period.release_reason is not None
        assert incarceration_period.release_date is not None
        assert incarceration_period.admission_date is not None
        self.assertEqual(
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.release_date,
                facility="PRISON3",
                county_of_residence=_COUNTY_OF_RESIDENCE,
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
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        for release_reason in StateIncarcerationPeriodReleaseReason:
            incarceration_period.release_reason = release_reason

            release_event = self._run_release_for_period_with_no_sentences(
                incarceration_period, _COUNTY_OF_RESIDENCE
            )

            self.assertIsNotNone(release_event)

    def test_release_event_for_period_county_jail(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="CJ19",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        release_event = self._run_release_for_period_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE
        )

        assert incarceration_period.release_reason is not None
        assert incarceration_period.release_date is not None
        assert incarceration_period.admission_date is not None
        self.assertEqual(
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.release_date,
                facility="CJ19",
                county_of_residence=_COUNTY_OF_RESIDENCE,
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

    def test_release_event_for_period_us_id(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_ID",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="SS",
        )

        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=111,
            start_date=incarceration_period.release_date,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_sentences = [
            StateSupervisionSentence.new_with_defaults(
                state_code="US_ID",
                supervision_periods=[supervision_period],
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            )
        ]

        incarceration_sentences: List[StateIncarcerationSentence] = []

        incarceration_period_index = PreProcessedIncarcerationPeriodIndex(
            incarceration_periods=[incarceration_period],
            ip_id_to_pfi_subtype=(
                {incarceration_period.incarceration_period_id: None}
                if incarceration_period.incarceration_period_id
                else {}
            ),
        )

        release_event = self.identifier._release_event_for_period(
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=supervision_sentences,
            incarceration_period=incarceration_period,
            incarceration_period_index=incarceration_period_index,
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        assert incarceration_period.release_reason is not None
        assert incarceration_period.release_date is not None
        assert incarceration_period.admission_date is not None
        self.assertEqual(
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.release_date,
                facility="PRISON3",
                county_of_residence=_COUNTY_OF_RESIDENCE,
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
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_MO",
            facility="PRISON3",
            admission_date=date(2013, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2019, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1111,
            state_code="US_MO",
            start_date=date(2019, 12, 4),
        )
        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=1111,
                state_code="US_MO",
                supervision_type=StateSupervisionType.PROBATION,
                start_date=date(2019, 11, 24),
                supervision_periods=[supervision_period],
                incarceration_periods=[incarceration_period],
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=date(2019, 12, 4),
                    end_date=None,
                    supervision_type=StateSupervisionType.PROBATION,
                )
            ],
        )
        incarceration_sentences: List[StateIncarcerationSentence] = []

        incarceration_period_index = PreProcessedIncarcerationPeriodIndex(
            incarceration_periods=[incarceration_period],
            ip_id_to_pfi_subtype=(
                {incarceration_period.incarceration_period_id: None}
                if incarceration_period.incarceration_period_id
                else {}
            ),
        )

        release_event = self.identifier._release_event_for_period(
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=[supervision_sentence],
            incarceration_period=incarceration_period,
            incarceration_period_index=incarceration_period_index,
            county_of_residence=_COUNTY_OF_RESIDENCE,
        )

        assert incarceration_period.release_date is not None
        assert incarceration_period.release_reason is not None
        assert incarceration_period.admission_date is not None
        self.assertEqual(
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=incarceration_period.release_date,
                facility="PRISON3",
                county_of_residence=_COUNTY_OF_RESIDENCE,
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
        admission_date = date(2008, 11, 20)
        release_date = date(2010, 12, 4)
        admission_reason = StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION
        release_reason = StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_ND",
            facility="PRISON3",
            admission_date=admission_date,
            admission_reason=admission_reason,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            release_date=release_date,
            release_reason=release_reason,
            release_reason_raw_text="RPAR",
        )

        release_event = self._run_release_for_period_with_no_sentences(
            incarceration_period, _COUNTY_OF_RESIDENCE
        )

        self.assertEqual(
            IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                event_date=release_date,
                facility="PRISON3",
                county_of_residence=_COUNTY_OF_RESIDENCE,
                release_reason=release_reason,
                release_reason_raw_text=incarceration_period.release_reason_raw_text,
                purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
                supervision_type_at_release=StateSupervisionPeriodSupervisionType.PAROLE,
                admission_reason=admission_reason,
                total_days_incarcerated=(release_date - admission_date).days,
            ),
            release_event,
        )


class TestGetUniquePeriodsFromSentenceGroupAndAddBackedges(unittest.TestCase):
    """Tests the get_unique_periods_from_sentence_groups_and_add_backedges function."""

    def setUp(self) -> None:
        self.identifier = identifier.IncarcerationIdentifier()

    def test_get_unique_periods_from_sentence_groups_and_add_backedges(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1112,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            state_code="US_XX",
            start_date=date(2008, 11, 20),
            termination_date=date(2010, 12, 4),
        )

        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2011, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2012, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=3333,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2013, 5, 22),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2015, 11, 9),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        sentence_groups = [
            StateSentenceGroup.new_with_defaults(
                state_code="US_XX",
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_sentences=[
                    StateIncarcerationSentence.new_with_defaults(
                        state_code="US_XX",
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                        incarceration_periods=[
                            incarceration_period_1,
                            incarceration_period_2,
                        ],
                    )
                ],
                supervision_sentences=[
                    StateSupervisionSentence.new_with_defaults(
                        state_code="US_XX",
                        incarceration_periods=[
                            incarceration_period_1,
                            incarceration_period_3,
                        ],
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                    )
                ],
            ),
            StateSentenceGroup.new_with_defaults(
                state_code="US_XX",
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                incarceration_sentences=[
                    StateIncarcerationSentence.new_with_defaults(
                        state_code="US_XX",
                        supervision_periods=[supervision_period],
                        incarceration_periods=[incarceration_period_3],
                        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                    )
                ],
            ),
        ]

        (
            incarceration_periods,
            supervision_periods,
        ) = self.identifier._get_unique_periods_from_sentence_groups_and_add_backedges(
            sentence_groups
        )

        expected_incarceration_periods = [
            incarceration_period_1,
            incarceration_period_2,
            incarceration_period_3,
        ]
        expected_supervision_periods = [supervision_period]

        self.assertEqual(expected_incarceration_periods, incarceration_periods)
        self.assertEqual(expected_supervision_periods, supervision_periods)

    def test_get_unique_periods_from_sentence_groups_and_add_backedges_no_periods(
        self,
    ) -> None:
        sentence_groups = [
            StateSentenceGroup.new_with_defaults(
                state_code="US_XX",
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            ),
            StateSentenceGroup.new_with_defaults(
                state_code="US_XX",
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            ),
        ]

        (
            incarceration_periods,
            supervision_periods,
        ) = self.identifier._get_unique_periods_from_sentence_groups_and_add_backedges(
            sentence_groups
        )

        expected_incarceration_periods: List[StateIncarcerationPeriod] = []
        expected_supervision_periods: List[StateSupervisionPeriod] = []

        self.assertEqual(expected_incarceration_periods, incarceration_periods)
        self.assertEqual(expected_supervision_periods, supervision_periods)

    def test_get_unique_periods_from_sentence_groups_and_add_backedges_no_sentence_groups(
        self,
    ) -> None:
        (
            incarceration_periods,
            supervision_periods,
        ) = self.identifier._get_unique_periods_from_sentence_groups_and_add_backedges(
            []
        )

        expected_incarceration_periods: List[StateIncarcerationPeriod] = []
        expected_supervision_periods: List[StateSupervisionPeriod] = []

        self.assertEqual(expected_incarceration_periods, incarceration_periods)
        self.assertEqual(expected_supervision_periods, supervision_periods)


class TestFindMostSeriousOffenseStatuteInSentenceGroup(unittest.TestCase):
    """Tests the find_most_serious_prior_charge_in_sentence_group function,"""

    def setUp(self) -> None:
        self.identifier = identifier.IncarcerationIdentifier()

    def test_find_most_serious_prior_charge_in_sentence_group(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            start_date=date(2007, 12, 10),
            incarceration_periods=[incarceration_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 11),
                    ncic_code="2703",
                    statute="9999",
                ),
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 11),
                    ncic_code="1316",
                    statute="8888",
                ),
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 11),
                    ncic_code="3619",
                    statute="7777",
                ),
            ],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence],
        )

        most_serious_charge = (
            self.identifier._find_most_serious_prior_charge_in_sentence_group(
                sentence_group, date(2008, 12, 31)
            )
        )

        assert most_serious_charge is not None
        self.assertEqual(most_serious_charge.statute, "8888")

    def test_find_most_serious_prior_charge_in_sentence_group_multiple_sentences(
        self,
    ) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_sentence_1 = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            start_date=date(2007, 12, 10),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[incarceration_period_1],
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 11),
                    ncic_code="3606",
                    statute="3606",
                ),
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 11),
                    ncic_code="3611",
                    statute="3611",
                ),
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 11),
                    ncic_code="3623",
                    statute="3623",
                ),
            ],
        )

        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=2222,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2003, 1, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_sentence_2 = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            start_date=date(2007, 12, 10),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[incarceration_period_2],
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2001, 12, 11),
                    ncic_code="3907",
                    statute="3907",
                ),
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2001, 12, 11),
                    ncic_code="3909",
                    statute="3909",
                ),
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2001, 12, 11),
                    ncic_code="3912",
                    statute="3912",
                ),
            ],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[
                incarceration_sentence_1,
                incarceration_sentence_2,
            ],
        )

        most_serious_charge = (
            self.identifier._find_most_serious_prior_charge_in_sentence_group(
                sentence_group, date(2008, 12, 31)
            )
        )

        assert most_serious_charge is not None
        self.assertEqual(most_serious_charge.statute, "3606")

    def test_find_most_serious_prior_charge_in_sentence_group_offense_after_date(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_sentence_1 = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            start_date=date(2007, 12, 10),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 11),
                    ncic_code="3611",
                    statute="1111",
                ),
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 11),
                    ncic_code="3623",
                    statute="3333",
                ),
            ],
        )

        incarceration_sentence_2 = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            start_date=date(2010, 12, 10),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2010, 12, 11),
                    ncic_code="3606",
                    statute="9999",
                ),
            ],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[
                incarceration_sentence_1,
                incarceration_sentence_2,
            ],
        )

        most_serious_charge = (
            self.identifier._find_most_serious_prior_charge_in_sentence_group(
                sentence_group, date(2008, 12, 31)
            )
        )

        assert most_serious_charge is not None
        self.assertEqual(most_serious_charge.statute, "1111")

    def test_find_most_serious_prior_charge_in_sentence_group_charges_no_ncic(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2010, 12, 11),
                    statute="9999",
                ),
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 11),
                    statute="1111",
                ),
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 11),
                    statute="3333",
                ),
            ],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence],
        )

        most_serious_charge = (
            self.identifier._find_most_serious_prior_charge_in_sentence_group(
                sentence_group, date(2008, 12, 31)
            )
        )

        self.assertIsNone(most_serious_charge)

    def test_find_most_serious_prior_charge_in_sentence_group_includes_chars(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            start_date=date(2007, 12, 10),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 11),
                    ncic_code="040A",
                    statute="xxxx",
                ),
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 11),
                    ncic_code="0101",
                    statute="9999",
                ),
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 11),
                    ncic_code="5301",
                    statute="1111",
                ),
            ],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence],
        )

        most_serious_charge = (
            self.identifier._find_most_serious_prior_charge_in_sentence_group(
                sentence_group, date(2008, 12, 31)
            )
        )

        assert most_serious_charge is not None
        self.assertEqual(most_serious_charge.statute, "9999")

    def test_find_most_serious_prior_offense_statute_no_offense_dates(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            start_date=date(2007, 12, 10),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    ncic_code="2703",
                    statute="9999",
                ),
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    ncic_code="1316",
                    statute="8888",
                ),
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    ncic_code="3619",
                    statute="7777",
                ),
            ],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence],
        )

        incarceration_period.incarceration_sentences = [incarceration_sentence]
        incarceration_sentence.sentence_group = sentence_group

        most_serious_charge = (
            self.identifier._find_most_serious_prior_charge_in_sentence_group(
                sentence_group, date(2008, 12, 31)
            )
        )

        assert most_serious_charge is not None
        self.assertEqual(most_serious_charge.statute, "8888")

    def test_find_most_serious_prior_offense_statute_offense_but_not_sentence_dates_before_cutoff(
        self,
    ) -> None:
        """We only look at the sentence date to determine the cutoff for most serious offense calculations."""
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            start_date=date(2009, 12, 10),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 10),
                    ncic_code="2703",
                    statute="9999",
                ),
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 10),
                    ncic_code="1316",
                    statute="8888",
                ),
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2007, 12, 10),
                    ncic_code="3619",
                    statute="7777",
                ),
            ],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence],
        )

        incarceration_period.incarceration_sentences = [incarceration_sentence]
        incarceration_sentence.sentence_group = sentence_group

        most_serious_charge = (
            self.identifier._find_most_serious_prior_charge_in_sentence_group(
                sentence_group, date(2008, 12, 31)
            )
        )

        self.assertIsNone(most_serious_charge)

    def test_find_most_serious_prior_offense_statute_sentence_but_not_offense_dates_before_cutoff(
        self,
    ) -> None:
        """We only look at the sentence date to determine the cutoff for most serious offense calculations."""
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1111,
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_XX",
            facility="PRISON3",
            admission_date=date(2008, 11, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2009, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            start_date=date(2007, 12, 10),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_periods=[incarceration_period],
            charges=[
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2009, 12, 10),
                    ncic_code="2703",
                    statute="9999",
                ),
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2009, 12, 10),
                    ncic_code="1316",
                    statute="8888",
                ),
                StateCharge.new_with_defaults(
                    state_code="US_XX",
                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                    offense_date=date(2009, 12, 10),
                    ncic_code="3619",
                    statute="7777",
                ),
            ],
        )

        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            incarceration_sentences=[incarceration_sentence],
        )

        incarceration_period.incarceration_sentences = [incarceration_sentence]
        incarceration_sentence.sentence_group = sentence_group

        most_serious_charge = (
            self.identifier._find_most_serious_prior_charge_in_sentence_group(
                sentence_group, date(2008, 12, 31)
            )
        )

        assert most_serious_charge is not None
        self.assertEqual(most_serious_charge.statute, "8888")


def expected_incarceration_stay_events(
    incarceration_period: StateIncarcerationPeriod,
    original_admission_reason: Optional[StateIncarcerationPeriodAdmissionReason] = None,
    original_admission_reason_raw_text: Optional[str] = None,
    most_serious_offense_statute: Optional[str] = None,
    most_serious_offense_ncic_code: Optional[str] = None,
    judicial_district_code: Optional[str] = None,
) -> List[IncarcerationStayEvent]:
    """Returns the expected incarceration stay events based on the provided |incarceration_period|."""

    expected_incarceration_events = []

    original_admission_reason = (
        original_admission_reason
        if original_admission_reason
        else incarceration_period.admission_reason
    )

    original_admission_reason_raw_text = (
        original_admission_reason_raw_text
        if original_admission_reason_raw_text
        else incarceration_period.admission_reason_raw_text
    )

    purpose_for_incarceration = (
        incarceration_period.specialized_purpose_for_incarceration
        or StateSpecializedPurposeForIncarceration.GENERAL
    )

    if incarceration_period.admission_date:
        release_date = min(
            (incarceration_period.release_date or date.max),
            date.today() + relativedelta(days=1),
        )

        days_incarcerated = [
            incarceration_period.admission_date + relativedelta(days=x)
            for x in range((release_date - incarceration_period.admission_date).days)
        ]

        if days_incarcerated:
            # Ensuring we're not counting the release date as a day spent incarcerated
            assert max(days_incarcerated) < release_date

        for stay_date in days_incarcerated:
            event = IncarcerationStayEvent(
                admission_reason=original_admission_reason,
                admission_reason_raw_text=original_admission_reason_raw_text,
                state_code=incarceration_period.state_code,
                facility=incarceration_period.facility,
                county_of_residence=_COUNTY_OF_RESIDENCE,
                event_date=stay_date,
                most_serious_offense_statute=most_serious_offense_statute,
                most_serious_offense_ncic_code=most_serious_offense_ncic_code,
                judicial_district_code=judicial_district_code,
                specialized_purpose_for_incarceration=purpose_for_incarceration,
            )

            expected_incarceration_events.append(event)

    return expected_incarceration_events
