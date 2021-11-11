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
"""Tests for the us_mo_supervision_period_pre_processing_delegate.py file"""
import builtins
import unittest
from datetime import date

from mock import patch
from mock.mock import Mock

from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_sentence_classification import (
    SupervisionTypeSpan,
    UsMoSentenceStatus,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_period_pre_processing_delegate import (
    UsMoSupervisionPreProcessingDelegate,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionPeriod,
    StateSupervisionSentence,
)
from recidiviz.tests.calculator.pipeline.utils.us_mo_fakes import (
    FakeUsMoSupervisionSentence,
)


def mock_id(_: object) -> int:
    return 123


# TODO(#2995): Move relevant tests from us_mo_sentence_classification_test.py here.
@patch.object(builtins, "id", side_effect=mock_id)
class TestUsMoSupervisionPeriodPreProcessingDelegate(unittest.TestCase):
    """Unit tests for UsMoSupervisionPeriodPreProcessingDelegate"""

    def setUp(self) -> None:
        self.delegate = UsMoSupervisionPreProcessingDelegate()

    def _build_sentence_status(
        self, status_code: str, status_description: str, status_date: date
    ) -> UsMoSentenceStatus:
        return UsMoSentenceStatus(
            sentence_external_id="test-sentence-external-id",
            sentence_status_external_id="test-sentence-status-external-id",
            status_code=status_code,
            status_description=status_description,
            status_date=status_date,
            person_external_id="test-person",
            is_incarceration_in_status=False,
            is_incarceration_out_status=False,
            is_investigation_status=False,
            is_lifetime_supervision_start_status=False,
            is_sentence_termimination_status=False,
            is_sentence_termination_status_candidate=False,
            is_supervision_in_status=False,
            is_supervision_out_status=False,
            is_supervision_type_critical_status=False,
        )

    def test_split_periods_based_on_sentences(self, _mock_id: Mock) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            external_id="sp1",
            state_code="US_MO",
            start_date=date(2020, 9, 1),
            termination_date=date(2020, 10, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.PARDONED,
            supervision_type=None,
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code="US_MO",
                supervision_sentence_id=111,
                start_date=date(2020, 9, 1),
                completion_date=date(2020, 10, 1),
                external_id="ss1",
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period],
                supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=date(2020, 9, 1),
                    end_date=date(2020, 9, 15),
                    supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
                    start_critical_statuses=[
                        self._build_sentence_status(
                            status_code="40O1010",
                            status_description="Parole Release",
                            status_date=date(2020, 9, 1),
                        )
                    ],
                    end_critical_statuses=[
                        self._build_sentence_status(
                            status_code="99O2010",
                            status_description="Parole Discharge",
                            status_date=date(2020, 9, 15),
                        ),
                    ],
                ),
                SupervisionTypeSpan(
                    start_date=date(2020, 9, 15),
                    end_date=date(2020, 10, 1),
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    start_critical_statuses=[
                        self._build_sentence_status(
                            status_code="99O2010",
                            status_description="Parole Discharge",
                            status_date=date(2020, 9, 15),
                        ),
                        self._build_sentence_status(
                            status_code="15I1000",
                            status_description="New Court Probation",
                            status_date=date(2020, 9, 15),
                        ),
                    ],
                    end_critical_statuses=[
                        self._build_sentence_status(
                            status_code="99O1000",
                            status_description="Court Probation Discharge",
                            status_date=date(2020, 10, 1),
                        )
                    ],
                ),
                SupervisionTypeSpan(
                    start_date=date(2020, 10, 1),
                    end_date=None,
                    supervision_type=None,
                    start_critical_statuses=[
                        self._build_sentence_status(
                            status_code="99O1000",
                            status_description="Court Probation Discharge",
                            status_date=date(2020, 10, 1),
                        )
                    ],
                    end_critical_statuses=None,
                ),
            ],
        )

        expected_periods = [
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                start_date=date(2020, 9, 1),
                termination_date=date(2020, 9, 15),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
                admission_reason_raw_text="40O1010",
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                termination_reason_raw_text="15I1000-99O2010",
                supervising_officer=None,
                supervision_site=None,
                supervision_period_id=123,
            ),
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                start_date=date(2020, 9, 15),
                termination_date=date(2020, 10, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                admission_reason_raw_text="15I1000-99O2010",
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                termination_reason_raw_text="99O1000",
                supervising_officer=None,
                supervision_site=None,
                supervision_period_id=123,
            ),
        ]

        results = self.delegate.split_periods_based_on_sentences(
            [supervision_period],
            incarceration_sentences=[],
            supervision_sentences=[supervision_sentence],
        )
        self.assertEqual(expected_periods, results)

    def test_split_periods_based_on_sentences_no_periods(self, _mock_id: Mock) -> None:
        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code="US_MO",
                supervision_sentence_id=111,
                start_date=date(2020, 9, 1),
                completion_date=date(2020, 10, 1),
                external_id="ss1",
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[],
                supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=date(2020, 9, 1),
                    end_date=date(2020, 9, 15),
                    supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
                    start_critical_statuses=[
                        self._build_sentence_status(
                            status_code="40O1010",
                            status_description="Parole Release",
                            status_date=date(2020, 9, 1),
                        )
                    ],
                    end_critical_statuses=[
                        self._build_sentence_status(
                            status_code="99O2010",
                            status_description="Parole Discharge",
                            status_date=date(2020, 9, 15),
                        ),
                    ],
                ),
                SupervisionTypeSpan(
                    start_date=date(2020, 9, 15),
                    end_date=date(2020, 10, 1),
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    start_critical_statuses=[
                        self._build_sentence_status(
                            status_code="99O2010",
                            status_description="Parole Discharge",
                            status_date=date(2020, 9, 15),
                        ),
                        self._build_sentence_status(
                            status_code="15I1000",
                            status_description="New Court Probation",
                            status_date=date(2020, 9, 15),
                        ),
                    ],
                    end_critical_statuses=[
                        self._build_sentence_status(
                            status_code="99O1000",
                            status_description="Court Probation Discharge",
                            status_date=date(2020, 10, 1),
                        )
                    ],
                ),
                SupervisionTypeSpan(
                    start_date=date(2020, 10, 1),
                    end_date=None,
                    supervision_type=None,
                    start_critical_statuses=[
                        self._build_sentence_status(
                            status_code="99O1000",
                            status_description="Court Probation Discharge",
                            status_date=date(2020, 10, 1),
                        )
                    ],
                    end_critical_statuses=None,
                ),
            ],
        )

        expected_periods = [
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                start_date=date(2020, 9, 1),
                termination_date=date(2020, 9, 15),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
                admission_reason_raw_text="40O1010",
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                termination_reason_raw_text="15I1000-99O2010",
                supervising_officer=None,
                supervision_site=None,
                supervision_period_id=123,
            ),
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                start_date=date(2020, 9, 15),
                termination_date=date(2020, 10, 1),
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                admission_reason_raw_text="15I1000-99O2010",
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                termination_reason_raw_text="99O1000",
                supervising_officer=None,
                supervision_site=None,
                supervision_period_id=123,
            ),
        ]

        results = self.delegate.split_periods_based_on_sentences(
            [],
            incarceration_sentences=[],
            supervision_sentences=[supervision_sentence],
        )

        self.assertEqual(expected_periods, results)

    def test_split_periods_based_on_sentences_no_end_date(self, _mock_id: Mock) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            external_id="sp1",
            state_code="US_MO",
            start_date=date(2020, 9, 1),
            termination_date=None,
            termination_reason=StateSupervisionPeriodTerminationReason.PARDONED,
            supervision_type=None,
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code="US_MO",
                supervision_sentence_id=111,
                start_date=date(2020, 9, 1),
                completion_date=date(2020, 10, 1),
                external_id="ss1",
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period],
                supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=date(2020, 9, 1),
                    end_date=date(2020, 9, 15),
                    supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
                    start_critical_statuses=[
                        self._build_sentence_status(
                            status_code="40O1010",
                            status_description="Parole Release",
                            status_date=date(2020, 9, 1),
                        )
                    ],
                    end_critical_statuses=[
                        self._build_sentence_status(
                            status_code="99O2010",
                            status_description="Parole Discharge",
                            status_date=date(2020, 9, 15),
                        ),
                    ],
                ),
                SupervisionTypeSpan(
                    start_date=date(2020, 9, 15),
                    end_date=None,
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    start_critical_statuses=[
                        self._build_sentence_status(
                            status_code="15I1000",
                            status_description="New Court Probation",
                            status_date=date(2020, 9, 15),
                        ),
                    ],
                    end_critical_statuses=None,
                ),
            ],
        )

        expected_periods = [
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                start_date=date(2020, 9, 1),
                termination_date=date(2020, 9, 15),
                supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
                admission_reason_raw_text="40O1010",
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                termination_reason_raw_text="15I1000-99O2010",
                supervising_officer=None,
                supervision_site=None,
                supervision_period_id=123,
            ),
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_MO",
                start_date=date(2020, 9, 15),
                termination_date=None,
                supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
                admission_reason_raw_text="15I1000-99O2010",
                termination_reason=None,
                termination_reason_raw_text=None,
                supervising_officer=None,
                supervision_site=None,
                supervision_period_id=123,
            ),
        ]

        results = self.delegate.split_periods_based_on_sentences(
            [supervision_period],
            incarceration_sentences=[],
            supervision_sentences=[supervision_sentence],
        )

        self.assertEqual(expected_periods, results)
