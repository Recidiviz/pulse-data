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
"""Tests for the us_id_supervision_period_normalization_delegate.py file"""
import unittest
from datetime import date

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_period_normalization_delegate import (
    UsIdSupervisionNormalizationDelegate,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod


class TestUsIdSupervisionPeriodNormalizationDelegate(unittest.TestCase):
    """Unit tests for UsIdSupervisionPreProcdessingDelegate"""

    def setUp(self) -> None:
        self.upper_bound_date = date(2018, 10, 10)
        self.delegate = UsIdSupervisionNormalizationDelegate()

    def test_supervision_admission_reason_override_preceded_by_investigation_override_reason(
        self,
    ) -> None:
        supervision_period_previous = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=2,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=supervision_period_previous.termination_date,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )
        found_admission_reason = self.delegate.supervision_admission_reason_override(
            supervision_period=supervision_period,
            supervision_periods=[supervision_period_previous, supervision_period],
        )
        self.assertEqual(
            StateSupervisionPeriodAdmissionReason.COURT_SENTENCE, found_admission_reason
        )

    def test_supervision_admission_reason_override_investigation_too_far_back(
        self,
    ) -> None:
        previous_termination_date = self.upper_bound_date - relativedelta(days=10)
        supervision_period_previous = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=previous_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=2,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=previous_termination_date + relativedelta(months=1, days=2),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )
        found_admission_reason = self.delegate.supervision_admission_reason_override(
            supervision_period=supervision_period,
            supervision_periods=[supervision_period, supervision_period_previous],
        )
        self.assertEqual(supervision_period.admission_reason, found_admission_reason)

    def test_supervision_admission_reason_override_not_preceded_by_investigation(
        self,
    ) -> None:
        supervision_period_previous = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=2,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=supervision_period_previous.termination_date,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )
        found_admission_reason = self.delegate.supervision_admission_reason_override(
            supervision_period=supervision_period,
            supervision_periods=[supervision_period, supervision_period_previous],
        )
        self.assertEqual(supervision_period.admission_reason, found_admission_reason)

    def test_supervision_admission_reason_override_ignore_null_supervision_type(
        self,
    ) -> None:
        previous_termination_date = self.upper_bound_date - relativedelta(days=10)
        supervision_period_previous = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=previous_termination_date,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )
        supervision_period_one_day = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=2,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=previous_termination_date,
            termination_date=previous_termination_date + relativedelta(days=10),
        )
        supervision_period_ongoing = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=3,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=supervision_period_one_day.termination_date,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        found_admission_reason_for_ongoing = (
            self.delegate.supervision_admission_reason_override(
                supervision_period=supervision_period_ongoing,
                supervision_periods=[
                    supervision_period_previous,
                    supervision_period_ongoing,
                    supervision_period_one_day,
                ],
            )
        )
        self.assertEqual(
            StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            found_admission_reason_for_ongoing,
        )

    def test_supervision_admission_reason_override_multiple_periods_start_on_investigation_end(
        self,
    ) -> None:
        supervision_period_previous = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=1,
            external_id="sp1",
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )
        supervision_period_one_day = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=2,
            external_id="sp2",
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=supervision_period_previous.termination_date,
            termination_date=supervision_period_previous.termination_date,
            supervision_type=StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION,
        )
        supervision_period_ongoing = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=3,
            external_id="sp3",
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=supervision_period_one_day.termination_date,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        found_admission_reason_for_one_day = (
            self.delegate.supervision_admission_reason_override(
                supervision_period=supervision_period_one_day,
                supervision_periods=[
                    supervision_period_previous,
                    supervision_period_one_day,
                    supervision_period_ongoing,
                ],
            )
        )
        self.assertEqual(
            StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            found_admission_reason_for_one_day,
        )
        found_admission_reason_for_ongoing = (
            self.delegate.supervision_admission_reason_override(
                supervision_period=supervision_period_ongoing,
                supervision_periods=[
                    supervision_period_previous,
                    supervision_period_one_day,
                    supervision_period_ongoing,
                ],
            )
        )
        self.assertEqual(
            StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            found_admission_reason_for_ongoing,
        )
