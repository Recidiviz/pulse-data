# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests for supervision_period_index.py."""

import unittest
from datetime import date

from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodTerminationReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodStatus,
)
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod


class TestSupervisionPeriodIndexSupervisionPeriodsConverter(unittest.TestCase):
    """Tests the sorting logic in the _supervision_periods_converter."""

    def test_supervision_periods_converter(self):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=date(2000, 10, 1),
            termination_date=date(2003, 3, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        sorted_supervision_periods = [supervision_period_1, supervision_period_2]

        self.assertEqual(
            sorted_supervision_periods, supervision_period_index.supervision_periods
        )

    def test_supervision_periods_converter_empty_termination_date(self):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 1, 1),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=date(2000, 1, 1),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        sorted_supervision_periods = [supervision_period_1, supervision_period_2]

        self.assertEqual(
            sorted_supervision_periods, supervision_period_index.supervision_periods
        )

    def test_supervision_periods_converter_sort_by_admission_reason_court(self):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        sorted_supervision_periods = [supervision_period_1, supervision_period_2]

        self.assertEqual(
            sorted_supervision_periods, supervision_period_index.supervision_periods
        )

    def test_supervision_periods_converter_sort_by_admission_reason_conditional_release(
        self,
    ):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        sorted_supervision_periods = [supervision_period_1, supervision_period_2]

        self.assertEqual(
            sorted_supervision_periods, supervision_period_index.supervision_periods
        )


class TestSupervisionStartDatesByPeriodID(unittest.TestCase):
    """Tests the supervision_start_dates_by_period_id variable setter on the SupervisionPeriodIndex class."""

    def test_supervision_start_dates_by_period_id(self):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        expected_output = {
            supervision_period_1.supervision_period_id: supervision_period_1.start_date,
            supervision_period_2.supervision_period_id: supervision_period_1.start_date,
        }

        self.assertEqual(
            expected_output,
            supervision_period_index.supervision_start_dates_by_period_id,
        )

    def test_supervision_start_dates_by_period_id_multiple_official_admissions(self):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_3 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=333,
            start_date=date(2020, 5, 1),
            termination_date=date(2020, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_4 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=444,
            start_date=date(2020, 10, 3),
            termination_date=date(2020, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [
            supervision_period_2,
            supervision_period_4,
            supervision_period_3,
            supervision_period_1,
        ]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        expected_output = {
            supervision_period_1.supervision_period_id: supervision_period_1.start_date,
            supervision_period_2.supervision_period_id: supervision_period_1.start_date,
            supervision_period_3.supervision_period_id: supervision_period_3.start_date,
            supervision_period_4.supervision_period_id: supervision_period_3.start_date,
        }

        self.assertEqual(
            expected_output,
            supervision_period_index.supervision_start_dates_by_period_id,
        )

    def test_supervision_start_dates_by_period_id_multiple_absconsion_periods(self):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
            termination_reason=StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_3 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=333,
            start_date=date(2000, 10, 12),
            termination_date=date(2001, 1, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_4 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=444,
            start_date=date(2001, 1, 4),
            termination_date=date(2001, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
            termination_reason=StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [
            supervision_period_2,
            supervision_period_4,
            supervision_period_3,
            supervision_period_1,
        ]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        expected_output = {
            supervision_period_1.supervision_period_id: supervision_period_1.start_date,
            supervision_period_2.supervision_period_id: supervision_period_1.start_date,
            supervision_period_3.supervision_period_id: supervision_period_1.start_date,
            supervision_period_4.supervision_period_id: supervision_period_1.start_date,
        }

        self.assertEqual(
            expected_output,
            supervision_period_index.supervision_start_dates_by_period_id,
        )

    def test_supervision_start_dates_by_period_id_no_official_admission(self):
        # The first supervision period always counts as the official start of supervision
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        expected_output = {
            supervision_period_1.supervision_period_id: supervision_period_1.start_date,
            supervision_period_2.supervision_period_id: supervision_period_1.start_date,
        }

        self.assertEqual(
            expected_output,
            supervision_period_index.supervision_start_dates_by_period_id,
        )

    def test_supervision_start_dates_by_period_id_official_transfer_admission_investigation(
        self,
    ):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # Transferring from INVESTIGATION to PROBATION is a new official start of supervision
        supervision_period_3 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=333,
            start_date=date(2000, 10, 12),
            termination_date=date(2001, 1, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [
            supervision_period_2,
            supervision_period_3,
            supervision_period_1,
        ]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        expected_output = {
            supervision_period_1.supervision_period_id: supervision_period_1.start_date,
            supervision_period_2.supervision_period_id: supervision_period_1.start_date,
            supervision_period_3.supervision_period_id: supervision_period_3.start_date,
        }

        self.assertEqual(
            expected_output,
            supervision_period_index.supervision_start_dates_by_period_id,
        )

    def test_supervision_start_dates_by_period_id_official_transfer_admission_informal_probation(
        self,
    ):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # Transferring from INFORMAL_PROBATION to PROBATION is a new official start of supervision
        supervision_period_3 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=333,
            start_date=date(2000, 10, 12),
            termination_date=date(2001, 1, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [
            supervision_period_2,
            supervision_period_3,
            supervision_period_1,
        ]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        expected_output = {
            supervision_period_1.supervision_period_id: supervision_period_1.start_date,
            supervision_period_2.supervision_period_id: supervision_period_1.start_date,
            supervision_period_3.supervision_period_id: supervision_period_3.start_date,
        }

        self.assertEqual(
            expected_output,
            supervision_period_index.supervision_start_dates_by_period_id,
        )

    def test_supervision_start_dates_by_period_id_official_transfer_types(self):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # Transferring from INVESTIGATION to INFORMAL_PROBATION is a new official start of supervision
        supervision_period_3 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=333,
            start_date=date(2000, 10, 12),
            termination_date=date(2001, 1, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [
            supervision_period_2,
            supervision_period_3,
            supervision_period_1,
        ]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        expected_output = {
            supervision_period_1.supervision_period_id: supervision_period_1.start_date,
            supervision_period_2.supervision_period_id: supervision_period_2.start_date,
            supervision_period_3.supervision_period_id: supervision_period_2.start_date,
        }

        self.assertEqual(
            expected_output,
            supervision_period_index.supervision_start_dates_by_period_id,
        )

    def test_supervision_start_dates_by_period_id_not_official_transfer_admissions(
        self,
    ):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        # Transferring from INVESTIGATION to INFORMAL_PROBATION is a new official start of supervision
        supervision_period_3 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=333,
            start_date=date(2000, 10, 12),
            termination_date=date(2001, 1, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [
            supervision_period_2,
            supervision_period_3,
            supervision_period_1,
        ]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        expected_output = {
            supervision_period_1.supervision_period_id: supervision_period_1.start_date,
            supervision_period_2.supervision_period_id: supervision_period_1.start_date,
            supervision_period_3.supervision_period_id: supervision_period_1.start_date,
        }

        self.assertEqual(
            expected_output,
            supervision_period_index.supervision_start_dates_by_period_id,
        )


class TestSupervisionPeriodsByTerminationMonth(unittest.TestCase):
    """Tests the supervision_periods_by_termination_month variable setter on the SupervisionPeriodIndex class."""

    def test_supervision_periods_by_termination_month(self):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        expected_output = {2000: {10: [supervision_period_1, supervision_period_2]}}

        self.assertEqual(
            expected_output,
            supervision_period_index.supervision_periods_by_termination_month,
        )

    def test_supervision_periods_by_termination_month_multiple_months(self):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 5, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        expected_output = {
            2000: {5: [supervision_period_1], 10: [supervision_period_2]}
        }

        self.assertEqual(
            expected_output,
            supervision_period_index.supervision_periods_by_termination_month,
        )

    def test_supervision_periods_by_termination_month_multiple_years(self):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 5, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=date(2020, 10, 3),
            termination_date=date(2020, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        expected_output = {
            2000: {5: [supervision_period_1]},
            2020: {10: [supervision_period_2]},
        }

        self.assertEqual(
            expected_output,
            supervision_period_index.supervision_periods_by_termination_month,
        )


class TestGetMostRecentPreviousSupervisionPeriod(unittest.TestCase):
    """Tests get_most_recent_previous_supervision_period."""

    def test_get_most_recent_previous_supervision_period_valid(self):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 10, 1),
            termination_date=date(2000, 11, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [supervision_period_1, supervision_period_2]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        self.assertEqual(
            supervision_period_1,
            supervision_period_index.get_most_recent_previous_supervision_period(
                supervision_period_2
            ),
        )

    def test_get_most_recent_previous_supervision_period_first_in_list(self):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2010, 10, 1),
            termination_date=date(2010, 11, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [supervision_period_1, supervision_period_2]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        self.assertIsNone(
            supervision_period_index.get_most_recent_previous_supervision_period(
                supervision_period_1
            )
        )

    def test_get_most_recent_previous_supervision_period_single_period_in_list(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_periods = [supervision_period]

        supervision_period_index = PreProcessedSupervisionPeriodIndex(
            supervision_periods=supervision_periods
        )

        self.assertIsNone(
            supervision_period_index.get_most_recent_previous_supervision_period(
                supervision_period
            )
        )
