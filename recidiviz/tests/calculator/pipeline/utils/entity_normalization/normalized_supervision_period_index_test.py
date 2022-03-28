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
"""Tests for normalized_supervision_period_index.py."""

import unittest
from datetime import date

from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateSupervisionPeriod,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_supervision_period_index import (
    _transfer_from_supervision_type_is_official_admission,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.tests.calculator.pipeline.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_sp_index_for_tests,
)


class TestSupervisionStartDatesByPeriodID(unittest.TestCase):
    """Tests the supervision_start_dates_by_period_id variable setter on the SupervisionPeriodIndex class."""

    def test_supervision_start_dates_by_period_id(self):
        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            sequence_num=0,
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
        )

        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            sequence_num=1,
            supervision_period_id=222,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        supervision_period_index = default_normalized_sp_index_for_tests(
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
        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            sequence_num=0,
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
        )

        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            sequence_num=1,
            supervision_period_id=222,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
        )

        supervision_period_3 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            sequence_num=2,
            supervision_period_id=333,
            start_date=date(2020, 5, 1),
            termination_date=date(2020, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
        )

        supervision_period_4 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            sequence_num=3,
            supervision_period_id=444,
            start_date=date(2020, 10, 3),
            termination_date=date(2020, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
        )

        supervision_periods = [
            supervision_period_2,
            supervision_period_4,
            supervision_period_3,
            supervision_period_1,
        ]

        supervision_period_index = default_normalized_sp_index_for_tests(
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
        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            sequence_num=0,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
        )

        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            sequence_num=1,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
            termination_reason=StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION,
        )

        supervision_period_3 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=333,
            sequence_num=2,
            start_date=date(2000, 10, 12),
            termination_date=date(2001, 1, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
        )

        supervision_period_4 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=444,
            sequence_num=3,
            start_date=date(2001, 1, 4),
            termination_date=date(2001, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
            termination_reason=StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION,
        )

        supervision_periods = [
            supervision_period_2,
            supervision_period_4,
            supervision_period_3,
            supervision_period_1,
        ]

        supervision_period_index = default_normalized_sp_index_for_tests(
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
        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            sequence_num=0,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        )

        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            sequence_num=1,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        supervision_period_index = default_normalized_sp_index_for_tests(
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
        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            sequence_num=0,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )

        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            sequence_num=1,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )

        # Transferring from INVESTIGATION to PROBATION is a new official start of supervision
        supervision_period_3 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=333,
            sequence_num=2,
            start_date=date(2000, 10, 12),
            termination_date=date(2001, 1, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_periods = [
            supervision_period_2,
            supervision_period_3,
            supervision_period_1,
        ]

        supervision_period_index = default_normalized_sp_index_for_tests(
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
        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            sequence_num=0,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION,
        )

        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            sequence_num=1,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION,
        )

        # Transferring from INFORMAL_PROBATION to PROBATION is a new official start of supervision
        supervision_period_3 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=333,
            sequence_num=2,
            start_date=date(2000, 10, 12),
            termination_date=date(2001, 1, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_periods = [
            supervision_period_2,
            supervision_period_3,
            supervision_period_1,
        ]

        supervision_period_index = default_normalized_sp_index_for_tests(
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
        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            sequence_num=0,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )

        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            sequence_num=1,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION,
        )

        # Transferring from INVESTIGATION to INFORMAL_PROBATION is a new official start of supervision
        supervision_period_3 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=333,
            sequence_num=2,
            start_date=date(2000, 10, 12),
            termination_date=date(2001, 1, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION,
        )

        supervision_periods = [
            supervision_period_2,
            supervision_period_3,
            supervision_period_1,
        ]

        supervision_period_index = default_normalized_sp_index_for_tests(
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
        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            sequence_num=0,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
        )

        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            sequence_num=1,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        # Transferring from INVESTIGATION to INFORMAL_PROBATION is a new official start of supervision
        supervision_period_3 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=333,
            sequence_num=2,
            start_date=date(2000, 10, 12),
            termination_date=date(2001, 1, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_periods = [
            supervision_period_2,
            supervision_period_3,
            supervision_period_1,
        ]

        supervision_period_index = default_normalized_sp_index_for_tests(
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
        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            sequence_num=0,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
        )

        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            sequence_num=1,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=supervision_periods
        )

        expected_output = {2000: {10: [supervision_period_1, supervision_period_2]}}

        self.assertEqual(
            expected_output,
            supervision_period_index.supervision_periods_by_termination_month,
        )

    def test_supervision_periods_by_termination_month_multiple_months(self):
        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            sequence_num=0,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 5, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
        )

        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            sequence_num=1,
            start_date=date(2000, 10, 3),
            termination_date=date(2000, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        supervision_period_index = default_normalized_sp_index_for_tests(
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
        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            sequence_num=0,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 5, 3),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
        )

        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            sequence_num=1,
            start_date=date(2020, 10, 3),
            termination_date=date(2020, 10, 11),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        supervision_period_index = default_normalized_sp_index_for_tests(
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
        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            sequence_num=0,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
        )
        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            sequence_num=1,
            start_date=date(2000, 10, 1),
            termination_date=date(2000, 11, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
        )

        supervision_periods = [supervision_period_1, supervision_period_2]

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=supervision_periods
        )

        self.assertEqual(
            supervision_period_1,
            supervision_period_index.get_most_recent_previous_supervision_period(
                supervision_period_2
            ),
        )

    def test_get_most_recent_previous_supervision_period_first_in_list(self):
        supervision_period_1 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            sequence_num=0,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
        )
        supervision_period_2 = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            sequence_num=1,
            start_date=date(2010, 10, 1),
            termination_date=date(2010, 11, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
        )

        supervision_periods = [supervision_period_1, supervision_period_2]

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=supervision_periods
        )

        self.assertIsNone(
            supervision_period_index.get_most_recent_previous_supervision_period(
                supervision_period_1
            )
        )

    def test_get_most_recent_previous_supervision_period_single_period_in_list(self):
        supervision_period = NormalizedStateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=date(2000, 1, 1),
            termination_date=date(2000, 10, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
        )

        supervision_periods = [supervision_period]

        supervision_period_index = default_normalized_sp_index_for_tests(
            supervision_periods=supervision_periods
        )

        self.assertIsNone(
            supervision_period_index.get_most_recent_previous_supervision_period(
                supervision_period
            )
        )

    def test_transfer_from_supervision_type_is_official_admission_all_supervision_types(
        self,
    ):
        for supervision_type in StateSupervisionPeriodSupervisionType:
            _transfer_from_supervision_type_is_official_admission(supervision_type)
