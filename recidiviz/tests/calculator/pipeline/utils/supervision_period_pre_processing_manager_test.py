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

"""Tests for supervision_period_pre_processing_manager.py."""
import datetime
import unittest
from typing import List, Optional

import attr
from freezegun import freeze_time

from recidiviz.calculator.pipeline.utils.supervision_period_pre_processing_manager import (
    SupervisionPreProcessingManager,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod
from recidiviz.tests.calculator.pipeline.utils.state_utils.us_xx.us_xx_supervision_period_pre_processing_delegate import (
    UsXxSupervisionPreProcessingDelegate,
)


class TestPreProcessedSupervisionPeriodsForCalculations(unittest.TestCase):
    """Tests the pre_processed_supervision_periods_for_calculations function."""

    @staticmethod
    def _pre_processed_supervision_periods_for_calculations(
        supervision_periods: List[StateSupervisionPeriod],
        earliest_death_date: Optional[datetime.date] = None,
    ) -> List[StateSupervisionPeriod]:
        sp_pre_processing_manager = SupervisionPreProcessingManager(
            supervision_periods=supervision_periods,
            delegate=UsXxSupervisionPreProcessingDelegate(),
            earliest_death_date=earliest_death_date,
            incarceration_sentences=None,
            supervision_sentences=None,
            incarceration_periods=None,
        )

        return (
            sp_pre_processing_manager.pre_processed_supervision_period_index_for_calculations().supervision_periods
        )

    def test_prepare_supervision_periods_for_calculations(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            state_code="US_XX",
            start_date=datetime.date(2006, 1, 1),
            termination_date=datetime.date(2007, 12, 31),
        )

        expected_period = attr.evolve(
            supervision_period,
            admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            termination_reason=StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
        )

        updated_periods = self._pre_processed_supervision_periods_for_calculations(
            [supervision_period],
        )

        self.assertEqual([expected_period], updated_periods)

    @freeze_time("2000-01-01")
    def test_prepare_supervision_periods_for_calculations_drop_future_dates(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            state_code="US_XX",
            start_date=datetime.date(2006, 1, 1),
            termination_date=datetime.date(2007, 12, 31),
        )

        updated_periods = self._pre_processed_supervision_periods_for_calculations(
            [supervision_period],
        )

        self.assertEqual([], updated_periods)

    @freeze_time("2000-01-01")
    def test_prepare_supervision_periods_for_calculations_unset_future_release_dates(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            state_code="US_XX",
            start_date=datetime.date(1990, 1, 1),
            termination_date=datetime.date(2007, 12, 31),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
        )

        updated_periods = self._pre_processed_supervision_periods_for_calculations(
            [supervision_period],
        )

        updated_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            state_code="US_XX",
            start_date=datetime.date(1990, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            termination_date=None,
            termination_reason=None,
        )

        self.assertEqual([updated_period], updated_periods)

    def test_prepare_supervision_periods_for_calculations_placeholder(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code="US_XX",
        )

        updated_periods = self._pre_processed_supervision_periods_for_calculations(
            [supervision_period],
        )
        self.assertEqual([], updated_periods)

    def test_prepare_supervision_periods_for_calculations_no_start_or_end_dates(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code="US_XX",
            start_date=None,
            termination_date=None,
        )
        updated_periods = self._pre_processed_supervision_periods_for_calculations(
            [supervision_period]
        )
        self.assertEqual([], updated_periods)

    def test_prepare_supervision_periods_for_calculations_drop_open_sp_after_death(
        self,
    ) -> None:
        """Tests if the open supervision periods after a period ending in death are dropped"""
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            state_code="US_XX",
            start_date=datetime.date(2000, 12, 29),
            termination_date=datetime.date(2001, 1, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.DEATH,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=2,
            state_code="US_XX",
            start_date=datetime.date(2001, 1, 5),
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION,
            termination_date=datetime.date(2001, 1, 6),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_TO_OTHER_JURISDICTION,
        )

        supervision_period_3 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=3,
            state_code="US_XX",
            start_date=datetime.date(2001, 1, 6),
        )

        expected_period = attr.evolve(
            supervision_period_1,
            admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            termination_reason=StateSupervisionPeriodTerminationReason.DEATH,
        )

        updated_periods = self._pre_processed_supervision_periods_for_calculations(
            [supervision_period_1, supervision_period_2, supervision_period_3],
            earliest_death_date=supervision_period_1.termination_date,
        )

        self.assertEqual([expected_period], updated_periods)

    def test_prepare_supervision_periods_for_calculations_close_open_sp_before_death(
        self,
    ) -> None:
        """Tests if the open supervision period with a start date within the time range of the
        period ending in death is closed and updated to be TERMINATED"""
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            state_code="US_XX",
            start_date=datetime.date(2001, 1, 1),
            termination_date=datetime.date(2001, 1, 30),
            termination_reason=StateSupervisionPeriodTerminationReason.DEATH,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=2,
            state_code="US_XX",
            start_date=datetime.date(2001, 1, 15),
        )

        updated_periods = self._pre_processed_supervision_periods_for_calculations(
            [supervision_period_1, supervision_period_2],
            earliest_death_date=supervision_period_1.termination_date,
        )

        expected_periods = [
            attr.evolve(
                supervision_period_1,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            ),
            attr.evolve(
                supervision_period_2,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
                termination_date=datetime.date(2001, 1, 30),
                termination_reason=StateSupervisionPeriodTerminationReason.DEATH,
            ),
        ]

        self.assertEqual(expected_periods, updated_periods)

    def test_prepare_supervision_periods_for_calculations_drop_open_sp_out_of_range_before_death(
        self,
    ) -> None:
        """Tests if the open supervision period with a start date outside the time range of the
        period ending in death is dropped"""
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            state_code="US_XX",
            start_date=datetime.date(2020, 1, 1),
            termination_date=datetime.date(2020, 1, 31),
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=2,
            state_code="US_XX",
            start_date=datetime.date(2020, 1, 15),
        )

        expected_periods = [
            attr.evolve(
                supervision_period_1,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
                termination_reason=StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
            ),
            attr.evolve(
                supervision_period_2,
                admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
                termination_date=datetime.date(2020, 3, 1),
                termination_reason=StateSupervisionPeriodTerminationReason.DEATH,
            ),
        ]
        updated_periods = self._pre_processed_supervision_periods_for_calculations(
            [supervision_period_1, supervision_period_2],
            earliest_death_date=datetime.date(2020, 3, 1),
        )

        self.assertEqual(expected_periods, updated_periods)

    def test_prepare_supervision_periods_for_calculations_sort(self):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=datetime.date(2000, 1, 1),
            termination_date=datetime.date(2000, 10, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=datetime.date(2000, 10, 1),
            termination_date=datetime.date(2003, 3, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        updated_periods = self._pre_processed_supervision_periods_for_calculations(
            supervision_periods,
        )

        sorted_supervision_periods = [supervision_period_1, supervision_period_2]

        self.assertEqual(sorted_supervision_periods, updated_periods)

    def test_prepare_supervision_periods_for_calculations_sort_empty_termination_date(
        self,
    ):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=datetime.date(2000, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_date=datetime.date(2000, 1, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=datetime.date(2000, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        updated_periods = self._pre_processed_supervision_periods_for_calculations(
            supervision_periods,
        )

        sorted_supervision_periods = [supervision_period_1, supervision_period_2]

        self.assertEqual(sorted_supervision_periods, updated_periods)

    def test_prepare_supervision_periods_for_calculations_sort_sort_by_admission_reason_court(
        self,
    ):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=datetime.date(2000, 1, 1),
            termination_date=datetime.date(2000, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=datetime.date(2000, 1, 1),
            termination_date=datetime.date(2000, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        updated_periods = self._pre_processed_supervision_periods_for_calculations(
            supervision_periods,
        )

        sorted_supervision_periods = [supervision_period_1, supervision_period_2]

        self.assertEqual(sorted_supervision_periods, updated_periods)

    def test_prepare_supervision_periods_for_calculations_sort_sort_by_admission_reason_conditional_release(
        self,
    ):
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=111,
            start_date=datetime.date(2000, 1, 1),
            termination_date=datetime.date(2000, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_XX",
            supervision_period_id=222,
            start_date=datetime.date(2000, 1, 1),
            termination_date=datetime.date(2000, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
        )

        supervision_periods = [supervision_period_2, supervision_period_1]

        updated_periods = self._pre_processed_supervision_periods_for_calculations(
            supervision_periods,
        )

        sorted_supervision_periods = [supervision_period_1, supervision_period_2]

        self.assertEqual(sorted_supervision_periods, updated_periods)
