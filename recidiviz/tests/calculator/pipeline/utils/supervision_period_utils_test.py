# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
# pylint: disable=unused-import,wrong-import-order

"""Tests for supervision_period_utils.py."""
import unittest
from datetime import date

from freezegun import freeze_time

from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    prepare_supervision_periods_for_calculations,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodAdmissionReason,
)
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod


class TestPrepareSupervisionPeriodsForCalculations(unittest.TestCase):
    """Tests the prepare_supervision_periods_for_calculations function."""

    def test_prepare_supervision_periods_for_calculations(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ND",
            start_date=date(2006, 1, 1),
            termination_date=date(2007, 12, 31),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        updated_periods = prepare_supervision_periods_for_calculations(
            [supervision_period],
            drop_federal_and_other_country_supervision_periods=False,
        )

        self.assertEqual([supervision_period], updated_periods)

    @freeze_time("2000-01-01")
    def test_prepare_supervision_periods_for_calculations_drop_future_dates(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ND",
            start_date=date(2006, 1, 1),
            termination_date=date(2007, 12, 31),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        updated_periods = prepare_supervision_periods_for_calculations(
            [supervision_period],
            drop_federal_and_other_country_supervision_periods=False,
        )

        self.assertEqual([], updated_periods)

    @freeze_time("2000-01-01")
    def test_prepare_supervision_periods_for_calculations_unset_future_release_dates(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
            state_code="US_ND",
            start_date=date(1990, 1, 1),
            termination_date=date(2007, 12, 31),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
        )

        updated_periods = prepare_supervision_periods_for_calculations(
            [supervision_period],
            drop_federal_and_other_country_supervision_periods=False,
        )

        updated_period = StateSupervisionPeriod.new_with_defaults(
            status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
            state_code="US_ND",
            start_date=date(1990, 1, 1),
            admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            termination_date=None,
            termination_reason=None,
        )

        self.assertEqual([updated_period], updated_periods)

    def test_prepare_supervision_periods_for_calculations_placeholder(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code="US_XX",
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        updated_periods = prepare_supervision_periods_for_calculations(
            [supervision_period],
            drop_federal_and_other_country_supervision_periods=False,
        )
        self.assertEqual([], updated_periods)

    def test_prepare_supervision_periods_for_calculations_usID_drop_federal(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            start_date=date(2006, 1, 1),
            termination_date=date(2007, 12, 31),
            custodial_authority=StateCustodialAuthority.FEDERAL,  # Not the state's authority
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        updated_periods = prepare_supervision_periods_for_calculations(
            [supervision_period],
            drop_federal_and_other_country_supervision_periods=True,
        )

        self.assertEqual([], updated_periods)

    def test_prepare_supervision_periods_for_calculations_usID_drop_other_country(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            start_date=date(2006, 1, 1),
            termination_date=date(2007, 12, 31),
            custodial_authority=StateCustodialAuthority.OTHER_COUNTRY,  # Not the state's authority
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        updated_periods = prepare_supervision_periods_for_calculations(
            [supervision_period],
            drop_federal_and_other_country_supervision_periods=True,
        )

        self.assertEqual([], updated_periods)

    def test_prepare_supervision_periods_for_calculations_drop_open_sp_after_death(
        self,
    ) -> None:
        """Tests if the open supervision periods after a period ending in death are dropped"""
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            start_date=date(2000, 12, 29),
            termination_date=date(2001, 1, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.DEATH,
            status=StateSupervisionPeriodStatus.TERMINATED,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            start_date=date(2001, 1, 5),
            admission_reason=StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION,
            termination_date=date(2001, 1, 6),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_OUT_OF_STATE,
            status=StateSupervisionPeriodStatus.TERMINATED,
        )

        supervision_period_3 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            start_date=date(2001, 1, 6),
            status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
        )

        updated_periods = prepare_supervision_periods_for_calculations(
            [supervision_period_1, supervision_period_2, supervision_period_3],
            drop_federal_and_other_country_supervision_periods=True,
        )

        self.assertEqual([supervision_period_1], updated_periods)

    def test_prepare_supervision_periods_for_calculations_close_open_sp_before_death(
        self,
    ) -> None:
        """Tests if the open supervision period with a start date within the time range of the
        period ending in death is closed and updated to be TERMINATED"""
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            start_date=date(2001, 1, 1),
            termination_date=date(2001, 1, 30),
            termination_reason=StateSupervisionPeriodTerminationReason.DEATH,
            status=StateSupervisionPeriodStatus.TERMINATED,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            start_date=date(2001, 1, 15),
            status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
        )

        updated_periods = prepare_supervision_periods_for_calculations(
            [supervision_period_1, supervision_period_2],
            drop_federal_and_other_country_supervision_periods=True,
        )

        updated_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            start_date=date(2001, 1, 15),
            admission_reason=StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
            termination_date=date(2001, 1, 30),
            termination_reason=StateSupervisionPeriodTerminationReason.DEATH,
            status=StateSupervisionPeriodStatus.TERMINATED,
        )

        self.assertEqual([supervision_period_1, updated_period_2], updated_periods)

    def test_prepare_supervision_periods_for_calculations_drop_open_sp_out_of_range_before_death(
        self,
    ) -> None:
        """Tests if the open supervision period with a start date outside the time range of the
        period ending in death is dropped"""
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            start_date=date(1999, 1, 15),
            status=StateSupervisionPeriodStatus.UNDER_SUPERVISION,
        )

        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            start_date=date(1999, 1, 15),
            termination_date=date(2001, 1, 1),
            status=StateSupervisionPeriodStatus.TERMINATED,
        )

        supervision_period_3 = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            start_date=date(2001, 1, 1),
            termination_date=date(2001, 1, 30),
            termination_reason=StateSupervisionPeriodTerminationReason.DEATH,
            status=StateSupervisionPeriodStatus.TERMINATED,
        )

        updated_periods = prepare_supervision_periods_for_calculations(
            [supervision_period_1, supervision_period_2, supervision_period_3],
            drop_federal_and_other_country_supervision_periods=True,
        )

        self.assertEqual([supervision_period_2, supervision_period_3], updated_periods)
