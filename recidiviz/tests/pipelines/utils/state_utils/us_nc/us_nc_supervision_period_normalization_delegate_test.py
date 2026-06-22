#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2026 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests us_nc_supervision_period_normalization_delegate.py."""
import unittest
from datetime import date

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.activity.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)
from recidiviz.pipelines.utils.state_utils.us_nc.us_nc_supervision_period_normalization_delegate import (
    UsNcSupervisionNormalizationDelegate,
)

_STATE_CODE = "US_NC"


def _supervision_period(
    start_date: date,
    termination_date: date | None,
) -> StateSupervisionPeriod:
    return StateSupervisionPeriod.new_with_defaults(
        state_code=_STATE_CODE,
        external_id="sp1",
        start_date=start_date,
        termination_date=termination_date,
    )


def _incarceration_period(admission_date: date | None) -> StateIncarcerationPeriod:
    return StateIncarcerationPeriod.new_with_defaults(
        state_code=_STATE_CODE,
        external_id="ip1",
        admission_date=admission_date,
    )


class TestUsNcSupervisionNormalizationDelegate(unittest.TestCase):
    """Tests functions in UsNcSupervisionNormalizationDelegate."""

    def test_open_period_returns_none(self) -> None:
        delegate = UsNcSupervisionNormalizationDelegate(incarceration_periods=[])
        sp = _supervision_period(
            start_date=date(2020, 1, 1),
            termination_date=None,
        )
        self.assertIsNone(delegate.supervision_termination_reason_override(sp))

    def test_closed_period_no_incarceration_returns_discharge(self) -> None:
        delegate = UsNcSupervisionNormalizationDelegate(incarceration_periods=[])
        sp = _supervision_period(
            start_date=date(2020, 1, 1),
            termination_date=date(2021, 6, 1),
        )
        self.assertEqual(
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            delegate.supervision_termination_reason_override(sp),
        )

    def test_incarceration_within_7_days_returns_revocation(self) -> None:
        delegate = UsNcSupervisionNormalizationDelegate(
            incarceration_periods=[_incarceration_period(date(2021, 6, 7))]
        )
        sp = _supervision_period(
            start_date=date(2020, 1, 1),
            termination_date=date(2021, 6, 1),
        )
        self.assertEqual(
            StateSupervisionPeriodTerminationReason.REVOCATION,
            delegate.supervision_termination_reason_override(sp),
        )

    def test_incarceration_on_same_day_returns_revocation(self) -> None:
        # end_date_exclusive == termination_date, so admission on the same calendar
        # day gives days_after=0, which is within the 7-day window.
        delegate = UsNcSupervisionNormalizationDelegate(
            incarceration_periods=[_incarceration_period(date(2021, 6, 1))]
        )
        sp = _supervision_period(
            start_date=date(2020, 1, 1),
            termination_date=date(2021, 6, 1),
        )
        self.assertEqual(
            StateSupervisionPeriodTerminationReason.REVOCATION,
            delegate.supervision_termination_reason_override(sp),
        )

    def test_incarceration_after_7_days_returns_discharge(self) -> None:
        delegate = UsNcSupervisionNormalizationDelegate(
            incarceration_periods=[_incarceration_period(date(2021, 6, 10))]
        )
        sp = _supervision_period(
            start_date=date(2020, 1, 1),
            termination_date=date(2021, 6, 1),
        )
        self.assertEqual(
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            delegate.supervision_termination_reason_override(sp),
        )

    def test_incarceration_during_supervision_returns_revocation(self) -> None:
        # TODO(OBT-33954): Ask the data team whether these ~160 NC cases where
        # incarceration starts during an active supervision period are actual
        # revocations or a data artifact.
        # Incarceration starting during the supervision period is treated as a
        # revocation — the supervision close date lags the actual revocation in
        # NC's snapshot data.
        delegate = UsNcSupervisionNormalizationDelegate(
            incarceration_periods=[_incarceration_period(date(2021, 5, 15))]
        )
        sp = _supervision_period(
            start_date=date(2020, 1, 1),
            termination_date=date(2021, 6, 1),
        )
        self.assertEqual(
            StateSupervisionPeriodTerminationReason.REVOCATION,
            delegate.supervision_termination_reason_override(sp),
        )

    def test_incarceration_before_supervision_start_returns_discharge(self) -> None:
        # Incarceration that predates the supervision period entirely is unrelated
        # and should not trigger revocation.
        delegate = UsNcSupervisionNormalizationDelegate(
            incarceration_periods=[_incarceration_period(date(2019, 12, 15))]
        )
        sp = _supervision_period(
            start_date=date(2020, 1, 1),
            termination_date=date(2021, 6, 1),
        )
        self.assertEqual(
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            delegate.supervision_termination_reason_override(sp),
        )
