#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests us_mi_supervision_period_normalization_delegate.py."""
import unittest
from datetime import date

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod
from recidiviz.pipelines.utils.state_utils.us_mi.us_mi_supervision_period_normalization_delegate import (
    UsMiSupervisionNormalizationDelegate,
)

_STATE_CODE = StateCode.US_TN.value


class TestUsMiSupervisionNormalizationDelegate(unittest.TestCase):
    """Tests functions in UsMiSupervisionNormalizationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsMiSupervisionNormalizationDelegate()

    # This tests that normalization will drop INVESTIGATION
    def test_drop_investigation(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-2",
            start_date=date(2023, 1, 1),
            termination_date=date(2023, 7, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
        )
        self.assertEqual(
            [],
            self.delegate.drop_bad_periods([supervision_period]),
        )

    # This tests that normalization will convert supervision level to IN_CUSTODY if supervision level is null and the period doesn't end in discharge
    def test_supervision_level_override(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-1",
            start_date=date(2023, 1, 1),
            termination_date=date(2023, 7, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            supervision_level=None,
        )

        self.assertEqual(
            StateSupervisionLevel.IN_CUSTODY,
            self.delegate.supervision_level_override(0, [supervision_period]),
        )

    # This tests that normalization will not convert supervision level to IN_CUSTODY if supervision level is null but the period ends in discharge
    def test_no_supervision_level_override(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="sp-1",
            start_date=date(2023, 1, 1),
            termination_date=date(2023, 7, 1),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            supervision_level=None,
        )

        self.assertEqual(
            None,
            self.delegate.supervision_level_override(0, [supervision_period]),
        )
