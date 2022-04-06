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
"""Tests us_tn_supervision_period_normalization_delegate.py."""
import unittest
from datetime import date

from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_supervision_period_normalization_delegate import (
    UsTnSupervisionNormalizationDelegate,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod

_STATE_CODE = StateCode.US_TN.value


class TestUsTnSupervisionNormalizationDelegate(unittest.TestCase):
    """Tests functions in UsTnSupervisionNormalizationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsTnSupervisionNormalizationDelegate()

    # TODO(#12028): Delete this when TN ingest rerun has eliminated the bad
    #  periods with dates of 9999-12-31.
    def test_drop_9999_12_31_periods(self) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            start_date=date(2010, 1, 1),
            termination_date=None,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            start_date=date(2010, 1, 1),
            termination_date=date(9999, 12, 31),
        )
        supervision_period_3 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_TN.value,
            start_date=date(2010, 1, 1),
            termination_date=date(2020, 12, 31),
        )

        self.assertEqual(
            [supervision_period, supervision_period_3],
            self.delegate.drop_bad_unmodified_periods(
                [supervision_period, supervision_period_2, supervision_period_3]
            ),
        )

    # ~~ Add new tests here ~~
