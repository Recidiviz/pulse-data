#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests us_or_supervision_period_normalization_delegate.py."""
import unittest
from datetime import date

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod
from recidiviz.pipelines.utils.state_utils.us_or.us_or_supervision_period_normalization_delegate import (
    UsOrSupervisionNormalizationDelegate,
)

_STATE_CODE = StateCode.US_OR.value


class TestUsOrSupervisionNormalizationDelegate(unittest.TestCase):
    """Tests functions in UsOrSupervisionNormalizationDelegate."""

    # TODO(#27146): Add more test cases

    def setUp(self) -> None:
        self.delegate = UsOrSupervisionNormalizationDelegate()

    def test_supervision_type_override(
        self,
    ) -> None:
        supervision_period_1 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_OR.value,
            external_id="sp1",
            start_date=date(2022, 12, 15),
            termination_date=None,
            admission_reason_raw_text="ABSC",
            admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
        )
        supervision_period_2 = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_OR.value,
            external_id="sp2",
            start_date=date(2023, 12, 15),
            termination_date=date(2024, 1, 12),
            admission_reason_raw_text="ESCA",
            admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.ABSCONSION,
            self.delegate.supervision_type_override(
                1,
                [
                    supervision_period_1,
                    supervision_period_2,
                ],
            ),
        )
