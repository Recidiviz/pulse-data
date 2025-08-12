#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests us_ne_supervision_period_normalization_delegate.py."""
import unittest
from datetime import date

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod
from recidiviz.pipelines.utils.state_utils.us_ne.us_ne_supervision_period_normalization_delegate import (
    UsNeSupervisionNormalizationDelegate,
)

_STATE_CODE = StateCode.US_NE.value


class TestUsNeSupervisionNormalizationDelegate(unittest.TestCase):
    """Tests functions in UsNeSupervisionNormalizationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsNeSupervisionNormalizationDelegate()

    def test_normalize_subsequent_absconsion_periods(
        self,
    ) -> None:
        """Test that subsequent absconsion periods are normalized correctly. And that
        once we see an end movement that is not transfer within state, we stop
        normalizing the supervision_type to ABSCONSION."""
        sp1 = StateSupervisionPeriod.new_with_defaults(
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            start_date=date(2024, 1, 5),
            termination_date=date(2024, 1, 20),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            state_code=_STATE_CODE,
            external_id="sp1",
        )

        sp2 = StateSupervisionPeriod.new_with_defaults(
            supervision_type=StateSupervisionPeriodSupervisionType.ABSCONSION,
            admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
            start_date=date(2024, 1, 20),
            termination_date=date(2024, 1, 25),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            state_code=_STATE_CODE,
            external_id="sp2",
        )
        sp3 = StateSupervisionPeriod.new_with_defaults(
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=date(2024, 1, 25),
            termination_date=date(2024, 2, 6),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            state_code=_STATE_CODE,
            external_id="sp3",
        )
        sp3_fixed = StateSupervisionPeriod.new_with_defaults(
            supervision_type=StateSupervisionPeriodSupervisionType.ABSCONSION,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=date(2024, 1, 25),
            termination_date=date(2024, 2, 6),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            state_code=_STATE_CODE,
            external_id="sp3",
        )
        sp4 = StateSupervisionPeriod.new_with_defaults(
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=date(2024, 2, 6),
            termination_date=date(2024, 3, 26),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            state_code=_STATE_CODE,
            external_id="sp4",
        )
        sp4_fixed = StateSupervisionPeriod.new_with_defaults(
            supervision_type=StateSupervisionPeriodSupervisionType.ABSCONSION,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=date(2024, 2, 6),
            termination_date=date(2024, 3, 26),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            state_code=_STATE_CODE,
            external_id="sp4",
        )
        sp5 = StateSupervisionPeriod.new_with_defaults(
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            start_date=date(2025, 1, 6),
            termination_date=date(2025, 1, 26),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            state_code=_STATE_CODE,
            external_id="sp5",
        )

        supervision_periods = [
            sp1,
            sp2,
            sp3,
            sp4,
            sp5,
        ]

        supervision_periods_fixed = [
            sp1,
            sp2,
            sp3_fixed,
            sp4_fixed,
            sp5,
        ]

        self.assertEqual(
            supervision_periods_fixed,
            self.delegate.normalize_subsequent_absconsion_periods(supervision_periods),
        )

    def test_normalize_subsequent_absconsion_periods_open(
        self,
    ) -> None:
        """Test that subsequent absconsion periods are normalized correctly when last
        period is open."""
        sp1 = StateSupervisionPeriod.new_with_defaults(
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            start_date=date(2024, 1, 5),
            termination_date=date(2024, 1, 30),
            termination_reason=StateSupervisionPeriodTerminationReason.ABSCONSION,
            state_code=_STATE_CODE,
            external_id="sp1",
        )

        sp2 = StateSupervisionPeriod.new_with_defaults(
            supervision_type=StateSupervisionPeriodSupervisionType.ABSCONSION,
            admission_reason=StateSupervisionPeriodAdmissionReason.ABSCONSION,
            start_date=date(2024, 1, 30),
            termination_date=date(2024, 2, 20),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            state_code=_STATE_CODE,
            external_id="sp2",
        )
        sp3 = StateSupervisionPeriod.new_with_defaults(
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=date(2024, 2, 20),
            termination_date=date(2024, 4, 6),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            state_code=_STATE_CODE,
            external_id="sp3",
        )
        sp3_fixed = StateSupervisionPeriod.new_with_defaults(
            supervision_type=StateSupervisionPeriodSupervisionType.ABSCONSION,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=date(2024, 2, 20),
            termination_date=date(2024, 4, 6),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            state_code=_STATE_CODE,
            external_id="sp3",
        )
        sp4 = StateSupervisionPeriod.new_with_defaults(
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=date(2024, 4, 6),
            termination_date=None,
            termination_reason=None,
            state_code=_STATE_CODE,
            external_id="sp4",
        )
        sp4_fixed = StateSupervisionPeriod.new_with_defaults(
            supervision_type=StateSupervisionPeriodSupervisionType.ABSCONSION,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=date(2024, 4, 6),
            termination_date=None,
            termination_reason=None,
            state_code=_STATE_CODE,
            external_id="sp4",
        )

        supervision_periods = [
            sp1,
            sp2,
            sp3,
            sp4,
        ]

        supervision_periods_fixed = [
            sp1,
            sp2,
            sp3_fixed,
            sp4_fixed,
        ]

        self.assertEqual(
            supervision_periods_fixed,
            self.delegate.normalize_subsequent_absconsion_periods(supervision_periods),
        )
