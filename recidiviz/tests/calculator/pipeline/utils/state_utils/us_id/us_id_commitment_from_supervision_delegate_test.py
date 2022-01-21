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
"""Tests the functions in us_id_commitment_from_supervision_delegate.py"""
import unittest
from datetime import date
from typing import List, Optional

from recidiviz.calculator.pipeline.utils.commitment_from_supervision_utils import (
    _get_commitment_from_supervision_supervision_period,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_commitment_from_supervision_delegate import (
    UsIdCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_incarceration_delegate import (
    UsIdIncarcerationDelegate,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)
from recidiviz.tests.calculator.pipeline.utils.entity_normalization.normalization_testing_utils import (
    default_normalized_ip_index_for_tests,
    default_normalized_sp_index_for_tests,
)


class TestPreCommitmentSupervisionPeriod(unittest.TestCase):
    """Tests the _get_commitment_from_supervision_supervision_period function when
    the UsIdCommitmentFromSupervisionDelegate is provided."""

    @staticmethod
    def _test_us_id_pre_commitment_supervision_period(
        admission_date: date,
        admission_reason: StateIncarcerationPeriodAdmissionReason,
        supervision_periods: List[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionPeriod]:
        ip = StateIncarcerationPeriod.new_with_defaults(
            state_code="US_ID",
            incarceration_period_id=111,
            admission_date=admission_date,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            admission_reason=admission_reason,
        )

        incarceration_periods = [ip]

        return _get_commitment_from_supervision_supervision_period(
            incarceration_period=ip,
            commitment_from_supervision_delegate=UsIdCommitmentFromSupervisionDelegate(),
            supervision_period_index=default_normalized_sp_index_for_tests(
                supervision_periods=supervision_periods
            ),
            incarceration_period_index=default_normalized_ip_index_for_tests(
                incarceration_periods=incarceration_periods,
                transfers_are_collapsed=True,
                incarceration_delegate=UsIdIncarcerationDelegate(),
            ),
        )

    def test_us_id_pre_commitment_supervision_period(self) -> None:
        supervision_period_set = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_period_unset = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=None,
        )

        supervision_periods = [supervision_period_set, supervision_period_unset]

        self.assertEqual(
            supervision_period_set,
            self._test_us_id_pre_commitment_supervision_period(
                admission_date=date(2017, 5, 11),
                admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
                supervision_periods=supervision_periods,
            ),
        )

    def test_us_id_pre_commitment_supervision_period_internal_unknown(
        self,
    ) -> None:
        supervision_period_set = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_period_unset = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            external_id="sp1",
            state_code="US_ID",
            start_date=date(2017, 3, 5),
            termination_date=date(2017, 5, 9),
            supervision_type=StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
        )

        supervision_periods = [supervision_period_set, supervision_period_unset]

        self.assertEqual(
            supervision_period_set,
            self._test_us_id_pre_commitment_supervision_period(
                admission_date=date(2017, 5, 11),
                admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
                supervision_periods=supervision_periods,
            ),
        )
