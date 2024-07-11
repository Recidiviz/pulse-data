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
"""Tests the functions in us_co_commitment_from_supervision_utils.py"""

import unittest
from datetime import date

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
)
from recidiviz.pipelines.utils.state_utils.us_co.us_co_commitment_from_supervision_utils import (
    UsCoCommitmentFromSupervisionDelegate,
)

_STATE_CODE = StateCode.US_CO.value


class TestUsCoCommitmentFromSupervisionDelegate(unittest.TestCase):
    """Tests functions in TestUsCoCommitmentFromSupervisionDelegate."""

    def setUp(self) -> None:
        self.delegate = UsCoCommitmentFromSupervisionDelegate()

    def test_us_co_commitment_supervision_type(self) -> None:
        """Tests that incarceration periods beginning with REVOCATION show coming from
        superivison type of PAROLE."""
        ip = NormalizedStateIncarcerationPeriod(
            state_code="US_CO",
            incarceration_period_id=111,
            external_id="ip1",
            admission_date=date(2019, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="10",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
            sequence_num=0,
        )

        results = self.delegate.get_commitment_from_supervision_supervision_type(
            ip, None
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertEqual(supervision_type, results)

    def test_us_co_commitment_supervision_type_not_revocation(self) -> None:
        """Tests that incarceration periods beginning with something other than REVOCATION do not show up as coming from
        superivison type of PAROLE."""
        ip = NormalizedStateIncarcerationPeriod(
            sequence_num=0,
            state_code="US_CO",
            incarceration_period_id=111,
            external_id="ip1",
            admission_date=date(2019, 5, 25),
            admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
            admission_reason_raw_text="10",
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
        )

        results = self.delegate.get_commitment_from_supervision_supervision_type(
            ip, None
        )

        supervision_type = StateSupervisionPeriodSupervisionType.PAROLE

        self.assertNotEqual(supervision_type, results)
