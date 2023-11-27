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
"""Tests us_oz_incarceration_period_normalization_delegate.py."""
import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.pipelines.utils.state_utils.us_oz.us_oz_incarceration_period_normalization_delegate import (
    UsOzIncarcerationNormalizationDelegate,
)

_STATE_CODE = StateCode.US_OZ.value


class TestUsOzIncarcerationNormalizationDelegate(unittest.TestCase):
    """Tests functions in TestUsOzIncarcerationNormalizationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsOzIncarcerationNormalizationDelegate()

    def test_incarceration_admission_reason_override(self) -> None:
        """Tests that LOTR data system admission reasons are RETURN FROM ESCAPE
        and do not affect existing data systems."""
        lotr_period_no_admission_reason = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id="LOTR-99"
        )
        self.assertEqual(
            self.delegate.incarceration_admission_reason_override(
                lotr_period_no_admission_reason, None
            ),
            StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
        )
        lotr_period_with_admission_reason = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="LOTR-42",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )
        self.assertEqual(
            self.delegate.incarceration_admission_reason_override(
                lotr_period_with_admission_reason, None
            ),
            StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
        )
        vfds_period_no_admission_reason = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE, external_id="VFDS-1"
        )
        self.assertNotEqual(
            self.delegate.incarceration_admission_reason_override(
                vfds_period_no_admission_reason, None
            ),
            StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
        )
        vfds_period_with_admission_reason = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="VFDS-2",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )
        self.assertEqual(
            self.delegate.incarceration_admission_reason_override(
                vfds_period_with_admission_reason, None
            ),
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )
