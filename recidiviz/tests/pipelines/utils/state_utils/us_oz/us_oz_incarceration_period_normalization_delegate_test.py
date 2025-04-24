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
import datetime
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
        """Tests that US_OZ admission overrides work as desired, but do not affect
        existing defaults.
        """
        lotr_period_no_admission_reason = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="LOTR-99",
            admission_date=datetime.date(2020, 1, 1),
        )
        self.assertEqual(
            self.delegate.incarceration_admission_reason_override(
                lotr_period_no_admission_reason
            ),
            StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
        )
        self.assertEqual(
            self.delegate.incarceration_admission_reason_override(
                lotr_period_no_admission_reason
            ),
            StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
        )
        sm_period_with_admission_reason = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="SM-1",
            admission_date=datetime.date(2020, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )
        self.assertEqual(
            self.delegate.incarceration_admission_reason_override(
                sm_period_with_admission_reason
            ),
            StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
        )
        vfds_period_no_admission_reason = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="VFDS-1",
            admission_date=datetime.date(2020, 1, 1),
        )
        self.assertNotEqual(
            self.delegate.incarceration_admission_reason_override(
                vfds_period_no_admission_reason
            ),
            StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
        )
        vfds_period_with_admission_reason = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="VFDS-2",
            admission_date=datetime.date(2020, 1, 1),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )
        self.assertEqual(
            self.delegate.incarceration_admission_reason_override(
                vfds_period_with_admission_reason
            ),
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )
        hg_period_no_admission_reason = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="HG-1",
            admission_date=datetime.date(2020, 1, 1),
        )
        self.assertEqual(
            self.delegate.incarceration_admission_reason_override(
                hg_period_no_admission_reason
            ),
            StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
        )

    def test_incarceration_facility_override(self) -> None:
        egt_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="EGT-1",
            admission_date=datetime.date(2020, 1, 1),
        )
        self.assertEqual(
            self.delegate.incarceration_facility_override(egt_period),
            "THE-CRIB",
        )
        hg_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="HG-1",
            admission_date=datetime.date(2020, 1, 1),
        )
        self.assertEqual(
            self.delegate.incarceration_facility_override(hg_period),
            None,
        )
