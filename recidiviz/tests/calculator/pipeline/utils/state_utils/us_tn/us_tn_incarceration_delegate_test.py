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
"""Tests us_tn_incarceration_delegate.py."""
import unittest
from datetime import date

from recidiviz.calculator.pipeline.utils.state_utils.us_tn.us_tn_incarceration_delegate import (
    UsTnIncarcerationDelegate,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod

_STATE_CODE = StateCode.US_TN.value


class TestUsTnIncarcerationDelegate(unittest.TestCase):
    """Tests functions in UsTnIncarcerationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsTnIncarcerationDelegate()

    def test_is_period_included_in_state_population_state_prison_custodial_authority(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            state_code=_STATE_CODE,
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        self.assertTrue(
            self.delegate.is_period_included_in_state_population(incarceration_period)
        )

    def test_is_period_included_in_state_population_by_custodial_authority(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            state_code=_STATE_CODE,
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        for custodial_authority in StateCustodialAuthority:
            incarceration_period.custodial_authority = custodial_authority
            if custodial_authority in (
                StateCustodialAuthority.STATE_PRISON,
                StateCustodialAuthority.COURT,
            ):
                self.assertTrue(
                    self.delegate.is_period_included_in_state_population(
                        incarceration_period
                    )
                )
            else:
                self.assertFalse(
                    self.delegate.is_period_included_in_state_population(
                        incarceration_period
                    )
                )

    def test_is_period_included_in_state_population_court_and_temporary_custody(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            custodial_authority=StateCustodialAuthority.COURT,
            state_code=_STATE_CODE,
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        )

        self.assertFalse(
            self.delegate.is_period_included_in_state_population(incarceration_period)
        )
