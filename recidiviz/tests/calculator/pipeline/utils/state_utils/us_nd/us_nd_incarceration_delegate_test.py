# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests us_nd_incarceration_delegate.py."""
import unittest
from datetime import date

from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_incarceration_delegate import (
    UsNdIncarcerationDelegate,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod

STATE_CODE = "US_ND"


class TestUsNdIncarcerationDelegate(unittest.TestCase):
    """Tests functions in UsNdIncarcerationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsNdIncarcerationDelegate()

    def test_is_period_included_in_state_population_state_prison_custodial_authority(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            state_code=STATE_CODE,
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        self.assertTrue(
            self.delegate.is_period_included_in_state_population(incarceration_period)
        )

    def test_is_period_included_in_state_population_not_state_prison_custodial_authority(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=STATE_CODE,
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        for custodial_authority in StateCustodialAuthority:
            if custodial_authority == StateCustodialAuthority.STATE_PRISON:
                continue

            # TODO(#3723): Delete this once OOS periods are no longer being included
            if custodial_authority == StateCustodialAuthority.OTHER_STATE:
                continue

            incarceration_period.custodial_authority = custodial_authority

            self.assertFalse(
                self.delegate.is_period_included_in_state_population(
                    incarceration_period
                )
            )

    def test_is_period_included_in_state_population_null_custodial_authority(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            custodial_authority=None,
            state_code=STATE_CODE,
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            release_date=date(2010, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        self.assertFalse(
            self.delegate.is_period_included_in_state_population(incarceration_period)
        )
