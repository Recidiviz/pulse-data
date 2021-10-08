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
"""Tests us_pa_incarceration_delegate.py."""
import unittest
from datetime import date

from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_incarceration_delegate import (
    UsPaIncarcerationDelegate,
)
from recidiviz.common.constants.state.shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod

STATE_CODE = "US_PA"


class TestUsPaIncarcerationDelegate(unittest.TestCase):
    """Tests functions in UsPaIncarcerationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsPaIncarcerationDelegate()

    def test_is_period_included_in_state_population_does_not_include_ccc_programs(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_PA",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            admission_reason_raw_text="CCIS-TRUE-INRS",
            release_date=date(2010, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        incarceration_period.specialized_purpose_for_incarceration = (
            StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN
        )
        incarceration_period.custodial_authority = (
            StateCustodialAuthority.SUPERVISION_AUTHORITY
        )

        self.assertFalse(
            self.delegate.is_period_included_in_state_population(incarceration_period)
        )

    def test_is_period_included_in_state_population_includes_shock_incarceration(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.COUNTY_JAIL,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_PA",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="CCIS-TRUE-INRS",
            release_date=date(2010, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        incarceration_period.specialized_purpose_for_incarceration = (
            StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION
        )
        incarceration_period.custodial_authority = (
            StateCustodialAuthority.SUPERVISION_AUTHORITY
        )

        self.assertTrue(
            self.delegate.is_period_included_in_state_population(incarceration_period)
        )

    def test_is_period_included_in_state_population_for_custodial_authority_state_prison(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
            state_code="US_PA",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            admission_reason_raw_text="60",
            release_date=date(2010, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        self.assertTrue(
            self.delegate.is_period_included_in_state_population(incarceration_period)
        )
