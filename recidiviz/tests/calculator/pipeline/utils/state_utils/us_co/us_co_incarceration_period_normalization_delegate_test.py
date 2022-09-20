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
"""Tests us_co_incarceration_period_normalization_delegate.py."""
import unittest
from datetime import date

from recidiviz.calculator.pipeline.utils.state_utils.us_co.us_co_incarceration_period_normalization_delegate import (
    UsCoIncarcerationNormalizationDelegate,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod

_STATE_CODE = StateCode.US_CO.value


class TestUsCoIncarcerationNormalizationDelegate(unittest.TestCase):
    """Tests functions in TestUsCoIncarcerationNormalizationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsCoIncarcerationNormalizationDelegate()

    # ~~ Add new tests here ~~
    def test_facility_override_fugitive_inmate_closed(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            state_code=_STATE_CODE,
            facility="JAIL BCKLG",
            admission_date=date(2010, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ESCAPE,
            release_date=date(2012, 12, 25),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        actual_facility = self.delegate.incarceration_facility_override(
            incarceration_period
        )
        self.assertEqual(actual_facility, "FUG-INMATE")

    def test_facility_override_fugitive_inmate_open(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1113,
            external_id="2",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            state_code=_STATE_CODE,
            facility="COMMUNITY CORRECTIONS",
            admission_date=date(2022, 2, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ESCAPE,
        )

        actual_facility = self.delegate.incarceration_facility_override(
            incarceration_period
        )
        self.assertEqual(actual_facility, "FUG-INMATE")

    def test_facility_override_not_3days(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1117,
            external_id="2",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            state_code=_STATE_CODE,
            facility="JAIL BCKLG",
            admission_date=date(2021, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.ESCAPE,
            release_date=date(2021, 12, 22),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        actual_facility = self.delegate.incarceration_facility_override(
            incarceration_period
        )
        self.assertEqual(actual_facility, "JAIL BCKLG")

    def test_facility_override_not_fi(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1119,
            external_id="2",
            custodial_authority=StateCustodialAuthority.STATE_PRISON,
            state_code=_STATE_CODE,
            facility="COMMUNITY CORRECTIONS",
            admission_date=date(2022, 2, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
            release_date=date(2022, 2, 24),
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER,
        )

        actual_facility = self.delegate.incarceration_facility_override(
            incarceration_period
        )
        self.assertEqual(actual_facility, "COMMUNITY CORRECTIONS")
