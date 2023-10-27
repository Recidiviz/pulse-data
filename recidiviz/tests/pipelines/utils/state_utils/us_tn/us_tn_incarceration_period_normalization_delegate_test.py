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
"""Tests us_tn_incarceration_period_normalization_delegate.py."""
import unittest
from datetime import date

from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_incarceration_period_normalization_delegate import (
    UsTnIncarcerationNormalizationDelegate,
)

_STATE_CODE = StateCode.US_TN.value


class TestUsTnIncarcerationNormalizationDelegate(unittest.TestCase):
    """Tests functions in TestUsTnIncarcerationNormalizationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsTnIncarcerationNormalizationDelegate()

    def test_get_incarceration_admission_violation_type_technical(self) -> None:
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            release_date=date(2022, 7, 1),
            admission_reason_raw_text="PAFA-VIOLT",
            external_id="ip-1",
        )

        result = self.delegate.get_incarceration_admission_violation_type(
            incarceration_period_1
        )

        self.assertEqual(result, StateSupervisionViolationType.TECHNICAL)

    def test_get_incarceration_admission_violation_type_law(self) -> None:
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            release_date=date(2022, 7, 1),
            admission_reason_raw_text="PRFA-VIOLW",
            external_id="ip-2",
        )

        result = self.delegate.get_incarceration_admission_violation_type(
            incarceration_period_2
        )

        self.assertEqual(result, StateSupervisionViolationType.LAW)

    def test_get_incarceration_admission_violation_type_none(self) -> None:
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            state_code=_STATE_CODE,
            admission_date=date(2022, 6, 1),
            release_date=date(2022, 7, 1),
            admission_reason_raw_text="PRFA-NEWCH",
            external_id="ip-2",
        )

        result = self.delegate.get_incarceration_admission_violation_type(
            incarceration_period_2
        )

        self.assertEqual(result, None)
