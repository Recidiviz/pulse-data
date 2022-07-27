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
"""Tests for the us_nd_supervision_delegate.py file"""
import unittest
from datetime import date
from typing import List, Optional

from parameterized import parameterized

from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_delegate import (
    UsNdSupervisionDelegate,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


class TestUsNdSupervisionDelegate(unittest.TestCase):
    """Unit tests for UsNdSupervisionDelegate"""

    def setUp(self) -> None:
        self.supervision_delegate = UsNdSupervisionDelegate()

    @parameterized.expand(
        [
            (
                "parole_types",
                ["RPAR", "PARL", "PV"],
                StateSupervisionPeriodSupervisionType.PAROLE,
            ),
            (
                "probation_types",
                ["RPRB", "PRB"],
                StateSupervisionPeriodSupervisionType.PROBATION,
            ),
        ]
    )
    def test_get_incarceration_period_supervision_type_at_release(
        self,
        _name: str,
        raw_text_values: List[str],
        expected_supervision_type: StateSupervisionPeriodSupervisionType,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_ND",
            facility="PRISON",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="Revocation",
            release_date=date(2008, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
        )

        for value in raw_text_values:
            incarceration_period.release_reason_raw_text = value
            supervision_type_at_release = self.supervision_delegate.get_incarceration_period_supervision_type_at_release(
                incarceration_period
            )

            self.assertEqual(
                expected_supervision_type,
                supervision_type_at_release,
            )

    def test_get_incarceration_period_supervision_type_at_release_no_supervision(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_ND",
            facility="PRISON",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="Revocation",
            release_date=date(2008, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            release_reason_raw_text="X",
        )

        supervision_type_at_release = self.supervision_delegate.get_incarceration_period_supervision_type_at_release(
            incarceration_period
        )

        self.assertIsNone(supervision_type_at_release)

    def test_get_incarceration_period_supervision_type_at_release_unexpected_raw_text(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1112,
            external_id="2",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code="US_ND",
            facility="PRISON",
            admission_date=date(2008, 12, 20),
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            admission_reason_raw_text="Revocation",
            release_date=date(2008, 12, 21),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            release_reason_raw_text="NOT A VALID RAW TEXT VALUE",
        )

        with self.assertRaises(ValueError):
            _ = self.supervision_delegate.get_incarceration_period_supervision_type_at_release(
                incarceration_period
            )

    @parameterized.expand(
        [
            (None, None),
            ("1", "Region 3"),
            ("2", "Region 2"),
            ("9", "Region 3"),
        ]
    )
    def test_supervision_location_from_supervision_site(
        self,
        supervision_site: Optional[str],
        expected_level_2_supervision_location: Optional[str],
    ) -> None:
        self.assertEqual(
            (supervision_site, expected_level_2_supervision_location),
            self.supervision_delegate.supervision_location_from_supervision_site(
                supervision_site
            ),
        )

    def test_supervision_location_from_supervision_site_unexpected(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "Found unexpected supervision_site value: 12345"
        ):
            _ = self.supervision_delegate.supervision_location_from_supervision_site(
                "12345"
            )

    @parameterized.expand(
        [
            (None, None, None),
            ("9", "Region 3", "9"),
        ]
    )
    def test_get_deprecated_supervising_district_external_id(
        self,
        level_1_supervision_location: Optional[str],
        level_2_supervision_location: Optional[str],
        expected_district_external_id: Optional[str],
    ) -> None:
        self.assertEqual(
            expected_district_external_id,
            self.supervision_delegate.get_deprecated_supervising_district_external_id(
                level_1_supervision_location, level_2_supervision_location
            ),
        )
