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
"""Tests for classes/utils in
recidiviz/common/constants/state/state_incarceration_period
.py."""

import unittest

from recidiviz.common.constants.state import state_incarceration_period
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)


class StateIncarcerationPeriodTest(unittest.TestCase):
    """Tests full enum coverage of various utils functions in
    state_incarceration_period.py"""

    def test_is_commitment_from_supervision_all_enums(self) -> None:
        for admission_reason in StateIncarcerationPeriodAdmissionReason:
            # Assert no error
            _ = state_incarceration_period.is_commitment_from_supervision(
                admission_reason
            )

    def test_is_official_admission_all_enums(self) -> None:
        for admission_reason in StateIncarcerationPeriodAdmissionReason:
            # Assert no error
            _ = state_incarceration_period.is_official_admission(admission_reason)

    def test_is_official_release_all_enums(self) -> None:
        for release_reason in StateIncarcerationPeriodReleaseReason:
            # Assert no error
            _ = state_incarceration_period.is_official_release(release_reason)

    def test_release_reason_overrides_released_from_temporary_custody(self) -> None:
        for release_reason in StateIncarcerationPeriodReleaseReason:
            # Assert no error
            _ = state_incarceration_period.release_reason_overrides_released_from_temporary_custody(
                release_reason
            )
