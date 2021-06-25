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
"""Tests enum coverage for various `..._dedup_priority` views used in sessions."""
import unittest

from recidiviz.calculator.query.state.views.analyst_data.admission_start_reason_dedup_priority import (
    INCARCERATION_START_REASON_ORDERED_PRIORITY,
    SUPERVISION_START_REASON_ORDERED_PRIORITY,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
)


# TODO(#7912): Add enum coverage for all relevant dedup priority views
class AdmissionStartReasonDedupPriorityEnumCoverageTest(unittest.TestCase):
    """Tests full enum coverage for the start reason priority lists in the
    admission_start_reason_dedup_priority view."""

    def test_supervision_start_reason(self) -> None:
        for admission_reason in StateSupervisionPeriodAdmissionReason:
            if admission_reason not in SUPERVISION_START_REASON_ORDERED_PRIORITY:
                raise ValueError(
                    f"Missing {admission_reason} in "
                    f"SUPERVISION_START_REASON_ORDERED_PRIORITY."
                )

    def test_incarceration_start_reason(self) -> None:
        for admission_reason in StateIncarcerationPeriodAdmissionReason:
            if admission_reason not in INCARCERATION_START_REASON_ORDERED_PRIORITY:
                raise ValueError(
                    f"Missing {admission_reason} in "
                    f"INCARCERATION_START_REASON_ORDERED_PRIORITY."
                )
