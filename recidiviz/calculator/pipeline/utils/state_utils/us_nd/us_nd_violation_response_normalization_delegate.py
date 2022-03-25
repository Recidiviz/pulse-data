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
"""Utils for state-specific normalization logic related to violations in US_ND."""

from recidiviz.calculator.pipeline.normalization.utils.normalization_managers.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
)


class UsNdViolationResponseNormalizationDelegate(
    StateSpecificViolationResponseNormalizationDelegate
):
    """US_ND implementation of the
    StateSpecificViolationResponseNormalizationDelegate."""

    def should_de_duplicate_responses_by_date(self) -> bool:
        """In US_ND we only learn about violations when a supervision period has been
        terminated. Since there are overlapping SPs, there may be multiple SPs
        terminated on the same day, each indicating the violation_type that caused
        the termination. To avoid over-counting violation responses in US_ND,
        we de-duplicate responses that share a response_date."""
        return True
