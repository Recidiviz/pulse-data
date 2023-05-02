# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Contains US_CO implementation of the StateSpecificCommitmentFromSupervisionDelegate."""
from typing import Optional

from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
    NormalizedStateSupervisionPeriod,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_commitment_from_supervision_delegate import (
    StateSpecificCommitmentFromSupervisionDelegate,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)


class UsCoCommitmentFromSupervisionDelegate(
    StateSpecificCommitmentFromSupervisionDelegate
):
    """US_CO implementation of the StateSpecificCommitmentFromSupervisionDelegate."""

    def get_commitment_from_supervision_supervision_type(
        self,
        incarceration_period: NormalizedStateIncarcerationPeriod,
        previous_supervision_period: Optional[NormalizedStateSupervisionPeriod],
    ) -> Optional[StateSupervisionPeriodSupervisionType]:
        """Determines the supervision type for the given supervision period that
        preceded the given incarceration period that represents a commitment from
        supervision.
        """
        if not incarceration_period.admission_reason:
            raise ValueError(
                "Unexpected missing admission_reason on incarceration period: "
                f"[{incarceration_period}]"
            )

        if (
            incarceration_period.admission_reason
            == StateIncarcerationPeriodAdmissionReason.REVOCATION
        ):
            default_supervision_type = StateSupervisionPeriodSupervisionType.PAROLE
            return default_supervision_type

        return None
