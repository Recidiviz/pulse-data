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
"""Utils for state-specific logic related to incarceration commitments from supervision
in US_MO."""
from typing import List, Optional

from recidiviz.calculator.pipeline.utils.state_utils.state_specific_commitment_from_supervision_delegate import (
    StateSpecificCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_utils import (
    us_mo_get_pre_incarceration_supervision_type,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateSupervisionPeriod,
    StateSupervisionSentence,
)


class UsMoCommitmentFromSupervisionDelegate(
    StateSpecificCommitmentFromSupervisionDelegate
):
    """US_MO implementation of the StateSpecificCommitmentFromSupervisionDelegate."""

    def get_commitment_from_supervision_supervision_type(
        self,
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_period: StateIncarcerationPeriod,
        previous_supervision_period: Optional[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionPeriodSupervisionType]:
        """In US_MO we calculate the pre-incarceration supervision type by
        determining the most recent type of supervision a given person was on using
        the overlapping sentences."""
        return us_mo_get_pre_incarceration_supervision_type(
            incarceration_sentences, supervision_sentences, incarceration_period
        )
