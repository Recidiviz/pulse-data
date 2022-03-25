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
"""Utils for state-specific logic related to incarceration commitments from
 supervision in US_PA."""
from typing import List, Optional

from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
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
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateSupervisionSentence,
)


class UsPaCommitmentFromSupervisionDelegate(
    StateSpecificCommitmentFromSupervisionDelegate
):
    """US_PA implementation of the StateSpecificCommitmentFromSupervisionDelegate."""

    # TODO(#8028): Improve pre-commitment supervision type identification for US_PA to
    #  actually identify what kind of supervision a person is coming from
    def get_commitment_from_supervision_supervision_type(
        self,
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_period: StateIncarcerationPeriod,
        previous_supervision_period: Optional[NormalizedStateSupervisionPeriod],
    ) -> Optional[StateSupervisionPeriodSupervisionType]:
        """Determines the supervision type associated with the commitment from
        supervision.

        If the admission_reason is REVOCATION, then infers that the person is coming from parole.

        If the admission_reason is SANCTION_ADMISSION, then infers that the person is
        coming from parole.
        """
        admission_reason = incarceration_period.admission_reason

        if not admission_reason:
            raise ValueError(
                "Unexpected null admission_reason on incarceration_period: "
                f"[{incarceration_period}]"
            )

        if admission_reason in (
            # TODO(#8028): For now the only revocations in US_PA are parole revocations.
            StateIncarcerationPeriodAdmissionReason.REVOCATION,
        ):
            return StateSupervisionPeriodSupervisionType.PAROLE

        if (
            admission_reason
            == StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION
        ):
            # TODO(#8028): For now, we assume a sanction admission comes from parole
            #  until we improve the pre-commitment supervision type identification
            #  for US_PA.
            return StateSupervisionPeriodSupervisionType.PAROLE

        raise ValueError(
            "Unexpected admission reason being classified as a"
            f"commitment from supervision for US_PA: [{admission_reason}]."
        )
