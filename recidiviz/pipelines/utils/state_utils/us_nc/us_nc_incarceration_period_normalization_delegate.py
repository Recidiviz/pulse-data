# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Contains state-specific logic for certain aspects of normalization US_NC
StateIncarcerationPeriod entities so that they are ready to be used in pipeline
calculations."""

from typing import List

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.persistence.entity.entity_utils import deep_entity_update
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)


class UsNcIncarcerationNormalizationDelegate(
    StateSpecificIncarcerationNormalizationDelegate
):
    """US_NC implementation of the StateSpecificIncarcerationNormalizationDelegate.

    ADMITTED_FROM_SUPERVISION is an ingest-only enum, and should only be used as a
    placeholder at ingest time if we are unable to determine whether an admission from
    supervision to prison is a sanction, revocation, or temporary custody admission.
    All periods with this value must be updated by the state’s IP normalization process
    to set the accurate admission reason."""

    def normalize_period_if_commitment_from_supervision(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        original_sorted_incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: NormalizedSupervisionPeriodIndex,
    ) -> StateIncarcerationPeriod:
        """Returns an updated version of the specified incarceration period if it is a
        commitment from supervision admission.

        Since we do not have information about the type of incarceration
        a person is serving, or parole condition violations or sanctions, we
        assume for the time being that all admissions from supervision are revocations.

        This should be updated when additional data about supervision periods, sanctions,
        or specialized purposes for incarceration are ingested.
        """

        incarceration_period = sorted_incarceration_periods[
            incarceration_period_list_index
        ]

        if (
            incarceration_period.admission_reason
            == StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
        ):
            if not incarceration_period.admission_date:
                raise ValueError(
                    "Unexpected missing admission_date on incarceration period: "
                    f"[{incarceration_period}]"
                )

            return deep_entity_update(
                incarceration_period,
                admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
            )

        # This period is not a commitment from supervision, so should not be updated at
        # this time
        return incarceration_period
