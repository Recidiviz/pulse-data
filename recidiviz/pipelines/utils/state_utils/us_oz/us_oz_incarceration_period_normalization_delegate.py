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
"""Contains US_OZ implementation of the StateSpecificIncarcerationNormalizationDelegate."""
from typing import List, Optional

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
from recidiviz.pipelines.utils.incarceration_period_utils import (
    legacy_standardize_purpose_for_incarceration_values,
)


class UsOzIncarcerationNormalizationDelegate(
    StateSpecificIncarcerationNormalizationDelegate
):
    """US_OZ implementation of the StateSpecificIncarcerationNormalizationDelegate."""

    def normalize_period_if_commitment_from_supervision(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        original_sorted_incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: Optional[NormalizedSupervisionPeriodIndex],
    ) -> StateIncarcerationPeriod:
        incarceration_period = sorted_incarceration_periods[
            incarceration_period_list_index
        ]

        # ADMITTED_FROM_SUPERVISION is ingest only so the pipeline will fail if we let
        # it through. In OZ we can just always map them to sanctions.
        return deep_entity_update(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
        )

    def incarceration_admission_reason_override(
        self, incarceration_period: StateIncarcerationPeriod
    ) -> Optional[StateIncarcerationPeriodAdmissionReason]:
        """Overrides admission reason for individuals in the LOTR data system.
        All other data systems return the existing admission reason attached to the incarceration period.
        """
        if incarceration_period.external_id and (
            incarceration_period.external_id.upper().startswith("LOTR")
            or incarceration_period.external_id.upper().startswith("SM")
        ):
            return StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE

        if incarceration_period.external_id and (
            incarceration_period.external_id.upper().startswith("HG")
        ):
            return StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY
        return incarceration_period.admission_reason

    def incarceration_facility_override(
        self,
        incarceration_period: StateIncarcerationPeriod,
    ) -> Optional[str]:
        """Overrides the facility for individuals in the EGT data system.
        All other data systems return the existing facility attached to the incarceration period.
        """
        if incarceration_period.external_id and (
            incarceration_period.external_id.upper().startswith("EGT")
        ):
            return "THE-CRIB"
        return incarceration_period.facility

    def standardize_purpose_for_incarceration_values(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateIncarcerationPeriod]:
        """Standardizing PFI using the legacy standardize_purpose_for_incarceration_values function
        for US_OZ since this was previously the default normalization behavior
        and there hasn't been a use case for skipping this inferrence yet"""

        return legacy_standardize_purpose_for_incarceration_values(
            incarceration_periods
        )
