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
"""Contains US_AR implementation of the StateSpecificIncarcerationNormalizationDelegate."""
from typing import List

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodTerminationReason,
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
from recidiviz.pipelines.utils.period_utils import (
    find_last_terminated_period_on_or_before_date,
)


class UsArIncarcerationNormalizationDelegate(
    StateSpecificIncarcerationNormalizationDelegate
):
    """US_AR implementation of the StateSpecificIncarcerationNormalizationDelegate."""

    def normalize_period_if_commitment_from_supervision(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        original_sorted_incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: NormalizedSupervisionPeriodIndex,
    ) -> StateIncarcerationPeriod:
        """Looks at incarceration periods with admission reason ADMITTED_FROM_SUPERVISION
        and infers whether or not the admission reason should be REVOCATION; if not, it's set
        to INTERNAL_UNKNOWN.
        """
        if (
            sorted_incarceration_periods[
                incarceration_period_list_index
            ].admission_reason
            == StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
        ):
            if _is_revocation_admission(
                incarceration_period_list_index,
                sorted_incarceration_periods,
                supervision_period_index,
            ):
                return deep_entity_update(
                    sorted_incarceration_periods[incarceration_period_list_index],
                    admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
                )
            return deep_entity_update(
                sorted_incarceration_periods[incarceration_period_list_index],
                admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
            )
        return sorted_incarceration_periods[incarceration_period_list_index]

    def standardize_purpose_for_incarceration_values(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateIncarcerationPeriod]:
        """Standardizes PFI in 3 steps:
        1. Set PFI to SHOCK_INCARCERATION for all periods with admission_reason = SANCTION_ADMISSION
        2. Use the legacy _standardize_purpose_for_incarceration_values function to propagate PFIs
        set in the previous step where relevant, and set PFI to GENERAL otherwise
        3. Set PFI to SHOCK_INCARCERATION for all periods with specialized_purpose_for_incarceration_raw_text = '90_DAY'

        We need to handle SHOCK_INCARCERATION PFIs in this step of normalization, because this
        function is called after admission reasons get standardized based on PFI in
        _normalize_commitment_from_supervision_admission_periods, which can't be modified in
        state-specific implementations of the incarceraation delegate. If incarceration periods
        had their PFI set to SHOCK_INCARCERATION during ingest or at an earlier step of normalization,
        then their admission reasons would be set to SANCTION_ADMISSION across the board,
        even though we allow periods in AR to use the REVOCATION admission reason with the
        SHOCK_INCARCERATION PFI.
        """

        ips_with_standardized_pfi = legacy_standardize_purpose_for_incarceration_values(
            [
                deep_entity_update(
                    ip,
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                )
                if ip.admission_reason
                == StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION
                else ip
                for ip in incarceration_periods
            ]
        )

        for ip in ips_with_standardized_pfi:
            if ip.specialized_purpose_for_incarceration_raw_text == "90_DAY":
                ip = deep_entity_update(
                    ip,
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION,
                )

        return ips_with_standardized_pfi


def _is_revocation_admission(
    incarceration_period_list_index: int,
    sorted_incarceration_periods: List[StateIncarcerationPeriod],
    supervision_period_index: NormalizedSupervisionPeriodIndex,
) -> bool:
    """
    Returns True for ADMITTED_FROM_SUPERVISION IPs that occur less than a year after a
    supervision period terminates in REVOCATION, with no other IPs between the REVOCATION
    termination and the ADMITTED_FROM_SUPERVISION admission. Defaults to false.
    """
    incarceration_period = sorted_incarceration_periods[incarceration_period_list_index]
    if not incarceration_period.admission_date:
        raise ValueError(f"Missing admission date for {incarceration_period}")

    most_recent_incarceration_period = find_last_terminated_period_on_or_before_date(
        upper_bound_date_inclusive=incarceration_period.admission_date,
        periods=sorted_incarceration_periods,
        maximum_months_proximity=12,
    )
    most_recent_supervision_period = find_last_terminated_period_on_or_before_date(
        upper_bound_date_inclusive=incarceration_period.admission_date,
        periods=supervision_period_index.sorted_supervision_periods,
        maximum_months_proximity=12,
    )

    if (
        most_recent_supervision_period
        and most_recent_supervision_period.termination_date
        and (
            not most_recent_incarceration_period
            or (
                most_recent_incarceration_period.admission_date
                and most_recent_incarceration_period.admission_date
                < most_recent_supervision_period.termination_date
            )
        )
    ):
        return (
            most_recent_supervision_period.termination_reason
            == StateSupervisionPeriodTerminationReason.REVOCATION
        )
    return False
