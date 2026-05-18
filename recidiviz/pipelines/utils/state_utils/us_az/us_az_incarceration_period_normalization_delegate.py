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
"""Contains US_AZ implementation of the StateSpecificIncarcerationNormalizationDelegate."""
from typing import List

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.persistence.entity.activity.entities import StateIncarcerationPeriod
from recidiviz.persistence.entity.entity_utils import deep_entity_update
from recidiviz.pipelines.ingest.activity.normalization.normalization_managers.incarceration_period_normalization_manager import (
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
from recidiviz.pipelines.utils.shared_constants import (
    SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
)
from recidiviz.pipelines.utils.supervision_period_utils import (
    filter_out_supervision_period_types_excluded_from_pre_admission_search,
)

_NEW_CRIME_RECOMMITMENT_RAW_TEXTS = frozenset({"RECOMMITMENT", "NEW COMMITMENT"})


class UsAzIncarcerationNormalizationDelegate(
    StateSpecificIncarcerationNormalizationDelegate
):
    """US_AZ implementation of the StateSpecificIncarcerationNormalizationDelegate."""

    def standardize_purpose_for_incarceration_values(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateIncarcerationPeriod]:
        """Standardizing PFI using the legacy standardize_purpose_for_incarceration_values function
        for US_AZ since this was previously the default normalization behavior
        and there hasn't been a use case for skipping this inferrence yet"""

        return legacy_standardize_purpose_for_incarceration_values(
            incarceration_periods
        )

    def normalize_period_if_commitment_from_supervision(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        original_sorted_incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: NormalizedSupervisionPeriodIndex,
    ) -> StateIncarcerationPeriod:
        """Reclassifies Recommitment/New Commitment periods as REVOCATION when
        a new felony crime was committed during supervision and a recent supervision
        period preceded the admission.
        """
        incarceration_period = sorted_incarceration_periods[
            incarceration_period_list_index
        ]

        raw_text = incarceration_period.admission_reason_raw_text
        if not raw_text or raw_text.upper() not in _NEW_CRIME_RECOMMITMENT_RAW_TEXTS:
            return incarceration_period

        if not incarceration_period.admission_date:
            return incarceration_period

        relevant_sps = (
            filter_out_supervision_period_types_excluded_from_pre_admission_search(
                supervision_period_index.sorted_supervision_periods
            )
        )

        pre_commitment_sp = find_last_terminated_period_on_or_before_date(
            upper_bound_date_inclusive=incarceration_period.admission_date,
            periods=relevant_sps,
            maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        )

        if not pre_commitment_sp:
            return incarceration_period

        return deep_entity_update(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.REVOCATION,
        )

    def get_incarceration_admission_violation_type(
        self,
        incarceration_period: StateIncarcerationPeriod,
    ) -> StateSupervisionViolationType | None:
        """Returns FELONY for new-crime recommitments during supervision (AZ DOC only
        houses felons, so any new-crime recommitment is a felony violation).
        """
        raw_text = incarceration_period.admission_reason_raw_text
        if (
            raw_text
            and raw_text.upper() in _NEW_CRIME_RECOMMITMENT_RAW_TEXTS
            and (
                incarceration_period.admission_reason
                == StateIncarcerationPeriodAdmissionReason.REVOCATION
            )
        ):
            return StateSupervisionViolationType.FELONY
        return None
