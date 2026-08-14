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
from recidiviz.ingest.direct.regions.us_az.us_az_custom_enum_parsers import (
    MOVEMENT_REASON_DELIMITER,
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

# ADCRR bases its published recidivism reporting on the movement reason recorded
# when a person is readmitted (AZ_DOC_INMATE_TRAFFIC_HISTORY.MOVEMENT_REASON_ID),
# not on the warrant that preceded the readmission.
_MOVEMENT_REASON_TO_VIOLATION_TYPE = {
    "TECHNICAL VIOLATOR": StateSupervisionViolationType.TECHNICAL,
    "ABSCOND SUPERVISION": StateSupervisionViolationType.ABSCONDED,
    "NEW FELONY CONVICTION": StateSupervisionViolationType.FELONY,
    # ADCRR records someone returned with charges filed but not yet adjudicated as
    # a technical violator, and updates the reason to "New Felony Conviction" only
    # once they are convicted and sentenced.
    "NEW CHARGES PENDING": StateSupervisionViolationType.TECHNICAL,
}


class UsAzIncarcerationNormalizationDelegate(
    StateSpecificIncarcerationNormalizationDelegate
):
    """US_AZ implementation of the StateSpecificIncarcerationNormalizationDelegate."""

    @staticmethod
    def _movement_description(
        incarceration_period: StateIncarcerationPeriod,
    ) -> str | None:
        """Returns the uppercased ADCRR movement description from the period's
        admission reason raw text, with any movement reason suffix removed.
        """
        raw_text = incarceration_period.admission_reason_raw_text
        if not raw_text:
            return None
        return raw_text.split(MOVEMENT_REASON_DELIMITER)[0].upper()

    @staticmethod
    def _movement_reason(
        incarceration_period: StateIncarcerationPeriod,
    ) -> str | None:
        """Returns the uppercased ADCRR movement reason encoded in the period's
        admission reason raw text, or None if the movement carries no reason.
        """
        raw_text = incarceration_period.admission_reason_raw_text
        if not raw_text or MOVEMENT_REASON_DELIMITER not in raw_text:
            return None
        return raw_text.split(MOVEMENT_REASON_DELIMITER, maxsplit=1)[1].upper()

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

        movement_description = self._movement_description(incarceration_period)
        if movement_description not in _NEW_CRIME_RECOMMITMENT_RAW_TEXTS:
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
        """Returns the violation type indicated by the movement that admitted this
        person to incarceration, or None if the movement indicates no violation.

        Prefers the ADCRR movement reason, which is what ADCRR itself uses for its
        published recidivism reporting. Falls back to treating a new-crime
        recommitment as a felony violation (AZ DOC only houses felons, so any
        new-crime recommitment is a felony violation).
        """
        if (
            incarceration_period.admission_reason
            != StateIncarcerationPeriodAdmissionReason.REVOCATION
        ):
            return None

        movement_reason = self._movement_reason(incarceration_period)
        if movement_reason in _MOVEMENT_REASON_TO_VIOLATION_TYPE:
            return _MOVEMENT_REASON_TO_VIOLATION_TYPE[movement_reason]

        if self._movement_description(incarceration_period) in (
            _NEW_CRIME_RECOMMITMENT_RAW_TEXTS
        ):
            return StateSupervisionViolationType.FELONY
        return None
