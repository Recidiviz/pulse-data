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
"""Contains state-specific logic for certain aspects of normalization US_MO
StateIncarcerationPeriod entities so that they are ready to be used in pipeline
calculations."""
from typing import List, Optional, Set

from recidiviz.calculator.pipeline.utils.entity_normalization.incarceration_period_normalization_manager import (
    PurposeForIncarcerationInfo,
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    SANCTION_ADMISSION_PURPOSE_FOR_INCARCERATION_VALUES,
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.str_field_utils import normalize
from recidiviz.ingest.direct.regions.us_mo.us_mo_legacy_enum_helpers import (
    SHOCK_SANCTION_STATUS_CODES,
    TREATMENT_SANCTION_STATUS_CODES,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionViolationResponse,
)


class UsMoIncarcerationNormalizationDelegate(
    StateSpecificIncarcerationNormalizationDelegate
):
    """US_MO implementation of the StateSpecificIncarcerationNormalizationDelegate."""

    def incarceration_types_to_filter(self) -> Set[StateIncarcerationType]:
        """US_MO drops all incarceration periods that aren't in a STATE_PRISON from
        calculations."""
        return {
            t
            for t in StateIncarcerationType
            if t != StateIncarcerationType.STATE_PRISON
        }

    def get_pfi_info_for_period_if_commitment_from_supervision(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        violation_responses: Optional[List[StateSupervisionViolationResponse]],
    ) -> PurposeForIncarcerationInfo:
        return _us_mo_get_pfi_info_for_period_if_commitment_from_supervision(
            incarceration_period_list_index,
            sorted_incarceration_periods,
        )

    def period_is_parole_board_hold(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
    ) -> bool:
        """In US_MO, we can infer that an incarceration period is a parole board hold
        period in a number of ways:

            1. The period has an admission_reason of TEMPORARY_CUSTODY, and a
                purpose_for_incarceration value of GENERAL
            2. The period does not start with an admission_reason of TEMPORARY_CUSTODY,
                but it has a release_reason of RELEASED_FROM_TEMPORARY_CUSTODY, and
                the next period is a parole revocation or sanction admission on the
                same day as the release from temporary custody
            3.  The period does not start with an admission_reason of TEMPORARY_CUSTODY,
                but it has a release_reason of RELEASED_FROM_TEMPORARY_CUSTODY, and
                the next period is a standard parole board hold
        """
        incarceration_period = sorted_incarceration_periods[
            incarceration_period_list_index
        ]

        if (
            incarceration_period.admission_reason
            == StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY
            and incarceration_period.specialized_purpose_for_incarceration
            == StateSpecializedPurposeForIncarceration.GENERAL
        ):
            # This is the standard representation of parole board holds in US_MO
            return True

        if (
            incarceration_period.admission_reason
            != StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY
            and incarceration_period.release_reason
            == StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY
        ):
            next_period = (
                sorted_incarceration_periods[incarceration_period_list_index + 1]
                if incarceration_period_list_index
                < (len(sorted_incarceration_periods) - 1)
                else None
            )

            if not next_period:
                # We're not quite sure what's going on here, and don't have enough
                # information to confidently classify this as a board hold.
                return False

            if (
                incarceration_period.release_date
                and next_period.admission_date
                and incarceration_period.release_date == next_period.admission_date
                and (
                    next_period.admission_reason
                    in (
                        StateIncarcerationPeriodAdmissionReason.REVOCATION,
                        StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                    )
                    or self.period_is_parole_board_hold(
                        incarceration_period_list_index=incarceration_period_list_index
                        + 1,
                        sorted_incarceration_periods=sorted_incarceration_periods,
                    )
                )
            ):
                # It's safe to assume this is a parole board hold since this period is
                # either directly followed by a parole revocation/sanction admission,
                # or by a parole board hold.
                return True

        return False

    def period_is_non_board_hold_temporary_custody(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
    ) -> bool:
        """The only periods of temporary custody in US_MO are parole board holds."""
        return False

    def normalization_relies_on_supervision_periods(self) -> bool:
        """IP normalization for US_MO does not rely on StateSupervisionPeriod
        entities."""
        return False


# TODO(#8118): Move this logic to ingest once we're putting the status codes in the
#  PFI raw text
def _us_mo_get_pfi_info_for_period_if_commitment_from_supervision(
    incarceration_period_list_index: int,
    sorted_incarceration_periods: List[StateIncarcerationPeriod],
) -> PurposeForIncarcerationInfo:
    """Infers the correct purpose_for_incarceration values for sanction admissions to
    periods that don't have the correct values added at ingest-time. Looks at the
    treatment and shock incarceration codes in the admission_reason_raw_text to
    determine what kind of sanction admission occurred."""
    ip = sorted_incarceration_periods[incarceration_period_list_index]
    pfi_override = None

    if (
        ip.admission_reason
        == StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION
        and ip.specialized_purpose_for_incarceration
        not in SANCTION_ADMISSION_PURPOSE_FOR_INCARCERATION_VALUES
        and ip.admission_reason_raw_text is not None
    ):
        # Find the correct pfi for this sanction admission
        status_codes = normalize(
            ip.admission_reason_raw_text,
            remove_punctuation=True,
        ).split(" ")

        num_treatment_status_codes = 0
        num_shock_status_codes = 0

        for code in status_codes:
            if code in TREATMENT_SANCTION_STATUS_CODES:
                num_treatment_status_codes += 1
            if code in SHOCK_SANCTION_STATUS_CODES:
                num_shock_status_codes += 1

        if num_treatment_status_codes == 0 and num_shock_status_codes == 0:
            raise ValueError(
                "admission_reason_raw_text: "
                f"[{ip.admission_reason_raw_text}] is being "
                "mapped to a SANCTION_ADMISSION without containing "
                "any sanction admission status codes."
            )

        pfi_override = (
            StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION
            # We don't ever expect to see a mix of treatment and shock codes,
            # but we handle this rare case by prioritizing TREATMENT_IN_PRISON
            if num_shock_status_codes > num_treatment_status_codes
            else StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
        )
    return PurposeForIncarcerationInfo(
        purpose_for_incarceration=(
            pfi_override or ip.specialized_purpose_for_incarceration
        ),
        # There are no defined pfi subtypes for US_MO
        purpose_for_incarceration_subtype=None,
    )
