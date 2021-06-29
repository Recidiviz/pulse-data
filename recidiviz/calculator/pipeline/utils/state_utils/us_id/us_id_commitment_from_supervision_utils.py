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
in US_ID."""
from typing import List, Optional

import attr

from recidiviz.calculator.pipeline.utils.commitment_from_supervision_utils import (
    SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
    StateSpecificCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.incarceration_period_utils import (
    periods_are_temporally_adjacent,
)
from recidiviz.calculator.pipeline.utils.period_utils import (
    find_last_terminated_period_before_date,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    filter_out_unknown_supervision_period_supervision_type_periods,
)
from recidiviz.calculator.pipeline.utils.supervision_type_identification import (
    get_revocation_admission_reason_from_revoked_supervision_period,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod


class UsIdCommitmentFromSupervisionDelegate(
    StateSpecificCommitmentFromSupervisionDelegate
):
    """US_ID implementation of the StateSpecificCommitmentFromSupervisionDelegate."""

    def should_filter_out_unknown_supervision_type_in_pre_commitment_sp_search(
        self,
    ) -> bool:
        """In US_ID it's common for there to be periods with unset
        supervision_period_supervision_type values prior to an admission to
        incarceration, since these periods may signify that there is a warrant out for
        the person's arrest. So, for US_ID we need to filter
        the list of supervision periods to only include ones with a set
        supervision_period_supervision_type before looking for a
        pre-commitment supervision period."""
        return True


def us_id_normalize_period_if_commitment_from_supervision(
    incarceration_period_list_index: int,
    sorted_incarceration_periods: List[StateIncarcerationPeriod],
    supervision_period_index: Optional[PreProcessedSupervisionPeriodIndex],
) -> StateIncarcerationPeriod:
    """Returns an updated version of the specified incarceration period if it is a
    commitment from supervision admission.

    For US_ID, commitments from supervision occur in the following circumstances:
        - The person is admitted to GENERAL incarceration from non-investigative
            supervision (typically a probation revocation).
        - The person is admitted to TREATMENT_IN_PRISON incarceration from
            non-investigative supervision (sanction admission).
        - The person is transferred from a PAROLE_BOARD_HOLD incarceration period to a
            GENERAL (parole revoked by the parole board) or TREATMENT_IN_PRISON
            (treatment mandated by the parole board) incarceration period.

    If the period represents an admission from INVESTIGATION supervision, sets the
    admission_reason to be NEW_ADMISSION.
    """
    if supervision_period_index is None:
        raise ValueError(
            "IP pre-processing relies on supervision periods for US_ID. "
            "Expected non-null supervision_period_index."
        )

    relevant_sps = filter_out_unknown_supervision_period_supervision_type_periods(
        supervision_period_index.supervision_periods
    )

    incarceration_period = sorted_incarceration_periods[incarceration_period_list_index]

    purpose_for_incarceration = (
        incarceration_period.specialized_purpose_for_incarceration
    )

    if (
        incarceration_period.admission_reason
        == StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
    ):
        if (
            incarceration_period.specialized_purpose_for_incarceration
            == StateSpecializedPurposeForIncarceration.SHOCK_INCARCERATION
        ):
            raise ValueError(
                "We do not expect to see SHOCK_INCARCERATION admissions "
                "from supervision in US_ID. If we have started ingesting "
                "valid SHOCK_INCARCERATION sanction admissions for "
                "US_ID, then this function logic needs to be updated to "
                "handle these periods."
            )

        if not incarceration_period.admission_date:
            raise ValueError(
                "Unexpected missing admission_date on incarceration period: "
                f"[{incarceration_period}]"
            )

        # US_ID does not have overlapping supervision periods, so there there is a
        # maximum of one pre-commitment period.
        pre_commitment_supervision_period = find_last_terminated_period_before_date(
            upper_bound_date=incarceration_period.admission_date,
            periods=relevant_sps,
            maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        )

        if (
            pre_commitment_supervision_period
            and pre_commitment_supervision_period.supervision_period_supervision_type
            == StateSupervisionPeriodSupervisionType.INVESTIGATION
        ):
            # The most recent supervision period was of type INVESTIGATION,
            # so this is actually a NEW_ADMISSION and not a commitment from
            # supervision
            return attr.evolve(
                incarceration_period,
                admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            )

        if (
            purpose_for_incarceration
            == StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
        ):
            # Admissions from supervision for treatment are sanction admissions
            return attr.evolve(
                incarceration_period,
                admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
            )
        if (
            pre_commitment_supervision_period
            and purpose_for_incarceration
            == StateSpecializedPurposeForIncarceration.GENERAL
        ):
            revocation_admission_reason = (
                get_revocation_admission_reason_from_revoked_supervision_period(
                    pre_commitment_supervision_period
                )
            )

            return attr.evolve(
                incarceration_period, admission_reason=revocation_admission_reason
            )
        # We are unable to classify this ADMITTED_FROM_SUPERVISION
        return attr.evolve(
            incarceration_period,
            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
        )

    preceding_incarceration_period: Optional[StateIncarcerationPeriod] = (
        sorted_incarceration_periods[incarceration_period_list_index - 1]
        if incarceration_period_list_index > 0
        else None
    )

    if (
        incarceration_period.admission_reason
        == StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE
        and purpose_for_incarceration
        in (
            StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON,
            StateSpecializedPurposeForIncarceration.GENERAL,
        )
    ):
        if not preceding_incarceration_period or not periods_are_temporally_adjacent(
            preceding_incarceration_period, incarceration_period
        ):
            raise ValueError(
                "An incarceration period should only have an "
                "admission_reason of STATUS_CHANGE if it has a preceding "
                "incarceration period, and the two incarceration periods "
                "are a valid status-change edge."
            )

        if (
            preceding_incarceration_period.specialized_purpose_for_incarceration
            == StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
        ):
            # Transfers from parole board holds to treatment in prison is a
            # sanction admission
            if (
                purpose_for_incarceration
                == StateSpecializedPurposeForIncarceration.TREATMENT_IN_PRISON
            ):
                return attr.evolve(
                    incarceration_period,
                    admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                )

            # This is a transfer from a parole board hold to general
            # incarceration, which indicates a parole revocation
            return attr.evolve(
                incarceration_period,
                admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            )

    # This period is not a commitment from supervision, so should not be updated at
    # this time
    return incarceration_period
