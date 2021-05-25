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
from typing import List, Tuple, Optional

from recidiviz.calculator.pipeline.utils.incarceration_period_index import (
    IncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.period_utils import (
    find_last_terminated_period_before_date,
)
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration as PurposeForIncarceration,
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
)


def us_id_pre_commitment_supervision_period_if_commitment(
    incarceration_period: StateIncarcerationPeriod,
    filtered_supervision_periods: List[StateSupervisionPeriod],
    incarceration_period_index: IncarcerationPeriodIndex,
) -> Tuple[bool, Optional[StateSupervisionPeriod]]:
    """Determines whether the incarceration_period started because of a commitment from
    supervision, which is either a sanction or a revocation admission. If a commitment
    did occur, finds the supervision period that the person was serving that caused the
    commitment.
    For US_ID, commitments from supervision occur in the following circumstances:
        - The person is admitted to GENERAL incarceration from non-investigative
            supervision (typically a probation revocation).
        - The person is admitted to TREATMENT_IN_PRISON incarceration from
            non-investigative supervision (sanction admission).
        - The person is transferred from a PAROLE_BOARD_HOLD incarceration period to a
            GENERAL (parole revoked by the parole board) or TREATMENT_IN_PRISON
            (treatment mandated by the parole board) incarceration period.
    Args:
        - incarceration_period: The StateIncarcerationPeriod being evaluated for an
            instance of commitment from supervision.
        - filtered_supervision_periods: A list of the person's StateSupervisionPeriods
            that have a set supervision_period_supervision_type.
        - incarceration_period_index: The index of incarceration periods for this person,
            including the incarceration period that occurred before the given
            incarceration_period, if the person has any preceding incarceration periods.
            Used to determine whether the person was transferred from a parole board
            hold or treatment.
    Returns:
        A tuple in the format [bool, Optional[StateSupervisionPeriod]] representing
        whether or not a commitment from supervision occurred and, if it did occur,
        the supervision period that caused the commitment if it can be identified. It is
        possible for this function to return True, None.
    """
    admission_date = incarceration_period.admission_date

    if not admission_date:
        raise ValueError(
            f"Admission date null for {incarceration_period}. Should be set in "
            f"the prepare_incarceration_periods_for_calculations process."
        )

    preceding_incarceration_period = (
        incarceration_period_index.preceding_incarceration_period(incarceration_period)
    )

    if not _us_id_admission_is_commitment_from_supervision(
        incarceration_period, preceding_incarceration_period
    ):
        return False, None

    incarceration_admission_date = None

    if (
        incarceration_period.admission_reason
        == StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
    ):
        if incarceration_period.specialized_purpose_for_incarceration in (
            PurposeForIncarceration.TREATMENT_IN_PRISON,
            PurposeForIncarceration.GENERAL,
        ):
            # An admission to treatment from supervision is a sanction admission,
            # and being admitted to prison from supervision for general incarceration is
            # a revocation
            incarceration_admission_date = incarceration_period.admission_date

    elif (
        incarceration_period.admission_reason
        == StateIncarcerationPeriodAdmissionReason.TRANSFER
    ):
        if incarceration_period.specialized_purpose_for_incarceration in (
            PurposeForIncarceration.TREATMENT_IN_PRISON,
            PurposeForIncarceration.GENERAL,
        ):
            if not preceding_incarceration_period:
                raise ValueError(
                    "Preceding incarceration period must exist for a transfer admission"
                    " to be counted as a commitment from supervision. Commitment"
                    " admission identification not working."
                )

            if not preceding_incarceration_period.admission_date:
                raise ValueError(
                    f"Admission date null for {incarceration_period}. Should be set in "
                    f"the IP pre-processing steps."
                )

            if (
                preceding_incarceration_period.specialized_purpose_for_incarceration
                == PurposeForIncarceration.PAROLE_BOARD_HOLD
            ):
                # This person was transferred from a parole board hold to incarceration.
                # The date that they entered prison was the date of the preceding
                # incarceration period.
                incarceration_admission_date = (
                    preceding_incarceration_period.admission_date
                )

    if incarceration_admission_date:
        # US_ID does not have overlapping supervision periods, so there there is a
        # maximum of one pre-commitment period.
        pre_commitment_supervision_period = find_last_terminated_period_before_date(
            upper_bound_date=incarceration_admission_date,
            periods=filtered_supervision_periods,
            maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        )

        if pre_commitment_supervision_period:
            if (
                pre_commitment_supervision_period.supervision_period_supervision_type
                == StateSupervisionPeriodSupervisionType.INVESTIGATION
            ):
                # If the most recent supervision period was of type INVESTIGATION,
                # this is not actually a sanction or revocation admission
                return False, None

        return True, pre_commitment_supervision_period

    raise ValueError(
        "Should not reach this point. This function is not properly classifying "
        "admissions as commitments from supervision."
    )


def us_id_filter_sps_for_commitment_from_supervision_identification(
    supervision_periods: List[StateSupervisionPeriod],
) -> List[StateSupervisionPeriod]:
    """Filters the list of supervision periods to only include ones with a set
    supervision_period_supervision_type."""
    # Drop any supervision periods that don't have a set
    # supervision_period_supervision_type (this could signify a bench warrant,
    # for example).
    return [
        period
        for period in supervision_periods
        if period.supervision_period_supervision_type is not None
        and period.supervision_period_supervision_type
        != StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
    ]


def _us_id_admission_is_commitment_from_supervision(
    incarceration_period: StateIncarcerationPeriod,
    preceding_incarceration_period: Optional[StateIncarcerationPeriod],
) -> bool:
    """Determines whether the admission to incarceration for the given purpose
    represents a commitment from supervision."""
    purpose_for_incarceration = (
        incarceration_period.specialized_purpose_for_incarceration
    )

    if (
        incarceration_period.admission_reason
        == StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
    ):
        return purpose_for_incarceration in (
            PurposeForIncarceration.TREATMENT_IN_PRISON,
            PurposeForIncarceration.GENERAL,
        )

    if (
        incarceration_period.admission_reason
        == StateIncarcerationPeriodAdmissionReason.TRANSFER
        and purpose_for_incarceration
        in (
            PurposeForIncarceration.TREATMENT_IN_PRISON,
            PurposeForIncarceration.GENERAL,
        )
    ):
        # Transfers from parole board holds to general incarceration or treatment in
        # prison are commitments from supervision
        if not preceding_incarceration_period:
            return False

        if not incarceration_period.admission_date:
            raise ValueError(
                f"Admission date null for {incarceration_period}. Should be set in "
                f"the prepare_incarceration_periods_for_calculations process."
            )

        if not preceding_incarceration_period.release_date:
            raise ValueError(
                f"Open incarceration period preceding another period. Incarceration period pre-processing "
                f"is not effectively setting missing dates. "
                f"Open incarceration_period_id = {preceding_incarceration_period.incarceration_period_id}"
                f"Next incarceration_period_id = {incarceration_period.incarceration_period_id}"
            )

        if (
            preceding_incarceration_period.release_date
            == incarceration_period.admission_date
        ):
            # Transfers from parole board holds to general incarceration or
            # treatment in prison are commitments from supervision
            if (
                preceding_incarceration_period.release_reason
                == StateIncarcerationPeriodReleaseReason.TRANSFER
                and preceding_incarceration_period.specialized_purpose_for_incarceration
                == PurposeForIncarceration.PAROLE_BOARD_HOLD
            ):
                return True

    return False
