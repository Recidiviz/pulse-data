# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Utils for state-specific logic related to identifying revocations in US_ID."""
from typing import List, Tuple, Optional

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


def us_id_revoked_supervision_period_if_revocation_occurred(
    incarceration_period: StateIncarcerationPeriod,
    filtered_supervision_periods: List[StateSupervisionPeriod],
    preceding_incarceration_period: Optional[StateIncarcerationPeriod],
) -> Tuple[bool, Optional[StateSupervisionPeriod]]:
    """Determines whether the incarceration_period started because of a revocation of supervision. If a revocation did
    occur, finds the supervision period that was revoked.
    For US_ID, revocations occur in the following circumstances:
        - The person is admitted to GENERAL incarceration from non-investigative supervision (typically a probation
            revocation).
        - The person is admitted to TREATMENT_IN_PRISON incarceration from non-investigative supervision.
        - The person is transferred from a PAROLE_BOARD_HOLD incarceration period to a GENERAL or
            TREATMENT_IN_PRISON incarceration period because they were revoked by the parole board.
    Args:
        - incarceration_period: The StateIncarcerationPeriod being evaluated for an instance of revocation.
        - filtered_supervision_periods: A list of the person's StateSupervisionPeriods that have a set
            supervision_period_supervision_type.
        - preceding_incarceration_period: The incarceration period that occurred before the given incarceration_period,
            if the person has any preceding incarceration periods. Used to determine whether the person was transferred
            from a parole board hold or treatment.
    Returns:
        A tuple in the format [bool, Optional[StateSupervisionPeriod]] representing whether or not a revocation occurred
        and, if a revocation did occur, the supervision period that was revoked if it can be identified. It is possible
        for this function to return True, None.
    """
    admission_date = incarceration_period.admission_date

    if not admission_date:
        raise ValueError(
            f"Admission date null for {incarceration_period}. Should be set in "
            f"the prepare_incarceration_periods_for_calculations process."
        )

    if not us_id_is_revocation_admission(
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
            # If returning from supervision to prison for general incarceration or treatment, it is a revocation.
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
                    "Preceding incarceration period must exist for a transfer admission to be counted as "
                    "a revocation. Revocation admission identification not working."
                )

            if not preceding_incarceration_period.admission_date:
                raise ValueError(
                    f"Admission date null for {incarceration_period}. Should be set in "
                    f"the prepare_incarceration_periods_for_calculations process."
                )

            if (
                preceding_incarceration_period.specialized_purpose_for_incarceration
                == PurposeForIncarceration.PAROLE_BOARD_HOLD
            ):
                # This person was transferred from a parole board hold to incarceration. The date that they
                # entered prison was the date of the preceding incarceration period.
                incarceration_admission_date = (
                    preceding_incarceration_period.admission_date
                )

    if incarceration_admission_date:
        # US_ID does not have overlapping supervision periods, so there there is a maximum of one revoked period.
        revoked_period = find_last_terminated_period_before_date(
            upper_bound_date=incarceration_admission_date,
            periods=filtered_supervision_periods,
            maximum_months_proximity=SUPERVISION_PERIOD_PROXIMITY_MONTH_LIMIT,
        )

        if revoked_period:
            if (
                revoked_period.supervision_period_supervision_type
                == StateSupervisionPeriodSupervisionType.INVESTIGATION
            ):
                # If the most recent supervision period was of type INVESTIGATION, this is not actually a revocation
                return False, None

        return True, revoked_period

    raise ValueError(
        "Should not reach this point. us_id_is_revocation_admission is not properly classifying "
        "revocations."
    )


def us_id_filter_supervision_periods_for_revocation_identification(
    supervision_periods: List[StateSupervisionPeriod],
) -> List[StateSupervisionPeriod]:
    """Filters the list of supervision periods to only include ones with a set supervision_period_supervision_type."""
    # Drop any supervision periods that don't have a set supervision_period_supervision_type (this could signify a
    # bench warrant, for example).
    return [
        period
        for period in supervision_periods
        if period.supervision_period_supervision_type is not None
        and period.supervision_period_supervision_type
        != StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
    ]


def us_id_get_pre_revocation_supervision_type(
    revoked_supervision_period: Optional[StateSupervisionPeriod],
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """Returns the supervision_period_supervision_type associated with the given revoked_supervision_period,
    if present. If not, returns None."""
    if revoked_supervision_period:
        return revoked_supervision_period.supervision_period_supervision_type
    return None


def us_id_is_revocation_admission(
    incarceration_period: StateIncarcerationPeriod,
    preceding_incarceration_period: Optional[StateIncarcerationPeriod],
) -> bool:
    """Determines whether the admission to incarceration for the given purpose indicates this person was revoked."""

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
        # Transfers from parole board holds to general incarceration or treatment in prison are revocation admissions
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
            # treatment in prison are revocation admissions
            if (
                preceding_incarceration_period.release_reason
                == StateIncarcerationPeriodReleaseReason.TRANSFER
                and preceding_incarceration_period.specialized_purpose_for_incarceration
                == PurposeForIncarceration.PAROLE_BOARD_HOLD
            ):
                return True

    return False
