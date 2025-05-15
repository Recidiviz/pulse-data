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
"""Contains US_TN implementation of the StateSpecificIncarcerationNormalizationDelegate."""
from datetime import timedelta
from typing import List, Optional

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import DateRange
from recidiviz.persistence.entity.entity_utils import deep_entity_update
from recidiviz.persistence.entity.normalized_entities_utils import (
    update_entity_with_globally_unique_id,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.pipelines.utils.incarceration_period_utils import (
    infer_incarceration_periods_from_in_custody_sps,
    legacy_standardize_purpose_for_incarceration_values,
    standard_date_sort_for_incarceration_periods,
)


class UsTnIncarcerationNormalizationDelegate(
    StateSpecificIncarcerationNormalizationDelegate
):
    """US_TN implementation of the StateSpecificIncarcerationNormalizationDelegate."""

    def normalize_period_if_commitment_from_supervision(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        original_sorted_incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: NormalizedSupervisionPeriodIndex,
    ) -> StateIncarcerationPeriod:
        return _us_tn_normalize_period_if_commitment_from_supervision(
            incarceration_period_list_index=incarceration_period_list_index,
            sorted_incarceration_periods=sorted_incarceration_periods,
            supervision_period_index=supervision_period_index,
        )

    def period_is_parole_board_hold(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
    ) -> bool:
        """There are no parole board hold incarceration periods in US_TN."""
        # TODO(#10294): It's unclear whether there are IPs in TN that represent time
        #  spent in a parole board hold. We need to get more information from US_TN,
        #  and then update this logic accordingly to classify the parole board periods
        #  if they do exist.
        return False

    def get_incarceration_admission_violation_type(
        self,
        incarceration_period: StateIncarcerationPeriod,
    ) -> Optional[StateSupervisionViolationType]:
        """TN specific implementation of get_incarceration_admission_violation_type
        that returns StateSupervisionViolationType.TECHNICAL or StateSupervisionViolationType.LAW
        depending on admission reason raw text. If admission reason raw text does not indicate
        this is a VIOLT or VIOLW admission, we return None
        """

        if incarceration_period.admission_reason_raw_text is None:
            return None

        # Movement reasons that indicate technical revocation in TN use
        # MovementReason = VIOLT which is defined as VIOLATION WARRANT-TECHNICAL

        if "VIOLT" in incarceration_period.admission_reason_raw_text:
            return StateSupervisionViolationType.TECHNICAL

        # Movement reasons that indicate warrant issued  in TN use
        # MovementReason = VIOLW which is defined as Warrant violation (new charge)

        if "VIOLW" in incarceration_period.admission_reason_raw_text:
            return StateSupervisionViolationType.LAW

        return None

    def infer_additional_periods(
        self,
        person_id: int,
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: NormalizedSupervisionPeriodIndex,
    ) -> List[StateIncarcerationPeriod]:
        return _us_tn_infer_additional_periods(
            person_id=person_id,
            incarceration_periods=incarceration_periods,
            supervision_period_index=supervision_period_index,
        )

    def standardize_purpose_for_incarceration_values(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateIncarcerationPeriod]:
        """Standardizing PFI using the legacy standardize_purpose_for_incarceration_values function
        for US_TN since this was previously the default normalization behavior
        and there hasn't been a use case for skipping this inferrence yet"""

        # The periods are normalized using legacy_standardize_purpose_for_incarceration_values
        # before any other logic gets called.
        legacy_normalized_periods = legacy_standardize_purpose_for_incarceration_values(
            incarceration_periods
        )

        updated_ips: List[StateIncarcerationPeriod] = []

        # This loop identifies safekeeping periods and sets the PFI for these periods to SAFEKEEPING,
        # leaving all other periods unchanged. It works as follows:

        # - If a period has SAREC (received for safekeeping) in the admission_reason_raw_text, then it's considered a safekeeping period.
        # - If a period doesn't have SAREC in the admission reason, but comes after a period that did, AND there's no period in between with
        #   SARET (returned from safekeeping) in the admission reason (including the period being looked at itself), then it's still considered a safekeeping period.
        # - Once the loop reaches a period that has SARET in the admission reason, the person is no longer considered to be in a safekeeping period, and won't be until
        #   the loop reaches another period with SARET in the admission reason.

        # Note that this loop only starts after legacy_standardize_purpose_for_incarceration_values has already
        # run. This means that if there's a safekeeping period in the middle of a set of periods that are all
        # transfers that would have their PFI overrided by legacy_standardize_purpose_for_incarceration_values,
        # then all the periods would be subject to legacy_standardize_purpose_for_incarceration_values, and
        # the safekeeping periods in the middle would then have their PFI reset again by this loop. The transfer
        # periods on either side of the safekeeping periods would keep whatever PFI was set by legacy_standardize_purpose_for_incarceration_values.
        in_safekeeping_period = False

        # If a period has one of these admission reasons, or has the same admission date as another period
        # with one of them, then any ongoing safekeeping period will close.
        SAFEKEEPING_PERIOD_END_ADMISSION_REASONS = [
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            StateIncarcerationPeriodAdmissionReason.REVOCATION,
        ]
        for ip in standard_date_sort_for_incarceration_periods(
            legacy_normalized_periods
        ):
            # This variable will be True if the period's admission reason OR the admission reason
            # of any other period starting on the same day is in SAFEKEEPING_PERIOD_END_ADMISSION_REASONS.
            safekeeping_end_same_day = any(
                i.admission_reason in SAFEKEEPING_PERIOD_END_ADMISSION_REASONS
                for i in legacy_normalized_periods
                if i.admission_date == ip.admission_date
            )
            in_safekeeping_period = (
                ip.admission_reason_raw_text is not None
                # and "-SARET" not in ip.admission_reason_raw_text
                and not safekeeping_end_same_day
                and ("-SAREC" in ip.admission_reason_raw_text or in_safekeeping_period)
            )
            if in_safekeeping_period:
                pfi = StateSpecializedPurposeForIncarceration.SAFEKEEPING

            else:
                in_safekeeping_period = False
                if ip.specialized_purpose_for_incarceration is not None:
                    pfi = ip.specialized_purpose_for_incarceration
                else:
                    pfi = None
            updated_ip = deep_entity_update(
                ip,
                specialized_purpose_for_incarceration=pfi,
            )
            updated_ips.append(updated_ip)

        return updated_ips


RELEASED_FROM_TEMPORARY_CUSTODY_RAW_TEXT_VALUES: List[str] = [
    # Revocations are logged after temporary custody due to a violation.
    "PAFA-PAVOK-P",  # Parole Revoked
    "PAFA-REVOK",  # Revocation
    "PRFA-PRVOK-P",  # Probation revoked
    "PRFA-PTVOK-P",  # Partial revocation
    "CCFA-REVOK-P",  # Revocation
    "CCFA-PTVOK-P",  # Partial revocation
    "PAFA-RECIS-P",  # Rescission
    "DVCT-PRVOK-P",  # Revocation
]


def _us_tn_normalize_period_if_commitment_from_supervision(
    incarceration_period_list_index: int,
    sorted_incarceration_periods: List[StateIncarcerationPeriod],
    supervision_period_index: NormalizedSupervisionPeriodIndex,
) -> StateIncarcerationPeriod:
    """Returns an updated version of the specified incarceration period if it is a
    commitment from supervision admission.

    For US_TN, commitments from supervision occur in the following circumstances:

    If the period represents an admission from XXX supervision, changes the NEW_ADMISSION admission_reason
    to be TEMPORARY CUSTODY.
    """
    incarceration_period = sorted_incarceration_periods[incarceration_period_list_index]

    if (
        incarceration_period.admission_reason
        == StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION
    ):
        if not incarceration_period.admission_date:
            raise ValueError(
                "Unexpected missing admission_date on incarceration period: "
                f"[{incarceration_period}]"
            )

        # Find most relevant pre- (or overlapping) commitment supervision period
        pre_commitment_supervision_period = (
            supervision_period_index.get_supervision_period_overlapping_with_date_range(
                date_range=DateRange(
                    incarceration_period.admission_date - timedelta(days=1),
                    incarceration_period.admission_date,
                )
            )
        )

        # Confirm that there is an overlapping or abutting supervision period
        if pre_commitment_supervision_period:
            # There is a supervision period that abuts or overlaps with this NEW_ADMISSION incarceration period
            # so this is actually a TEMPORARY CUSTODY period, not a NEW ADMISSION.
            incarceration_period.admission_reason = (
                StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY
            )
            incarceration_period.specialized_purpose_for_incarceration = (
                StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY
            )

            if (
                incarceration_period.release_reason
                == StateIncarcerationPeriodReleaseReason.RELEASED_TO_SUPERVISION
                or incarceration_period.release_reason_raw_text
                in RELEASED_FROM_TEMPORARY_CUSTODY_RAW_TEXT_VALUES
            ):
                incarceration_period.release_reason = (
                    StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY
                )

    return incarceration_period


def _us_tn_infer_additional_periods(
    person_id: int,
    incarceration_periods: List[StateIncarcerationPeriod],
    supervision_period_index: NormalizedSupervisionPeriodIndex,
) -> List[StateIncarcerationPeriod]:
    """
    If we have a supervision period in TN with the supervision_level of IN_CUSTODY, we want to infer an
    incarceration_period for that time in order to begin sessions at the correct incarceration start.

    We then override the custodial authority for certain periods (if the custodial authority changes
    in a period that has "-T" in the admission reason, and the "T" period comes after a period with "-P"
    in the admission reason, then the "T" period will be updated to use the custodial authority from the "P" period).

    Finally, infer periods to fill the gaps between certain temporary transfer periods. Periods
    will only be inferred between 2 periods if the 2 periods have a gap (i.e. release date for the first period
    is before the admission date for the second), if the 2 periods have the same custodial authority,
    the 1st period has a "-T" in the release reason and the 2nd has a "-T" in the admission reason,
    and the release reason for the 1st period and the admission reason for the 2nd period both
    include paired movement codes indicating a transfer to another jurisdiction or a work release.
    """

    # Infer a temporary custody incarceration period if supervision level is IN_CUSTODY
    incarceration_periods_with_inferred_temp_custody_periods = (
        infer_incarceration_periods_from_in_custody_sps(
            person_id=person_id,
            state_code=StateCode.US_TN,
            incarceration_periods=incarceration_periods,
            supervision_period_index=supervision_period_index,
            temp_custody_custodial_authority=StateCustodialAuthority.COUNTY,
        )
    )

    # Override custodial authority for "T" periods
    incarceration_periods_with_custodial_authority_overrides = (
        _us_tn_override_custodial_authority_for_temporary_movements(
            incarceration_periods_with_inferred_temp_custody_periods
        )
    )

    # Infer periods between certain "T" transfer periods
    all_incarceration_periods = _us_tn_infer_periods_between_temporary_transfers(
        person_id, incarceration_periods_with_custodial_authority_overrides
    )

    return all_incarceration_periods


def _us_tn_override_custodial_authority_for_temporary_movements(
    incarceration_periods: List[StateIncarcerationPeriod],
) -> List[StateIncarcerationPeriod]:
    """
    Override the custodial authority for certain periods (if the custodial authority changes
    in a period that has "-T" in the admission reason, and the "T" period comes after a period with "-P"
    in the admission reason, then the "T" period will be updated to use the custodial authority from the "P" period).
    """

    sorted_incarceration_periods = standard_date_sort_for_incarceration_periods(
        incarceration_periods
    )
    # Get all the periods that don't have a "-T" flag in their admission reason.
    non_temporary_movement_periods = [
        ip
        for ip in sorted_incarceration_periods
        if ip.admission_reason_raw_text
        and not ip.admission_reason_raw_text.endswith("-T")
        and ip.admission_date
        and ip.release_date
    ]
    updated_incarceration_periods: List[StateIncarcerationPeriod] = []

    if len(non_temporary_movement_periods) > 0:
        for ip in sorted_incarceration_periods:
            # Store the period's original custodial authority to use as a default value
            new_custodial_authority = ip.custodial_authority
            if (
                ip.admission_reason_raw_text
                and ip.admission_date
                and ip.admission_reason_raw_text.endswith("-T")
            ):
                # If the period has a "-T" flag, retrieve the periods from non_temporary_movement_periods
                # that start before the "T" period.
                prior_non_temporary_movement_periods = [
                    past_ip
                    for past_ip in non_temporary_movement_periods
                    if past_ip.custodial_authority
                    and past_ip.admission_reason_raw_text
                    and past_ip.admission_date
                    and past_ip.admission_date < ip.admission_date
                ]

                # prior_non_temporary_movement_periods is sorted with a custom function that
                # orders the periods by admission date, then by period length (descending).
                # This keeps the selection of the most recent period deterministic. Periods
                # should only share an admission date with one another if one of them is
                # a zero-day period, so this sorting ensures that if we're ever picking
                # between two periods with the same admission date, we use the non-zero-day one.
                # Note that this means that zero-day periods CAN be used to get the custodial
                # authority override, if they have a more recent admission date than any of
                # the non-zero-day "P" periods.
                prior_non_temporary_movement_periods.sort(
                    key=lambda x: (
                        x.admission_date,
                        x.release_date is None,
                        x.release_date,
                    ),
                    reverse=True,
                )
                # Get the subset of prior_non_temporary_movement_periods that
                # have "-P" flags.
                prior_permanent_movement_periods = [
                    pmp
                    for pmp in prior_non_temporary_movement_periods
                    if pmp.admission_reason_raw_text
                    and pmp.admission_reason_raw_text.endswith("-P")
                ]
                if len(prior_permanent_movement_periods) > 0:
                    # If there are periods with "P" flags before the "T" period, check
                    # to see if the most recent "P" or unflagged period has a different
                    # custodial authority than the "T" period.
                    if (
                        ip.custodial_authority
                        != prior_non_temporary_movement_periods[0].custodial_authority
                    ):
                        # If the custodial authority has changed since the most recent "P"
                        # or unflagged period, then get the custodial authority from the most
                        # recent "P" period to use as an override for the "T" period's custodial
                        # authority.
                        new_custodial_authority = prior_permanent_movement_periods[
                            0
                        ].custodial_authority
            # This new period will be the same as the original unless the custodial authority
            # override occurred.
            updated_ip = deep_entity_update(
                ip, custodial_authority=new_custodial_authority
            )
            updated_incarceration_periods.append(updated_ip)
        return updated_incarceration_periods

    # If non_temporary_movement_periods is empty, then this function will just
    # use the input as output.
    return sorted_incarceration_periods


def _us_tn_infer_periods_between_temporary_transfers(
    person_id: int, incarceration_periods: List[StateIncarcerationPeriod]
) -> List[StateIncarcerationPeriod]:
    """
    This function infers periods to fill the gap (if one exists) between certain temporary
    transfer movements. Specifically, if someone is transferred to another jurisdiction
    or sent to work release, then if the next incarceration period shows them returning
    from another jurisdiction/work release and has an admission date greater than (NOT
    equal to) the release date of the prior period, infers a period to fill the gap. This
    period will have very little information, but will have the same custodial authority
    as the surrounding periods so that the person's custody period will no longer be interrupted.
    """
    sorted_incarceration_periods = standard_date_sort_for_incarceration_periods(
        incarceration_periods
    )
    inferred_incarceration_periods: List[StateIncarcerationPeriod] = []
    for index, ip in enumerate(sorted_incarceration_periods):
        # By default, we will not infer a period. can_infer_period will be set to TRUE if
        # certain conditions below are met.
        can_infer_period = False
        # These default values should never show up, because if the conditions for inferring
        # a period are met, these values will always be set. Pylint doesn't recognize this,
        # though, so we specify default values so that there's clearly no chance of these
        # variables being referenced before assignment. If we ever see an ID or facility
        # containing these strings, we'll know that something's gone wrong with the logic here.
        new_period_id_flag = "ERROR"
        new_period_facility = "ERROR"
        if index + 1 != len(sorted_incarceration_periods):
            next_ip = sorted_incarceration_periods[index + 1]
            # For a given period, check if the release reason and the admission reason for
            # the next period both have "T" flags, and if the next period starts after (not on)
            # the date that the period ends, and if the period has the same custodial authority
            # as the next period.
            if (
                ip.release_reason_raw_text
                and next_ip.admission_reason_raw_text
                and ip.release_date
                and next_ip.admission_date
                and next_ip.admission_date > ip.release_date
            ):
                # Would like to include these conditions with the if statement above, but
                # pylint doesn't want there to be >5 conditions in a single if statement
                if (
                    ip.custodial_authority
                    and next_ip.custodial_authority
                    and ip.custodial_authority == next_ip.custodial_authority
                    and ip.release_reason_raw_text.endswith("-T")
                    and next_ip.admission_reason_raw_text.endswith("-T")
                ):
                    # If the period and the next period have paired "OJ" codes in the release/admission
                    # reasons, we can infer a "other jurisdiction" period to fill the gap between them.
                    if ip.release_reason_raw_text.startswith(
                        "FAOJ"
                    ) and next_ip.admission_reason_raw_text.startswith("OJFA"):
                        can_infer_period = True
                        new_period_id_flag = "OJ"
                        new_period_facility = "INFERRED_OTHER_JURISDICTION"
                    # If the period and the next period have paired "WR" codes in the release/admission
                    # reasons, we can infer a "work release" period to fill the gap between them.
                    elif ip.release_reason_raw_text.startswith(
                        "FAWR"
                    ) and next_ip.admission_reason_raw_text.startswith("WRFA"):
                        can_infer_period = True
                        new_period_id_flag = "WR"
                        new_period_facility = "INFERRED_WORK_RELEASE"

                    if can_infer_period:
                        # Infer a period to fill the gap. Because we don't know the details of
                        # the person's incarceration during this gap, we don't hydrate most
                        # fields for the inferred period. When a period is inferred between 2
                        # periods, the external ID will be the same as the first period's ID,
                        # with "-INFERRED-OJ" or "-INFERRED-WR" appended to it. The custodial authority
                        # will be the same as it is for both periods, which necessarily will
                        # have the same custodial authority as one another. Facility is hydrated
                        # with a placeholder string (INFERRED_OTHER_JURISDICTION or INFERRED_WORK_RELEASE),
                        # indicating that we don't know the specifics of where the person was
                        # during the inferred period.
                        new_incarceration_period = StateIncarcerationPeriod(
                            state_code=StateCode.US_TN.value,
                            external_id=f"{ip.external_id}-INFERRED-{new_period_id_flag}",
                            admission_date=ip.release_date,
                            release_date=next_ip.admission_date,
                            custodial_authority=ip.custodial_authority,
                            custodial_authority_raw_text=ip.custodial_authority_raw_text,
                            facility=new_period_facility,
                            admission_reason=StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN,
                            release_reason=StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN,
                        )

                        # Add a unique id to the new IP
                        update_entity_with_globally_unique_id(
                            root_entity_id=person_id, entity=new_incarceration_period
                        )
                        inferred_incarceration_periods.append(new_incarceration_period)

    # Sort and return the original incarceration periods along with any periods that were inferred
    # in the loop above.
    return standard_date_sort_for_incarceration_periods(
        sorted_incarceration_periods + inferred_incarceration_periods
    )
