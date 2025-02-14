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
"""Contains US_MI implementation of the StateSpecificIncarcerationNormalizationDelegate."""
from typing import List, Optional

from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodCustodyLevel,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import CriticalRangesBuilder
from recidiviz.persistence.entity.normalized_entities_utils import (
    update_entity_with_globally_unique_id,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.pipelines.utils.incarceration_period_utils import (
    infer_incarceration_periods_from_in_custody_sps,
    legacy_standardize_purpose_for_incarceration_values,
)


class UsMiIncarcerationNormalizationDelegate(
    StateSpecificIncarcerationNormalizationDelegate
):
    """US_MI implementation of the StateSpecificIncarcerationNormalizationDelegate."""

    _REASON_RAW_TEXT_TO_INCARCERATION_ADMISSION_VIOLATION_TYPE_MAP = {
        # Movement reasons that indicate technical revocation
        # 15 - New Commitment - Probation Technical Violator (Rec Ctr Only)
        "15": StateSupervisionViolationType.TECHNICAL,
        # 17 - Returned as Parole Technical Rule Violator
        "17": StateSupervisionViolationType.TECHNICAL,
        # Movement reasons that indicate new sentence revocation
        # 12 - New Commitment - Parole Viol. w/ New Sentence (Rec Ctr Only)
        "12": StateSupervisionViolationType.LAW,
        # 14 - New Commitment - Probationer w/ New Sentence (Rec Ctr. Only)
        "14": StateSupervisionViolationType.LAW,
    }

    _MAX_INFERRENCE_GAP_LENGTH = 30

    def incarceration_admission_reason_override(
        self, incarceration_period: StateIncarcerationPeriod
    ) -> Optional[StateIncarcerationPeriodAdmissionReason]:
        if (
            incarceration_period.admission_reason
            == StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
        ):
            return StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION
        return incarceration_period.admission_reason

    def infer_additional_periods(
        self,
        person_id: int,
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: NormalizedSupervisionPeriodIndex,
    ) -> List[StateIncarcerationPeriod]:
        """
        In MI, we'll infer additional incarceration periods for a variety of scenarios.
        """

        # Scenario 1: infer an incarceration period in gaps between IPs that end with release reason TEMPORARY RELEASE
        #             and IPs that begin with admission reason RETURN_FROM_TEMPORARY_RELEASE
        new_incarceration_periods = (
            self._us_mi_infer_additional_temporary_release_periods(
                person_id=person_id,
                incarceration_periods=incarceration_periods,
            )
        )

        # Scenario 2: infer an incarceration period for any period of time where
        #             supervision level = IN_CUSTODY and there's not already an incarceration period during that time
        new_incarceration_periods = infer_incarceration_periods_from_in_custody_sps(
            person_id=person_id,
            state_code=StateCode.US_MI,
            incarceration_periods=new_incarceration_periods,
            supervision_period_index=supervision_period_index,
            temp_custody_custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
        )

        # Scenario 3: infer an incarceration period in gaps between SPs and IPs that start with REVOCATION or SANCTION_ADMISSION
        #             if the IP starts within 30 days of the SP.  If there are any existing IPs between the SP and the IP that
        #             starts with REVOCATION or SANCTION_ADMISSION, set the admission reason raw text of the existing IP in the gap
        #             to be the admission reason raw text of the IP that starts with REVOCATION or SANCTION ADMISSION
        new_incarceration_periods = (
            self._us_mi_infer_additional_periods_before_revocation_sanction_admission(
                person_id=person_id,
                incarceration_periods=new_incarceration_periods,
                supervision_period_index=supervision_period_index,
            )
        )

        # Scenario 4: infer an incarceration period in gaps between SPs that end with REVOCATION or ADMITTED_TO_INCARCERATION and subsequent  IPs
        #             if the next IP starts within 30 days of the SP.  If there is already an existing IP that follows directly after the SP that
        #             ends with REVOCATION or ADMITTED_TO_INCARCERATION, set the admission reason raw text of the existing IP to be the
        #             termination reason raw text of the SP that ends with REVOCATION or ADMITTED_TO_INCARCERATION
        new_incarceration_periods = self._us_mi_infer_additional_periods_after_revocation_admitted_to_incarceration(
            person_id=person_id,
            incarceration_periods=new_incarceration_periods,
            supervision_period_index=supervision_period_index,
        )

        return new_incarceration_periods

    def get_incarceration_admission_violation_type(
        self,
        incarceration_period: StateIncarcerationPeriod,
    ) -> Optional[StateSupervisionViolationType]:
        """MI specific implementation of get_incarceration_admission_violation_type
        that returns StateSupervisionViolationType.TECHNICAL or StateSupervisionViolationType.LAW
        depending on admission reason raw text. If admission reason raw text does not indicate
        this is a revocation admission, we return None
        """

        admission_reason_raw_text = incarceration_period.admission_reason_raw_text

        if admission_reason_raw_text:
            return (
                self._REASON_RAW_TEXT_TO_INCARCERATION_ADMISSION_VIOLATION_TYPE_MAP.get(
                    admission_reason_raw_text
                )
            )

        return None

    def standardize_purpose_for_incarceration_values(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateIncarcerationPeriod]:
        """Standardizing PFI using the legacy _standardize_purpose_for_incarceration_values function
        for US_MI since this was previously the default normalization behavior
        and there hasn't been a use case for skipping this inferrence yet"""

        return legacy_standardize_purpose_for_incarceration_values(
            incarceration_periods
        )

    @staticmethod
    def _us_mi_infer_additional_temporary_release_periods(
        person_id: int,
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateIncarcerationPeriod]:
        """
        Infer an additional IP for gaps between IPs that end because of temporary release
        and subsequent non-adjacent IPs that start because of return from temporary release
        """

        new_incarceration_periods: List[StateIncarcerationPeriod] = []

        critical_range_builder = CriticalRangesBuilder([*incarceration_periods])
        for critical_range in critical_range_builder.get_sorted_critical_ranges():
            ips_overlapping_range = (
                critical_range_builder.get_objects_overlapping_with_critical_range(
                    critical_range, StateIncarcerationPeriod
                )
            )
            if ips_overlapping_range:
                continue

            ips_preceding_range = (
                critical_range_builder.get_objects_directly_preceding_range(
                    critical_range, StateIncarcerationPeriod
                )
            )

            ips_ending_in_temp_release = [
                ip
                for ip in ips_preceding_range
                if ip.release_reason
                == StateIncarcerationPeriodReleaseReason.TEMPORARY_RELEASE
            ]

            if not ips_ending_in_temp_release:
                continue

            ips_following_range = (
                critical_range_builder.get_objects_directly_following_range(
                    critical_range, StateIncarcerationPeriod
                )
            )

            if ips_following_range is None or not any(
                ip.admission_reason
                == StateIncarcerationPeriodAdmissionReason.RETURN_FROM_TEMPORARY_RELEASE
                for ip in ips_following_range
            ):
                # I'd rather continue rather than raise an error here because I could see scenarios
                # where the raw movements data could be like "temporary release" and then "held in custody" or something,
                # and in that case we wouldn't have a RETURN_FROM_TEMPORARY_RELEASE admission reason in the next IP,
                # and in those cases, we'll just not infer an additional period in the gap
                continue

            previous_ip = ips_ending_in_temp_release[0]
            next_ip = ips_following_range[0]

            # Create a new incarceration period for this TEMPORARY RELEASE period in the
            # gap.
            new_incarceration_period = StateIncarcerationPeriod(
                state_code=StateCode.US_MI.value,
                external_id=f"{previous_ip.external_id}-TEMPORARY_RELEASE",
                admission_date=previous_ip.release_date,
                release_date=next_ip.admission_date,
                admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_RELEASE,
                release_reason=StateIncarcerationPeriodReleaseReason.RETURN_FROM_TEMPORARY_RELEASE,
                county_code=previous_ip.county_code,
                custodial_authority=StateCustodialAuthority.STATE_PRISON,
                custodial_authority_raw_text=previous_ip.custodial_authority_raw_text,
                specialized_purpose_for_incarceration=previous_ip.specialized_purpose_for_incarceration,
                specialized_purpose_for_incarceration_raw_text=previous_ip.specialized_purpose_for_incarceration_raw_text,
                custody_level=previous_ip.custody_level,
                custody_level_raw_text=previous_ip.custody_level_raw_text,
                incarceration_type=previous_ip.incarceration_type,
                incarceration_type_raw_text=previous_ip.incarceration_type_raw_text,
            )

            # Add a unique id to the new IP
            update_entity_with_globally_unique_id(
                root_entity_id=person_id, entity=new_incarceration_period
            )

            new_incarceration_periods.append(new_incarceration_period)

        return incarceration_periods + new_incarceration_periods

    def _us_mi_infer_additional_periods_before_revocation_sanction_admission(
        self,
        person_id: int,
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: NormalizedSupervisionPeriodIndex,
    ) -> List[StateIncarcerationPeriod]:
        """
        Infer an incarceration period in gaps between SPs and IPs that start with REVOCATION or SANCTION_ADMISSION
        if the IP starts within 30 days of the last SP.  If there are any existing IPs between the SP and the IP that
        starts with REVOCATION or SANCTION_ADMISSION, set the admission reason raw text of the existing IP in the gap
        to be the admission reason raw text of the IP that starts with REVOCATION or SANCTION ADMISSION.  We do this
        inference in order to capture transitions from supervision compartments to incarceration compartments (aka
        incarceration starts) correctly.  In addition, we also pull in the admission reason from the original REVOCATION or SANCTION
        ADMISSION IP onto any IPs in this gap so that we can infer incarceration admission violation type appropriately
        (since we can't reliably use violation history in MI to determine incarceration reason).
        """

        # for this inference, the relevant supervision periods are those with a type that would ladder up to a SUPERVISION compartment
        relevant_supervision_periods = [
            period
            for period in supervision_period_index.sorted_supervision_periods
            if period.supervision_type
            in (
                StateSupervisionPeriodSupervisionType.PAROLE,
                StateSupervisionPeriodSupervisionType.PROBATION,
                StateSupervisionPeriodSupervisionType.DUAL,
                StateSupervisionPeriodSupervisionType.BENCH_WARRANT,
                StateSupervisionPeriodSupervisionType.ABSCONSION,
                StateSupervisionPeriodSupervisionType.WARRANT_STATUS,
            )
        ]

        # build a set of critical ranges based on all incarceration periods and the relevant supervision periods
        critical_range_builder = CriticalRangesBuilder(
            [
                *incarceration_periods,
                *relevant_supervision_periods,
            ]
        )
        critical_ranges = critical_range_builder.get_sorted_critical_ranges()

        inferred_incarceration_periods = []
        most_recent_supervision_period_range_index = None

        # for each critical range
        for i, critical_range in enumerate(critical_ranges):

            # identify any overlapping sps
            overlapping_sps = (
                critical_range_builder.get_objects_overlapping_with_critical_range(
                    critical_range, NormalizedStateSupervisionPeriod
                )
            )

            # if this current critical range overlaps with a supervision period, set most_recent_supervision_period_range_index to current index
            if overlapping_sps:
                most_recent_supervision_period_range_index = i
                # Now that there is already a supervision period overlapping with this critical range, we don't need to fill it in with an IP so let's continue
                continue

            # if we get here and a most_recent_supervision_period_range_index hasn't been set yet, there's nothing to infer so let's continue
            if most_recent_supervision_period_range_index is None:
                continue

            # get overlapping ips with this curent critical range
            overlapping_ips = (
                critical_range_builder.get_objects_overlapping_with_critical_range(
                    critical_range, StateIncarcerationPeriod
                )
            )

            # set most recent sp range to the critical range associated with the most_recent_supervision_period_range_index
            most_recent_sp_range = critical_ranges[
                most_recent_supervision_period_range_index
            ]

            # only proceed in loop if the last sp ended and was within the last 30 days of the current critical range

            if most_recent_sp_range.upper_bound_exclusive_date is None:
                continue

            days_since_last_sp = (
                critical_range.lower_bound_inclusive_date
                - most_recent_sp_range.upper_bound_exclusive_date
            ).days

            if days_since_last_sp > self._MAX_INFERRENCE_GAP_LENGTH:
                continue

            revocation_admission_ip = None
            inferred_period_index = 0

            # identify any IPs that start with revocation or sanction admission by looping through each ip that overlaps with this critical range
            for ip in overlapping_ips:
                if ip.start_date_inclusive != critical_range.lower_bound_inclusive_date:
                    # This period didn't start on this critical range and should already
                    # have been handled.
                    continue

                # if this overlapping IP starts with revocation or sanction admission, set revocation_admission_ip
                if ip.admission_reason in (
                    StateIncarcerationPeriodAdmissionReason.REVOCATION,
                    StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                ):
                    revocation_admission_ip = ip
                    break

            # if we didn't identify any IPs that start with revocation or sanction admission, there's nothing to infer yet so let's continue on
            if not revocation_admission_ip:
                continue

            # If we get to this point, that means we've identified a most recent SP and a subsequent IP that starts with revocation or sanction admission.
            # Thus, look at all the time periods between this SP and this revocation IP
            for between_sp_and_revocation_range_index in range(
                most_recent_supervision_period_range_index + 1, i
            ):
                # identify any existing IPs whose admission reason raw text we may need to modify
                potential_ips_to_modify = (
                    critical_range_builder.get_objects_overlapping_with_critical_range(
                        critical_ranges[between_sp_and_revocation_range_index],
                        StateIncarcerationPeriod,
                    )
                )

                # Set admission_reason_raw_text_to_infer as the admission reason raw text of the
                # revocation IP if it is one of the raw text values found in _REASON_RAW_TEXT_TO_INCARCERATION_ADMISSION_VIOLATION_TYPE_MAP
                admission_reason_raw_text_to_infer = (
                    revocation_admission_ip.admission_reason_raw_text
                    if revocation_admission_ip.admission_reason_raw_text
                    in self._REASON_RAW_TEXT_TO_INCARCERATION_ADMISSION_VIOLATION_TYPE_MAP
                    else None
                )

                # if admission reason raw text needs to be reset and there are existing ips to modify:
                if potential_ips_to_modify:
                    if admission_reason_raw_text_to_infer is not None:
                        for ip_between_sp_and_revocation in potential_ips_to_modify:
                            ip_between_sp_and_revocation.admission_reason_raw_text = (
                                admission_reason_raw_text_to_infer
                            )
                    continue

                # else, that means there are no existing ips in this critical range and we should infer one to fill in the gap
                new_ip = StateIncarcerationPeriod(
                    state_code=StateCode.US_MI.value,
                    external_id=f"{revocation_admission_ip.external_id}-{inferred_period_index}-INFERRED(REVOCATION_IP)",
                    admission_date=(
                        critical_ranges[between_sp_and_revocation_range_index]
                    ).lower_bound_inclusive_date,
                    release_date=(
                        critical_ranges[between_sp_and_revocation_range_index]
                    ).upper_bound_exclusive_date,
                    admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
                    admission_reason_raw_text=admission_reason_raw_text_to_infer,
                    release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
                    custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
                    custody_level=StateIncarcerationPeriodCustodyLevel.INTERNAL_UNKNOWN,
                    incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
                )

                # Add a unique id to the new IP
                update_entity_with_globally_unique_id(
                    root_entity_id=person_id, entity=new_ip
                )

                # increment inferred_period_index by 1
                inferred_period_index += 1

                inferred_incarceration_periods.append(new_ip)

        return incarceration_periods + inferred_incarceration_periods

    def _us_mi_infer_additional_periods_after_revocation_admitted_to_incarceration(
        self,
        person_id: int,
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: NormalizedSupervisionPeriodIndex,
    ) -> List[StateIncarcerationPeriod]:
        """
        Infer an incarceration period in gaps between SPs that end with REVOCATION or ADMITTED_TO_INCARCERATION and subsequent IPs
        if the subsequent IP starts within 30 days of the last SP.  If there already exists an IP that directly follows the SP
        ending with REVOCATION or ADMITTED_TO_INCARCERATION, set the admission reason raw text of the existing IP
        to be the termination reason raw text of the SP that ends with REVOCATION or ADMITTED_TO_INCARCERATION.  We do this
        inference in order to capture transitions from supervision compartments to incarceration compartments (aka
        incarceration starts) correctly.  In addition, we also pull in the admission reason from the REVOCATION or ADMITTED_TO_INCARCERATION
        SP onto any IPs directly following it so that we can infer incarceration admission violation type appropriately
        (since we can't reliably use violation history in MI to determine incarceration reason).
        """

        # for this inference, the relevant supervision periods are those with a type that would ladder up to a SUPERVISION compartment in sessions
        # and have a termination reason of REVOCATION or ADMITTED TO INCARCERATION
        relevant_supervision_periods = [
            period
            for period in supervision_period_index.sorted_supervision_periods
            if period.supervision_type
            in (
                StateSupervisionPeriodSupervisionType.PAROLE,
                StateSupervisionPeriodSupervisionType.PROBATION,
                StateSupervisionPeriodSupervisionType.DUAL,
                StateSupervisionPeriodSupervisionType.BENCH_WARRANT,
                StateSupervisionPeriodSupervisionType.ABSCONSION,
                StateSupervisionPeriodSupervisionType.WARRANT_STATUS,
            )
            and period.termination_reason
            in (
                StateSupervisionPeriodTerminationReason.REVOCATION,
                StateSupervisionPeriodTerminationReason.ADMITTED_TO_INCARCERATION,
            )
        ]

        # build a set of critical ranges based on all incarceration periods and the relevant supervision periods
        critical_range_builder = CriticalRangesBuilder(
            [
                *incarceration_periods,
                *relevant_supervision_periods,
            ]
        )
        critical_ranges = critical_range_builder.get_sorted_critical_ranges()

        inferred_incarceration_periods = []
        revocation_sp = None

        # for each critical range
        for critical_range in critical_ranges:

            # Identify SPs that ended on the date this range started
            sps_directly_preceding_range = (
                critical_range_builder.get_objects_directly_preceding_range(
                    critical_range, NormalizedStateSupervisionPeriod
                )
            )
            if not sps_directly_preceding_range:
                continue

            revocation_sp = sps_directly_preceding_range[0]

            # set admission_reason_raw_text_to_infer based on the termination reason raw text of the revocation sp
            admission_reason_raw_text_to_infer = (
                revocation_sp.termination_reason_raw_text
                if revocation_sp.termination_reason_raw_text
                in self._REASON_RAW_TEXT_TO_INCARCERATION_ADMISSION_VIOLATION_TYPE_MAP
                else None
            )

            ips_overlapping_range = (
                critical_range_builder.get_objects_overlapping_with_critical_range(
                    critical_range, StateIncarcerationPeriod
                )
            )

            # if there is an existing ip in this critical range, set the admission_reason_raw_text of any existing ips that start with this critical range
            # to the admission_reason_raw_text_to_infer and then continue with the loop
            if ips_overlapping_range:
                if admission_reason_raw_text_to_infer is not None:
                    for ip in ips_overlapping_range:
                        if ip.admission_date == revocation_sp.termination_date:
                            ip.admission_reason_raw_text = (
                                admission_reason_raw_text_to_infer
                            )
                continue

            # if there isn't an existing ip in this critical range, but this current critical range (which is a gap in periods) is open or over 30 days long, then we won't infer anything either
            # so in that case continue with the loop

            if critical_range.upper_bound_exclusive_date is None:
                continue

            length_of_critical_range = (
                critical_range.upper_bound_exclusive_date
                - critical_range.lower_bound_inclusive_date
            ).days

            if length_of_critical_range > self._MAX_INFERRENCE_GAP_LENGTH:
                continue

            ips_directly_following_range = (
                critical_range_builder.get_objects_directly_following_range(
                    critical_range, StateIncarcerationPeriod
                )
            )

            if not ips_directly_following_range:
                continue

            new_ip = StateIncarcerationPeriod(
                state_code=StateCode.US_MI.value,
                external_id=f"{revocation_sp.external_id}-INFERRED(REVOCATION_SP)",
                admission_date=critical_range.lower_bound_inclusive_date,
                release_date=critical_range.upper_bound_exclusive_date,
                admission_reason=StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY,
                admission_reason_raw_text=admission_reason_raw_text_to_infer,
                release_reason=StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY,
                custodial_authority=StateCustodialAuthority.INTERNAL_UNKNOWN,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
                custody_level=StateIncarcerationPeriodCustodyLevel.INTERNAL_UNKNOWN,
                incarceration_type=StateIncarcerationType.INTERNAL_UNKNOWN,
            )

            # Add a unique id to the new IP
            update_entity_with_globally_unique_id(
                root_entity_id=person_id, entity=new_ip
            )

            inferred_incarceration_periods.append(new_ip)

        # return sorted incarceration periods
        return incarceration_periods + inferred_incarceration_periods
