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
"""Contains the default logic for pre-processing StateIncarcerationPeriod entities so
that they are ready to be used in pipeline calculations."""
import logging
from copy import deepcopy
from datetime import date
from typing import Dict, List, Optional, Set, Tuple

import attr

from recidiviz.calculator.pipeline.utils.incarceration_period_utils import (
    ip_is_nested_in_previous_period,
    period_edges_are_valid_transfer,
    periods_are_temporally_adjacent,
    standard_date_sort_for_incarceration_periods,
)
from recidiviz.calculator.pipeline.utils.pre_processed_incarceration_period_index import (
    PreProcessedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    SANCTION_ADMISSION_PURPOSE_FOR_INCARCERATION_VALUES,
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
    is_commitment_from_supervision,
    is_official_admission,
    release_reason_overrides_released_from_temporary_custody,
)
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    is_placeholder,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionViolationResponse,
)

ATTRIBUTES_TRIGGERING_STATUS_CHANGE = [
    "custodial_authority",
    "specialized_purpose_for_incarceration",
]


@attr.s(kw_only=True, frozen=True)
class PreProcessingConfiguration:
    # Whether or not to collapse chronologically adjacent periods that are
    # connected by a transfer release and transfer admission
    collapse_transfers: bool = attr.ib()
    # Whether or not to overwrite facility information when collapsing
    # transfer edges
    overwrite_facility_information_in_transfers: bool = attr.ib()


@attr.s
class PurposeForIncarcerationInfo:
    # Purpose for incarceration
    purpose_for_incarceration: Optional[
        StateSpecializedPurposeForIncarceration
    ] = attr.ib(default=None)

    # A string subtype to capture more information about the
    # purpose_for_incarceration, e.g. the length of stay for a
    # SHOCK_INCARCERATION admission
    purpose_for_incarceration_subtype: Optional[str] = attr.ib(default=None)


class StateSpecificIncarcerationPreProcessingDelegate:
    """Interface for state-specific decisions involved in pre-processing
    incarceration periods for calculations."""

    def normalize_period_if_commitment_from_supervision(  # pylint: disable=unused-argument
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: Optional[PreProcessedSupervisionPeriodIndex],
    ) -> StateIncarcerationPeriod:
        """State-specific implementations of this class should return a new
        StateIncarcerationPeriod with updated attributes if this period represents a
        commitment from supervision admission and requires updated attribute values.

        By default, returns the original period unchanged.
        """
        return sorted_incarceration_periods[incarceration_period_list_index]

    def get_pfi_info_for_period_if_commitment_from_supervision(  # pylint: disable=unused-argument
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        violation_responses: Optional[List[StateSupervisionViolationResponse]],
    ) -> PurposeForIncarcerationInfo:
        """State-specific implementations of this class should return a
        PurposeForIncarcerationInfo object complete with the correct
        purpose_for_incarceration for the incarceration period, if it differs from the
        purpose_for_incarceration value currently set on the period and if the
        period represents a commitment from supervision admission.

        By default, returns a PurposeForIncarcerationInfo object with the period's
        current purpose_for_incarceration value and an unset
        purpose_for_incarceration_subtype.
        """
        return PurposeForIncarcerationInfo(
            purpose_for_incarceration=sorted_incarceration_periods[
                incarceration_period_list_index
            ].specialized_purpose_for_incarceration,
            purpose_for_incarceration_subtype=None,
        )

    def incarceration_types_to_filter(self) -> Set[StateIncarcerationType]:
        """State-specific implementations of this class should return a non-empty set if
        there are certain incarceration types that indicate a period should be dropped
        entirely from calculations.

        By default, returns an empty set.
        """
        return set()

    def handle_erroneously_set_temporary_custody_period(  # pylint: disable=unused-argument
        self,
        incarceration_period: StateIncarcerationPeriod,
        previous_incarceration_period: Optional[StateIncarcerationPeriod],
    ) -> StateIncarcerationPeriod:
        """State-specific implementations of this class should return a new
        StateIncarcerationPeriod with updated attributes if this period was
        erroneously set as a temporary custody period during ingest and requires
        updated attributes.

        This happens when there are limitations in ingest mapping logic. For example,
        logic that requires looking at more than one period to determine the correct
        pfi value.

        By default, returns the original period unchanged.
        """
        return incarceration_period

    def period_is_parole_board_hold(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
    ) -> bool:
        """State-specific implementations of this class should return True if the
        incarceration_period represents a period of time spent in a parole board hold.

        Default behavior is that a period is a parole board hold if it has a
        specialized_purpose_for_incarceration of PAROLE_BOARD_HOLD.
        """
        return (
            sorted_incarceration_periods[
                incarceration_period_list_index
            ].specialized_purpose_for_incarceration
            == StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
        )

    def period_is_non_board_hold_temporary_custody(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
    ) -> bool:
        """State-specific implementations of this class should return True if the
        |incarceration_period| represents a period of time spent in a form of
        temporary custody that is NOT a parole board hold.

        Default behavior returns True if the period is not a board hold, if the
        admission_reason is TEMPORARY_CUSTODY, and if the
        specialized_purpose_for_incarceration is one of: None, GENERAL,
        TEMPORARY_CUSTODY.
        """
        incarceration_period = sorted_incarceration_periods[
            incarceration_period_list_index
        ]
        return (
            not self.period_is_parole_board_hold(
                incarceration_period_list_index, sorted_incarceration_periods
            )
            and incarceration_period.admission_reason
            == StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY
            and incarceration_period.specialized_purpose_for_incarceration
            in (
                None,
                StateSpecializedPurposeForIncarceration.GENERAL,
                StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
            )
        )

    def pre_processing_relies_on_supervision_periods(self) -> bool:
        """State-specific implementations of this class should return whether the IP
        pre-processing logic for the state relies on information in
        StateSupervisionPeriod entities.

        Default is False.
        """
        return False

    def pre_processing_relies_on_violation_responses(self) -> bool:
        """State-specific implementations of this class should return whether the IP
        pre-processing logic for the state relies on information in
        StateSupervisionViolationResponse entities.

        Default is False.
        """
        return False


class IncarcerationPreProcessingManager:
    """Interface for generalized and state-specific pre-processing of
    StateIncarcerationPeriods for use in calculations."""

    def __init__(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
        pre_processing_delegate: StateSpecificIncarcerationPreProcessingDelegate,
        incarceration_delegate: StateSpecificIncarcerationDelegate,
        pre_processed_supervision_period_index: Optional[
            PreProcessedSupervisionPeriodIndex
        ],
        violation_responses: Optional[List[StateSupervisionViolationResponse]],
        field_index: CoreEntityFieldIndex,
        earliest_death_date: Optional[date] = None,
    ):
        self._incarceration_periods = deepcopy(incarceration_periods)
        self.pre_processing_delegate = pre_processing_delegate
        self.incarceration_delegate = incarceration_delegate
        self._pre_processed_incarceration_period_index_for_calculations: Dict[
            PreProcessingConfiguration, PreProcessedIncarcerationPeriodIndex
        ] = {}
        # Only store the PreProcessedSupervisionPeriodIndex if StateSupervisionPeriod
        # entities are required for this state's StateIncarcerationPeriod
        # pre-processing
        self._pre_processed_supervision_period_index: Optional[
            PreProcessedSupervisionPeriodIndex
        ] = (
            pre_processed_supervision_period_index
            if self.pre_processing_delegate.pre_processing_relies_on_supervision_periods()
            else None
        )

        # Only store the violation_responses if StateSupervisionViolationResponse
        # entities are required for this state's StateIncarcerationPeriod
        # pre-processing
        self._violation_responses: Optional[List[StateSupervisionViolationResponse]] = (
            violation_responses
            if self.pre_processing_delegate.pre_processing_relies_on_violation_responses()
            else None
        )

        # The end date of the earliest incarceration or supervision period ending in
        # death. None if no periods end in death.
        self.earliest_death_date = earliest_death_date

        self.field_index = field_index

    def pre_processed_incarceration_period_index_for_calculations(
        self,
        *,
        collapse_transfers: bool,
        overwrite_facility_information_in_transfers: bool,
    ) -> PreProcessedIncarcerationPeriodIndex:
        """Validates, sorts, and collapses the incarceration period inputs.
        Ensures the necessary dates and fields are set on each incarceration period.

        If collapse_transfers is True, collapses adjacent periods connected by
        TRANSFER.
        """
        config = PreProcessingConfiguration(
            collapse_transfers=collapse_transfers,
            overwrite_facility_information_in_transfers=overwrite_facility_information_in_transfers,
        )
        if (
            config
            not in self._pre_processed_incarceration_period_index_for_calculations
        ):
            if not self._incarceration_periods:
                # If there are no incarceration_periods, return an empty index
                self._pre_processed_incarceration_period_index_for_calculations[
                    config
                ] = PreProcessedIncarcerationPeriodIndex(
                    incarceration_periods=self._incarceration_periods,
                    ip_id_to_pfi_subtype={},
                    incarceration_delegate=self.incarceration_delegate,
                )
            else:
                # Make a deep copy of the original incarceration periods to preprocess
                # with the given config
                periods_for_pre_processing = deepcopy(self._incarceration_periods)

                # Drop placeholder IPs with no information on them
                mid_processing_periods = self._drop_placeholder_periods(
                    periods_for_pre_processing
                )

                # Drop placeholder IPs with no start and no end dates
                mid_processing_periods = self._drop_missing_date_periods(
                    mid_processing_periods
                )

                # Sort periods, and infer as much missing information as possible
                mid_processing_periods = (
                    self._sort_and_infer_missing_dates_and_statuses(
                        mid_processing_periods
                    )
                )

                # Handle any periods that may have been erroneously set to have a
                # TEMPORARY_CUSTODY pfi at ingest due to limitations in ingest
                # mapping logic. (For example, logic that requires looking at more
                # than one period to determine the correct pfi value.)
                mid_processing_periods = (
                    self._handle_erroneously_set_temporary_custody_periods(
                        mid_processing_periods
                    )
                )

                # Update transfers that should be status change edges
                mid_processing_periods = self._update_transfers_to_status_changes(
                    mid_processing_periods
                )

                # Update parole board hold and other temporary custody period attributes
                # to match standardized values
                mid_processing_periods = (
                    self._standardize_temporary_custody_and_board_hold_periods(
                        mid_processing_periods
                    )
                )

                # Override values on the incarceration periods that are
                # commitment from supervision admissions
                (
                    mid_processing_periods,
                    ip_id_to_pfi_subtype,
                ) = self._normalize_commitment_from_supervision_admission_periods(
                    incarceration_periods=mid_processing_periods,
                    supervision_period_index=self._pre_processed_supervision_period_index,
                    violation_responses=self._violation_responses,
                )

                # Drop certain periods entirely from the calculations
                mid_processing_periods = self._drop_periods_from_calculations(
                    mid_processing_periods
                )

                # Ensure that the purpose_for_incarceration values on all periods is
                # what we expect
                mid_processing_periods = (
                    self._standardize_purpose_for_incarceration_values(
                        mid_processing_periods
                    )
                )

                if config.collapse_transfers:
                    # Collapse adjacent periods connected by a TRANSFER
                    mid_processing_periods = self._collapse_incarceration_period_transfers(
                        incarceration_periods=mid_processing_periods,
                        overwrite_facility_information_in_transfers=config.overwrite_facility_information_in_transfers,
                    )

                # Validate IPs
                self._validate_ip_invariants(mid_processing_periods)

                self._pre_processed_incarceration_period_index_for_calculations[
                    config
                ] = PreProcessedIncarcerationPeriodIndex(
                    incarceration_periods=mid_processing_periods,
                    ip_id_to_pfi_subtype=ip_id_to_pfi_subtype,
                    incarceration_delegate=self.incarceration_delegate,
                )
        return self._pre_processed_incarceration_period_index_for_calculations[config]

    def _drop_placeholder_periods(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateIncarcerationPeriod]:
        """Removes any incarceration periods that are placeholders."""
        filtered_periods = [
            ip
            for ip in incarceration_periods
            if not is_placeholder(ip, self.field_index)
        ]
        return filtered_periods

    def _drop_missing_date_periods(
        self, incarceration_periods: List[StateIncarcerationPeriod]
    ) -> List[StateIncarcerationPeriod]:
        """Removes any incarceration periods that do not have start nor end dates."""
        return [
            ip
            for ip in incarceration_periods
            if ip.start_date_inclusive or ip.end_date_exclusive
        ]

    def _drop_periods_from_calculations(
        self, incarceration_periods: List[StateIncarcerationPeriod]
    ) -> List[StateIncarcerationPeriod]:
        """Drops periods entirely if they are zero-day erroneous periods, or if they
        have otherwise been defined as periods that should be dropped from
        calculations."""
        filtered_periods: List[StateIncarcerationPeriod] = []

        for idx, ip in enumerate(incarceration_periods):
            if (
                ip.incarceration_type
                in self.pre_processing_delegate.incarceration_types_to_filter()
            ):
                continue

            previous_ip = filtered_periods[-1] if filtered_periods else None
            next_ip = (
                incarceration_periods[idx + 1]
                if (idx + 1 < len(incarceration_periods))
                else None
            )

            if self._is_zero_day_erroneous_period(
                ip=ip, previous_ip=previous_ip, next_ip=next_ip
            ):
                continue
            filtered_periods.append(ip)
        return filtered_periods

    @staticmethod
    def _is_zero_day_erroneous_period(
        ip: StateIncarcerationPeriod,
        previous_ip: Optional[StateIncarcerationPeriod],
        next_ip: Optional[StateIncarcerationPeriod],
    ) -> bool:
        """Returns whether the period is a zero-day erroneous period. Zero-day
        erroneous periods are periods where the admission_date is the same as the
        release_date, and any of the following are true:
        - Person was released from an erroneous admission after a non-transfer admission
        - Person was admitted from supervision and then conditionally released on the
            same day
        - The admission is on the same day as the admission to the person's next
            incarceration period, both periods have the same admission_reason, and
            the edge between the periods isn't a TRANSFER edge
        - The release is on the same day as the release from the person's previous
            incarceration period,both periods have the same release_reason, and
            the edge between the periods isn't a TRANSFER edge

        It is reasonable to assume that these periods are erroneous and should not be
        considered in any metrics involving incarceration.
        """
        if ip.admission_date != ip.release_date:
            # This isn't a zero-day period
            return False

        if (
            ip.release_reason
            == StateIncarcerationPeriodReleaseReason.RELEASED_FROM_ERRONEOUS_ADMISSION
            and ip.admission_reason != StateIncarcerationPeriodAdmissionReason.TRANSFER
        ):
            # A release from an erroneous admission on a non-transfer zero-day
            # period is reliably an entirely erroneous period
            return True

        if (
            ip.admission_reason
            == StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
            and ip.release_reason
            == StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE
        ):
            # A zero-day return from supervision and then immediate conditional
            # release is reliably an entirely erroneous period
            return True

        if previous_ip:
            if (
                ip.release_date == previous_ip.release_date
                and ip.release_reason == previous_ip.release_reason
            ):
                if (
                    previous_ip.release_reason
                    == StateIncarcerationPeriodReleaseReason.TRANSFER
                    and ip.admission_reason
                    == StateIncarcerationPeriodAdmissionReason.TRANSFER
                ):
                    # These transfers will be handled by the transfer collapsing logic
                    return False

                # This is a single-day period that borders the end of the previous
                # period and has the same release_reason. Drop it.
                return True

        if next_ip:
            if (
                ip.admission_date == next_ip.admission_date
                and ip.admission_reason == next_ip.admission_reason
            ):
                if (
                    ip.release_reason == StateIncarcerationPeriodReleaseReason.TRANSFER
                    and next_ip.admission_reason
                    == StateIncarcerationPeriodAdmissionReason.TRANSFER
                ):
                    # These transfers will be handled by the transfer collapsing logic
                    return False

                # This is a single-day period that borders the start of the next
                # period and has the same admission_reason. Drop it.
                return True

        return False

    def _sort_and_infer_missing_dates_and_statuses(
        self, incarceration_periods: List[StateIncarcerationPeriod]
    ) -> List[StateIncarcerationPeriod]:
        """First, sorts the incarceration_periods in chronological order. Then, for
        any periods missing dates and statuses, infers this information given
        the other incarceration periods.

        Assumes incarceration_periods are sorted chronologically at the time this
        function is called.
        """
        standard_date_sort_for_incarceration_periods(incarceration_periods)

        updated_periods: List[StateIncarcerationPeriod] = []

        for index, ip in enumerate(incarceration_periods):
            previous_ip = incarceration_periods[index - 1] if index > 0 else None
            next_ip = (
                incarceration_periods[index + 1]
                if index < len(incarceration_periods) - 1
                else None
            )

            if self.earliest_death_date:
                if ip.admission_date and self.earliest_death_date <= ip.admission_date:
                    # If a period starts after the earliest_death_date, drop the period.
                    logging.info(
                        "Dropping incarceration period with with an admission_date "
                        "after a release due to death: [%s]",
                        ip,
                    )
                    continue
                if (
                    ip.release_date and ip.release_date > self.earliest_death_date
                ) or ip.release_date is None:
                    # If the incarceration period duration exceeds the
                    # earliest_death_date or is not terminated, set the release date
                    # to earliest_death_date, change release_reason to DEATH, update
                    # status
                    ip.release_date = self.earliest_death_date
                    ip.release_reason = StateIncarcerationPeriodReleaseReason.DEATH

            if ip.release_date is None:
                if next_ip:
                    # This is not the last incarceration period in the list. Set the
                    # release date to the next admission or release date.
                    ip.release_date = (
                        next_ip.admission_date
                        if next_ip.admission_date
                        else next_ip.release_date
                    )

                    if ip.release_reason is None:
                        if (
                            next_ip.admission_reason
                            == StateIncarcerationPeriodAdmissionReason.TRANSFER
                        ):
                            # If they were transferred into the next period, infer that
                            # this release was a transfer
                            ip.release_reason = (
                                StateIncarcerationPeriodReleaseReason.TRANSFER
                            )
                elif ip.release_reason or ip.release_reason_raw_text:
                    # This is the last incarceration period in the list. The
                    # existence of a release reason indicates that this period should
                    # be closed. Set the release date to the admission date.
                    ip.release_date = ip.admission_date
                    if not ip.release_reason:
                        ip.release_reason = (
                            StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN
                        )
            elif ip.release_date > date.today():
                # This is an erroneous release_date in the future. For the purpose of
                # calculations, clear the release_date and the release_reason.
                ip.release_date = None
                ip.release_reason = None

            if ip.admission_date is None:
                if previous_ip:
                    # If the admission date is not set, and this is not the first
                    # incarceration period, then set the admission_date to be the
                    # same as the release_date or admission_date of the preceding period
                    ip.admission_date = (
                        previous_ip.release_date
                        if previous_ip.release_date
                        else previous_ip.admission_date
                    )

                    if ip.admission_reason is None:
                        if (
                            previous_ip.release_reason
                            == StateIncarcerationPeriodReleaseReason.TRANSFER
                        ):
                            # If they were transferred out of the previous period, infer
                            # that this admission was a transfer
                            ip.admission_reason = (
                                StateIncarcerationPeriodAdmissionReason.TRANSFER
                            )
                else:
                    # If the admission date is not set, and this is the
                    # first incarceration period, then set the admission_date to be
                    # the same as the release_date
                    ip.admission_date = ip.release_date
                    ip.admission_reason = (
                        StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN
                    )
            elif ip.admission_date > date.today():
                logging.info(
                    "Dropping incarceration period with admission_date in the future: [%s]",
                    ip,
                )
                continue

            if ip.admission_reason is None:
                # We have no idea what this admission reason was.
                # Set as INTERNAL_UNKNOWN.
                ip.admission_reason = (
                    StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN
                )
            if ip.release_date is not None and ip.release_reason is None:
                # We have no idea what this release reason was.
                # Set as INTERNAL_UNKNOWN.
                ip.release_reason = (
                    StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN
                )

            if ip.admission_date and ip.release_date:
                if ip.release_date < ip.admission_date:
                    logging.info(
                        "Dropping incarceration period with release before admission: [%s]",
                        ip,
                    )
                    continue

                if updated_periods:
                    most_recent_valid_period = updated_periods[-1]

                    if ip_is_nested_in_previous_period(ip, most_recent_valid_period):
                        # This period is entirely nested within the period before it.
                        # Do not include in the list of periods.
                        logging.info(
                            "Dropping incarceration period [%s] that is nested in period [%s]",
                            ip,
                            most_recent_valid_period,
                        )
                        continue

            updated_periods.append(ip)

        return updated_periods

    @staticmethod
    def _is_status_change_edge(
        ip_1: StateIncarcerationPeriod, ip_2: StateIncarcerationPeriod
    ) -> bool:
        """Returns whether the release from ip_1 and the admission into ip_2 is a
        transfer between two periods that qualifies as a STATUS_CHANGE."""
        if period_edges_are_valid_transfer(
            ip_1,
            ip_2,
        ):
            # If the two IPs are valid transfers and they have different values for any
            # of the attributes listed in ATTRIBUTES_TRIGGERING_STATUS_CHANGE, then they
            # are a status change edge
            for attribute in ATTRIBUTES_TRIGGERING_STATUS_CHANGE:
                distinct_values = set()
                distinct_values.add(getattr(ip_1, attribute))
                distinct_values.add(getattr(ip_2, attribute))
                if len(distinct_values) > 1:
                    return True
        return False

    @staticmethod
    def _update_transfers_to_status_changes(
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateIncarcerationPeriod]:
        """Updates the admission and release reasons on adjacent periods that qualify
        as valid status-change transitions to be STATUS_CHANGE instead of TRANSFER.

        It's possible that a person also changed facilities when one of their statuses
        changed, but the STATUS_CHANGE edge takes precedence in these cases.
        """
        for index, _ in enumerate(incarceration_periods):
            ip = incarceration_periods[index]
            next_ip = (
                incarceration_periods[index + 1]
                if index < len(incarceration_periods) - 1
                else None
            )

            if next_ip and IncarcerationPreProcessingManager._is_status_change_edge(
                ip, next_ip
            ):
                # Update the release_reason on the IP to STATUS_CHANGE
                incarceration_periods[index] = attr.evolve(
                    ip,
                    release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
                )

                # Update the admission_reason on the next IP to STATUS_CHANGE
                incarceration_periods[index + 1] = attr.evolve(
                    next_ip,
                    admission_reason=StateIncarcerationPeriodAdmissionReason.STATUS_CHANGE,
                )

        return incarceration_periods

    def _standardize_temporary_custody_and_board_hold_periods(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateIncarcerationPeriod]:
        """Ensures that all periods representing time in a parole board hold or some
        other kind of temporary custody have the expected admission_reason and
        specialized_purpose_for_incarceration values.

        In some cases, overrides the set release_reason to be
        RELEASED_FROM_TEMPORARY_CUSTODY."""
        updated_periods: List[StateIncarcerationPeriod] = []

        valid_temporary_custody_admission_pfi_values: List[
            StateSpecializedPurposeForIncarceration
        ] = [
            StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
            StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY,
        ]

        for index, ip in enumerate(incarceration_periods):
            period_is_board_hold = (
                self.pre_processing_delegate.period_is_parole_board_hold(
                    incarceration_period_list_index=index,
                    sorted_incarceration_periods=incarceration_periods,
                )
            )
            period_is_non_board_hold_temp_custody = (
                self.pre_processing_delegate.period_is_non_board_hold_temporary_custody(
                    incarceration_period_list_index=index,
                    sorted_incarceration_periods=incarceration_periods,
                )
            )

            if period_is_non_board_hold_temp_custody and period_is_board_hold:
                raise ValueError(
                    "A period of incarceration cannot be both a parole "
                    "board hold period AND a non-board hold temporary "
                    "custody period. Invalid logic in the pre_processing_delegate."
                )

            previous_ip: Optional[StateIncarcerationPeriod] = None
            if index > 0:
                previous_ip = updated_periods[-1]
            updated_previous_ip_release_reason: Optional[
                StateIncarcerationPeriodReleaseReason
            ] = None

            updated_pfi: Optional[StateSpecializedPurposeForIncarceration] = None
            updated_admission_reason: Optional[
                StateIncarcerationPeriodAdmissionReason
            ] = None
            updated_release_reason: Optional[
                StateIncarcerationPeriodReleaseReason
            ] = None

            if period_is_board_hold:
                # Standard values for parole board hold periods
                updated_admission_reason = (
                    StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY
                )
                updated_pfi = StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD
            elif period_is_non_board_hold_temp_custody:
                # Standard values for non-board hold temporary custody periods
                updated_admission_reason = (
                    StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY
                )
                updated_pfi = StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY
            else:
                if (
                    ip.admission_reason
                    == StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY
                ):
                    # Periods should only have a TEMPORARY_CUSTODY admission reason if
                    # they are parole board holds or other periods of temporary custody.
                    updated_admission_reason = (
                        StateIncarcerationPeriodAdmissionReason.INTERNAL_UNKNOWN
                    )

                if (
                    ip.release_reason
                    == StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY
                ):
                    # Periods should only have a RELEASED_FROM_TEMPORARY_CUSTODY
                    # release reason if they are parole board holds or other periods
                    # of temporary custody.
                    updated_release_reason = (
                        StateIncarcerationPeriodReleaseReason.INTERNAL_UNKNOWN
                    )

                if (
                    ip.specialized_purpose_for_incarceration
                    in valid_temporary_custody_admission_pfi_values
                ):
                    # Periods should not have PAROLE_BOARD_HOLD or TEMPORARY_CUSTODY
                    # pfi values if they are not parole board holds or other periods of
                    # temporary custody.
                    updated_pfi = (
                        StateSpecializedPurposeForIncarceration.INTERNAL_UNKNOWN
                    )

            if (
                not updated_admission_reason
                and not updated_release_reason
                and not updated_pfi
            ):
                # No need to update this period
                updated_periods.append(ip)
                continue

            if (
                updated_admission_reason
                == StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY
            ):
                # TEMPORARY_CUSTODY admissions should have
                # RELEASED_FROM_TEMPORARY_CUSTODY releases unless the period has a
                # release_reason that has more important information (e.g. DEATH)
                # about the release from temporary custody
                if (
                    ip.release_reason
                    and not release_reason_overrides_released_from_temporary_custody(
                        ip.release_reason
                    )
                ):
                    updated_release_reason = (
                        StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY
                    )

                # TEMPORARY_CUSTODY admissions should have a TRANSFER admission reason
                # if the previous period has a RELEASED_FROM_TEMPORARY_CUSTODY release,
                # they take place right after each other, and they share the same purpose_for_incarceration value.
                if (
                    previous_ip
                    and previous_ip.release_reason
                    == (
                        StateIncarcerationPeriodReleaseReason.RELEASED_FROM_TEMPORARY_CUSTODY
                    )
                    and previous_ip.specialized_purpose_for_incarceration == updated_pfi
                    and periods_are_temporally_adjacent(previous_ip, ip)
                ):
                    updated_admission_reason = (
                        StateIncarcerationPeriodAdmissionReason.TRANSFER
                    )
                    updated_previous_ip_release_reason = (
                        StateIncarcerationPeriodReleaseReason.TRANSFER
                    )

            if updated_previous_ip_release_reason and previous_ip:
                # Update previous period
                updated_periods[-1] = attr.evolve(
                    previous_ip, release_reason=updated_previous_ip_release_reason
                )

            # Update the period with expected values
            updated_ip = attr.evolve(
                ip,
                admission_reason=(updated_admission_reason or ip.admission_reason),
                specialized_purpose_for_incarceration=(
                    updated_pfi or ip.specialized_purpose_for_incarceration
                ),
                release_reason=(updated_release_reason or ip.release_reason),
            )
            updated_periods.append(updated_ip)

        return updated_periods

    def _normalize_commitment_from_supervision_admission_periods(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
        supervision_period_index: Optional[PreProcessedSupervisionPeriodIndex],
        violation_responses: Optional[List[StateSupervisionViolationResponse]],
    ) -> Tuple[List[StateIncarcerationPeriod], Dict[int, Optional[str]]]:
        """Updates attributes on incarceration periods that represent a commitment
        from supervision admission and require updated attribute values,
        and identifies the purpose_for_incarceration_subtype value for these periods.

        Applies both state-specific and universal overrides for commitment from
        supervision admissions so that the period match expected values.
        """
        updated_periods: List[StateIncarcerationPeriod] = []
        ip_id_to_pfi_subtype: Dict[int, Optional[str]] = {}
        for index, original_ip in enumerate(incarceration_periods):
            # First, identify the purpose_for_incarceration information for the given
            # period if it's a commitment from supervision admission
            pfi_info = self.pre_processing_delegate.get_pfi_info_for_period_if_commitment_from_supervision(
                incarceration_period_list_index=index,
                sorted_incarceration_periods=incarceration_periods,
                violation_responses=violation_responses,
            )

            # Then, apply any state-specific overrides if the period is a  commitment
            # from supervision admission
            updated_ip = self.pre_processing_delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=index,
                sorted_incarceration_periods=incarceration_periods,
                supervision_period_index=supervision_period_index,
            )

            if (
                original_ip.specialized_purpose_for_incarceration
                != updated_ip.specialized_purpose_for_incarceration
            ):
                raise ValueError(
                    "The specialized_purpose_for_incarceration value of "
                    f"incarceration period [{updated_ip.incarceration_period_id}] was "
                    "updated during the call to "
                    "normalize_period_if_commitment_from_supervision. "
                    "All updates to this attribute for commitment from "
                    "supervision admissions should be determined in "
                )

            # Then, universally apply overrides to ensure commitment from supervision
            # admissions match expected values
            if is_commitment_from_supervision(
                updated_ip.admission_reason, allow_ingest_only_enum_values=True
            ):
                # Set the purpose_for_incarceration from the pfi_info onto the period,
                # if set. Default to GENERAL for any unset purpose_for_incarceration
                # values at this point in pre-processing.
                updated_ip = attr.evolve(
                    updated_ip,
                    specialized_purpose_for_incarceration=(
                        pfi_info.purpose_for_incarceration
                        or StateSpecializedPurposeForIncarceration.GENERAL
                    ),
                )

                if (
                    updated_ip.specialized_purpose_for_incarceration
                    in SANCTION_ADMISSION_PURPOSE_FOR_INCARCERATION_VALUES
                ):
                    # Any commitment from supervision for SHOCK_INCARCERATION or
                    # TREATMENT_IN_PRISON should actually be classified as a
                    # SANCTION_ADMISSION
                    updated_ip = attr.evolve(
                        updated_ip,
                        admission_reason=StateIncarcerationPeriodAdmissionReason.SANCTION_ADMISSION,
                    )

            updated_periods.append(updated_ip)

            if not updated_ip.incarceration_period_id:
                raise ValueError(
                    "Unexpected incarceration period without an "
                    f"incarceration_period_id: {updated_ip}."
                )

            ip_id_to_pfi_subtype[
                updated_ip.incarceration_period_id
            ] = pfi_info.purpose_for_incarceration_subtype

        return updated_periods, ip_id_to_pfi_subtype

    @staticmethod
    def _standardize_purpose_for_incarceration_values(
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateIncarcerationPeriod]:
        """For any period that doesn't have a set purpose_for_incarceration value,
        sets the default value of GENERAL.
        """

        updated_ips: List[StateIncarcerationPeriod] = []

        for index, ip in enumerate(incarceration_periods):
            pfi_override = None

            if index > 0:
                previous_ip = updated_ips[-1]

                if period_edges_are_valid_transfer(
                    first_incarceration_period=previous_ip,
                    second_incarceration_period=ip,
                ):
                    # We propagate the pfi from the previous period if the edge
                    # between these two periods is a valid transfer. All edges that
                    # are valid STATUS_CHANGE edges have already been updated at this
                    # point.
                    pfi_override = previous_ip.specialized_purpose_for_incarceration

            if not pfi_override and not ip.specialized_purpose_for_incarceration:
                pfi_override = StateSpecializedPurposeForIncarceration.GENERAL

            updated_ip = attr.evolve(
                ip,
                specialized_purpose_for_incarceration=(
                    pfi_override or ip.specialized_purpose_for_incarceration
                ),
            )
            updated_ips.append(updated_ip)

        return updated_ips

    @staticmethod
    def _validate_ip_invariants(incarceration_periods: List[StateIncarcerationPeriod]):
        """Validates that no IPs violate standards that we can expect to be
        met for all periods in all states at the end of IP pre-processing."""
        for ip in incarceration_periods:
            if (
                ip.admission_reason
                == StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
            ):
                raise ValueError(
                    "Unexpected ingest-only admission_reason ADMITTED_FROM_SUPERVISION "
                    f"on ip: {ip}. We should have handled this value by the end of "
                    "IP pre-processing."
                )
            if not ip.admission_date:
                raise ValueError(
                    f"Unexpected missing admission_date on ip: {ip}. All IPs should "
                    "have set admission_dates by the end of IP pre-processing."
                )
            if not ip.admission_reason:
                raise ValueError(
                    f"Unexpected missing admission_reason on ip: {ip}. All IPs should "
                    "have a set admission_reason by the end of IP pre-processing."
                )
            if not ip.specialized_purpose_for_incarceration:
                raise ValueError(
                    "Unexpected missing specialized_purpose_for_incarceration on "
                    f"ip: {ip}. All IPs should have a set "
                    "specialized_purpose_for_incarceration by the end of IP "
                    "pre-processing."
                )

    def _collapse_incarceration_period_transfers(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
        overwrite_facility_information_in_transfers: bool,
    ) -> List[StateIncarcerationPeriod]:
        """Collapses any incarceration periods that are connected by transfers.
        Loops through all of the StateIncarcerationPeriods and combines adjacent
        periods that are connected by a transfer. Only connects two periods if the
        release reason of the first is `TRANSFER` and the admission reason for the
        second is also `TRANSFER`.

        Returns:
            A list of collapsed StateIncarcerationPeriods.
        """

        new_incarceration_periods: List[StateIncarcerationPeriod] = []
        open_transfer = False

        # TODO(#1782): Check to see if back to back incarceration periods are related
        #  to the same StateIncarcerationSentence or SentenceGroup to be sure we
        #  aren't counting stacked sentences or related periods as recidivism.
        for incarceration_period in incarceration_periods:
            if open_transfer:
                admission_reason = incarceration_period.admission_reason

                # Do not collapse any period with an official admission reason
                if (
                    not is_official_admission(admission_reason)
                    and admission_reason
                    == StateIncarcerationPeriodAdmissionReason.TRANSFER
                ):
                    # If there is an open transfer period and they were
                    # transferred into this incarceration period, then combine this
                    # period with the open transfer period.
                    start_period = new_incarceration_periods.pop(-1)

                    combined_period = self._combine_incarceration_periods(
                        start_period,
                        incarceration_period,
                        overwrite_facility_information=overwrite_facility_information_in_transfers,
                    )
                    new_incarceration_periods.append(combined_period)
                else:
                    # They weren't transferred here. Add this as a new
                    # incarceration period.
                    # TODO(#1790): Analyze how often a transfer out is followed by an
                    #  admission type that isn't a transfer to ensure we aren't
                    #  making bad assumptions with this transfer logic.
                    new_incarceration_periods.append(incarceration_period)
            else:
                # TODO(#1790): Analyze how often an incarceration period that starts
                #  with a transfer in is not preceded by a transfer out of a
                #  different facility.
                new_incarceration_periods.append(incarceration_period)

            # If this incarceration period ended in a transfer, then flag
            # that there's an open transfer period.
            open_transfer = (
                incarceration_period.release_reason
                == StateIncarcerationPeriodReleaseReason.TRANSFER
            )

        return new_incarceration_periods

    @staticmethod
    def _combine_incarceration_periods(
        start: StateIncarcerationPeriod,
        end: StateIncarcerationPeriod,
        overwrite_facility_information: bool = False,
    ) -> StateIncarcerationPeriod:
        """Combines two StateIncarcerationPeriods.
        Brings together two StateIncarcerationPeriods by setting the following
        fields on a deep copy of the |start| StateIncarcerationPeriod to the values
        on the |end| StateIncarcerationPeriod:
            [status, release_date, facility, housing_unit, facility_security_level,
            facility_security_level_raw_text, projected_release_reason,
            projected_release_reason_raw_text, release_reason,
            release_reason_raw_text]
            Args:
                start: The starting StateIncarcerationPeriod.
                end: The ending StateIncarcerationPeriod.
                overwrite_admission_reason: Whether to use the end admission reason instead of the start admission reason.
                overwrite_facility_information: Whether to use the facility, housing, and purpose for incarceration
                    information on the end period instead of on the start period.
        """

        collapsed_incarceration_period = deepcopy(start)

        if overwrite_facility_information:
            collapsed_incarceration_period.facility = end.facility
            collapsed_incarceration_period.facility_security_level = (
                end.facility_security_level
            )
            collapsed_incarceration_period.facility_security_level_raw_text = (
                end.facility_security_level_raw_text
            )
            collapsed_incarceration_period.housing_unit = end.housing_unit
            # We want the latest non-null specialized_purpose_for_incarceration
            if end.specialized_purpose_for_incarceration is not None:
                collapsed_incarceration_period.specialized_purpose_for_incarceration = (
                    end.specialized_purpose_for_incarceration
                )
                collapsed_incarceration_period.specialized_purpose_for_incarceration_raw_text = (
                    end.specialized_purpose_for_incarceration_raw_text
                )

        collapsed_incarceration_period.release_date = end.release_date
        collapsed_incarceration_period.projected_release_reason = (
            end.projected_release_reason
        )
        collapsed_incarceration_period.projected_release_reason_raw_text = (
            end.projected_release_reason_raw_text
        )
        collapsed_incarceration_period.release_reason = end.release_reason
        collapsed_incarceration_period.release_reason_raw_text = (
            end.release_reason_raw_text
        )

        return collapsed_incarceration_period

    def _handle_erroneously_set_temporary_custody_periods(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
    ):
        """For periods with a pfi of TEMPORARY_CUSTODY, sends the period to the
        state-specific pre-processing delegate to be updated if necessary."""
        updated_periods: List[StateIncarcerationPeriod] = []

        for idx, ip in enumerate(incarceration_periods):
            if (
                ip.specialized_purpose_for_incarceration
                == StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY
            ):
                ip = self.pre_processing_delegate.handle_erroneously_set_temporary_custody_period(
                    incarceration_period=ip,
                    previous_incarceration_period=updated_periods[-1]
                    if idx > 0
                    else None,
                )
            updated_periods.append(ip)

        return updated_periods
