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
"""Contains the default logic for normalizing StateIncarcerationPeriod entities so
that they are ready to be used in pipeline calculations."""
import logging
from copy import deepcopy
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type

import attr

from recidiviz.common.constants.state.state_incarceration_period import (
    SANCTION_ADMISSION_PURPOSE_FOR_INCARCERATION_VALUES,
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateSpecializedPurposeForIncarceration,
    is_commitment_from_supervision,
    release_reason_overrides_released_from_temporary_custody,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.date import current_date_us_eastern
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import deep_entity_update
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
    get_shared_additional_attributes_map_for_entities,
    merge_additional_attributes_maps,
)
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.pipelines.ingest.state.normalization.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.pipelines.utils.incarceration_period_utils import (
    ip_is_nested_in_previous_period,
    period_edges_are_valid_transfer,
    periods_are_temporally_adjacent,
    standard_date_sort_for_incarceration_periods,
)
from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)

ATTRIBUTES_TRIGGERING_STATUS_CHANGE = [
    "custodial_authority",
    "specialized_purpose_for_incarceration",
]


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


class StateSpecificIncarcerationNormalizationDelegate(StateSpecificDelegate):
    """Interface for state-specific decisions involved in normalizing
    incarceration periods for calculations."""

    def normalize_period_if_commitment_from_supervision(
        self,
        # pylint: disable=unused-argument
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        original_sorted_incarceration_periods: List[StateIncarcerationPeriod],
        # TODO(#25800): When the delegate instantiation is refactored, we should be able to pass
        #  supervision_period_index only to state-specific delegates that need them via the constructor.
        supervision_period_index: NormalizedSupervisionPeriodIndex,
    ) -> StateIncarcerationPeriod:
        """State-specific implementations of this class should return a new
        StateIncarcerationPeriod with updated attributes if this period represents a
        commitment from supervision admission and requires updated attribute values.

        By default, returns the original period unchanged.
        """
        return sorted_incarceration_periods[incarceration_period_list_index]

    def incarceration_admission_reason_override(
        self, incarceration_period: StateIncarcerationPeriod
    ) -> Optional[StateIncarcerationPeriodAdmissionReason]:
        """States may have specific logic that determines the admission reason for an
        incarceration period.
        By default, uses the one on the incarceration period as ingested."""
        return incarceration_period.admission_reason

    # TODO(#16507) - Refactor our incarceration admission reason methods so that they are either combined or named
    #  based on how they are determining the admission reason.
    def incarceration_transfer_admission_reason_override(
        self,
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
    ) -> Optional[StateIncarcerationPeriodAdmissionReason]:
        """States may require specific logic that determines a TRANSFER admission reason for an
        incarceration period using inference from other periods.
        By default, uses the one on the incarceration period as ingested."""
        return sorted_incarceration_periods[
            incarceration_period_list_index
        ].admission_reason

    def incarceration_facility_override(
        self,
        incarceration_period: StateIncarcerationPeriod,
    ) -> Optional[str]:
        """States may have specific logic that determines the facility for an
        incarceration period.
        By default, uses the one on the incarceration period as ingested."""
        return incarceration_period.facility

    def get_pfi_info_for_period_if_commitment_from_supervision(
        self,
        # pylint: disable=unused-argument
        incarceration_period_list_index: int,
        sorted_incarceration_periods: List[StateIncarcerationPeriod],
        # TODO(#25800): When the delegate instantiation is refactored, we should be able to pass
        #  violation_responses only to state-specific delegates that need them via the constructor.
        violation_responses: List[NormalizedStateSupervisionViolationResponse],
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

    def get_incarceration_admission_violation_type(
        self,
        # pylint: disable=unused-argument
        incarceration_period: StateIncarcerationPeriod,
    ) -> Optional[StateSupervisionViolationType]:
        """State-specific implementations of this class should return a
        StateSupervisionViolationType object if the incarceration admission was due
        to a revocation and we have some information on the incarceration period
        that indicates the violation type that led to this revocation.

        By default, returns None
        """
        return None

    def handle_erroneously_set_temporary_custody_period(
        self,
        # pylint: disable=unused-argument
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

    def infer_additional_periods(
        self,
        # pylint: disable=unused-argument
        person_id: int,
        incarceration_periods: List[StateIncarcerationPeriod],
        # TODO(#25800): When the delegate instantiation is refactored, we should be able to pass
        #  supervision_period_index only to state-specific delegates that need them via the constructor.
        supervision_period_index: NormalizedSupervisionPeriodIndex,
    ) -> List[StateIncarcerationPeriod]:
        """Some states may require additional incarceration periods to be inserted
        based on gaps in information.
        """
        return incarceration_periods

    def standardize_purpose_for_incarceration_values(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateIncarcerationPeriod]:
        """Some states may want to standardize PFI values based on custom logic
        or based on the legacy _standardize_purpose_for_incarceration_values function
        that was previously the default for all states"""
        return incarceration_periods


class IncarcerationPeriodNormalizationManager(EntityNormalizationManager):
    """Interface for generalized and state-specific normalization of
    StateIncarcerationPeriods for use in calculations."""

    def __init__(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
        normalization_delegate: StateSpecificIncarcerationNormalizationDelegate,
        normalized_supervision_period_index: NormalizedSupervisionPeriodIndex,
        normalized_violation_responses: List[
            NormalizedStateSupervisionViolationResponse
        ],
        person_id: int,
        earliest_death_date: Optional[date] = None,
    ):
        self._person_id = person_id
        self._original_incarceration_periods = deepcopy(incarceration_periods)
        self._normalized_incarceration_periods_and_additional_attributes: Optional[
            Tuple[List[StateIncarcerationPeriod], AdditionalAttributesMap]
        ] = None
        self.normalization_delegate = normalization_delegate
        # Only store the NormalizedSupervisionPeriodIndex if StateSupervisionPeriod
        # entities are required for this state's StateIncarcerationPeriod
        # normalization
        self._normalized_supervision_period_index = normalized_supervision_period_index
        self._violation_responses = normalized_violation_responses

        # The end date of the earliest incarceration or supervision period ending in
        # death. None if no periods end in death.
        self.earliest_death_date = earliest_death_date

    @staticmethod
    def normalized_entity_classes() -> List[Type[Entity]]:
        return [StateIncarcerationPeriod]

    def get_normalized_incarceration_periods(
        self,
    ) -> list[NormalizedStateIncarcerationPeriod]:
        (
            processed_ips,
            additional_attributes,
        ) = self.normalized_incarceration_periods_and_additional_attributes()
        return convert_entity_trees_to_normalized_versions(
            processed_ips, NormalizedStateIncarcerationPeriod, additional_attributes
        )

    def normalized_incarceration_periods_and_additional_attributes(
        self,
    ) -> Tuple[List[StateIncarcerationPeriod], AdditionalAttributesMap]:
        """Validates, sorts, and updates the incarceration period inputs.
        Ensures the necessary dates and fields are set on each incarceration period.

        Returns the processed incarceration periods and the AdditionalAttributesMap
        storing the additional values to be set on the Normalized version of the
        periods.
        """
        if self._normalized_incarceration_periods_and_additional_attributes:
            return self._normalized_incarceration_periods_and_additional_attributes

        # Make a deep copy of the original incarceration periods
        periods_for_normalization = deepcopy(self._original_incarceration_periods)

        # Sort periods, and infer as much missing information as possible
        mid_processing_periods = self._sort_and_infer_missing_dates_and_statuses(
            periods_for_normalization
        )

        # Infer missing periods
        mid_processing_periods = self.normalization_delegate.infer_additional_periods(
            self._person_id,
            mid_processing_periods,
            supervision_period_index=self._normalized_supervision_period_index,
        )

        # Sorting periods after inference of additional periods
        mid_processing_periods = standard_date_sort_for_incarceration_periods(
            mid_processing_periods
        )

        original_sorted_periods = deepcopy(mid_processing_periods)

        # Handle any periods that may have been erroneously set to have a
        # TEMPORARY_CUSTODY pfi at ingest due to limitations in ingest
        # mapping logic. (For example, logic that requires looking at more
        # than one period to determine the correct pfi value.)
        mid_processing_periods = self._handle_erroneously_set_temporary_custody_periods(
            mid_processing_periods
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
            mid_processing_periods=mid_processing_periods,
            original_sorted_periods=original_sorted_periods,
            supervision_period_index=self._normalized_supervision_period_index,
            violation_responses=self._violation_responses,
        )

        # Drop certain periods entirely from the calculations
        mid_processing_periods = self._drop_periods_from_calculations(
            mid_processing_periods
        )

        # Ensure that the purpose_for_incarceration values on all periods is
        # what we expect
        mid_processing_periods = (
            self.normalization_delegate.standardize_purpose_for_incarceration_values(
                mid_processing_periods
            )
        )

        # Process fields on final incarceration period set
        mid_processing_periods = self._process_fields_on_final_incarceration_period_set(
            mid_processing_periods
        )

        # Generates map of admisson violation type information
        ip_id_to_violation_type = (
            self._generate_incarceration_admission_violation_types(
                mid_processing_periods=mid_processing_periods,
            )
        )

        # Validate IPs
        self.validate_ip_invariants(mid_processing_periods)

        self._normalized_incarceration_periods_and_additional_attributes = (
            mid_processing_periods,
            self.additional_attributes_map_for_normalized_ips(
                incarceration_periods=mid_processing_periods,
                ip_id_to_pfi_subtype=ip_id_to_pfi_subtype,
                ip_id_to_violation_type=ip_id_to_violation_type,
            ),
        )

        return self._normalized_incarceration_periods_and_additional_attributes

    def _drop_periods_from_calculations(
        self, incarceration_periods: List[StateIncarcerationPeriod]
    ) -> List[StateIncarcerationPeriod]:
        """Drops periods entirely if they are zero-day erroneous periods, or if they
        have otherwise been defined as periods that should be dropped from
        calculations."""
        filtered_periods: List[StateIncarcerationPeriod] = []

        for idx, ip in enumerate(incarceration_periods):
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
            next_ip = (
                incarceration_periods[index + 1]
                if index < len(incarceration_periods) - 1
                else None
            )

            if self.earliest_death_date:
                if self.earliest_death_date <= ip.admission_date:
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
                    # release date to the next admission date
                    ip.release_date = next_ip.admission_date

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
            elif ip.release_date > current_date_us_eastern():
                # This is an erroneous release_date in the future. For the purpose of
                # calculations, clear the release_date and the release_reason.
                ip.release_date = None
                ip.release_reason = None
                ip.release_reason_raw_text = None

            if ip.admission_date > current_date_us_eastern():
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

            if ip.release_date:
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

    @classmethod
    def _update_transfers_to_status_changes(
        cls,
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateIncarcerationPeriod]:
        """Updates the admission and release reasons on adjacent periods that qualify
        as valid status-change transitions to be STATUS_CHANGE instead of TRANSFER.

        It's possible that a person also changed facilities when one of their statuses
        changed, but the STATUS_CHANGE edge takes precedence in these cases.
        """
        for index, ip in enumerate(incarceration_periods):
            next_ip = (
                incarceration_periods[index + 1]
                if index < len(incarceration_periods) - 1
                else None
            )

            if next_ip and cls._is_status_change_edge(ip, next_ip):
                # Update the release_reason on the IP to STATUS_CHANGE
                incarceration_periods[index] = deep_entity_update(
                    ip,
                    release_reason=StateIncarcerationPeriodReleaseReason.STATUS_CHANGE,
                )

                # Update the admission_reason on the next IP to STATUS_CHANGE
                incarceration_periods[index + 1] = deep_entity_update(
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
                self.normalization_delegate.period_is_parole_board_hold(
                    incarceration_period_list_index=index,
                    sorted_incarceration_periods=incarceration_periods,
                )
            )
            period_is_non_board_hold_temp_custody = (
                self.normalization_delegate.period_is_non_board_hold_temporary_custody(
                    incarceration_period_list_index=index,
                    sorted_incarceration_periods=incarceration_periods,
                )
            )

            if period_is_non_board_hold_temp_custody and period_is_board_hold:
                raise ValueError(
                    "A period of incarceration cannot be both a parole "
                    "board hold period AND a non-board hold temporary "
                    "custody period. Invalid logic in the normalization_delegate."
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
                updated_periods[-1] = deep_entity_update(
                    previous_ip, release_reason=updated_previous_ip_release_reason
                )

            # Update the period with expected values
            updated_ip = deep_entity_update(
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
        mid_processing_periods: List[StateIncarcerationPeriod],
        original_sorted_periods: List[StateIncarcerationPeriod],
        supervision_period_index: NormalizedSupervisionPeriodIndex,
        violation_responses: List[NormalizedStateSupervisionViolationResponse],
    ) -> Tuple[List[StateIncarcerationPeriod], Dict[int, Optional[str]]]:
        """Updates attributes on incarceration periods that represent a commitment
        from supervision admission and require updated attribute values,
        and identifies the purpose_for_incarceration_subtype value for these periods.

        Applies both state-specific and universal overrides for commitment from
        supervision admissions so that the period match expected values.
        """
        updated_periods: List[StateIncarcerationPeriod] = []
        ip_id_to_pfi_subtype: Dict[int, Optional[str]] = {}
        for index, partially_processed_ip in enumerate(mid_processing_periods):
            # First, identify the purpose_for_incarceration information for the given
            # period if it's a commitment from supervision admission
            pfi_info = self.normalization_delegate.get_pfi_info_for_period_if_commitment_from_supervision(
                incarceration_period_list_index=index,
                sorted_incarceration_periods=mid_processing_periods,
                violation_responses=violation_responses,
            )

            # Then, apply any state-specific overrides if the period is a  commitment
            # from supervision admission
            updated_ip = self.normalization_delegate.normalize_period_if_commitment_from_supervision(
                incarceration_period_list_index=index,
                sorted_incarceration_periods=mid_processing_periods,
                original_sorted_incarceration_periods=original_sorted_periods,
                supervision_period_index=supervision_period_index,
            )

            if (
                partially_processed_ip.specialized_purpose_for_incarceration
                != updated_ip.specialized_purpose_for_incarceration
            ):
                raise ValueError(
                    "The specialized_purpose_for_incarceration value of "
                    f"incarceration period [{updated_ip.incarceration_period_id}] was "
                    "updated during the call to "
                    "normalize_period_if_commitment_from_supervision. "
                    "All updates to this attribute for commitment from "
                    "supervision admissions should be determined in "
                    "get_pfi_info_for_period_if_commitment_from_supervision."
                )

            # Then, universally apply overrides to ensure commitment from supervision
            # admissions match expected values
            if is_commitment_from_supervision(
                updated_ip.admission_reason, allow_ingest_only_enum_values=True
            ):
                # Set the purpose_for_incarceration from the pfi_info onto the period,
                # if set. Default to GENERAL for any unset purpose_for_incarceration
                # values at this point in normalization.
                updated_ip = deep_entity_update(
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
                    updated_ip = deep_entity_update(
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

    def _generate_incarceration_admission_violation_types(
        self,
        mid_processing_periods: List[StateIncarcerationPeriod],
    ) -> Dict[int, Optional[StateSupervisionViolationType]]:
        """Generates a map of incarceration admission violation type information
        to be added to each period."""

        return {
            ip.incarceration_period_id: self.normalization_delegate.get_incarceration_admission_violation_type(
                ip
            )
            for ip in mid_processing_periods
            if ip.incarceration_period_id
        }

    @staticmethod
    def validate_ip_invariants(
        incarceration_periods: Sequence[
            StateIncarcerationPeriod | NormalizedStateIncarcerationPeriod
        ],
    ) -> None:
        """Validates that no IPs violate standards that we can expect to be
        met for all periods in all states at the end of IP normalization."""
        for ip in incarceration_periods:
            if (
                ip.admission_reason
                == StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION
            ):
                raise ValueError(
                    "Unexpected ingest-only admission_reason ADMITTED_FROM_SUPERVISION "
                    f"on ip: {ip}. We should have handled this value by the end of "
                    "IP normalization."
                )
            if not ip.admission_reason:
                raise ValueError(
                    f"Unexpected missing admission_reason on ip: {ip}. All IPs should "
                    "have a set admission_reason by the end of IP normalization."
                )
            if not ip.specialized_purpose_for_incarceration:
                raise ValueError(
                    "Unexpected missing specialized_purpose_for_incarceration on "
                    f"ip: {ip}. All IPs should have a set "
                    "specialized_purpose_for_incarceration by the end of IP "
                    "normalization."
                )

    def _handle_erroneously_set_temporary_custody_periods(
        self,
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateIncarcerationPeriod]:
        """For periods with a pfi of TEMPORARY_CUSTODY, sends the period to the
        state-specific normalization delegate to be updated if necessary."""
        updated_periods: List[StateIncarcerationPeriod] = []

        for idx, ip in enumerate(incarceration_periods):
            if (
                ip.specialized_purpose_for_incarceration
                == StateSpecializedPurposeForIncarceration.TEMPORARY_CUSTODY
            ):
                ip = self.normalization_delegate.handle_erroneously_set_temporary_custody_period(
                    incarceration_period=ip,
                    previous_incarceration_period=updated_periods[-1]
                    if idx > 0
                    else None,
                )
            updated_periods.append(ip)

        return updated_periods

    @classmethod
    def additional_attributes_map_for_normalized_ips(
        cls,
        incarceration_periods: List[StateIncarcerationPeriod],
        ip_id_to_pfi_subtype: Dict[int, Optional[str]],
        ip_id_to_violation_type: Dict[int, Optional[StateSupervisionViolationType]],
    ) -> AdditionalAttributesMap:
        """Returns the attributes that should be set on the normalized version of
        each of the incarceration periods for each of the attributes that are unique
        to the NormalizedStateIncarcerationPeriod."""

        shared_additional_attributes_map = (
            get_shared_additional_attributes_map_for_entities(
                entities=incarceration_periods
            )
        )

        ip_additional_attributes_map: Dict[str, Dict[int, Dict[str, Any]]] = {
            StateIncarcerationPeriod.__name__: {}
        }

        for ip in incarceration_periods:
            if not ip.incarceration_period_id:
                raise ValueError(
                    "Expected non-null incarceration_period_id values "
                    f"at this point. Found {ip}."
                )

            ip_additional_attributes_map[StateIncarcerationPeriod.__name__][
                ip.incarceration_period_id
            ] = {
                "purpose_for_incarceration_subtype": ip_id_to_pfi_subtype[
                    ip.incarceration_period_id
                ],
                "incarceration_admission_violation_type": ip_id_to_violation_type[
                    ip.incarceration_period_id
                ],
            }

        return merge_additional_attributes_maps(
            [shared_additional_attributes_map, ip_additional_attributes_map]
        )

    def _process_fields_on_final_incarceration_period_set(
        self, incarceration_periods: List[StateIncarcerationPeriod]
    ) -> List[StateIncarcerationPeriod]:
        """After all incarceration periods are processed, continue to update fields of remaining
        incarceration periods prior to adding to the index by:
               - Updating any admission reasons based on state-specific logic.
        """
        updated_periods: List[StateIncarcerationPeriod] = []

        for index, ip in enumerate(incarceration_periods):
            # for admission reason changes inferred using adjacent incarceration periods
            ip.admission_reason = self.normalization_delegate.incarceration_transfer_admission_reason_override(
                incarceration_period_list_index=index,
                sorted_incarceration_periods=incarceration_periods,
            )

        for ip in incarceration_periods:
            # for admission reasons changes inferred using periods and incarceration sentences
            ip.admission_reason = (
                self.normalization_delegate.incarceration_admission_reason_override(ip)
            )
            # for facility
            ip.facility = self.normalization_delegate.incarceration_facility_override(
                ip
            )
            updated_periods.append(ip)
        return updated_periods
