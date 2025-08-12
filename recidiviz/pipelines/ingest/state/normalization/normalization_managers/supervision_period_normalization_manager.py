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
"""Contains the default logic for normalizing StateSupervisionPeriod entities so
that they are ready to be used in pipeline calculations."""
import abc
import datetime
import logging
from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple, Type

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.date import current_date_us_eastern
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import deep_entity_update
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
    get_shared_additional_attributes_map_for_entities,
    merge_additional_attributes_maps,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.pipelines.ingest.state.normalization.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
)
from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.pipelines.utils.supervision_period_utils import (
    standard_date_sort_for_supervision_periods,
)


class StateSpecificSupervisionNormalizationDelegate(abc.ABC, StateSpecificDelegate):
    """Interface for state-specific decisions involved in normalization
    supervision periods for calculations."""

    def drop_bad_periods(
        self, sorted_supervision_periods: List[StateSupervisionPeriod]
    ) -> List[StateSupervisionPeriod]:
        """States may have specific logic to drop periods that should be fully ignored
        before the rest of supervision period normalization proceeds.
        """
        return sorted_supervision_periods

    def close_incorrectly_open_supervision_periods(
        self,
        # pylint: disable=unused-argument
        sorted_supervision_periods: List[StateSupervisionPeriod],
        incarceration_periods: List[StateIncarcerationPeriod],
    ) -> List[StateSupervisionPeriod]:
        """States may have specific logic to close periods that should closed
        before the rest of supervision period normalization proceeds.
        """
        return sorted_supervision_periods

    def supervision_level_override(
        self,
        supervision_period_list_index: int,
        sorted_supervision_periods: List[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionLevel]:
        """States may have specific logic that determines the supervision level for a supervision period.
        By default, uses the one on the supervision period as ingested."""
        return sorted_supervision_periods[
            supervision_period_list_index
        ].supervision_level

    def supervision_type_override(
        self,
        supervision_period_list_index: int,
        sorted_supervision_periods: List[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionPeriodSupervisionType]:
        """States may have specific logic that determines the supervision type for a supervision period.
        By default, uses the one on the supervision period as ingested."""
        return sorted_supervision_periods[
            supervision_period_list_index
        ].supervision_type

    def supervision_admission_reason_override(
        self,
        # pylint: disable=unused-argument
        supervision_period: StateSupervisionPeriod,
        supervision_periods: List[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionPeriodAdmissionReason]:
        """States may have specific logic that determines the admission reason for a
        supervision period.
        By default, uses the one on the supervision period as ingested."""
        return supervision_period.admission_reason

    def supervision_termination_reason_override(
        self, supervision_period: StateSupervisionPeriod
    ) -> Optional[StateSupervisionPeriodTerminationReason]:
        """States may have specific logic that determines the termination reason for a
        supervision period.
        By default, uses the one on the supervision period as ingested."""
        return supervision_period.termination_reason

    def split_periods_based_on_sentences(
        self,
        # pylint: disable=unused-argument
        person_id: int,
        supervision_periods: List[StateSupervisionPeriod],
    ) -> List[StateSupervisionPeriod]:
        """Some states may use sentence information to split a period of supervision
        into multiple distinct periods with specific attributes. For example, if the
        supervision type can change over the duration of a given supervision period,
        a state may implement logic here that splits the period into discrete periods of
        time on each supervision type."""
        return supervision_periods

    def infer_additional_periods(
        self,
        # pylint: disable=unused-argument
        person_id: int,
        supervision_periods: List[StateSupervisionPeriod],
    ) -> List[StateSupervisionPeriod]:
        """Some states may require additional supervision periods to be inserted
        based on gaps in information. For instance, periods that represent active absconsions.
        """
        return supervision_periods

    def normalize_subsequent_absconsion_periods(
        self,
        sorted_supervision_periods: List[StateSupervisionPeriod],
    ) -> List[StateSupervisionPeriod]:
        """States may have specific logic to override all supervision types following
        an absconsion until returned or supervision is ended.
        """
        return sorted_supervision_periods


class SupervisionPeriodNormalizationManager(EntityNormalizationManager):
    """Handles the normalization of StateSupervisionPeriods for use in calculations."""

    def __init__(
        self,
        person_id: int,
        supervision_periods: List[StateSupervisionPeriod],
        incarceration_periods: List[StateIncarcerationPeriod],
        delegate: StateSpecificSupervisionNormalizationDelegate,
        staff_external_id_to_staff_id: Dict[Tuple[str, str], int],
        earliest_death_date: Optional[datetime.date] = None,
    ):
        self._person_id = person_id
        self._supervision_periods = deepcopy(supervision_periods)
        self._normalized_supervision_periods_and_additional_attributes: Optional[
            Tuple[List[StateSupervisionPeriod], AdditionalAttributesMap]
        ] = None
        self.staff_external_id_to_staff_id = staff_external_id_to_staff_id
        self.delegate = delegate
        self._incarceration_periods = incarceration_periods

        # The end date of the earliest incarceration or supervision period ending in
        # death. None if no periods end in death.
        self.earliest_death_date = earliest_death_date

    @staticmethod
    def normalized_entity_classes() -> List[Type[Entity]]:
        return [StateSupervisionPeriod, StateSupervisionCaseTypeEntry]

    def get_normalized_supervision_periods(
        self,
    ) -> list[NormalizedStateSupervisionPeriod]:
        (
            processed_sps,
            additional_attributes,
        ) = self.normalized_supervision_periods_and_additional_attributes()
        return convert_entity_trees_to_normalized_versions(
            processed_sps, NormalizedStateSupervisionPeriod, additional_attributes
        )

    def normalized_supervision_periods_and_additional_attributes(
        self,
    ) -> Tuple[List[StateSupervisionPeriod], AdditionalAttributesMap]:
        """Validates and sorts the supervision period inputs.

        Ensures the necessary dates and fields are set on each supervision period.

        DISCLAIMER: IP normalization may rely on normalized StateSupervisionPeriod
        entities for some states. Tread carefully if you are implementing any changes to
        SP normalization that may create circular dependencies between these processes.
        """
        if not self._normalized_supervision_periods_and_additional_attributes:
            # Make a deep copy of the original supervision periods to preprocess
            periods_for_normalization = deepcopy(self._supervision_periods)

            mid_processing_periods = self.delegate.drop_bad_periods(
                standard_date_sort_for_supervision_periods(periods_for_normalization)
            )

            # Sort periods, and infer as much missing information as possible
            mid_processing_periods = self._infer_missing_dates_and_statuses(
                mid_processing_periods
            )

            mid_processing_periods = self.delegate.split_periods_based_on_sentences(
                self._person_id, mid_processing_periods
            )

            if self.earliest_death_date:
                mid_processing_periods = (
                    self._drop_and_close_open_supervision_periods_for_deceased(
                        mid_processing_periods
                    )
                )

            mid_processing_periods = self.delegate.infer_additional_periods(
                self._person_id, mid_processing_periods
            )

            mid_processing_periods = (
                self.delegate.normalize_subsequent_absconsion_periods(
                    mid_processing_periods
                )
            )

            mid_processing_periods = (
                self.delegate.close_incorrectly_open_supervision_periods(
                    mid_processing_periods,
                    self._incarceration_periods,
                )
            )
            # Process fields on final supervision period set
            mid_processing_periods = (
                self._process_fields_on_final_supervision_period_set(
                    mid_processing_periods
                )
            )
            self._normalized_supervision_periods_and_additional_attributes = (
                mid_processing_periods,
                self.additional_attributes_map_for_normalized_sps(
                    supervision_periods=mid_processing_periods
                ),
            )
        return self._normalized_supervision_periods_and_additional_attributes

    @staticmethod
    def _infer_missing_dates_and_statuses(
        supervision_periods: List[StateSupervisionPeriod],
    ) -> List[StateSupervisionPeriod]:
        """First, sorts the supervision_periods in chronological order of the start and
        termination dates. Then, for any periods missing dates and statuses, infers this
        information given the other supervision periods.
        """
        standard_date_sort_for_supervision_periods(supervision_periods)

        updated_periods: List[StateSupervisionPeriod] = []

        for sp in supervision_periods:
            if sp.termination_date is None:
                if sp.termination_reason or sp.termination_reason_raw_text:
                    # There is no termination date on this period, but the set termination_reason indicates that the person
                    # is no longer in custody. Set the termination date to the start date.
                    sp.termination_date = sp.start_date

                    logging.warning(
                        "No termination_date for supervision period (%d) with nonnull "
                        "termination_reason (%s) or termination_reason_raw_text (%s)",
                        sp.supervision_period_id,
                        sp.termination_reason,
                        sp.termination_reason_raw_text,
                    )

            elif sp.termination_date > current_date_us_eastern():
                # This is an erroneous termination_date in the future. For the purpose
                # of calculations, clear the termination_date and the termination_reason.
                sp.termination_date = None
                sp.termination_reason = None

            if sp.start_date is None:
                logging.info("Dropping supervision period without start_date: [%s]", sp)
                continue
            if sp.start_date > current_date_us_eastern():
                logging.info(
                    "Dropping supervision period with start_date in the future: [%s]",
                    sp,
                )
                continue

            if sp.admission_reason is None:
                # We have no idea what this admission reason was. Set as INTERNAL_UNKNOWN.
                sp.admission_reason = (
                    StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN
                )
            if sp.termination_date is not None and sp.termination_reason is None:
                # We have no idea what this termination reason was. Set as INTERNAL_UNKNOWN.
                sp.termination_reason = (
                    StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN
                )

            if sp.start_date and sp.termination_date:
                if sp.termination_date < sp.start_date:
                    logging.info(
                        "Dropping supervision period with termination before admission: [%s]",
                        sp,
                    )
                    continue

            updated_periods.append(sp)

        return updated_periods

    def _drop_and_close_open_supervision_periods_for_deceased(
        self,
        supervision_periods: List[StateSupervisionPeriod],
    ) -> List[StateSupervisionPeriod]:
        """Updates supervision periods for people who are deceased by
        - Dropping open supervision periods that start after the |earliest_death_date|
        - Setting supervision periods with termination dates after the
          |earliest_death_date| to have a termination date of |earliest_death_date| and
          a termination reason of DEATH
        - Closing any open supervision period that start before the |earliest_death_date|
          to have a termination date of |earliest_death_date| and a termination reason
          of DEATH"""
        if not self.earliest_death_date:
            raise ValueError(
                "This function should only be called when we have a set "
                "earliest_death_date for the individual."
            )

        updated_periods: List[StateSupervisionPeriod] = []

        for sp in supervision_periods:
            updated_termination_date = None
            updated_termination_reason = None
            if not sp.start_date:
                raise ValueError(f"Period cannot have unset start dates: {sp}")

            if sp.start_date >= self.earliest_death_date:
                # Drop open supervision periods that start after the person's death
                continue

            if (sp.termination_date is None) or (
                self.earliest_death_date < sp.termination_date
            ):
                # If the supervision period is open, or if the termination_date is after
                # the earliest_death_date, set the termination_date to the
                # earliest_death_date and update the termination_reason and status
                updated_termination_date = self.earliest_death_date
                updated_termination_reason = (
                    StateSupervisionPeriodTerminationReason.DEATH
                )

            updated_periods.append(
                deep_entity_update(
                    sp,
                    termination_date=(updated_termination_date or sp.termination_date),
                    termination_reason=(
                        updated_termination_reason or sp.termination_reason
                    ),
                )
            )

        return updated_periods

    def _process_fields_on_final_supervision_period_set(
        self, supervision_periods: List[StateSupervisionPeriod]
    ) -> List[StateSupervisionPeriod]:
        """After all supervision periods are sorted, dropped and split as necessary,
        continue to update fields of remaining supervision periods prior to adding to
        the index by
            - Updating any admission reasons based on state-specific logic."""
        updated_periods: List[StateSupervisionPeriod] = []

        for index, sp in enumerate(supervision_periods):
            # For supervision level changes inferred using adjacent periods or other information
            sp.supervision_level = self.delegate.supervision_level_override(
                supervision_period_list_index=index,
                sorted_supervision_periods=supervision_periods,
            )
            # For supervision type changes inferred using adjacent periods or other information
            sp.supervision_type = self.delegate.supervision_type_override(
                supervision_period_list_index=index,
                sorted_supervision_periods=supervision_periods,
            )
            sp.admission_reason = self.delegate.supervision_admission_reason_override(
                sp, supervision_periods
            )
            sp.termination_reason = (
                self.delegate.supervision_termination_reason_override(sp)
            )
            updated_periods.append(sp)
        return updated_periods

    def additional_attributes_map_for_normalized_sps(
        self,
        supervision_periods: List[StateSupervisionPeriod],
    ) -> AdditionalAttributesMap:
        """Get additional attributes for each StateSupervisionPeriod."""
        shared_additional_attributes_map = (
            get_shared_additional_attributes_map_for_entities(
                entities=supervision_periods
            )
        )

        supervision_periods_additional_attributes_map: Dict[
            str, Dict[int, Dict[str, Any]]
        ] = {StateSupervisionPeriod.__name__: {}}

        for supervision_period in supervision_periods:
            if not supervision_period.supervision_period_id:
                raise ValueError(
                    "Expected non-null supervision_period_id values"
                    f"at this point. Found {supervision_period}."
                )
            supervising_officer_staff_id = None
            if supervision_period.supervising_officer_staff_external_id:
                if not supervision_period.supervising_officer_staff_external_id_type:
                    raise ValueError(
                        f"Found no supervising_officer_staff_external_id_type for supervising_officer_staff_external_id"
                        f" {supervision_period.supervising_officer_staff_external_id} on person "
                        f"{supervision_period.person}"
                    )
                supervising_officer_staff_id = self.staff_external_id_to_staff_id[
                    (
                        supervision_period.supervising_officer_staff_external_id,
                        supervision_period.supervising_officer_staff_external_id_type,
                    )
                ]
            supervision_periods_additional_attributes_map[
                StateSupervisionPeriod.__name__
            ][supervision_period.supervision_period_id] = {
                "supervising_officer_staff_id": supervising_officer_staff_id,
            }
        return merge_additional_attributes_maps(
            [
                shared_additional_attributes_map,
                supervision_periods_additional_attributes_map,
            ]
        )
