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
from typing import List, Optional, Type

from recidiviz.calculator.pipeline.utils.entity_normalization.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    standard_date_sort_for_supervision_periods,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    deep_entity_update,
    is_placeholder,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateSupervisionCaseTypeEntry,
    StateSupervisionPeriod,
    StateSupervisionSentence,
)


# pylint: disable=unused-argument
class StateSpecificSupervisionNormalizationDelegate(abc.ABC):
    """Interface for state-specific decisions involved in normalization
    supervision periods for calculations."""

    def supervision_admission_reason_override(
        self,
        supervision_period: StateSupervisionPeriod,
        supervision_periods: List[StateSupervisionPeriod],
    ) -> Optional[StateSupervisionPeriodAdmissionReason]:
        """States may have specific logic that determines the admission reason for a
        supervision period.
        By default, uses the one on the supervision period as ingested."""
        return supervision_period.admission_reason

    def split_periods_based_on_sentences(
        self,
        supervision_periods: List[StateSupervisionPeriod],
        incarceration_sentences: Optional[List[StateIncarcerationSentence]],
        supervision_sentences: Optional[List[StateSupervisionSentence]],
    ) -> List[StateSupervisionPeriod]:
        """Some states may use sentence information to split a period of supervision
        into multiple distinct periods with specific attributes. For example, if the
        supervision type can change over the duration of a given supervision period,
        a state may implement logic here that splits the period into discrete periods of
        time on each supervision type."""
        return supervision_periods

    def infer_additional_periods(
        self,
        supervision_periods: List[StateSupervisionPeriod],
        incarceration_periods: Optional[List[StateIncarcerationPeriod]],
    ) -> List[StateSupervisionPeriod]:
        """Some states may require additional supervision periods to be inserted
        based on gaps in information. For instance, periods that represent active absconsions."""
        return supervision_periods

    def normalization_relies_on_sentences(self) -> bool:
        """State-specific implementations of this class should return whether the SP
        normalization logic for the state relies on information in
        StateIncarcerationSentence and StateSupervisionSentence entities."""
        return False

    def normalization_relies_on_incarceration_periods(self) -> bool:
        """State-specific implementations of this class should return whether the SP
        normalization logic for the state relies on information in
        StateIncarcerationPeriod entities."""
        return False


class SupervisionPeriodNormalizationManager(EntityNormalizationManager):
    """Handles the normalization of StateSupervisionPeriods for use in calculations."""

    def __init__(
        self,
        supervision_periods: List[StateSupervisionPeriod],
        delegate: StateSpecificSupervisionNormalizationDelegate,
        incarceration_sentences: Optional[List[StateIncarcerationSentence]],
        supervision_sentences: Optional[List[StateSupervisionSentence]],
        incarceration_periods: Optional[List[StateIncarcerationPeriod]],
        earliest_death_date: Optional[datetime.date] = None,
    ):
        self._supervision_periods = deepcopy(supervision_periods)
        self._normalized_supervision_period_index: Optional[
            NormalizedSupervisionPeriodIndex
        ] = None

        self.delegate = delegate

        self._incarceration_sentences: Optional[List[StateIncarcerationSentence]] = (
            incarceration_sentences
            if self.delegate.normalization_relies_on_sentences()
            else None
        )
        self._supervision_sentences: Optional[List[StateSupervisionSentence]] = (
            supervision_sentences
            if self.delegate.normalization_relies_on_sentences()
            else None
        )
        self._incarceration_periods: Optional[List[StateIncarcerationPeriod]] = (
            incarceration_periods
            if self.delegate.normalization_relies_on_incarceration_periods()
            else None
        )

        # The end date of the earliest incarceration or supervision period ending in
        # death. None if no periods end in death.
        self.earliest_death_date = earliest_death_date

        self.field_index = CoreEntityFieldIndex()

    @staticmethod
    def normalized_entity_classes() -> List[Type[Entity]]:
        return [StateSupervisionPeriod, StateSupervisionCaseTypeEntry]

    def normalized_supervision_period_index_for_calculations(
        self,
    ) -> NormalizedSupervisionPeriodIndex:
        """Validates and sorts the supervision period inputs.

        Ensures the necessary dates and fields are set on each supervision period.

        DISCLAIMER: IP normalization may rely on normalized StateSupervisionPeriod
        entities for some states. Tread carefully if you are implementing any changes to
        SP normalization that may create circular dependencies between these processes.
        """
        if not self._normalized_supervision_period_index:
            # Make a deep copy of the original supervision periods to preprocess
            periods_for_normalization = deepcopy(self._supervision_periods)

            # Drop placeholder periods
            mid_processing_periods = self._drop_placeholder_periods(
                periods_for_normalization
            )

            # Drop periods that have neither a start nor end date
            mid_processing_periods = self._drop_missing_date_periods(
                mid_processing_periods
            )

            # Sort periods, and infer as much missing information as possible
            mid_processing_periods = self._infer_missing_dates_and_statuses(
                mid_processing_periods
            )

            mid_processing_periods = self.delegate.split_periods_based_on_sentences(
                mid_processing_periods,
                self._incarceration_sentences,
                self._supervision_sentences,
            )

            if self.earliest_death_date:
                mid_processing_periods = (
                    self._drop_and_close_open_supervision_periods_for_deceased(
                        mid_processing_periods
                    )
                )

            mid_processing_periods = self.delegate.infer_additional_periods(
                mid_processing_periods, self._incarceration_periods
            )

            # Process fields on final supervision period set
            mid_processing_periods = (
                self._process_fields_on_final_supervision_period_set(
                    mid_processing_periods
                )
            )

            self._normalized_supervision_period_index = (
                NormalizedSupervisionPeriodIndex(
                    supervision_periods=mid_processing_periods
                )
            )
        return self._normalized_supervision_period_index

    def _drop_placeholder_periods(
        self,
        supervision_periods: List[StateSupervisionPeriod],
    ) -> List[StateSupervisionPeriod]:
        """Drops all placeholder supervision periods."""
        return [
            period
            for period in supervision_periods
            if not is_placeholder(period, self.field_index)
        ]

    def _drop_missing_date_periods(
        self, supervision_periods: List[StateSupervisionPeriod]
    ) -> List[StateSupervisionPeriod]:
        """Drops all periods that have no start and no end dates."""
        return [
            period
            for period in supervision_periods
            if period.start_date_inclusive or period.end_date_exclusive
        ]

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

            elif sp.termination_date > datetime.date.today():
                # This is an erroneous termination_date in the future. For the purpose
                # of calculations, clear the termination_date and the termination_reason.
                sp.termination_date = None
                sp.termination_reason = None

            if sp.start_date is None:
                logging.info("Dropping supervision period without start_date: [%s]", sp)
                continue
            if sp.start_date > datetime.date.today():
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

        for sp in supervision_periods:
            sp.admission_reason = self.delegate.supervision_admission_reason_override(
                sp, supervision_periods
            )
            updated_periods.append(sp)

        return updated_periods
