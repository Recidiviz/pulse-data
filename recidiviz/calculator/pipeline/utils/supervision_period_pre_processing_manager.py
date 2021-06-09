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
"""Contains the default logic for pre-processing StateSupervisionPeriod entities so
that they are ready to be used in pipeline calculations."""
import datetime
import logging
from copy import deepcopy
from typing import List, Optional

from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    standard_date_sort_for_supervision_periods,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodStatus,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.entity_utils import is_placeholder
from recidiviz.persistence.entity.state.entities import StateSupervisionPeriod


class SupervisionPreProcessingManager:
    """Handles the pre-processing of StateSupervisionPeriods for use in calculations."""

    def __init__(
        self,
        supervision_periods: List[StateSupervisionPeriod],
        earliest_death_date: Optional[datetime.date] = None,
    ):
        self._supervision_periods = deepcopy(supervision_periods)
        self._pre_processed_supervision_period_index: Optional[
            PreProcessedSupervisionPeriodIndex
        ] = None

        # The end date of the earliest incarceration or supervision period ending in
        # death. None if no periods end in death.
        self.earliest_death_date = earliest_death_date

    def pre_processed_supervision_period_index_for_calculations(
        self,
    ) -> PreProcessedSupervisionPeriodIndex:
        """Validates and sorts the supervision period inputs.

        Ensures the necessary dates and fields are set on each supervision period.

        DISCLAIMER: IP pre-processing may rely on pre-processed StateSupervisionPeriod
        entities for some states. Tread carefully if you are implementing any changes to
        SP pre-processing that may create circular dependencies between these processes.
        """
        if not self._pre_processed_supervision_period_index:
            # Make a deep copy of the original supervision periods to preprocess
            periods_for_pre_processing = deepcopy(self._supervision_periods)

            # Drop placeholder periods
            mid_processing_periods = self._drop_placeholder_periods(
                periods_for_pre_processing
            )

            # Sort periods, and infer as much missing information as possible
            mid_processing_periods = self._infer_missing_dates_and_statuses(
                mid_processing_periods
            )

            if self.earliest_death_date:
                mid_processing_periods = (
                    self._drop_and_close_open_supervision_periods_for_deceased(
                        mid_processing_periods
                    )
                )

            self._pre_processed_supervision_period_index = (
                PreProcessedSupervisionPeriodIndex(
                    supervision_periods=mid_processing_periods
                )
            )
        return self._pre_processed_supervision_period_index

    @staticmethod
    def _drop_placeholder_periods(
        supervision_periods: List[StateSupervisionPeriod],
    ) -> List[StateSupervisionPeriod]:
        """Drops all placeholder supervision periods."""
        return [period for period in supervision_periods if not is_placeholder(period)]

    @staticmethod
    def _infer_missing_dates_and_statuses(
        supervision_periods: List[StateSupervisionPeriod],
    ) -> List[StateSupervisionPeriod]:
        """First, sorts the supervision_periods in chronological order of the start and termination dates. Then, for any
        periods missing dates and statuses, infers this information given the other supervision periods.
        """
        standard_date_sort_for_supervision_periods(supervision_periods)

        updated_periods: List[StateSupervisionPeriod] = []

        for sp in supervision_periods:
            if sp.termination_date is None:
                if sp.status != StateSupervisionPeriodStatus.UNDER_SUPERVISION:
                    # If the person is not under supervision on this period, set the termination date to the start date.
                    sp.termination_date = sp.start_date
                    sp.termination_reason = (
                        StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN
                    )
                elif sp.termination_reason or sp.termination_reason_raw_text:
                    # There is no termination date on this period, but the set termination_reason indicates that the person
                    # is no longer in custody. Set the termination date to the start date.
                    sp.termination_date = sp.start_date
                    sp.status = StateSupervisionPeriodStatus.TERMINATED

                    logging.warning(
                        "No termination_date for supervision period (%d) with nonnull termination_reason (%s) "
                        "or termination_reason_raw_text (%s)",
                        sp.supervision_period_id,
                        sp.termination_reason,
                        sp.termination_reason_raw_text,
                    )

            elif sp.termination_date > datetime.date.today():
                # This is an erroneous termination_date in the future. For the purpose of calculations, clear the
                # termination_date and the termination_reason.
                sp.termination_date = None
                sp.termination_reason = None
                sp.status = StateSupervisionPeriodStatus.UNDER_SUPERVISION

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
        - Setting supervision periods with termination dates after the |earliest_death_date| to have a termination
          date of |earliest_death_date| and a termination reason of DEATH
        - Closing any open supervision period that start before the |earliest_death_date| to have a termination date
          of |earliest_death_date| and a termination reason of DEATH"""
        if not self.earliest_death_date:
            raise ValueError(
                "This function should only be called when we have a set "
                "earliest_death_date for the individual."
            )

        updated_periods: List[StateSupervisionPeriod] = []

        for sp in supervision_periods:
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
                sp.termination_date = self.earliest_death_date
                sp.termination_reason = StateSupervisionPeriodTerminationReason.DEATH
                sp.status = StateSupervisionPeriodStatus.TERMINATED

            updated_periods.append(sp)

        return updated_periods
