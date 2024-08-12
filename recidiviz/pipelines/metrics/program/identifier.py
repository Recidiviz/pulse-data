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
"""Identifies instances of interaction with a program."""
import logging
from datetime import date
from typing import List, Set, Type

from dateutil.relativedelta import relativedelta

from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.persistence.entity.normalized_entities_utils import (
    sort_normalized_entities_by_sequence_num,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePerson,
    NormalizedStateProgramAssignment,
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.metrics.base_identifier import (
    BaseIdentifier,
    IdentifierContext,
)
from recidiviz.pipelines.metrics.program.events import (
    ProgramEvent,
    ProgramParticipationEvent,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.pipelines.utils.identifier_models import IdentifierResult
from recidiviz.pipelines.utils.supervision_period_utils import (
    supervision_periods_overlapping_with_date,
)

EXTERNAL_UNKNOWN_VALUE = "EXTERNAL_UNKNOWN"


class ProgramIdentifier(BaseIdentifier[List[ProgramEvent]]):
    """Identifier class for interaction with a program."""

    def __init__(self) -> None:
        self.identifier_result_class = ProgramEvent

    def identify(
        self,
        _person: NormalizedStatePerson,
        identifier_context: IdentifierContext,
        included_result_classes: Set[Type[IdentifierResult]],
    ) -> List[ProgramEvent]:
        if included_result_classes != {ProgramParticipationEvent}:
            raise NotImplementedError(
                "Filtering of events is not yet implemented for the program pipeline."
            )

        return self._find_program_events(
            program_assignments=identifier_context[
                NormalizedStateProgramAssignment.__name__
            ],
            supervision_periods=identifier_context[
                NormalizedStateSupervisionPeriod.__name__
            ],
        )

    def _find_program_events(
        self,
        supervision_periods: List[NormalizedStateSupervisionPeriod],
        program_assignments: List[NormalizedStateProgramAssignment],
    ) -> List[ProgramEvent]:
        """Finds instances of interaction with a program.

        Identifies instances of being referred to a program and actively participating
        in a program.

        Args:
            - program_assignments: All of the person's StateProgramAssignments
            - assessments: All of the person's recorded StateAssessments
            - supervision_periods: All of the person's supervision_periods

        Returns:
            A list of ProgramEvents for the person.
        """
        program_events: List[ProgramEvent] = []

        if not program_assignments:
            return program_events

        sorted_program_assignments = sort_normalized_entities_by_sequence_num(
            program_assignments
        )

        sp_index = NormalizedSupervisionPeriodIndex(
            sorted_supervision_periods=supervision_periods
        )

        for program_assignment in sorted_program_assignments:

            program_participation_events = self._find_program_participation_events(
                program_assignment, sp_index.sorted_supervision_periods
            )

            program_events.extend(program_participation_events)

        return program_events

    def _find_program_participation_events(
        self,
        program_assignment: NormalizedStateProgramAssignment,
        supervision_periods: List[NormalizedStateSupervisionPeriod],
    ) -> List[ProgramParticipationEvent]:
        """Finds instances of actively participating in a program. Produces a
        ProgramParticipationEvent for each day that the person was actively
        participating in the program. If the program_assignment has a
        participation_status of IN_PROGRESS and has a set start_date, produces a
        ProgramParticipationEvent for every day between the start_date and today. If
        the program_assignment has a participation_status of DISCHARGED, produces a
        ProgramParticipationEvent for every day between the start_date and
        discharge_date, end date exclusive.

        Where possible, identifies what types of supervision the person is on on the
        date of the participation.

        TODO(#8818): Consider producing only one ProgramParticipationEvent per referral
        If there are multiple overlapping supervision periods, returns one
        ProgramParticipationEvent for each supervision period that overlaps.

        Returns a list of ProgramReferralEvents.
        """
        program_participation_events: List[ProgramParticipationEvent] = []

        state_code = program_assignment.state_code
        participation_status = program_assignment.participation_status

        if participation_status not in (
            StateProgramAssignmentParticipationStatus.IN_PROGRESS,
            StateProgramAssignmentParticipationStatus.DISCHARGED_SUCCESSFUL,
            StateProgramAssignmentParticipationStatus.DISCHARGED_SUCCESSFUL_WITH_DISCRETION,
            StateProgramAssignmentParticipationStatus.DISCHARGED_UNSUCCESSFUL,
            StateProgramAssignmentParticipationStatus.DISCHARGED_OTHER,
            StateProgramAssignmentParticipationStatus.DISCHARGED_UNKNOWN,
        ):
            return program_participation_events

        start_date = program_assignment.start_date

        if not start_date:
            return program_participation_events

        discharge_date = program_assignment.discharge_date

        if discharge_date is None:
            if (
                participation_status
                != StateProgramAssignmentParticipationStatus.IN_PROGRESS
            ):
                logging.warning(
                    "StateProgramAssignment with a DISCHARGED status but no "
                    "discharge_date: %s",
                    program_assignment,
                )
                return program_participation_events

            # This person is actively participating in this program. Set the
            # discharge_date for tomorrow.
            discharge_date = date.today() + relativedelta(days=1)

        program_id = (
            program_assignment.program_id
            if program_assignment.program_id
            else EXTERNAL_UNKNOWN_VALUE
        )
        program_location_id = (
            program_assignment.program_location_id
            if program_assignment.program_location_id
            else EXTERNAL_UNKNOWN_VALUE
        )

        participation_date = start_date

        while participation_date < discharge_date:
            overlapping_supervision_periods = supervision_periods_overlapping_with_date(
                participation_date, supervision_periods
            )
            is_first_day_in_program = participation_date == start_date

            if overlapping_supervision_periods:
                # TODO(#8818): Consider producing only one ProgramParticipationEvent per
                #  referral
                for supervision_period in supervision_periods:
                    program_participation_events.append(
                        ProgramParticipationEvent(
                            state_code=state_code,
                            event_date=participation_date,
                            is_first_day_in_program=is_first_day_in_program,
                            program_id=program_id,
                            program_location_id=program_location_id,
                            supervision_type=supervision_period.supervision_type,
                        )
                    )
            else:
                program_participation_events.append(
                    ProgramParticipationEvent(
                        state_code=state_code,
                        event_date=participation_date,
                        is_first_day_in_program=is_first_day_in_program,
                        program_id=program_id,
                        program_location_id=program_location_id,
                    )
                )

            participation_date = participation_date + relativedelta(days=1)

        return program_participation_events
