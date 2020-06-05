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
"""Identifies instances of interaction with a program."""

from datetime import date
from typing import List, Optional, Dict, Any, Set

from recidiviz.calculator.pipeline.program.program_event import \
    ProgramReferralEvent, ProgramEvent
from recidiviz.calculator.pipeline.utils.assessment_utils import \
    find_most_recent_assessment
from recidiviz.calculator.pipeline.utils.supervision_period_utils import prepare_supervision_periods_for_calculations
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType
from recidiviz.common.constants.state.state_program_assignment import StateProgramAssignmentParticipationStatus
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.persistence.entity.entity_utils import is_placeholder
from recidiviz.persistence.entity.state.entities import \
    StateProgramAssignment, StateAssessment, StateSupervisionPeriod


def find_program_events(
        program_assignments: List[StateProgramAssignment],
        assessments: List[StateAssessment],
        supervision_periods: List[StateSupervisionPeriod],
        supervision_period_to_agent_associations:
        Dict[int, Dict[Any, Any]]) -> List[ProgramEvent]:
    """Finds instances of interaction with a program.

    Right now, identifies instances of being referred to a program by
    transforming each StateProgramAssignment into instances of being referred to
    the program.

    Args:
        - program_assignments: All of the person's StateProgramAssignments
        - assessments: All of the person's recorded StateAssessments
        - supervision_periods: All of the person's supervision_periods
        - supervision_period_to_agent_associations: dictionary associating
            StateSupervisionPeriod ids to information about the corresponding StateAgent

    Returns:
        A list of ProgramEvents for the person.
    """
    supervision_periods = prepare_supervision_periods_for_calculations(supervision_periods)

    program_events: List[ProgramEvent] = []

    # TODO(2855): Bring in supervision and incarceration sentences to infer the supervision type on supervision
    #  periods that don't have a set supervision type
    program_events.extend(find_program_referrals(
        program_assignments, assessments,
        supervision_periods,
        supervision_period_to_agent_associations))

    return program_events


def find_program_referrals(
        program_assignments: List[StateProgramAssignment],
        assessments: List[StateAssessment],
        supervision_periods: List[StateSupervisionPeriod],
        supervision_period_to_agent_associations:
        Dict[int, Dict[Any, Any]]) -> \
        List[ProgramReferralEvent]:
    """Finds instances of being referred to a program.

    Looks at the program assignments that have a referral date and a program id
    to find referrals to a program. Then, using date-based logic, connects
    that referral to assessment and supervision data where possible to build
    ProgramReferralEvents. For assessments, identifies the most recent
    assessment at the time of the referral. For supervision, identifies any
    supervision periods that were active at the time of the referral. If there
    are multiple overlapping supervision periods, returns one
    ProgramReferralEvent for each unique supervision type for each supervision
    period that overlapped.

    Returns a list of ProgramReferralEvents.
    """
    program_referrals: List[ProgramReferralEvent] = []

    for program_assignment in program_assignments:
        referral_date = program_assignment.referral_date
        program_id = program_assignment.program_id

        if not program_id:
            program_id = 'EXTERNAL_UNKNOWN'

        if referral_date and program_id:
            assessment_score, _, assessment_type = \
                find_most_recent_assessment(referral_date,
                                            assessments)

            relevant_supervision_periods = \
                find_supervision_periods_during_referral(
                    referral_date, supervision_periods)

            program_referrals.extend(referrals_for_supervision_periods(
                program_assignment.state_code,
                program_id,
                referral_date,
                program_assignment.participation_status,
                assessment_score,
                assessment_type,
                relevant_supervision_periods,
                supervision_period_to_agent_associations
            ))

    return program_referrals


def find_supervision_periods_during_referral(
        referral_date: date,
        supervision_periods: List[StateSupervisionPeriod],
) -> List[StateSupervisionPeriod]:
    """Identifies supervision_periods where the referral_date falls between
    the start and end of the supervision period, indicating that the person
    was serving this supervision period at the time of the referral."""

    # Get all valid supervision periods with a start date before or on the
    # referral date
    applicable_supervision_periods = [
        sp for sp in supervision_periods if
        not is_placeholder(sp)
        and sp.start_date is not None
        and sp.start_date <= referral_date
        and (sp.termination_date is None
             or sp.termination_date >= referral_date)
    ]

    return applicable_supervision_periods


def referrals_for_supervision_periods(
        state_code: str, program_id: str, referral_date: date,
        participation_status: Optional[StateProgramAssignmentParticipationStatus],
        assessment_score: Optional[int],
        assessment_type: Optional[StateAssessmentType],
        supervision_periods: Optional[List[StateSupervisionPeriod]],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]]
) -> List[ProgramReferralEvent]:
    """Builds ProgramReferralEvents with data from the relevant supervision
    periods at the time of the referral. Returns one ProgramReferralEvent for
    each unique supervision type of the supervision periods that overlap with
    the referral."""

    program_referrals: List[ProgramReferralEvent] = []
    # TODO(2891): Update this logic to mirror how the supervision pipeline handles supervision type inference
    supervision_types_represented: Set[Optional[StateSupervisionType]] = set()

    if supervision_periods:
        for supervision_period in supervision_periods:
            # Return one ProgramReferralEvent per supervision period
            supervising_officer_external_id = None
            supervising_district_external_id = None

            if supervision_period.supervision_period_id:
                agent_info = supervision_period_to_agent_associations.get(supervision_period.supervision_period_id)

                if agent_info is not None:
                    supervising_officer_external_id = agent_info.get('agent_external_id')
                    supervising_district_external_id = agent_info.get('district_external_id')

            if supervision_period.supervision_type not in \
                    supervision_types_represented:
                program_referrals.append(
                    ProgramReferralEvent(
                        state_code=state_code,
                        program_id=program_id,
                        event_date=referral_date,
                        participation_status=participation_status,
                        assessment_score=assessment_score,
                        assessment_type=assessment_type,
                        supervision_type=supervision_period.supervision_type,
                        supervising_officer_external_id=supervising_officer_external_id,
                        supervising_district_external_id=supervising_district_external_id
                    )
                )

            supervision_types_represented.add(supervision_period.supervision_type)
    else:
        # Return a ProgramReferralEvent without any supervision details
        return [
            ProgramReferralEvent(
                state_code=state_code,
                program_id=program_id,
                event_date=referral_date,
                participation_status=participation_status,
                assessment_score=assessment_score,
                assessment_type=assessment_type
            )
        ]

    return program_referrals
