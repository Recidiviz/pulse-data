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
import logging
from datetime import date
from typing import List, Optional, Dict, Any

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.program.program_event import \
    ProgramReferralEvent, ProgramEvent, ProgramParticipationEvent
from recidiviz.calculator.pipeline.utils import assessment_utils
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import \
    only_state_custodial_authority_in_supervision_population
from recidiviz.calculator.pipeline.utils.supervision_period_utils import prepare_supervision_periods_for_calculations
from recidiviz.common.constants.state.state_assessment import \
    StateAssessmentType, StateAssessmentClass
from recidiviz.common.constants.state.state_program_assignment import StateProgramAssignmentParticipationStatus
from recidiviz.persistence.entity.entity_utils import is_placeholder, get_single_state_code
from recidiviz.persistence.entity.state.entities import \
    StateProgramAssignment, StateAssessment, StateSupervisionPeriod

EXTERNAL_UNKNOWN_VALUE = 'EXTERNAL_UNKNOWN'


def find_program_events(
        program_assignments: List[StateProgramAssignment],
        assessments: List[StateAssessment],
        supervision_periods: List[StateSupervisionPeriod],
        supervision_period_to_agent_associations:
        Dict[int, Dict[Any, Any]]) -> List[ProgramEvent]:
    """Finds instances of interaction with a program.

    Identifies instances of being referred to a program and actively participating in a program.

    Args:
        - program_assignments: All of the person's StateProgramAssignments
        - assessments: All of the person's recorded StateAssessments
        - supervision_periods: All of the person's supervision_periods
        - supervision_period_to_agent_associations: dictionary associating StateSupervisionPeriod ids to information
            about the corresponding StateAgent

    Returns:
        A list of ProgramEvents for the person.
    """
    # TODO(#2855): Bring in supervision and incarceration sentences to infer the supervision type on supervision
    #  periods that don't have a set supervision type
    program_events: List[ProgramEvent] = []

    if not program_assignments:
        return program_events

    state_code = get_single_state_code(program_assignments)

    should_drop_non_state_custodial_authority_periods = \
        only_state_custodial_authority_in_supervision_population(state_code)

    supervision_periods = prepare_supervision_periods_for_calculations(
        supervision_periods,
        drop_non_state_custodial_authority_periods=should_drop_non_state_custodial_authority_periods)

    for program_assignment in program_assignments:
        program_referrals = find_program_referrals(
            program_assignment,
            assessments,
            supervision_periods,
            supervision_period_to_agent_associations)

        program_events.extend(program_referrals)

        program_participation_events = find_program_participation_events(
            program_assignment,
            supervision_periods
        )

        program_events.extend(program_participation_events)

    return program_events


def find_program_referrals(
        program_assignment: StateProgramAssignment,
        assessments: List[StateAssessment],
        supervision_periods: List[StateSupervisionPeriod],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]]) -> List[ProgramReferralEvent]:
    """Finds instances of being referred to a program.

    If the program assignment has a referral date, then using date-based logic, connects that referral to assessment
    and supervision data where possible to build ProgramReferralEvents. For assessments, identifies the most recent
    assessment at the time of the referral. For supervision, identifies any supervision periods that were active at the
    time of the referral. If there are multiple overlapping supervision periods, returns one ProgramReferralEvent for
    each unique supervision type for each supervision period that overlapped.

    Returns a list of ProgramReferralEvents.
    """
    program_referrals: List[ProgramReferralEvent] = []

    referral_date = program_assignment.referral_date
    program_id = program_assignment.program_id

    if not program_id:
        program_id = EXTERNAL_UNKNOWN_VALUE

    if referral_date and program_id:
        assessment_score, _, assessment_type = assessment_utils.most_recent_applicable_assessment_attributes_for_class(
            referral_date,
            assessments,
            assessment_class=StateAssessmentClass.RISK,
            state_code=program_assignment.state_code)

        relevant_supervision_periods = find_supervision_periods_overlapping_with_date(
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


def find_program_participation_events(program_assignment: StateProgramAssignment,
                                      supervision_periods: List[StateSupervisionPeriod]) -> \
        List[ProgramParticipationEvent]:
    """Finds instances of actively participating in a program. Produces a ProgramParticipationEvent for each day that
    the person was actively participating in the program. If the program_assignment has a participation_status of
    IN_PROGRESS and has a set start_date, produces a ProgramParticipationEvent for every day between the start_date and
    today. If the program_assignment has a participation_status of DISCHARGED, produces a ProgramParticipationEvent for
    every day between the start_date and discharge_date, end date exclusive.

    Where possible, identifies what types of supervision the person is on on the date of the participation.

    If there are multiple overlapping supervision periods, returns one ProgramParticipationEvent for each supervision
    period that overlaps.

    Returns a list of ProgramReferralEvents.
    """
    program_participation_events: List[ProgramParticipationEvent] = []

    state_code = program_assignment.state_code
    participation_status = program_assignment.participation_status

    if (participation_status not in (StateProgramAssignmentParticipationStatus.IN_PROGRESS,
                                     StateProgramAssignmentParticipationStatus.DISCHARGED)):
        return program_participation_events

    start_date = program_assignment.start_date

    if not start_date:
        return program_participation_events

    discharge_date = program_assignment.discharge_date

    if discharge_date is None:
        if participation_status != StateProgramAssignmentParticipationStatus.IN_PROGRESS:
            logging.warning("StateProgramAssignment with a DISCHARGED status but no discharge_date: %s",
                            program_assignment)
            return program_participation_events

        # This person is actively participating in this program. Set the discharge_date for tomorrow.
        discharge_date = date.today() + relativedelta(days=1)

    program_id = program_assignment.program_id if program_assignment.program_id else EXTERNAL_UNKNOWN_VALUE
    program_location_id = (program_assignment.program_location_id
                           if program_assignment.program_location_id else EXTERNAL_UNKNOWN_VALUE)

    participation_date = start_date

    while participation_date < discharge_date:
        overlapping_supervision_periods = find_supervision_periods_overlapping_with_date(participation_date,
                                                                                         supervision_periods)
        is_first_day_in_program = participation_date == start_date

        if overlapping_supervision_periods:
            for supervision_period in supervision_periods:
                program_participation_events.append(
                    ProgramParticipationEvent(
                        state_code=state_code,
                        event_date=participation_date,
                        is_first_day_in_program=is_first_day_in_program,
                        program_id=program_id,
                        program_location_id=program_location_id,
                        # TODO(#2891): Use supervision_period_supervision_type
                        supervision_type=supervision_period.supervision_type
                    ))
        else:
            program_participation_events.append(ProgramParticipationEvent(
                state_code=state_code,
                event_date=participation_date,
                is_first_day_in_program=is_first_day_in_program,
                program_id=program_id,
                program_location_id=program_location_id
            ))

        participation_date = participation_date + relativedelta(days=1)

    return program_participation_events


def referrals_for_supervision_periods(
        state_code: str, program_id: str, referral_date: date,
        participation_status: Optional[StateProgramAssignmentParticipationStatus],
        assessment_score: Optional[int],
        assessment_type: Optional[StateAssessmentType],
        supervision_periods: Optional[List[StateSupervisionPeriod]],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]]
) -> List[ProgramReferralEvent]:
    """Builds ProgramReferralEvents with data from the relevant supervision periods at the time of the referral.
    Returns one ProgramReferralEvent for each of the supervision periods that overlap with the referral."""
    program_referrals: List[ProgramReferralEvent] = []

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

            program_referrals.append(
                ProgramReferralEvent(
                    state_code=state_code,
                    program_id=program_id,
                    event_date=referral_date,
                    participation_status=participation_status,
                    assessment_score=assessment_score,
                    assessment_type=assessment_type,
                    # TODO(#2891): Use supervision_period_supervision_type
                    supervision_type=supervision_period.supervision_type,
                    supervising_officer_external_id=supervising_officer_external_id,
                    supervising_district_external_id=supervising_district_external_id
                )
            )
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


def find_supervision_periods_overlapping_with_date(
        event_date: date,
        supervision_periods: List[StateSupervisionPeriod],
) -> List[StateSupervisionPeriod]:
    """Identifies supervision_periods where the event_date falls between the start and end of the supervision period,
    inclusive of the start date and exclusive of the end date."""
    return [
        sp for sp in supervision_periods if
        not is_placeholder(sp)
        and sp.start_date is not None
        and sp.start_date <= event_date
        and (sp.termination_date is None or event_date < sp.termination_date)
    ]
