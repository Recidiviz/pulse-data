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
from typing import Any, Dict, List, Optional

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.metrics.base_identifier import (
    BaseIdentifier,
    IdentifierContextT,
)
from recidiviz.calculator.pipeline.metrics.program.events import (
    ProgramEvent,
    ProgramParticipationEvent,
    ProgramReferralEvent,
)
from recidiviz.calculator.pipeline.utils import assessment_utils
from recidiviz.calculator.pipeline.utils.entity_normalization.entity_normalization_manager_utils import (
    entity_normalization_managers_for_periods,
    normalized_program_assignments_for_calculations,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities import (
    NormalizedStateSupervisionPeriod,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.program_assignment_normalization_manager import (
    StateSpecificProgramAssignmentNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.execution_utils import (
    list_of_dicts_to_dict_with_keys,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    supervising_officer_and_location_info,
    supervision_periods_overlapping_with_date,
)
from recidiviz.calculator.query.state.views.reference.supervision_period_to_agent_association import (
    SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StatePerson,
    StateProgramAssignment,
    StateSupervisionPeriod,
)

EXTERNAL_UNKNOWN_VALUE = "EXTERNAL_UNKNOWN"


class ProgramIdentifier(BaseIdentifier[List[ProgramEvent]]):
    """Identifier class for interaction with a program."""

    def __init__(self) -> None:
        self.identifier_event_class = ProgramEvent
        self.field_index = CoreEntityFieldIndex()

    def find_events(
        self, person: StatePerson, identifier_context: IdentifierContextT
    ) -> List[ProgramEvent]:
        if not person.person_id:
            raise ValueError(f"Found StatePerson with unset person_id value: {person}.")

        return self._find_program_events(
            person_id=person.person_id,
            ip_normalization_delegate=identifier_context[
                StateSpecificIncarcerationNormalizationDelegate.__name__
            ],
            sp_normalization_delegate=identifier_context[
                StateSpecificSupervisionNormalizationDelegate.__name__
            ],
            program_assignment_normalization_delegate=identifier_context[
                StateSpecificProgramAssignmentNormalizationDelegate.__name__
            ],
            supervision_delegate=identifier_context[
                StateSpecificSupervisionDelegate.__name__
            ],
            program_assignments=identifier_context[StateProgramAssignment.__name__],
            assessments=identifier_context[StateAssessment.__name__],
            supervision_periods=identifier_context[StateSupervisionPeriod.__name__],
            supervision_period_to_agent_association=identifier_context[
                SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME
            ],
        )

    def _find_program_events(
        self,
        person_id: int,
        ip_normalization_delegate: StateSpecificIncarcerationNormalizationDelegate,
        sp_normalization_delegate: StateSpecificSupervisionNormalizationDelegate,
        program_assignment_normalization_delegate: StateSpecificProgramAssignmentNormalizationDelegate,
        supervision_delegate: StateSpecificSupervisionDelegate,
        program_assignments: List[StateProgramAssignment],
        assessments: List[StateAssessment],
        supervision_periods: List[StateSupervisionPeriod],
        supervision_period_to_agent_association: List[Dict[str, Any]],
    ) -> List[ProgramEvent]:
        """Finds instances of interaction with a program.

        Identifies instances of being referred to a program and actively participating
        in a program.

        Args:
            - program_assignments: All of the person's StateProgramAssignments
            - assessments: All of the person's recorded StateAssessments
            - supervision_periods: All of the person's supervision_periods
            - supervision_period_to_agent_associations: dictionary associating
                StateSupervisionPeriod ids to information about the corresponding
                StateAgent

        Returns:
            A list of ProgramEvents for the person.
        """
        program_events: List[ProgramEvent] = []

        if not program_assignments:
            return program_events

        program_assignments = normalized_program_assignments_for_calculations(
            program_assignment_normalization_delegate=program_assignment_normalization_delegate,
            program_assignments=program_assignments,
        )

        supervision_period_to_agent_associations = list_of_dicts_to_dict_with_keys(
            supervision_period_to_agent_association,
            StateSupervisionPeriod.get_class_id_name(),
        )

        (_, sp_normalization_manager,) = entity_normalization_managers_for_periods(
            person_id=person_id,
            ip_normalization_delegate=ip_normalization_delegate,
            sp_normalization_delegate=sp_normalization_delegate,
            incarceration_periods=None,
            supervision_periods=supervision_periods,
            normalized_violation_responses=None,
            field_index=self.field_index,
            incarceration_sentences=None,
            supervision_sentences=None,
        )

        if not sp_normalization_manager:
            raise ValueError("Expected normalized SPs for this pipeline.")

        # TODO(#10731): Remove calls to sp_normalization_manager and conversion to
        #  NormalizedStateSupervisionPeriods once this metric pipeline is hydrating
        #  Normalized versions of all entities
        (
            processed_sps,
            additional_sp_attributes,
        ) = (
            sp_normalization_manager.normalized_supervision_periods_and_additional_attributes()
        )

        normalized_sps = convert_entity_trees_to_normalized_versions(
            root_entities=processed_sps,
            normalized_entity_class=NormalizedStateSupervisionPeriod,
            additional_attributes_map=additional_sp_attributes,
            field_index=self.field_index,
        )

        for program_assignment in program_assignments:
            program_referrals = self._find_program_referrals(
                program_assignment,
                assessments,
                normalized_sps,
                supervision_period_to_agent_associations,
                supervision_delegate,
            )

            program_events.extend(program_referrals)

            program_participation_events = self._find_program_participation_events(
                program_assignment, normalized_sps
            )

            program_events.extend(program_participation_events)

        return program_events

    def _find_program_referrals(
        self,
        program_assignment: StateProgramAssignment,
        assessments: List[StateAssessment],
        supervision_periods: List[NormalizedStateSupervisionPeriod],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        supervision_delegate: StateSpecificSupervisionDelegate,
    ) -> List[ProgramReferralEvent]:
        """Finds instances of being referred to a program.

        If the program assignment has a referral date, then using date-based logic,
        connects that referral to assessment and supervision data where possible to
        build ProgramReferralEvents. For assessments, identifies the most recent
        assessment at the time of the referral. For supervision, identifies any
        supervision periods that were active at the time of the referral. If there
        are multiple overlapping supervision periods, returns one ProgramReferralEvent
        for each unique supervision type for each supervision period that overlapped.

        Returns a list of ProgramReferralEvents.
        """
        program_referrals: List[ProgramReferralEvent] = []

        referral_date = program_assignment.referral_date
        program_id = program_assignment.program_id

        if not program_id:
            program_id = EXTERNAL_UNKNOWN_VALUE

        if referral_date and program_id:
            (
                assessment_score,
                _,
                assessment_type,
            ) = assessment_utils.most_recent_applicable_assessment_attributes_for_class(
                referral_date,
                assessments,
                assessment_class=StateAssessmentClass.RISK,
                supervision_delegate=supervision_delegate,
            )

            relevant_supervision_periods = supervision_periods_overlapping_with_date(
                referral_date, supervision_periods
            )

            program_referrals.extend(
                self._referrals_for_supervision_periods(
                    program_assignment.state_code,
                    program_id,
                    referral_date,
                    program_assignment.participation_status,
                    assessment_score,
                    assessment_type,
                    relevant_supervision_periods,
                    supervision_period_to_agent_associations,
                    supervision_delegate,
                )
            )

        return program_referrals

    def _find_program_participation_events(
        self,
        program_assignment: StateProgramAssignment,
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
            StateProgramAssignmentParticipationStatus.DISCHARGED,
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

    def _referrals_for_supervision_periods(
        self,
        state_code: str,
        program_id: str,
        referral_date: date,
        participation_status: Optional[StateProgramAssignmentParticipationStatus],
        assessment_score: Optional[int],
        assessment_type: Optional[StateAssessmentType],
        supervision_periods: Optional[List[NormalizedStateSupervisionPeriod]],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        supervision_delegate: StateSpecificSupervisionDelegate,
    ) -> List[ProgramReferralEvent]:
        """Builds ProgramReferralEvents with data from the relevant supervision periods
        at the time of the referral.

        # TODO(#8818): Consider producing only one ProgramReferralEvent per referral
        Returns one ProgramReferralEvent for each of the supervision periods that
        overlap with the referral."""
        program_referrals: List[ProgramReferralEvent] = []

        assessment_score_bucket = assessment_utils.assessment_score_bucket(
            assessment_type=assessment_type,
            assessment_score=assessment_score,
            assessment_level=None,
            supervision_delegate=supervision_delegate,
        )

        if supervision_periods:
            for supervision_period in supervision_periods:
                # Return one ProgramReferralEvent per supervision period
                (
                    supervising_officer_external_id,
                    level_1_supervision_location_external_id,
                    level_2_supervision_location_external_id,
                ) = supervising_officer_and_location_info(
                    supervision_period,
                    supervision_period_to_agent_associations,
                    supervision_delegate,
                )

                deprecated_supervising_district_external_id = (
                    level_2_supervision_location_external_id
                    or level_1_supervision_location_external_id
                )

                program_referrals.append(
                    ProgramReferralEvent(
                        state_code=state_code,
                        program_id=program_id,
                        event_date=referral_date,
                        participation_status=participation_status,
                        assessment_score=assessment_score,
                        assessment_type=assessment_type,
                        assessment_score_bucket=assessment_score_bucket,
                        supervision_type=supervision_period.supervision_type,
                        supervising_officer_external_id=supervising_officer_external_id,
                        supervising_district_external_id=deprecated_supervising_district_external_id,
                        level_1_supervision_location_external_id=level_1_supervision_location_external_id,
                        level_2_supervision_location_external_id=level_2_supervision_location_external_id,
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
                    assessment_type=assessment_type,
                    assessment_score_bucket=assessment_score_bucket,
                )
            ]

        return program_referrals
