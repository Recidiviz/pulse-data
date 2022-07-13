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
"""Identifies various events related to supervision."""
import datetime
import logging
from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Optional, Tuple, Type, Union

import attr
from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.metrics.base_identifier import (
    BaseIdentifier,
    IdentifierContext,
)
from recidiviz.calculator.pipeline.metrics.supervision.events import (
    ProjectedSupervisionCompletionEvent,
    SupervisionEvent,
    SupervisionPopulationEvent,
    SupervisionStartEvent,
    SupervisionTerminationEvent,
)
from recidiviz.calculator.pipeline.metrics.supervision.metrics import (
    SupervisionMetricType,
)
from recidiviz.calculator.pipeline.metrics.supervision.supervision_case_compliance import (
    SupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.metrics.utils.violation_utils import (
    filter_violation_responses_for_violation_history,
    get_violation_and_response_history,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    sort_normalized_entities_by_sequence_num,
)
from recidiviz.calculator.pipeline.utils import assessment_utils
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_incarceration_period_index import (
    NormalizedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.execution_utils import (
    list_of_dicts_to_dict_with_keys,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_state_specific_case_compliance_manager,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.supervision_period_utils import (
    identify_most_severe_case_type,
    should_produce_supervision_event_for_period,
    supervising_officer_and_location_info,
)
from recidiviz.calculator.pipeline.utils.supervision_type_identification import (
    sentence_supervision_type_to_supervision_periods_supervision_type,
)
from recidiviz.calculator.query.state.views.reference.supervision_period_judicial_district_association import (
    SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.supervision_period_to_agent_association import (
    SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.common.date import DateRange, DateRangeDiff, last_day_of_month
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateIncarcerationSentence,
    StatePerson,
    StateSupervisionContact,
    StateSupervisionSentence,
)


class SupervisionIdentifier(BaseIdentifier[List[SupervisionEvent]]):
    """Identifier class for events related to supervision."""

    def __init__(self) -> None:
        self.identifier_result_class = SupervisionEvent
        self.field_index = CoreEntityFieldIndex()

    def identify(
        self, person: StatePerson, identifier_context: IdentifierContext
    ) -> List[SupervisionEvent]:
        return self._find_supervision_events(
            incarceration_delegate=identifier_context[
                StateSpecificIncarcerationDelegate.__name__
            ],
            supervision_delegate=identifier_context[
                StateSpecificSupervisionDelegate.__name__
            ],
            violation_delegate=identifier_context[
                StateSpecificViolationDelegate.__name__
            ],
            person=person,
            supervision_sentences=identifier_context[StateSupervisionSentence.__name__],
            incarceration_sentences=identifier_context[
                StateIncarcerationSentence.__name__
            ],
            supervision_periods=identifier_context[
                NormalizedStateSupervisionPeriod.base_class_name()
            ],
            incarceration_periods=identifier_context[
                NormalizedStateIncarcerationPeriod.base_class_name()
            ],
            assessments=identifier_context[StateAssessment.__name__],
            violation_responses=identifier_context[
                NormalizedStateSupervisionViolationResponse.base_class_name()
            ],
            supervision_contacts=identifier_context[StateSupervisionContact.__name__],
            supervision_period_to_agent_association=identifier_context[
                SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME
            ],
            supervision_period_judicial_district_association=identifier_context[
                SUPERVISION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME
            ],
        )

    def _find_supervision_events(
        self,
        incarceration_delegate: StateSpecificIncarcerationDelegate,
        supervision_delegate: StateSpecificSupervisionDelegate,
        violation_delegate: StateSpecificViolationDelegate,
        person: StatePerson,
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_periods: List[NormalizedStateSupervisionPeriod],
        incarceration_periods: List[NormalizedStateIncarcerationPeriod],
        assessments: List[StateAssessment],
        violation_responses: List[NormalizedStateSupervisionViolationResponse],
        supervision_contacts: List[StateSupervisionContact],
        supervision_period_to_agent_association: List[Dict[str, Any]],
        supervision_period_judicial_district_association: List[Dict[str, Any]],
    ) -> List[SupervisionEvent]:
        """Identifies various events related to being on supervision.

        Args:
            - supervision_sentences: list of StateSupervisionSentences for a person
            - supervision_periods: list of StateSupervisionPeriods for a person
            - incarceration_periods: list of StateIncarcerationPeriods for a person
            - assessments: list of StateAssessments for a person
            - violations: list of StateSupervisionViolations for a person
            - violation_responses: list of StateSupervisionViolationResponses for a person
            - supervision_period_to_agent_associations: A list of dictionaries associating StateSupervisionPeriod ids to
                information about the corresponding StateAgent
            - supervision_period_judicial_district_association: a list of dictionaries with information connecting
                StateSupervisionPeriod ids to the judicial district responsible for the period of supervision

        Returns:
            A list of SupervisionEvents for the person.
        """
        if not person.person_id:
            raise ValueError(f"Found StatePerson with unset person_id value: {person}.")

        if not supervision_periods and not incarceration_periods:
            return []

        supervision_period_to_judicial_district_associations = (
            list_of_dicts_to_dict_with_keys(
                supervision_period_judicial_district_association,
                NormalizedStateSupervisionPeriod.get_class_id_name(),
            )
        )

        supervision_period_to_agent_associations = list_of_dicts_to_dict_with_keys(
            supervision_period_to_agent_association,
            NormalizedStateSupervisionPeriod.get_class_id_name(),
        )

        incarceration_period_index = NormalizedIncarcerationPeriodIndex(
            sorted_incarceration_periods=incarceration_periods,
            incarceration_delegate=incarceration_delegate,
        )

        supervision_period_index = NormalizedSupervisionPeriodIndex(
            sorted_supervision_periods=supervision_periods
        )

        sorted_violation_responses = sort_normalized_entities_by_sequence_num(
            violation_responses
        )

        violation_responses_for_history = (
            filter_violation_responses_for_violation_history(
                violation_delegate,
                violation_responses=sorted_violation_responses,
                include_follow_up_responses=False,
            )
        )

        supervision_events: List[SupervisionEvent] = []

        projected_supervision_completion_events = self._classify_supervision_success(
            supervision_sentences,
            incarceration_sentences,
            supervision_period_index,
            incarceration_period_index,
            supervision_delegate,
            supervision_period_to_agent_associations,
            supervision_period_to_judicial_district_associations,
        )

        supervision_events.extend(projected_supervision_completion_events)
        for supervision_period in supervision_period_index.sorted_supervision_periods:
            if should_produce_supervision_event_for_period(supervision_period):
                judicial_district_code = self._get_judicial_district_code(
                    supervision_period,
                    supervision_period_to_judicial_district_associations,
                )

                supervision_events.extend(
                    self._find_population_events_for_supervision_period(
                        person=person,
                        supervision_sentences=supervision_sentences,
                        incarceration_sentences=incarceration_sentences,
                        supervision_period=supervision_period,
                        supervision_period_index=supervision_period_index,
                        incarceration_period_index=incarceration_period_index,
                        assessments=assessments,
                        violation_responses_for_history=violation_responses_for_history,
                        supervision_contacts=supervision_contacts,
                        supervision_period_to_agent_associations=supervision_period_to_agent_associations,
                        violation_delegate=violation_delegate,
                        supervision_delegate=supervision_delegate,
                        judicial_district_code=judicial_district_code,
                    )
                )

                supervision_termination_event = self._find_supervision_termination_event(
                    supervision_period=supervision_period,
                    supervision_period_index=supervision_period_index,
                    incarceration_period_index=incarceration_period_index,
                    assessments=assessments,
                    violation_responses_for_history=violation_responses_for_history,
                    supervision_period_to_agent_associations=supervision_period_to_agent_associations,
                    violation_delegate=violation_delegate,
                    supervision_delegate=supervision_delegate,
                    judicial_district_code=judicial_district_code,
                )

                if supervision_termination_event:
                    supervision_events.append(supervision_termination_event)

                supervision_start_event = self._find_supervision_start_event(
                    supervision_period=supervision_period,
                    supervision_period_index=supervision_period_index,
                    incarceration_period_index=incarceration_period_index,
                    supervision_delegate=supervision_delegate,
                    supervision_period_to_agent_associations=supervision_period_to_agent_associations,
                    judicial_district_code=judicial_district_code,
                )
                if supervision_start_event:
                    supervision_events.append(supervision_start_event)

        if supervision_delegate.supervision_types_mutually_exclusive():
            supervision_events = self._convert_events_to_dual(supervision_events)
        else:
            supervision_events = self._expand_dual_supervision_events(
                supervision_events
            )

        return supervision_events

    def _find_population_events_for_supervision_period(
        self,
        person: StatePerson,
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_period: NormalizedStateSupervisionPeriod,
        supervision_period_index: NormalizedSupervisionPeriodIndex,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        assessments: List[StateAssessment],
        violation_responses_for_history: List[
            NormalizedStateSupervisionViolationResponse
        ],
        supervision_contacts: List[StateSupervisionContact],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        violation_delegate: StateSpecificViolationDelegate,
        supervision_delegate: StateSpecificSupervisionDelegate,
        judicial_district_code: Optional[str] = None,
    ) -> List[SupervisionPopulationEvent]:
        """Finds days that this person was on supervision for the given
        StateSupervisionPeriod.

        Args:
            - person: StatePerson encoding of the person under supervision
            - supervision_sentences: List of StateSupervisionSentences for a person
            - incarceration_sentences: List of StateIncarcerationSentence for a person
            - supervision_period: The supervision period the person was on
            - supervision_period_index: Class containing information about this person's supervision periods
            - incarceration_period_index: Class containing information about this person's incarceration periods
            - assessments: List of StateAssessment for a person
            - violation_responses_for_history: List of
                StateSupervisionViolationResponse for a person, sorted by response_date and
                filtered to those that are applicable when analyzing violation history
            - supervision_period_to_agent_associations: dictionary associating StateSupervisionPeriod ids to information
                about the corresponding StateAgent on the period
            - violation_delegate: the state-specific violation delegate
            - judicial_district_code: The judicial district responsible for the period of supervision
        Returns
            - A set of unique SupervisionPopulationEvents for the person for the given
            StateSupervisionPeriod.
        """
        supervision_population_events: List[SupervisionPopulationEvent] = []

        start_date = supervision_period.start_date
        termination_date = supervision_period.termination_date

        if start_date is None:
            raise ValueError(
                "Unexpected missing start_date. Inconsistent periods should have been fixed or dropped at "
                "this point."
            )

        event_date = start_date

        (
            supervising_officer_external_id,
            level_1_supervision_location_external_id,
            level_2_supervision_location_external_id,
        ) = supervising_officer_and_location_info(
            supervision_period,
            supervision_period_to_agent_associations,
            supervision_delegate,
        )
        case_type = identify_most_severe_case_type(supervision_period)

        if not supervision_period.supervision_period_id:
            raise ValueError(
                "Unexpected supervision period without a supervision_period_id."
            )

        start_of_supervision = (
            supervision_period_index.supervision_start_dates_by_period_id.get(
                supervision_period.supervision_period_id
            )
        )

        if not start_of_supervision:
            raise ValueError(
                "SupervisionPeriodIndex.supervision_start_dates_by_period_id incomplete."
            )

        state_specific_case_compliance_manager = (
            # TODO(#10891): Change how the StateSupervisionCaseComplianceManager
            #  manager is structured (with one base manager class and state-specific
            #  delegates) so that we can get the state-specific delegate without
            #  having all of the entities ready.
            get_state_specific_case_compliance_manager(
                person,
                supervision_period,
                case_type,
                start_of_supervision,
                assessments,
                supervision_contacts,
                violation_responses_for_history,
                incarceration_sentences,
                incarceration_period_index,
                supervision_delegate,
            )
        )

        end_date = (
            termination_date
            if termination_date
            else date.today() + relativedelta(days=1)
        )

        while event_date < end_date:
            if self._in_supervision_population_for_period_on_date(
                event_date,
                supervision_period,
                incarceration_period_index,
                supervision_delegate,
                supervising_officer_external_id,
            ):

                supervision_type = (
                    supervision_period.supervision_type
                    if supervision_period.supervision_type
                    else StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
                )

                assessment_score = None
                assessment_level = None
                assessment_type = None

                most_recent_assessment = assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                    event_date,
                    assessments,
                    assessment_class=StateAssessmentClass.RISK,
                    supervision_delegate=supervision_delegate,
                )

                if most_recent_assessment:
                    assessment_score = most_recent_assessment.assessment_score
                    assessment_level = most_recent_assessment.assessment_level
                    assessment_type = most_recent_assessment.assessment_type

                assessment_score_bucket = assessment_utils.assessment_score_bucket(
                    assessment_type,
                    assessment_score,
                    assessment_level,
                    supervision_delegate,
                )

                violation_history = get_violation_and_response_history(
                    upper_bound_exclusive_date=(event_date + relativedelta(days=1)),
                    violation_responses_for_history=violation_responses_for_history,
                    violation_delegate=violation_delegate,
                )

                case_compliance: Optional[SupervisionCaseCompliance] = None

                if state_specific_case_compliance_manager:
                    case_compliance = state_specific_case_compliance_manager.get_case_compliance_on_date(
                        event_date
                    )

                deprecated_supervising_district_external_id = (
                    level_2_supervision_location_external_id
                    or level_1_supervision_location_external_id
                )

                supervision_level_downgrade_occurred = False
                previous_supervision_level = None
                if event_date == supervision_period.start_date:
                    (
                        supervision_level_downgrade_occurred,
                        previous_supervision_level,
                    ) = self._get_supervision_downgrade_details_if_downgrade_occurred(
                        supervision_period_index, supervision_period
                    )

                projected_end_date = supervision_delegate.get_projected_completion_date(
                    supervision_period=supervision_period,
                    incarceration_sentences=incarceration_sentences,
                    supervision_sentences=supervision_sentences,
                )

                supervision_population_events.append(
                    SupervisionPopulationEvent(
                        state_code=supervision_period.state_code,
                        year=event_date.year,
                        month=event_date.month,
                        event_date=event_date,
                        supervision_type=supervision_type,
                        case_type=case_type,
                        assessment_score=assessment_score,
                        assessment_level=assessment_level,
                        assessment_type=assessment_type,
                        assessment_score_bucket=assessment_score_bucket,
                        most_severe_violation_type=violation_history.most_severe_violation_type,
                        most_severe_violation_type_subtype=violation_history.most_severe_violation_type_subtype,
                        most_severe_response_decision=violation_history.most_severe_response_decision,
                        response_count=violation_history.response_count,
                        supervising_officer_external_id=supervising_officer_external_id,
                        supervising_district_external_id=deprecated_supervising_district_external_id,
                        level_1_supervision_location_external_id=level_1_supervision_location_external_id,
                        level_2_supervision_location_external_id=level_2_supervision_location_external_id,
                        supervision_level=supervision_period.supervision_level,
                        supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                        case_compliance=case_compliance,
                        judicial_district_code=judicial_district_code,
                        custodial_authority=supervision_period.custodial_authority,
                        supervision_level_downgrade_occurred=supervision_level_downgrade_occurred,
                        previous_supervision_level=previous_supervision_level,
                        projected_end_date=projected_end_date,
                    )
                )

            event_date = event_date + relativedelta(days=1)

        return supervision_population_events

    def _supervision_period_counts_towards_supervision_population_in_date_range(
        self,
        date_range: DateRange,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        supervision_period: NormalizedStateSupervisionPeriod,
        supervising_officer_external_id: Optional[str],
        supervision_delegate: StateSpecificSupervisionDelegate,
    ) -> bool:
        """Returns True if the existence of the |supervision_period| means a person can be counted towards the supervision
        population in the provided date range.
        """
        is_excluded_from_supervision_population_for_range = incarceration_period_index.is_excluded_from_supervision_population_for_range(
            date_range
        )

        if is_excluded_from_supervision_population_for_range:
            return False

        supervision_overlapping_range = DateRangeDiff(
            supervision_period.duration, date_range
        ).overlapping_range

        if not supervision_overlapping_range:
            return False

        return supervision_delegate.supervision_period_in_supervision_population_in_non_excluded_date_range(
            supervision_period=supervision_period,
            supervising_officer_external_id=supervising_officer_external_id,
        )

    def _in_supervision_population_for_period_on_date(
        self,
        evaluation_date: date,
        supervision_period: NormalizedStateSupervisionPeriod,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        supervision_delegate: StateSpecificSupervisionDelegate,
        supervising_officer_external_id: Optional[str],
        validate_duration: bool = True,
    ) -> bool:
        """Determines whether the person was on supervision on a given date."""
        if validate_duration and not supervision_period.duration.contains_day(
            evaluation_date
        ):
            raise ValueError(
                "evaluation_date must fall between the start and end of the supervision_period"
            )

        day_range = DateRange.for_day(evaluation_date)

        supervision_period_counts_towards_supervision_population_on_date = self._supervision_period_counts_towards_supervision_population_in_date_range(
            date_range=day_range,
            supervision_period=supervision_period,
            incarceration_period_index=incarceration_period_index,
            supervising_officer_external_id=supervising_officer_external_id,
            supervision_delegate=supervision_delegate,
        )

        return supervision_period_counts_towards_supervision_population_on_date

    def _in_supervision_population_on_date(
        self,
        evaluation_date: date,
        supervision_period_index: NormalizedSupervisionPeriodIndex,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        supervision_delegate: StateSpecificSupervisionDelegate,
        supervising_officer_external_id: Optional[str],
    ) -> bool:
        """Determines whether the person was on supervision on a given date, across any and
        all of the supervision periods included in the given supervision period index.

        This calls out to on_supervision_on_date for each supervision period in the
        supervision period index.
        """
        for period in supervision_period_index.sorted_supervision_periods:
            # validate_duration=False because in this use case we are handling supervision
            # periods that may have, for example, the same start and termination date
            if self._in_supervision_population_for_period_on_date(
                evaluation_date,
                period,
                incarceration_period_index,
                supervision_delegate,
                supervising_officer_external_id,
                validate_duration=False,
            ):
                return True

        return False

    def _find_supervision_start_event(
        self,
        supervision_period: NormalizedStateSupervisionPeriod,
        supervision_period_index: NormalizedSupervisionPeriodIndex,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        supervision_delegate: StateSpecificSupervisionDelegate,
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        judicial_district_code: Optional[str] = None,
    ) -> Optional[SupervisionEvent]:
        """Identifies an instance of supervision start, assuming the provided |supervision_period| has a valid start
        date, and returns all relevant info as a SupervisionStartEvent.
        """

        # Do not create start metrics if no start date
        if supervision_period.start_date is None:
            return None

        start_date = supervision_period.start_date
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

        in_incarceration_population_on_date = (
            incarceration_period_index.was_in_incarceration_population_on_date(
                start_date
            )
        )

        in_supervision_population_on_date = self._in_supervision_population_on_date(
            start_date,
            supervision_period_index,
            incarceration_period_index,
            supervision_delegate,
            supervising_officer_external_id,
        )

        return SupervisionStartEvent(
            state_code=supervision_period.state_code,
            admission_reason=supervision_period.admission_reason,
            event_date=start_date,
            year=start_date.year,
            month=start_date.month,
            in_incarceration_population_on_date=in_incarceration_population_on_date,
            in_supervision_population_on_date=in_supervision_population_on_date,
            supervision_type=supervision_period.supervision_type,
            case_type=identify_most_severe_case_type(supervision_period),
            supervision_level=supervision_period.supervision_level,
            supervision_level_raw_text=supervision_period.supervision_level_raw_text,
            supervising_officer_external_id=supervising_officer_external_id,
            supervising_district_external_id=deprecated_supervising_district_external_id,
            level_1_supervision_location_external_id=level_1_supervision_location_external_id,
            level_2_supervision_location_external_id=level_2_supervision_location_external_id,
            judicial_district_code=judicial_district_code,
            custodial_authority=supervision_period.custodial_authority,
        )

    def _find_supervision_termination_event(
        self,
        supervision_period: NormalizedStateSupervisionPeriod,
        supervision_period_index: NormalizedSupervisionPeriodIndex,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        assessments: List[StateAssessment],
        violation_responses_for_history: List[
            NormalizedStateSupervisionViolationResponse
        ],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        violation_delegate: StateSpecificViolationDelegate,
        supervision_delegate: StateSpecificSupervisionDelegate,
        judicial_district_code: Optional[str] = None,
    ) -> Optional[SupervisionEvent]:
        """Identifies an instance of supervision termination. If the given supervision_period has a valid start_date and
        termination_date, then returns a SupervisionTerminationEvent with the details of the termination.

        Calculates the change in assessment score from the beginning of supervision to the termination of supervision.
        This is done by identifying the first reassessment (or, the second score) and the last score during a
        supervision period, and taking the difference between the last score and the first reassessment score.

        If a person has multiple supervision periods that end in a given month, the the earliest start date and the latest
        termination date of the periods is used to estimate the start and end dates of the supervision. These dates
        are then used to determine what the second and last assessment scores are. However, the termination_date on the
        SupervisionTerminationEvent will always be the termination_date on the supervision_period.

        If this supervision does not have a termination_date, then None is returned.
        """
        if (
            supervision_period.start_date is not None
            and supervision_period.termination_date is not None
        ):
            assessment_start_date = supervision_period.start_date
            termination_date = supervision_period.termination_date
            assessment_termination_date = supervision_period.termination_date

            termination_year = termination_date.year
            termination_month = termination_date.month

            periods_terminated_in_year = (
                supervision_period_index.supervision_periods_by_termination_month.get(
                    termination_year
                )
            )

            if periods_terminated_in_year:
                periods_terminated_in_month = periods_terminated_in_year.get(
                    termination_month
                )

                if periods_terminated_in_month:
                    for period in periods_terminated_in_month:
                        if (
                            period.start_date
                            and period.start_date < assessment_start_date
                        ):
                            assessment_start_date = period.start_date

                        if (
                            period.termination_date
                            and period.termination_date > termination_date
                        ):
                            assessment_termination_date = period.termination_date

            (
                assessment_score_change,
                end_assessment_score,
                end_assessment_level,
                end_assessment_type,
            ) = self._find_assessment_score_change(
                assessment_start_date,
                assessment_termination_date,
                assessments,
                supervision_delegate,
            )
            end_assessment_score_bucket = assessment_utils.assessment_score_bucket(
                end_assessment_type,
                end_assessment_score,
                end_assessment_level,
                supervision_delegate,
            )

            violation_history = get_violation_and_response_history(
                upper_bound_exclusive_date=(termination_date + relativedelta(days=1)),
                violation_responses_for_history=violation_responses_for_history,
                violation_delegate=violation_delegate,
            )

            (
                supervising_officer_external_id,
                level_1_supervision_location_external_id,
                level_2_supervision_location_external_id,
            ) = supervising_officer_and_location_info(
                supervision_period,
                supervision_period_to_agent_associations,
                supervision_delegate,
            )

            case_type = identify_most_severe_case_type(supervision_period)

            supervision_type = (
                supervision_period.supervision_type
                if supervision_period.supervision_type
                else StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
            )

            deprecated_supervising_district_external_id = (
                level_2_supervision_location_external_id
                or level_1_supervision_location_external_id
            )

            in_incarceration_population_on_date = (
                incarceration_period_index.was_in_incarceration_population_on_date(
                    termination_date
                )
            )

            # We check if the person is counted in the supervision population on the day
            # before the termination date, because the termination date is exclusive, and
            # what we are trying to determine is whether this termination comes at the end
            # of some number of days of being counted in the supervision population
            in_supervision_population_on_date = self._in_supervision_population_on_date(
                termination_date - relativedelta(days=1),
                supervision_period_index,
                incarceration_period_index,
                supervision_delegate,
                supervising_officer_external_id,
            )

            return SupervisionTerminationEvent(
                state_code=supervision_period.state_code,
                event_date=termination_date,
                year=termination_date.year,
                month=termination_date.month,
                in_incarceration_population_on_date=in_incarceration_population_on_date,
                in_supervision_population_on_date=in_supervision_population_on_date,
                supervision_type=supervision_type,
                case_type=case_type,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                assessment_score=end_assessment_score,
                assessment_level=end_assessment_level,
                assessment_type=end_assessment_type,
                assessment_score_bucket=end_assessment_score_bucket,
                termination_reason=supervision_period.termination_reason,
                assessment_score_change=assessment_score_change,
                supervising_officer_external_id=supervising_officer_external_id,
                supervising_district_external_id=deprecated_supervising_district_external_id,
                level_1_supervision_location_external_id=level_1_supervision_location_external_id,
                level_2_supervision_location_external_id=level_2_supervision_location_external_id,
                judicial_district_code=judicial_district_code,
                custodial_authority=supervision_period.custodial_authority,
                response_count=violation_history.response_count,
                most_severe_response_decision=violation_history.most_severe_response_decision,
                most_severe_violation_type=violation_history.most_severe_violation_type,
                most_severe_violation_type_subtype=violation_history.most_severe_violation_type_subtype,
            )

        return None

    def _classify_supervision_success(
        self,
        supervision_sentences: List[StateSupervisionSentence],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_period_index: NormalizedSupervisionPeriodIndex,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        supervision_delegate: StateSpecificSupervisionDelegate,
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        supervision_period_to_judicial_district_associations: Dict[int, Dict[Any, Any]],
    ) -> List[ProjectedSupervisionCompletionEvent]:
        """This classifies whether supervision projected to end in a given month was completed successfully.

        Will use both incarceration and supervision sentences in determining supervision success.

        For supervision and incarceration sentence with a projected_completion_date, where that date is before or on today's date,
        and the sentence has a set completion_date, looks at all supervision periods that have terminated and
        finds the one with the latest termination date. From that supervision period, classifies the termination as either
        successful or not successful.
        """
        projected_completion_events: List[ProjectedSupervisionCompletionEvent] = []

        all_sentences: List[
            Union[StateIncarcerationSentence, StateSupervisionSentence]
        ] = []
        all_sentences.extend(supervision_sentences)
        all_sentences.extend(incarceration_sentences)

        for sentence in all_sentences:
            sentence_start_date = sentence.start_date
            sentence_completion_date = sentence.completion_date

            # These fields must be set to be included in any supervision success metrics
            # Ignore sentences with erroneous start_dates in the future
            if (
                not sentence_start_date
                or not sentence_completion_date
                or sentence_start_date > date.today()
            ):
                continue

            if isinstance(sentence, StateIncarcerationSentence):
                # This handles max_length_days that would otherwise cause a null or overflow error
                if (
                    not sentence.max_length_days
                    or sentence.max_length_days > 547500  # 1500 years
                ):
                    continue

                projected_completion_date = sentence_start_date + datetime.timedelta(
                    days=sentence.max_length_days
                )

                sentence_supervision_type: Optional[
                    StateSupervisionSentenceSupervisionType
                ] = StateSupervisionSentenceSupervisionType.PAROLE
            elif isinstance(sentence, StateSupervisionSentence):
                if not sentence.projected_completion_date:
                    continue
                projected_completion_date = sentence.projected_completion_date
                sentence_supervision_type = sentence.supervision_type
            else:
                raise ValueError(
                    f"Sentence instance type does not match expected sentence types. "
                    f"Expected: StateIncarcerationSentence/StateSupervisionSentence, "
                    f"Actual: {type(sentence)}"
                )

            # Only include sentences that are projected to have already ended
            if (
                not projected_completion_date
                or projected_completion_date > date.today()
            ):
                continue

            latest_supervision_period = None
            supervision_period_supervision_type_for_sentence = (
                sentence_supervision_type_to_supervision_periods_supervision_type(
                    sentence_supervision_type
                )
            ) or StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN

            for (
                supervision_period
            ) in supervision_period_index.sorted_supervision_periods:
                termination_date = supervision_period.termination_date

                # Skips supervision periods without termination dates or not within sentence bounds
                if (
                    not termination_date
                    or not supervision_period.start_date
                    or sentence_start_date > supervision_period.start_date
                    or sentence_completion_date < termination_date
                ):
                    continue

                # If there is a non-null sentence_supervision_type and
                # supervision_type on the period, assert that they
                # match
                if (
                    supervision_period.supervision_type
                    and sentence_supervision_type
                    and supervision_period.supervision_type
                    != supervision_period_supervision_type_for_sentence
                ):
                    continue

                if (
                    not latest_supervision_period
                    or latest_supervision_period.termination_date < termination_date
                ):
                    latest_supervision_period = supervision_period

            if latest_supervision_period:
                completion_event = self._get_projected_completion_event(
                    sentence=sentence,
                    projected_completion_date=projected_completion_date,
                    supervision_period=latest_supervision_period,
                    supervision_type_for_event=supervision_period_supervision_type_for_sentence,
                    incarceration_period_index=incarceration_period_index,
                    supervision_delegate=supervision_delegate,
                    supervision_period_to_agent_associations=supervision_period_to_agent_associations,
                    supervision_period_to_judicial_district_associations=supervision_period_to_judicial_district_associations,
                )

                if completion_event:
                    projected_completion_events.append(completion_event)

        return projected_completion_events

    def _get_projected_completion_event(
        self,
        sentence: Union[StateSupervisionSentence, StateIncarcerationSentence],
        projected_completion_date: date,
        supervision_period: NormalizedStateSupervisionPeriod,
        supervision_type_for_event: StateSupervisionPeriodSupervisionType,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        supervision_delegate: StateSpecificSupervisionDelegate,
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        supervision_period_to_judicial_district_associations: Dict[int, Dict[Any, Any]],
    ) -> Optional[ProjectedSupervisionCompletionEvent]:
        """Returns a ProjectedSupervisionCompletionEvent for the given supervision
        sentence and its last terminated period, if the sentence should be included
        in the success metric counts. If the sentence should not be included in success
        metrics, then returns None."""
        termination_reason = supervision_period.termination_reason

        # TODO(#2596): Assert that the sentence status is COMPLETED or COMMUTED to
        #  qualify as successful
        (
            include_termination_in_success_metric,
            successful_completion,
        ) = self._termination_is_successful_if_should_include_in_success_metric(
            termination_reason
        )

        if not include_termination_in_success_metric:
            return None

        if successful_completion is None:
            raise ValueError(
                "Expected set value for successful_completion if the "
                "termination should be included in the metrics."
            )

        start_date = sentence.start_date
        completion_date = sentence.completion_date
        if start_date is None or completion_date is None:
            logging.warning(
                "start_date and completion_date must be non-None for sentence: %s",
                sentence,
            )
            return None

        if completion_date < start_date:
            logging.warning(
                "Sentence completion date is before the start date: %s",
                sentence,
            )
            return None

        sentence_days_served = (completion_date - start_date).days

        incarcerated_during_sentence = (
            incarceration_period_index.incarceration_admissions_between_dates(
                start_date, completion_date
            )
        )

        (
            supervising_officer_external_id,
            level_1_supervision_location_external_id,
            level_2_supervision_location_external_id,
        ) = supervising_officer_and_location_info(
            supervision_period,
            supervision_period_to_agent_associations,
            supervision_delegate,
        )

        case_type = identify_most_severe_case_type(supervision_period)

        judicial_district_code = self._get_judicial_district_code(
            supervision_period, supervision_period_to_judicial_district_associations
        )

        last_day_of_projected_month = last_day_of_month(projected_completion_date)

        deprecated_supervising_district_external_id = (
            level_2_supervision_location_external_id
            or level_1_supervision_location_external_id
        )

        # TODO(#2975): Note that this metric measures success by projected completion month. Update or expand this
        #  metric to capture the success of early termination as well
        return ProjectedSupervisionCompletionEvent(
            state_code=supervision_period.state_code,
            year=projected_completion_date.year,
            month=projected_completion_date.month,
            event_date=last_day_of_projected_month,
            supervision_type=supervision_type_for_event,
            supervision_level=supervision_period.supervision_level,
            supervision_level_raw_text=supervision_period.supervision_level_raw_text,
            case_type=case_type,
            successful_completion=successful_completion,
            incarcerated_during_sentence=incarcerated_during_sentence,
            sentence_days_served=sentence_days_served,
            supervising_officer_external_id=supervising_officer_external_id,
            supervising_district_external_id=deprecated_supervising_district_external_id,
            level_1_supervision_location_external_id=level_1_supervision_location_external_id,
            level_2_supervision_location_external_id=level_2_supervision_location_external_id,
            judicial_district_code=judicial_district_code,
            custodial_authority=supervision_period.custodial_authority,
        )

    def _termination_is_successful_if_should_include_in_success_metric(
        self,
        termination_reason: Optional[StateSupervisionPeriodTerminationReason],
    ) -> Tuple[bool, Optional[bool]]:
        """Determines whether the given termination of supervision should be included in
        a supervision success metric and, if it should be included, determines whether a
        termination of supervision with the given |termination_reason| should be
        considered a successful termination.

        Returns a tuple in the format [bool, Optional[bool]], representing
        (should_include_in_success_metric, termination_was_successful).
        """
        # If this is the last supervision period termination status, there is some kind
        # of data error and we cannot determine whether this sentence ended
        # successfully - exclude these entirely
        if not termination_reason or termination_reason in (
            StateSupervisionPeriodTerminationReason.EXTERNAL_UNKNOWN,
            StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
            StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION,
            StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
        ):
            return False, None

        # If this is the last supervision period termination status, then the
        # supervision sentence ended in some sort of truncated, not-necessarily
        # successful manner unrelated to a failure on supervision - exclude these
        # entirely
        if termination_reason in (
            StateSupervisionPeriodTerminationReason.DEATH,
            StateSupervisionPeriodTerminationReason.TRANSFER_TO_OTHER_JURISDICTION,
            StateSupervisionPeriodTerminationReason.SUSPENSION,
        ):
            return False, None

        # If the last period is an investigative period, then the person was never
        # actually sentenced formally. In this case we should not include the period
        # in our "success" metrics.
        if termination_reason == StateSupervisionPeriodTerminationReason.INVESTIGATION:
            return False, None

        if termination_reason in (
            # Successful terminations
            StateSupervisionPeriodTerminationReason.COMMUTED,
            StateSupervisionPeriodTerminationReason.DISCHARGE,
            StateSupervisionPeriodTerminationReason.DISMISSED,
            StateSupervisionPeriodTerminationReason.EXPIRATION,
            StateSupervisionPeriodTerminationReason.PARDONED,
            StateSupervisionPeriodTerminationReason.VACATED,
        ):
            return True, True

        if termination_reason in (
            # Unsuccessful terminations
            StateSupervisionPeriodTerminationReason.ABSCONSION,
            StateSupervisionPeriodTerminationReason.REVOCATION,
            StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION,
            StateSupervisionPeriodTerminationReason.ADMITTED_TO_INCARCERATION,
        ):
            return True, False

        raise ValueError(
            f"Unexpected StateSupervisionPeriodTerminationReason: {termination_reason}"
        )

    def _expand_dual_supervision_events(
        self,
        supervision_events: List[SupervisionEvent],
    ) -> List[SupervisionEvent]:
        """For any SupervisionEvents that are of DUAL supervision type, makes a copy of the event that has a PAROLE
        supervision type and a copy of the event that has a PROBATION supervision type. Returns all events, including the
        duplicated events for each of the DUAL supervision events, because we want these events to contribute to PAROLE,
        PROBATION, and DUAL breakdowns of any metric."""
        additional_supervision_months: List[SupervisionEvent] = []

        for supervision_event in supervision_events:
            if (
                supervision_event.supervision_type
                == StateSupervisionPeriodSupervisionType.DUAL
            ):
                parole_copy = attr.evolve(
                    supervision_event,
                    supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
                )

                additional_supervision_months.append(parole_copy)

                probation_copy = attr.evolve(
                    supervision_event,
                    supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
                )

                additional_supervision_months.append(probation_copy)

        supervision_events.extend(additional_supervision_months)

        return supervision_events

    # Each SupervisionMetricType with a list of the SupervisionEvents that contribute to that metric
    EVENT_TYPES_FOR_METRIC: Dict[
        SupervisionMetricType, List[Type[SupervisionEvent]]
    ] = {
        SupervisionMetricType.SUPERVISION_COMPLIANCE: [SupervisionPopulationEvent],
        SupervisionMetricType.SUPERVISION_POPULATION: [
            SupervisionPopulationEvent,
        ],
        SupervisionMetricType.SUPERVISION_OUT_OF_STATE_POPULATION: [
            SupervisionPopulationEvent,
        ],
        SupervisionMetricType.SUPERVISION_START: [SupervisionStartEvent],
        SupervisionMetricType.SUPERVISION_SUCCESS: [
            ProjectedSupervisionCompletionEvent
        ],
        SupervisionMetricType.SUPERVISION_TERMINATION: [SupervisionTerminationEvent],
        SupervisionMetricType.SUPERVISION_DOWNGRADE: [SupervisionPopulationEvent],
    }

    def _convert_events_to_dual(
        self,
        supervision_events: List[SupervisionEvent],
    ) -> List[SupervisionEvent]:
        """For some states, we want to track DUAL supervision as distinct from both PAROLE and PROBATION. For these states,
        if someone has two events on the same day that will contribute to the same type of metric, and these events are
        of different supervision types (one is PAROLE and one is PROBATION, or one is DUAL and the other is something other
        than DUAL), then we want that person to only contribute to metrics with a supervision type of DUAL. All events of
        that type on that day are then replaced with ones that have DUAL as the set supervision_type.

        Returns an updated list of SupervisionEvents.
        """
        events_by_date: Dict[date, List[SupervisionEvent]] = defaultdict(list)

        for event in supervision_events:
            event_date = event.event_date
            events_by_date[event_date].append(event)

        day_event_groups = list(events_by_date.values())

        updated_supervision_events: List[SupervisionEvent] = []

        for day_event_group in day_event_groups:
            if len(day_event_group) < 2:
                # We only need to convert events if there are two that fall in the same time period
                updated_supervision_events.extend(day_event_group)
                continue

            for _, event_types in self.EVENT_TYPES_FOR_METRIC.items():
                events_for_this_metric = [
                    event
                    for event in day_event_group
                    for event_type in event_types
                    if isinstance(event, event_type)
                ]

                # If there is more than one event for this metric on this day
                if events_for_this_metric and len(events_for_this_metric) > 1:
                    parole_events = self._get_events_with_supervision_type(
                        events_for_this_metric,
                        StateSupervisionPeriodSupervisionType.PAROLE,
                    )
                    probation_events = self._get_events_with_supervision_type(
                        events_for_this_metric,
                        StateSupervisionPeriodSupervisionType.PROBATION,
                    )
                    dual_events = self._get_events_with_supervision_type(
                        events_for_this_metric,
                        StateSupervisionPeriodSupervisionType.DUAL,
                    )

                    # If they were on both parole and probation on this day, change every event for this metric
                    # to have a supervision type of DUAL
                    if (parole_events and probation_events) or dual_events:
                        for event in events_for_this_metric:
                            updated_event = attr.evolve(
                                event,
                                supervision_type=StateSupervisionPeriodSupervisionType.DUAL,
                            )
                            day_event_group.remove(event)
                            day_event_group.append(updated_event)

            updated_supervision_events.extend(day_event_group)

        return updated_supervision_events

    def _get_events_with_supervision_type(
        self,
        events: List[SupervisionEvent],
        supervision_type: StateSupervisionPeriodSupervisionType,
    ) -> List[SupervisionEvent]:
        """Returns each SupervisionEvent in events if the supervision_type on the event matches the given
        supervision_type."""
        return [event for event in events if event.supervision_type == supervision_type]

    def _get_judicial_district_code(
        self,
        supervision_period: NormalizedStateSupervisionPeriod,
        supervision_period_to_judicial_district: Dict[int, Dict[Any, Any]],
    ) -> Optional[str]:
        """Retrieves the judicial_district_code corresponding to the supervision_period, if one exists."""
        supervision_period_id = supervision_period.supervision_period_id

        if supervision_period_id is None:
            raise ValueError("Unexpected unset supervision_period_id.")

        ip_info = supervision_period_to_judicial_district.get(supervision_period_id)

        if ip_info is not None:
            return ip_info.get("judicial_district_code")

        return None

    def _find_assessment_score_change(
        self,
        start_date: date,
        termination_date: date,
        assessments: List[StateAssessment],
        supervision_delegate: StateSpecificSupervisionDelegate,
    ) -> Tuple[
        Optional[int],
        Optional[int],
        Optional[StateAssessmentLevel],
        Optional[StateAssessmentType],
    ]:
        """Finds the difference in scores between the last assessment that happened between the start_date and
        termination_date (inclusive) and the the first "reliable" assessment that was conducted after the start of
        supervision. The first "reliable" assessment is either the first or second assessment, depending on state-specific
        logic. Returns the assessment score change, the ending assessment score, the ending assessment level, and the
        assessment type. If there aren't enough assessments to compare, or the first reliable assessment and the last
        assessment are not of the same type, returns (None, None, None, None)."""
        if assessments:
            assessments_in_period = [
                assessment
                for assessment in assessments
                if assessment.assessment_date is not None
                and start_date <= assessment.assessment_date <= termination_date
            ]

            index_of_first_reliable_assessment = (
                supervision_delegate.get_index_of_first_reliable_supervision_assessment()
            )
            min_assessments = 2 + index_of_first_reliable_assessment

            # If this person had less than the min number of assessments then we cannot compare the first reliable
            # assessment to the most recent assessment.
            if assessments_in_period and len(assessments_in_period) >= min_assessments:
                # Mypy complains that assessment_date might be None, even though that has already been filtered above.
                assessments_in_period.sort(key=lambda b: b.assessment_date)  # type: ignore[arg-type,return-value]

                first_reliable_assessment = assessments_in_period[
                    index_of_first_reliable_assessment
                ]
                last_assessment = assessments_in_period[-1]

                # Assessments must be of the same type
                if (
                    last_assessment.assessment_type
                    == first_reliable_assessment.assessment_type
                ):
                    first_reliable_assessment_date = (
                        first_reliable_assessment.assessment_date
                    )
                    last_assessment_date = last_assessment.assessment_date

                    # Ensure these assessments were actually issued on different days
                    if (
                        first_reliable_assessment_date
                        and last_assessment_date
                        and last_assessment_date > first_reliable_assessment_date
                    ):
                        first_reliable_assessment_score = (
                            first_reliable_assessment.assessment_score
                        )
                        last_assessment_score = last_assessment.assessment_score

                        if (
                            first_reliable_assessment_score is not None
                            and last_assessment_score is not None
                        ):
                            assessment_score_change = (
                                last_assessment_score - first_reliable_assessment_score
                            )

                            return (
                                assessment_score_change,
                                last_assessment.assessment_score,
                                last_assessment.assessment_level,
                                last_assessment.assessment_type,
                            )

        return None, None, None, None

    def _get_supervision_downgrade_details_if_downgrade_occurred(
        self,
        supervision_period_index: NormalizedSupervisionPeriodIndex,
        current_supervision_period: NormalizedStateSupervisionPeriod,
    ) -> Tuple[bool, Optional[StateSupervisionLevel]]:
        """Given a supervision period and the supervision period index it belongs to, determine whether a supervision
        level downgrade has occurred between the current supervision period and the most recent previous supervision
        period."""
        # TODO(#4895): Consider updating SupervisionLevelDowngrade calculation to capture downgrades separated by
        # SupervisionPeriods with null levels.
        most_recent_previous_supervision_period: Optional[
            NormalizedStateSupervisionPeriod
        ] = supervision_period_index.get_most_recent_previous_supervision_period(
            current_supervision_period
        )

        if (
            current_supervision_period.supervision_level
            and most_recent_previous_supervision_period
            and most_recent_previous_supervision_period.supervision_level
        ):
            # We only want consider supervision level downgrade between two adjacent supervision periods.
            if (
                current_supervision_period.start_date
                != most_recent_previous_supervision_period.termination_date
            ):
                return False, None

            return self._supervision_level_downgrade_occurred(
                most_recent_previous_supervision_period.supervision_level,
                current_supervision_period.supervision_level,
            )

        return False, None

    def _supervision_level_downgrade_occurred(
        self,
        previous_supervision_level: StateSupervisionLevel,
        current_supervision_level: StateSupervisionLevel,
    ) -> Tuple[bool, Optional[StateSupervisionLevel]]:
        """Given a current and previous supervision level, return whether a supervision level downgrade has taken place
        between the previous level and the current."""
        not_applicable_supervision_levels: List[StateSupervisionLevel] = [
            StateSupervisionLevel.EXTERNAL_UNKNOWN,
            StateSupervisionLevel.INTERNAL_UNKNOWN,
            StateSupervisionLevel.PRESENT_WITHOUT_INFO,
            StateSupervisionLevel.DIVERSION,
            StateSupervisionLevel.INCARCERATED,
            StateSupervisionLevel.IN_CUSTODY,
            StateSupervisionLevel.INTERSTATE_COMPACT,
            StateSupervisionLevel.LIMITED,
            StateSupervisionLevel.UNASSIGNED,
        ]

        supervision_level_to_number: Dict[StateSupervisionLevel, int] = {
            StateSupervisionLevel.UNSUPERVISED: 0,
            StateSupervisionLevel.ELECTRONIC_MONITORING_ONLY: 1,
            StateSupervisionLevel.MINIMUM: 2,
            StateSupervisionLevel.MEDIUM: 3,
            StateSupervisionLevel.HIGH: 4,
            StateSupervisionLevel.MAXIMUM: 5,
        }

        if (
            previous_supervision_level in not_applicable_supervision_levels
            or current_supervision_level in not_applicable_supervision_levels
        ):
            return False, None

        if previous_supervision_level not in supervision_level_to_number:
            raise ValueError(
                f"Previous supervision level ({previous_supervision_level}) was not listed as a supervision "
                f"level that is out of scope for downgrades and was not found in the list of levels that can "
                f"be compared for downgrades. Please add it to one of the two."
            )

        if current_supervision_level not in supervision_level_to_number:
            raise ValueError(
                f"Current supervision level ({current_supervision_level}) was not listed as a supervision "
                f"level that is out of scope for downgrades and was not found in the list of levels that can "
                f"be compared for downgrades. Please add it to one of the two."
            )

        if (
            supervision_level_to_number[previous_supervision_level]
            > supervision_level_to_number[current_supervision_level]
        ):
            return True, previous_supervision_level

        return False, None
