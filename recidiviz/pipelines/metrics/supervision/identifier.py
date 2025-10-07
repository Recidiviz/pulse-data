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
from collections import defaultdict
from datetime import date
from typing import Dict, List, Optional, Set, Tuple, Type

import attr
from dateutil.relativedelta import relativedelta

from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import DateRange, DateRangeDiff, current_date_us_eastern
from recidiviz.persistence.entity.normalized_entities_utils import (
    sort_normalized_entities_by_sequence_num,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateAssessment,
    NormalizedStateIncarcerationPeriod,
    NormalizedStatePerson,
    NormalizedStateSupervisionContact,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.assessment_normalization_manager import (
    DEFAULT_ASSESSMENT_SCORE_BUCKET,
)
from recidiviz.pipelines.metrics.base_identifier import (
    BaseIdentifier,
    IdentifierContext,
)
from recidiviz.pipelines.metrics.supervision.events import (
    SupervisionEvent,
    SupervisionPopulationEvent,
    SupervisionStartEvent,
    SupervisionTerminationEvent,
)
from recidiviz.pipelines.metrics.supervision.metrics import SupervisionMetricType
from recidiviz.pipelines.metrics.supervision.supervision_case_compliance import (
    SupervisionCaseCompliance,
)
from recidiviz.pipelines.metrics.utils.supervision_utils import (
    is_supervision_out_of_state,
)
from recidiviz.pipelines.metrics.utils.violation_utils import (
    VIOLATION_HISTORY_WINDOW_MONTHS,
    filter_violation_responses_for_violation_history,
    get_violation_and_response_history,
)
from recidiviz.pipelines.utils import assessment_utils
from recidiviz.pipelines.utils.entity_normalization.normalized_incarceration_period_index import (
    NormalizedIncarcerationPeriodIndex,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.pipelines.utils.identifier_models import IdentifierResult
from recidiviz.pipelines.utils.state_utils.state_calculation_config_manager import (
    get_state_specific_case_compliance_manager,
    get_state_specific_incarceration_delegate,
    get_state_specific_supervision_delegate,
    get_state_specific_violation_delegate,
)
from recidiviz.pipelines.utils.supervision_period_utils import (
    identify_most_severe_case_type,
    supervising_location_info,
)
from recidiviz.utils.range_querier import RangeQuerier


class SupervisionIdentifier(BaseIdentifier[List[SupervisionEvent]]):
    """Identifier class for events related to supervision."""

    def __init__(self, state_code: StateCode) -> None:
        self.identifier_result_class = SupervisionEvent
        self.supervision_delegate = get_state_specific_supervision_delegate(
            state_code.value
        )
        self.violation_delegate = get_state_specific_violation_delegate(
            state_code.value
        )
        self.incarceration_delegate = get_state_specific_incarceration_delegate(
            state_code.value
        )

    def identify(
        self,
        person: NormalizedStatePerson,
        identifier_context: IdentifierContext,
        included_result_classes: Set[Type[IdentifierResult]],
    ) -> List[SupervisionEvent]:
        return self._find_supervision_events(
            included_result_classes=included_result_classes,
            person=person,
            supervision_periods=identifier_context[
                NormalizedStateSupervisionPeriod.__name__
            ],
            incarceration_periods=identifier_context[
                NormalizedStateIncarcerationPeriod.__name__
            ],
            assessments=identifier_context[NormalizedStateAssessment.__name__],
            violation_responses=identifier_context[
                NormalizedStateSupervisionViolationResponse.__name__
            ],
            supervision_contacts=identifier_context[
                NormalizedStateSupervisionContact.__name__
            ],
        )

    def _find_supervision_events(
        self,
        *,
        included_result_classes: Set[Type[IdentifierResult]],
        person: NormalizedStatePerson,
        supervision_periods: List[NormalizedStateSupervisionPeriod],
        incarceration_periods: List[NormalizedStateIncarcerationPeriod],
        assessments: List[NormalizedStateAssessment],
        violation_responses: List[NormalizedStateSupervisionViolationResponse],
        supervision_contacts: List[NormalizedStateSupervisionContact],
    ) -> List[SupervisionEvent]:
        """Identifies various events related to being on supervision.

        Args:
            - supervision_periods: list of StateSupervisionPeriods for a person
            - incarceration_periods: list of StateIncarcerationPeriods for a person
            - assessments: list of StateAssessments for a person
            - violations: list of StateSupervisionViolations for a person
            - violation_responses: list of StateSupervisionViolationResponses for a person

        Returns:
            A list of SupervisionEvents for the person.
        """
        if not person.person_id:
            raise ValueError(
                f"Found NormalizedStatePerson with unset person_id value: {person}."
            )

        if not supervision_periods and not incarceration_periods:
            return []

        incarceration_period_index = NormalizedIncarcerationPeriodIndex(
            sorted_incarceration_periods=incarceration_periods,
            incarceration_delegate=self.incarceration_delegate,
        )
        supervision_period_index = NormalizedSupervisionPeriodIndex(
            sorted_supervision_periods=supervision_periods
        )
        assessments_by_date = RangeQuerier(
            assessments, lambda assessment: assessment.assessment_date
        )
        supervision_contacts_by_date = RangeQuerier(
            supervision_contacts, lambda contact: contact.contact_date
        )

        sorted_violation_responses = sort_normalized_entities_by_sequence_num(
            violation_responses
        )
        violation_responses_for_history = (
            filter_violation_responses_for_violation_history(
                self.violation_delegate,
                violation_responses=sorted_violation_responses,
                include_follow_up_responses=False,
            )
        )

        supervision_events: List[SupervisionEvent] = []

        for supervision_period in supervision_period_index.sorted_supervision_periods:
            if SupervisionPopulationEvent in included_result_classes:
                supervision_events.extend(
                    self._find_population_events_for_supervision_period(
                        person=person,
                        supervision_period=supervision_period,
                        supervision_period_index=supervision_period_index,
                        incarceration_period_index=incarceration_period_index,
                        assessments_by_date=assessments_by_date,
                        violation_responses_for_history=violation_responses_for_history,
                        supervision_contacts_by_date=supervision_contacts_by_date,
                    )
                )

            if SupervisionTerminationEvent in included_result_classes:
                supervision_termination_event = (
                    self._find_supervision_termination_event(
                        supervision_period=supervision_period,
                        supervision_period_index=supervision_period_index,
                        incarceration_period_index=incarceration_period_index,
                        assessments=assessments,
                        violation_responses_for_history=violation_responses_for_history,
                    )
                )

                if supervision_termination_event:
                    supervision_events.append(supervision_termination_event)

            if SupervisionStartEvent in included_result_classes:
                supervision_start_event = self._find_supervision_start_event(
                    supervision_period=supervision_period,
                    supervision_period_index=supervision_period_index,
                    incarceration_period_index=incarceration_period_index,
                )
                if supervision_start_event:
                    supervision_events.append(supervision_start_event)

        if self.supervision_delegate.supervision_types_mutually_exclusive():
            supervision_events = self._convert_events_to_dual(supervision_events)
        else:
            supervision_events = self._expand_dual_supervision_events(
                supervision_events
            )

        return supervision_events

    def _find_population_events_for_supervision_period(
        self,
        person: NormalizedStatePerson,
        supervision_period: NormalizedStateSupervisionPeriod,
        supervision_period_index: NormalizedSupervisionPeriodIndex,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        assessments_by_date: RangeQuerier[date, NormalizedStateAssessment],
        violation_responses_for_history: List[
            NormalizedStateSupervisionViolationResponse
        ],
        supervision_contacts_by_date: RangeQuerier[
            date, NormalizedStateSupervisionContact
        ],
    ) -> List[SupervisionPopulationEvent]:
        """Finds days that this person was on supervision for the given
        StateSupervisionPeriod.

        Args:
            - person: NormalizedStatePerson encoding of the person under supervision
            - supervision_period: The supervision period the person was on
            - supervision_period_index: Class containing information about this person's supervision periods
            - incarceration_period_index: Class containing information about this person's incarceration periods
            - assessments: List of StateAssessment for a person
            - violation_responses_for_history: List of
                StateSupervisionViolationResponse for a person, sorted by response_date and
                filtered to those that are applicable when analyzing violation history
            - violation_delegate: the state-specific violation delegate
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
            level_1_supervision_location_external_id,
            level_2_supervision_location_external_id,
        ) = supervising_location_info(
            supervision_period,
            self.supervision_delegate,
        )
        case_type, case_type_raw_text = identify_most_severe_case_type(
            supervision_period
        )

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
                assessments_by_date,
                supervision_contacts_by_date,
                violation_responses_for_history,
                incarceration_period_index,
                self.supervision_delegate,
            )
        )

        end_date = (
            termination_date
            if termination_date
            else current_date_us_eastern() + relativedelta(days=1)
        )

        while event_date < end_date:
            if self._in_supervision_population_for_period_on_date(
                event_date,
                supervision_period,
                incarceration_period_index,
            ):
                supervision_type = (
                    supervision_period.supervision_type
                    if supervision_period.supervision_type
                    else StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
                )

                assessment_score = None
                assessment_level = None
                assessment_type = None
                assessment_score_bucket = DEFAULT_ASSESSMENT_SCORE_BUCKET

                most_recent_assessment = assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                    event_date,
                    assessments_by_date,
                    assessment_class=StateAssessmentClass.RISK,
                    supervision_delegate=self.supervision_delegate,
                )

                if most_recent_assessment:
                    assessment_score = most_recent_assessment.assessment_score
                    assessment_level = most_recent_assessment.assessment_level
                    assessment_type = most_recent_assessment.assessment_type
                    assessment_score_bucket = (
                        most_recent_assessment.assessment_score_bucket
                        or DEFAULT_ASSESSMENT_SCORE_BUCKET
                    )

                violation_history = get_violation_and_response_history(
                    upper_bound_exclusive_date=(event_date + relativedelta(days=1)),
                    violation_responses_for_history=violation_responses_for_history,
                    violation_delegate=self.violation_delegate,
                    incarceration_period=None,
                )

                case_compliance: Optional[SupervisionCaseCompliance] = None

                if state_specific_case_compliance_manager:
                    case_compliance = state_specific_case_compliance_manager.get_case_compliance_on_date(
                        event_date
                    )

                supervision_out_of_state = is_supervision_out_of_state(
                    supervision_period.custodial_authority,
                )
                event = SupervisionPopulationEvent(
                    state_code=supervision_period.state_code,
                    year=event_date.year,
                    month=event_date.month,
                    event_date=event_date,
                    supervision_type=supervision_type,
                    case_type=case_type,
                    case_type_raw_text=case_type_raw_text,
                    assessment_score=assessment_score,
                    assessment_level=assessment_level,
                    assessment_type=assessment_type,
                    assessment_score_bucket=assessment_score_bucket,
                    most_severe_violation_type=violation_history.most_severe_violation_type,
                    most_severe_violation_type_subtype=violation_history.most_severe_violation_type_subtype,
                    most_severe_violation_id=violation_history.most_severe_violation_id,
                    violation_history_id_array=violation_history.violation_history_id_array,
                    most_severe_response_decision=violation_history.most_severe_response_decision,
                    response_count=violation_history.response_count,
                    supervising_officer_staff_id=supervision_period.supervising_officer_staff_id,
                    level_1_supervision_location_external_id=level_1_supervision_location_external_id,
                    level_2_supervision_location_external_id=level_2_supervision_location_external_id,
                    supervision_level=supervision_period.supervision_level,
                    supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                    case_compliance=case_compliance,
                    custodial_authority=supervision_period.custodial_authority,
                    supervision_out_of_state=supervision_out_of_state,
                )

                supervision_population_events.append(event)

            event_date = event_date + relativedelta(days=1)

        return supervision_population_events

    def _supervision_period_counts_towards_supervision_population_in_date_range(
        self,
        date_range: DateRange,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        supervision_period: NormalizedStateSupervisionPeriod,
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

        return self.supervision_delegate.supervision_period_in_supervision_population_in_non_excluded_date_range(
            supervision_period=supervision_period,
        )

    def _in_supervision_population_for_period_on_date(
        self,
        evaluation_date: date,
        supervision_period: NormalizedStateSupervisionPeriod,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
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
        )

        return supervision_period_counts_towards_supervision_population_on_date

    def _in_supervision_population_on_date(
        self,
        evaluation_date: date,
        supervision_period_index: NormalizedSupervisionPeriodIndex,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
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
                validate_duration=False,
            ):
                return True

        return False

    def _find_supervision_start_event(
        self,
        supervision_period: NormalizedStateSupervisionPeriod,
        supervision_period_index: NormalizedSupervisionPeriodIndex,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
    ) -> Optional[SupervisionStartEvent]:
        """Identifies an instance of supervision start, assuming the provided |supervision_period| has a valid start
        date, and returns all relevant info as a SupervisionStartEvent.
        """

        # Do not create start metrics if no start date
        if supervision_period.start_date is None:
            return None

        start_date = supervision_period.start_date
        (
            level_1_supervision_location_external_id,
            level_2_supervision_location_external_id,
        ) = supervising_location_info(
            supervision_period,
            self.supervision_delegate,
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
        )
        case_type, case_type_raw_text = identify_most_severe_case_type(
            supervision_period
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
            case_type=case_type,
            case_type_raw_text=case_type_raw_text,
            supervision_level=supervision_period.supervision_level,
            supervision_level_raw_text=supervision_period.supervision_level_raw_text,
            supervising_officer_staff_id=supervision_period.supervising_officer_staff_id,
            level_1_supervision_location_external_id=level_1_supervision_location_external_id,
            level_2_supervision_location_external_id=level_2_supervision_location_external_id,
            custodial_authority=supervision_period.custodial_authority,
        )

    def _find_supervision_termination_event(
        self,
        supervision_period: NormalizedStateSupervisionPeriod,
        supervision_period_index: NormalizedSupervisionPeriodIndex,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        assessments: List[NormalizedStateAssessment],
        violation_responses_for_history: List[
            NormalizedStateSupervisionViolationResponse
        ],
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
                end_assessment_score_bucket,
            ) = self._find_assessment_score_change(
                assessment_start_date,
                assessment_termination_date,
                assessments,
            )

            violation_history_window = self.violation_delegate.violation_history_window_relevant_to_critical_date(
                critical_date=supervision_period.termination_date,
                sorted_and_filtered_violation_responses=violation_responses_for_history,
                default_violation_history_window_months=VIOLATION_HISTORY_WINDOW_MONTHS,
            )

            # Get details about the violation and response history leading up to the
            # supervision termination
            violation_history = get_violation_and_response_history(
                upper_bound_exclusive_date=violation_history_window.upper_bound_exclusive_date,
                violation_responses_for_history=violation_responses_for_history,
                violation_delegate=self.violation_delegate,
                incarceration_period=None,
                lower_bound_inclusive_date_override=violation_history_window.lower_bound_inclusive_date,
            )

            (
                level_1_supervision_location_external_id,
                level_2_supervision_location_external_id,
            ) = supervising_location_info(
                supervision_period,
                self.supervision_delegate,
            )

            case_type, case_type_raw_text = identify_most_severe_case_type(
                supervision_period
            )

            supervision_type = (
                supervision_period.supervision_type
                if supervision_period.supervision_type
                else StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
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
                case_type_raw_text=case_type_raw_text,
                supervision_level=supervision_period.supervision_level,
                supervision_level_raw_text=supervision_period.supervision_level_raw_text,
                assessment_score=end_assessment_score,
                assessment_level=end_assessment_level,
                assessment_type=end_assessment_type,
                assessment_score_bucket=end_assessment_score_bucket,
                termination_reason=supervision_period.termination_reason,
                assessment_score_change=assessment_score_change,
                supervising_officer_staff_id=supervision_period.supervising_officer_staff_id,
                level_1_supervision_location_external_id=level_1_supervision_location_external_id,
                level_2_supervision_location_external_id=level_2_supervision_location_external_id,
                custodial_authority=supervision_period.custodial_authority,
                response_count=violation_history.response_count,
                most_severe_response_decision=violation_history.most_severe_response_decision,
                most_severe_violation_type=violation_history.most_severe_violation_type,
                most_severe_violation_type_subtype=violation_history.most_severe_violation_type_subtype,
                most_severe_violation_id=violation_history.most_severe_violation_id,
                violation_history_id_array=violation_history.violation_history_id_array,
            )

        return None

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
        SupervisionMetricType.SUPERVISION_TERMINATION: [SupervisionTerminationEvent],
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

    def _find_assessment_score_change(
        self,
        start_date: date,
        termination_date: date,
        assessments: List[NormalizedStateAssessment],
    ) -> Tuple[
        Optional[int],
        Optional[int],
        Optional[StateAssessmentLevel],
        Optional[StateAssessmentType],
        Optional[str],
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
                self.supervision_delegate.get_index_of_first_reliable_supervision_assessment()
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
                                last_assessment.assessment_score_bucket,
                            )

        return None, None, None, None, DEFAULT_ASSESSMENT_SCORE_BUCKET
