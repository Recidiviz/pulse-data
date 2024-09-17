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
"""Identifier class for events related to incarceration."""
import logging
from datetime import date
from typing import Dict, List, Optional, Set, Tuple, Type, Union

from dateutil.relativedelta import relativedelta

from recidiviz.common.constants.state.state_assessment import StateAssessmentClass
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    is_commitment_from_supervision,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.normalized_entities_utils import (
    sort_normalized_entities_by_sequence_num,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateAssessment,
    NormalizedStateIncarcerationPeriod,
    NormalizedStatePerson,
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
from recidiviz.pipelines.metrics.incarceration.events import (
    IncarcerationAdmissionEvent,
    IncarcerationCommitmentFromSupervisionAdmissionEvent,
    IncarcerationEvent,
    IncarcerationReleaseEvent,
    IncarcerationStandardAdmissionEvent,
)
from recidiviz.pipelines.metrics.utils.commitment_from_supervision_utils import (
    count_temporary_custody_as_commitment_from_supervision,
    get_commitment_from_supervision_details,
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
    get_state_specific_commitment_from_supervision_delegate,
    get_state_specific_incarceration_delegate,
    get_state_specific_supervision_delegate,
    get_state_specific_violation_delegate,
)
from recidiviz.pipelines.utils.supervision_period_utils import (
    get_post_incarceration_supervision_type,
)
from recidiviz.pipelines.utils.violation_response_utils import (
    get_most_severe_response_decision,
    responses_on_most_recent_response_date,
    violation_responses_in_window,
)
from recidiviz.utils.range_querier import RangeQuerier


class IncarcerationIdentifier(BaseIdentifier[List[IncarcerationEvent]]):
    """Identifier class for events related to incarceration."""

    def __init__(self, state_code: StateCode) -> None:
        self.identifier_result_class = IncarcerationEvent
        self.incarceration_delegate = get_state_specific_incarceration_delegate(
            state_code.value
        )
        self.commitment_from_supervision_delegate = (
            get_state_specific_commitment_from_supervision_delegate(state_code.value)
        )
        self.violation_delegate = get_state_specific_violation_delegate(
            state_code.value
        )
        self.supervision_delegate = get_state_specific_supervision_delegate(
            state_code.value
        )

    def identify(
        self,
        _person: NormalizedStatePerson,
        identifier_context: IdentifierContext,
        included_result_classes: Set[Type[IdentifierResult]],
    ) -> List[IncarcerationEvent]:
        if included_result_classes != {
            IncarcerationStandardAdmissionEvent,
            IncarcerationCommitmentFromSupervisionAdmissionEvent,
            IncarcerationReleaseEvent,
        }:
            raise NotImplementedError(
                "Filtering of events is not yet implemented for the incarceration pipeline."
            )

        return self._find_incarceration_events(
            incarceration_periods=identifier_context[
                NormalizedStateIncarcerationPeriod.__name__
            ],
            supervision_periods=identifier_context[
                NormalizedStateSupervisionPeriod.__name__
            ],
            assessments=identifier_context[NormalizedStateAssessment.__name__],
            violation_responses=identifier_context[
                NormalizedStateSupervisionViolationResponse.__name__
            ],
        )

    def _find_incarceration_events(
        self,
        incarceration_periods: List[NormalizedStateIncarcerationPeriod],
        supervision_periods: List[NormalizedStateSupervisionPeriod],
        assessments: List[NormalizedStateAssessment],
        violation_responses: List[NormalizedStateSupervisionViolationResponse],
    ) -> List[IncarcerationEvent]:
        """Finds instances of various events related to incarceration.
        Transforms the person's StateIncarcerationPeriods into various
        IncarcerationEvents.

        Returns:
            A list of IncarcerationEvents for the person.
        """
        incarceration_events: List[IncarcerationEvent] = []

        if not incarceration_periods:
            return incarceration_events

        # Convert the list of dictionaries into one dictionary where the keys are the
        # incarceration_period_id values
        normalized_violation_responses = sort_normalized_entities_by_sequence_num(
            violation_responses
        )

        ip_index = NormalizedIncarcerationPeriodIndex(
            sorted_incarceration_periods=incarceration_periods,
            incarceration_delegate=self.incarceration_delegate,
        )

        sp_index = NormalizedSupervisionPeriodIndex(
            sorted_supervision_periods=supervision_periods
        )
        assessments_by_date = RangeQuerier(
            assessments, lambda assessment: assessment.assessment_date
        )

        admission_and_release_events = self._find_all_admission_release_events(
            ip_index=ip_index,
            sp_index=sp_index,
            assessments_by_date=assessments_by_date,
            sorted_violation_responses=normalized_violation_responses,
        )

        incarceration_events.extend(admission_and_release_events)

        return incarceration_events

    def _find_all_admission_release_events(
        self,
        ip_index: NormalizedIncarcerationPeriodIndex,
        sp_index: NormalizedSupervisionPeriodIndex,
        assessments_by_date: RangeQuerier[date, NormalizedStateAssessment],
        sorted_violation_responses: List[NormalizedStateSupervisionViolationResponse],
    ) -> List[Union[IncarcerationAdmissionEvent, IncarcerationReleaseEvent]]:
        """Given the |original_incarceration_periods| generates and returns all IncarcerationAdmissionEvents and
        IncarcerationReleaseEvents.
        """
        incarceration_events: List[
            Union[IncarcerationAdmissionEvent, IncarcerationReleaseEvent]
        ] = []

        commitments_from_supervision: Dict[
            date, IncarcerationCommitmentFromSupervisionAdmissionEvent
        ] = {}

        for incarceration_period in ip_index.sorted_incarceration_periods:
            admission_event = self._admission_event_for_period(
                incarceration_period=incarceration_period,
                incarceration_period_index=ip_index,
                supervision_period_index=sp_index,
                assessments_by_date=assessments_by_date,
                sorted_violation_responses=sorted_violation_responses,
            )

            if admission_event:
                incarceration_events.append(admission_event)
                if isinstance(
                    admission_event,
                    IncarcerationCommitmentFromSupervisionAdmissionEvent,
                ):
                    commitments_from_supervision[
                        admission_event.admission_date
                    ] = admission_event

        for incarceration_period in ip_index.sorted_incarceration_periods:
            release_event = self._release_event_for_period(
                incarceration_period=incarceration_period,
                incarceration_period_index=ip_index,
                supervision_period_index=sp_index,
                commitments_from_supervision=commitments_from_supervision,
            )

            if release_event:
                incarceration_events.append(release_event)

        return incarceration_events

    def _admission_event_for_period(
        self,
        incarceration_period: NormalizedStateIncarcerationPeriod,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        supervision_period_index: NormalizedSupervisionPeriodIndex,
        assessments_by_date: RangeQuerier[date, NormalizedStateAssessment],
        sorted_violation_responses: List[NormalizedStateSupervisionViolationResponse],
    ) -> Optional[IncarcerationAdmissionEvent]:
        """Returns an IncarcerationAdmissionEvent if this incarceration period represents an
        admission to incarceration."""

        admission_date: Optional[date] = incarceration_period.admission_date
        admission_reason: Optional[
            StateIncarcerationPeriodAdmissionReason
        ] = incarceration_period.admission_reason

        if (
            admission_date
            and admission_reason
            and self.incarceration_delegate.should_include_in_state_admissions(
                admission_reason
            )
        ):
            # We are including TEMPORARY_CUSTODY separately because the function
            # is_commitment_from_supervision is also used in normalization, and
            # currently, the use of TEMPORARY_CUSTODY is to support metrics. When the
            # results are more satisfactory, we can revisit whether to add this to
            # normalization results as well.
            if is_commitment_from_supervision(admission_reason) or (
                admission_reason
                == StateIncarcerationPeriodAdmissionReason.TEMPORARY_CUSTODY
                and count_temporary_custody_as_commitment_from_supervision(
                    incarceration_period, incarceration_period_index
                )
            ):
                return self._commitment_from_supervision_event_for_period(
                    incarceration_period=incarceration_period,
                    incarceration_period_index=incarceration_period_index,
                    supervision_period_index=supervision_period_index,
                    assessments_by_date=assessments_by_date,
                    sorted_violation_responses=sorted_violation_responses,
                )

            return IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period.state_code,
                included_in_state_population=self.incarceration_delegate.is_period_included_in_state_population(
                    incarceration_period
                ),
                event_date=admission_date,
                facility=incarceration_period.facility,
                admission_reason=admission_reason,
                admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
                specialized_purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
            )

        return None

    def _commitment_from_supervision_event_for_period(
        self,
        incarceration_period: NormalizedStateIncarcerationPeriod,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        supervision_period_index: NormalizedSupervisionPeriodIndex,
        assessments_by_date: RangeQuerier[date, NormalizedStateAssessment],
        sorted_violation_responses: List[NormalizedStateSupervisionViolationResponse],
    ) -> IncarcerationCommitmentFromSupervisionAdmissionEvent:
        """
        Returns the IncarcerationCommitmentFromSupervisionAdmissionEvent corresponding to
        the admission to the |incarceration_period| that qualifies as a commitment from
        supervision admission. Includes details about the period of supervision that
        preceded the admission.
        """
        admission_date = incarceration_period.admission_date
        admission_reason = incarceration_period.admission_reason

        if not admission_date or not admission_reason:
            raise ValueError(
                "Should only be calling this function with a set "
                "admission_date and admission_reason."
            )

        most_recent_assessment = (
            assessment_utils.find_most_recent_applicable_assessment_of_class_for_state(
                cutoff_date=admission_date,
                assessments_by_date=assessments_by_date,
                assessment_class=StateAssessmentClass.RISK,
                supervision_delegate=self.supervision_delegate,
            )
        )
        assessment_score = (
            most_recent_assessment.assessment_score if most_recent_assessment else None
        )
        assessment_level = (
            most_recent_assessment.assessment_level if most_recent_assessment else None
        )
        assessment_type = (
            most_recent_assessment.assessment_type if most_recent_assessment else None
        )
        assessment_score_bucket = (
            most_recent_assessment.assessment_score_bucket
            if most_recent_assessment
            else DEFAULT_ASSESSMENT_SCORE_BUCKET
        )

        violation_responses_for_history = (
            filter_violation_responses_for_violation_history(
                self.violation_delegate,
                violation_responses=sorted_violation_responses,
                include_follow_up_responses=False,
            )
        )

        violation_history_window = (
            self.violation_delegate.violation_history_window_relevant_to_critical_date(
                critical_date=admission_date,
                sorted_and_filtered_violation_responses=violation_responses_for_history,
                default_violation_history_window_months=VIOLATION_HISTORY_WINDOW_MONTHS,
            )
        )

        # Get details about the violation and response history leading up to the
        # admission to incarceration
        violation_history = get_violation_and_response_history(
            upper_bound_exclusive_date=violation_history_window.upper_bound_exclusive_date,
            violation_responses_for_history=violation_responses_for_history,
            violation_delegate=self.violation_delegate,
            incarceration_period=incarceration_period,
            lower_bound_inclusive_date_override=violation_history_window.lower_bound_inclusive_date,
        )

        most_severe_violation_type = violation_history.most_severe_violation_type

        most_severe_violation_type_subtype = (
            violation_history.most_severe_violation_type_subtype
        )

        most_severe_violation_id = violation_history.most_severe_violation_id

        responses_for_decision_evaluation = violation_responses_for_history

        if (
            self.violation_delegate.include_decisions_on_follow_up_responses_for_most_severe_response()
        ):
            # Get a new state-specific list of violation responses that includes follow-up
            # responses
            responses_for_decision_evaluation = (
                filter_violation_responses_for_violation_history(
                    violation_delegate=self.violation_delegate,
                    violation_responses=sorted_violation_responses,
                    include_follow_up_responses=True,
                )
            )

        responses_in_window_for_decision_evaluation = violation_responses_in_window(
            violation_responses=responses_for_decision_evaluation,
            upper_bound_exclusive=(admission_date + relativedelta(days=1)),
            # We're just looking for the most recent response
            lower_bound_inclusive=None,
        )

        # Find the most severe decision on the most recent response decisions
        most_recent_responses = responses_on_most_recent_response_date(
            responses_in_window_for_decision_evaluation
        )
        most_recent_response_decision = get_most_severe_response_decision(
            most_recent_responses
        )

        commitment_details = get_commitment_from_supervision_details(
            incarceration_period=incarceration_period,
            incarceration_period_index=incarceration_period_index,
            supervision_period_index=supervision_period_index,
            commitment_from_supervision_delegate=self.commitment_from_supervision_delegate,
            supervision_delegate=self.supervision_delegate,
        )

        return IncarcerationCommitmentFromSupervisionAdmissionEvent(
            state_code=incarceration_period.state_code,
            included_in_state_population=self.incarceration_delegate.is_period_included_in_state_population(
                incarceration_period
            ),
            event_date=admission_date,
            facility=incarceration_period.facility,
            admission_reason=admission_reason,
            admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
            supervision_type=commitment_details.supervision_type,
            specialized_purpose_for_incarceration=commitment_details.purpose_for_incarceration,
            purpose_for_incarceration_subtype=commitment_details.purpose_for_incarceration_subtype,
            case_type=commitment_details.case_type,
            supervision_level=commitment_details.supervision_level,
            supervision_level_raw_text=commitment_details.supervision_level_raw_text,
            assessment_score=assessment_score,
            assessment_level=assessment_level,
            assessment_type=assessment_type,
            assessment_score_bucket=assessment_score_bucket,
            most_severe_violation_type=most_severe_violation_type,
            most_severe_violation_type_subtype=most_severe_violation_type_subtype,
            most_severe_violation_id=most_severe_violation_id,
            violation_history_id_array=violation_history.violation_history_id_array,
            most_severe_response_decision=violation_history.most_severe_response_decision,
            most_recent_response_decision=most_recent_response_decision,
            response_count=violation_history.response_count,
            violation_history_description=violation_history.violation_history_description,
            violation_type_frequency_counter=violation_history.violation_type_frequency_counter,
            supervising_officer_staff_id=commitment_details.supervising_officer_staff_id,
            level_1_supervision_location_external_id=(
                commitment_details.level_1_supervision_location_external_id
            ),
            level_2_supervision_location_external_id=(
                commitment_details.level_2_supervision_location_external_id
            ),
        )

    def _release_event_for_period(
        self,
        incarceration_period: NormalizedStateIncarcerationPeriod,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        supervision_period_index: NormalizedSupervisionPeriodIndex,
        commitments_from_supervision: Dict[
            date, IncarcerationCommitmentFromSupervisionAdmissionEvent
        ],
    ) -> Optional[IncarcerationReleaseEvent]:
        """Returns an IncarcerationReleaseEvent if this incarceration period represents an release from incarceration."""
        release_date: Optional[date] = incarceration_period.release_date
        release_reason: Optional[
            StateIncarcerationPeriodReleaseReason
        ] = incarceration_period.release_reason

        incarceration_period_id: Optional[
            int
        ] = incarceration_period.incarceration_period_id

        if not incarceration_period_id:
            raise ValueError(
                "Unexpected incarceration period without an incarceration_period_id."
            )

        original_admission_reasons_by_period_id: Dict[
            int, Tuple[StateIncarcerationPeriodAdmissionReason, Optional[str], date]
        ] = incarceration_period_index.original_admission_reasons_by_period_id

        (
            original_admission_reason,
            _,
            original_admission_date,
        ) = original_admission_reasons_by_period_id[incarceration_period_id]

        if (
            release_date
            and release_reason
            and release_reason != StateIncarcerationPeriodReleaseReason.TRANSFER
        ):
            supervision_type_at_release: Optional[
                StateSupervisionPeriodSupervisionType
            ] = get_post_incarceration_supervision_type(
                incarceration_period,
                supervision_period_index,
                self.supervision_delegate,
            )

            commitment_from_supervision_supervision_type: Optional[
                StateSupervisionPeriodSupervisionType
            ] = None
            if is_commitment_from_supervision(original_admission_reason):
                associated_commitment = commitments_from_supervision[
                    original_admission_date
                ]
                if not associated_commitment:
                    raise ValueError(
                        f"There must be a commitment from supervision event associated with "
                        f"incarceration period: {incarceration_period}"
                    )
                commitment_from_supervision_supervision_type = (
                    associated_commitment.supervision_type
                )

            total_days_incarcerated = (release_date - original_admission_date).days

            if total_days_incarcerated < 0:
                logging.warning(
                    "release_date before admission_date on incarceration period: %s",
                    incarceration_period,
                )
                total_days_incarcerated = 0

            return IncarcerationReleaseEvent(
                state_code=incarceration_period.state_code,
                included_in_state_population=self.incarceration_delegate.is_period_included_in_state_population(
                    incarceration_period
                ),
                event_date=release_date,
                facility=incarceration_period.facility,
                release_reason=release_reason,
                release_reason_raw_text=incarceration_period.release_reason_raw_text,
                purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
                supervision_type_at_release=supervision_type_at_release,
                admission_reason=original_admission_reason,
                total_days_incarcerated=total_days_incarcerated,
                commitment_from_supervision_supervision_type=commitment_from_supervision_supervision_type,
            )

        return None
