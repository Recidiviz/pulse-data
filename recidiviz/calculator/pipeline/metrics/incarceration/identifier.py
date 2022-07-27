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
from typing import Any, Dict, List, Optional, Tuple, Union

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.metrics.base_identifier import (
    BaseIdentifier,
    IdentifierContext,
)
from recidiviz.calculator.pipeline.metrics.incarceration.events import (
    IncarcerationAdmissionEvent,
    IncarcerationCommitmentFromSupervisionAdmissionEvent,
    IncarcerationEvent,
    IncarcerationReleaseEvent,
    IncarcerationStandardAdmissionEvent,
    IncarcerationStayEvent,
)
from recidiviz.calculator.pipeline.metrics.utils.commitment_from_supervision_utils import (
    get_commitment_from_supervision_details,
)
from recidiviz.calculator.pipeline.metrics.utils.violation_utils import (
    VIOLATION_HISTORY_WINDOW_MONTHS,
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
    extract_county_of_residence_from_rows,
    list_of_dicts_to_dict_with_keys,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_commitment_from_supervision_delegate import (
    StateSpecificCommitmentFromSupervisionDelegate,
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
    get_post_incarceration_supervision_type,
)
from recidiviz.calculator.pipeline.utils.violation_response_utils import (
    get_most_severe_response_decision,
    responses_on_most_recent_response_date,
    violation_responses_in_window,
)
from recidiviz.calculator.query.state.views.reference.incarceration_period_judicial_district_association import (
    INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.persons_to_recent_county_of_residence import (
    PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.supervision_period_to_agent_association import (
    SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME,
)
from recidiviz.common.constants.state.state_assessment import StateAssessmentClass
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    is_commitment_from_supervision,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import StateAssessment, StatePerson


class IncarcerationIdentifier(BaseIdentifier[List[IncarcerationEvent]]):
    """Identifier class for events related to incarceration."""

    def __init__(self) -> None:
        self.identifier_result_class = IncarcerationEvent
        self.field_index = CoreEntityFieldIndex()

    def identify(
        self, _person: StatePerson, identifier_context: IdentifierContext
    ) -> List[IncarcerationEvent]:

        return self._find_incarceration_events(
            incarceration_delegate=identifier_context[
                StateSpecificIncarcerationDelegate.__name__
            ],
            supervision_delegate=identifier_context[
                StateSpecificSupervisionDelegate.__name__
            ],
            violation_delegate=identifier_context[
                StateSpecificViolationDelegate.__name__
            ],
            commitment_from_supervision_delegate=identifier_context[
                StateSpecificCommitmentFromSupervisionDelegate.__name__
            ],
            incarceration_periods=identifier_context[
                NormalizedStateIncarcerationPeriod.base_class_name()
            ],
            supervision_periods=identifier_context[
                NormalizedStateSupervisionPeriod.base_class_name()
            ],
            assessments=identifier_context[StateAssessment.__name__],
            violation_responses=identifier_context[
                NormalizedStateSupervisionViolationResponse.base_class_name()
            ],
            incarceration_period_judicial_district_association=identifier_context[
                INCARCERATION_PERIOD_JUDICIAL_DISTRICT_ASSOCIATION_VIEW_NAME
            ],
            persons_to_recent_county_of_residence=identifier_context[
                PERSONS_TO_RECENT_COUNTY_OF_RESIDENCE_VIEW_NAME
            ],
            supervision_period_to_agent_association=identifier_context[
                SUPERVISION_PERIOD_TO_AGENT_ASSOCIATION_VIEW_NAME
            ],
        )

    def _find_incarceration_events(
        self,
        incarceration_delegate: StateSpecificIncarcerationDelegate,
        supervision_delegate: StateSpecificSupervisionDelegate,
        violation_delegate: StateSpecificViolationDelegate,
        commitment_from_supervision_delegate: StateSpecificCommitmentFromSupervisionDelegate,
        incarceration_periods: List[NormalizedStateIncarcerationPeriod],
        supervision_periods: List[NormalizedStateSupervisionPeriod],
        assessments: List[StateAssessment],
        violation_responses: List[NormalizedStateSupervisionViolationResponse],
        incarceration_period_judicial_district_association: List[Dict[str, Any]],
        persons_to_recent_county_of_residence: List[Dict[str, Any]],
        supervision_period_to_agent_association: List[Dict[str, Any]],
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

        county_of_residence: Optional[str] = extract_county_of_residence_from_rows(
            persons_to_recent_county_of_residence
        )

        # Convert the list of dictionaries into one dictionary where the keys are the
        # incarceration_period_id values
        incarceration_period_to_judicial_district: Dict[
            int, Dict[str, Any]
        ] = list_of_dicts_to_dict_with_keys(
            incarceration_period_judicial_district_association,
            key=NormalizedStateIncarcerationPeriod.get_class_id_name(),
        )

        supervision_period_to_agent_associations = list_of_dicts_to_dict_with_keys(
            supervision_period_to_agent_association,
            NormalizedStateSupervisionPeriod.get_class_id_name(),
        )

        normalized_violation_responses = sort_normalized_entities_by_sequence_num(
            violation_responses
        )

        ip_index = NormalizedIncarcerationPeriodIndex(
            sorted_incarceration_periods=incarceration_periods,
            incarceration_delegate=incarceration_delegate,
        )

        sp_index = NormalizedSupervisionPeriodIndex(
            sorted_supervision_periods=supervision_periods
        )

        (
            release_events,
            commitments_from_supervision,
        ) = self._find_all_admission_release_events(
            incarceration_delegate=incarceration_delegate,
            commitment_from_supervision_delegate=commitment_from_supervision_delegate,
            violation_delegate=violation_delegate,
            supervision_delegate=supervision_delegate,
            ip_index=ip_index,
            sp_index=sp_index,
            assessments=assessments,
            sorted_violation_responses=normalized_violation_responses,
            supervision_period_to_agent_associations=supervision_period_to_agent_associations,
            county_of_residence=county_of_residence,
        )

        incarceration_events.extend(release_events)
        incarceration_events.extend(
            self._find_all_stay_events(
                ip_index=ip_index,
                incarceration_period_to_judicial_district=incarceration_period_to_judicial_district,
                incarceration_delegate=incarceration_delegate,
                county_of_residence=county_of_residence,
                commitments_from_supervision=commitments_from_supervision,
            )
        )

        return incarceration_events

    def _find_all_admission_release_events(
        self,
        incarceration_delegate: StateSpecificIncarcerationDelegate,
        commitment_from_supervision_delegate: StateSpecificCommitmentFromSupervisionDelegate,
        violation_delegate: StateSpecificViolationDelegate,
        supervision_delegate: StateSpecificSupervisionDelegate,
        ip_index: NormalizedIncarcerationPeriodIndex,
        sp_index: NormalizedSupervisionPeriodIndex,
        assessments: List[StateAssessment],
        sorted_violation_responses: List[NormalizedStateSupervisionViolationResponse],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        county_of_residence: Optional[str],
    ) -> Tuple[
        List[Union[IncarcerationAdmissionEvent, IncarcerationReleaseEvent]],
        Dict[date, IncarcerationCommitmentFromSupervisionAdmissionEvent],
    ]:
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
                incarceration_delegate=incarceration_delegate,
                commitment_from_supervision_delegate=commitment_from_supervision_delegate,
                violation_delegate=violation_delegate,
                supervision_delegate=supervision_delegate,
                incarceration_period=incarceration_period,
                incarceration_period_index=ip_index,
                supervision_period_index=sp_index,
                assessments=assessments,
                sorted_violation_responses=sorted_violation_responses,
                supervision_period_to_agent_associations=supervision_period_to_agent_associations,
                county_of_residence=county_of_residence,
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
                incarceration_delegate=incarceration_delegate,
                supervision_delegate=supervision_delegate,
                commitments_from_supervision=commitments_from_supervision,
                county_of_residence=county_of_residence,
            )

            if release_event:
                incarceration_events.append(release_event)

        return incarceration_events, commitments_from_supervision

    def _find_all_stay_events(
        self,
        ip_index: NormalizedIncarcerationPeriodIndex,
        incarceration_period_to_judicial_district: Dict[int, Dict[str, Any]],
        incarceration_delegate: StateSpecificIncarcerationDelegate,
        commitments_from_supervision: Dict[
            date, IncarcerationCommitmentFromSupervisionAdmissionEvent
        ],
        county_of_residence: Optional[str],
    ) -> List[IncarcerationStayEvent]:
        """Generates and returns all IncarcerationStayEvents for the
        normalized incarceration periods provided by the
        |ip_normalization_manager|.
        """
        incarceration_stay_events: List[IncarcerationStayEvent] = []

        for incarceration_period in ip_index.sorted_incarceration_periods:
            period_stay_events: List[
                IncarcerationStayEvent
            ] = self._find_incarceration_stays(
                incarceration_period,
                ip_index,
                incarceration_period_to_judicial_district,
                incarceration_delegate,
                commitments_from_supervision,
                county_of_residence,
            )

            if period_stay_events:
                incarceration_stay_events.extend(period_stay_events)

        return incarceration_stay_events

    def _find_incarceration_stays(
        self,
        incarceration_period: NormalizedStateIncarcerationPeriod,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        incarceration_period_to_judicial_district: Dict[int, Dict[str, Any]],
        incarceration_delegate: StateSpecificIncarcerationDelegate,
        commitments_from_supervision: Dict[
            date, IncarcerationCommitmentFromSupervisionAdmissionEvent
        ],
        county_of_residence: Optional[str],
    ) -> List[IncarcerationStayEvent]:
        """Finds all days for which this person was incarcerated."""
        incarceration_stay_events: List[IncarcerationStayEvent] = []

        admission_date: Optional[date] = incarceration_period.admission_date
        release_date: Optional[date] = incarceration_period.release_date

        if release_date is None:
            # This person is in custody for this period. Set the release date for
            # tomorrow.
            release_date = date.today() + relativedelta(days=1)

        if admission_date is None:
            return incarceration_stay_events

        judicial_district_code: Optional[str] = self._get_judicial_district_code(
            incarceration_period, incarceration_period_to_judicial_district
        )

        stay_date: date = admission_date

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
            original_admission_reason_raw_text,
            original_admission_date,
        ) = original_admission_reasons_by_period_id[incarceration_period_id]

        commitment_from_supervision_supervision_type: Optional[
            StateSupervisionPeriodSupervisionType
        ] = None
        if is_commitment_from_supervision(original_admission_reason):
            event: Optional[
                IncarcerationCommitmentFromSupervisionAdmissionEvent
            ] = commitments_from_supervision.get(original_admission_date)

            if event:
                commitment_from_supervision_supervision_type = event.supervision_type

        while stay_date < release_date:
            incarceration_stay_events.append(
                IncarcerationStayEvent(
                    state_code=incarceration_period.state_code,
                    included_in_state_population=incarceration_delegate.is_period_included_in_state_population(
                        incarceration_period
                    ),
                    event_date=stay_date,
                    facility=incarceration_period.facility,
                    county_of_residence=county_of_residence,
                    admission_reason=original_admission_reason,
                    admission_reason_raw_text=original_admission_reason_raw_text,
                    judicial_district_code=judicial_district_code,
                    specialized_purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
                    commitment_from_supervision_supervision_type=commitment_from_supervision_supervision_type,
                    custodial_authority=incarceration_period.custodial_authority,
                )
            )

            stay_date = stay_date + relativedelta(days=1)

        return incarceration_stay_events

    def _get_judicial_district_code(
        self,
        incarceration_period: NormalizedStateIncarcerationPeriod,
        incarceration_period_to_judicial_district: Dict[int, Dict[str, Any]],
    ) -> Optional[str]:
        """Retrieves the judicial_district_code corresponding to the incarceration period, if one exists."""
        incarceration_period_id: Optional[
            int
        ] = incarceration_period.incarceration_period_id

        if incarceration_period_id is None:
            raise ValueError("Unexpected unset incarceration_period_id.")

        ip_info: Optional[
            Dict[Any, Any]
        ] = incarceration_period_to_judicial_district.get(incarceration_period_id)

        if ip_info is not None:
            return ip_info.get("judicial_district_code")

        return None

    def _admission_event_for_period(
        self,
        incarceration_delegate: StateSpecificIncarcerationDelegate,
        commitment_from_supervision_delegate: StateSpecificCommitmentFromSupervisionDelegate,
        violation_delegate: StateSpecificViolationDelegate,
        supervision_delegate: StateSpecificSupervisionDelegate,
        incarceration_period: NormalizedStateIncarcerationPeriod,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        supervision_period_index: NormalizedSupervisionPeriodIndex,
        assessments: List[StateAssessment],
        sorted_violation_responses: List[NormalizedStateSupervisionViolationResponse],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        county_of_residence: Optional[str],
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
            and admission_reason != StateIncarcerationPeriodAdmissionReason.TRANSFER
        ):
            if is_commitment_from_supervision(admission_reason):
                return self._commitment_from_supervision_event_for_period(
                    incarceration_period=incarceration_period,
                    incarceration_period_index=incarceration_period_index,
                    supervision_period_index=supervision_period_index,
                    assessments=assessments,
                    sorted_violation_responses=sorted_violation_responses,
                    supervision_period_to_agent_associations=supervision_period_to_agent_associations,
                    county_of_residence=county_of_residence,
                    commitment_from_supervision_delegate=commitment_from_supervision_delegate,
                    violation_delegate=violation_delegate,
                    supervision_delegate=supervision_delegate,
                    incarceration_delegate=incarceration_delegate,
                )

            return IncarcerationStandardAdmissionEvent(
                state_code=incarceration_period.state_code,
                included_in_state_population=incarceration_delegate.is_period_included_in_state_population(
                    incarceration_period
                ),
                event_date=admission_date,
                facility=incarceration_period.facility,
                admission_reason=admission_reason,
                admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
                specialized_purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
                county_of_residence=county_of_residence,
            )

        return None

    def _commitment_from_supervision_event_for_period(
        self,
        incarceration_period: NormalizedStateIncarcerationPeriod,
        incarceration_period_index: NormalizedIncarcerationPeriodIndex,
        supervision_period_index: NormalizedSupervisionPeriodIndex,
        assessments: List[StateAssessment],
        sorted_violation_responses: List[NormalizedStateSupervisionViolationResponse],
        supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
        county_of_residence: Optional[str],
        commitment_from_supervision_delegate: StateSpecificCommitmentFromSupervisionDelegate,
        violation_delegate: StateSpecificViolationDelegate,
        supervision_delegate: StateSpecificSupervisionDelegate,
        incarceration_delegate: StateSpecificIncarcerationDelegate,
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

        (
            assessment_score,
            assessment_level,
            assessment_type,
        ) = assessment_utils.most_recent_applicable_assessment_attributes_for_class(
            admission_date,
            assessments,
            assessment_class=StateAssessmentClass.RISK,
            supervision_delegate=supervision_delegate,
        )
        assessment_score_bucket = assessment_utils.assessment_score_bucket(
            assessment_type, assessment_score, assessment_level, supervision_delegate
        )
        violation_responses_for_history = (
            filter_violation_responses_for_violation_history(
                violation_delegate,
                violation_responses=sorted_violation_responses,
                include_follow_up_responses=False,
            )
        )

        violation_history_window = commitment_from_supervision_delegate.violation_history_window_pre_commitment_from_supervision(
            admission_date=admission_date,
            sorted_and_filtered_violation_responses=violation_responses_for_history,
            default_violation_history_window_months=VIOLATION_HISTORY_WINDOW_MONTHS,
        )

        # Get details about the violation and response history leading up to the
        # admission to incarceration
        violation_history = get_violation_and_response_history(
            upper_bound_exclusive_date=violation_history_window.upper_bound_exclusive_date,
            lower_bound_inclusive_date_override=violation_history_window.lower_bound_inclusive_date,
            violation_responses_for_history=violation_responses_for_history,
            violation_delegate=violation_delegate,
            incarceration_period=incarceration_period,
        )

        responses_for_decision_evaluation = violation_responses_for_history

        if (
            violation_delegate.include_decisions_on_follow_up_responses_for_most_severe_response()
        ):
            # Get a new state-specific list of violation responses that includes follow-up
            # responses
            responses_for_decision_evaluation = (
                filter_violation_responses_for_violation_history(
                    violation_delegate=violation_delegate,
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
            commitment_from_supervision_delegate=commitment_from_supervision_delegate,
            supervision_period_to_agent_associations=supervision_period_to_agent_associations,
            supervision_delegate=supervision_delegate,
        )

        deprecated_supervising_district_external_id = (
            supervision_delegate.get_deprecated_supervising_district_external_id(
                commitment_details.level_1_supervision_location_external_id,
                commitment_details.level_2_supervision_location_external_id,
            )
        )

        return IncarcerationCommitmentFromSupervisionAdmissionEvent(
            state_code=incarceration_period.state_code,
            included_in_state_population=incarceration_delegate.is_period_included_in_state_population(
                incarceration_period
            ),
            event_date=admission_date,
            facility=incarceration_period.facility,
            admission_reason=admission_reason,
            admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
            supervision_type=commitment_details.supervision_type,
            specialized_purpose_for_incarceration=commitment_details.purpose_for_incarceration,
            purpose_for_incarceration_subtype=commitment_details.purpose_for_incarceration_subtype,
            county_of_residence=county_of_residence,
            case_type=commitment_details.case_type,
            supervision_level=commitment_details.supervision_level,
            supervision_level_raw_text=commitment_details.supervision_level_raw_text,
            assessment_score=assessment_score,
            assessment_level=assessment_level,
            assessment_type=assessment_type,
            assessment_score_bucket=assessment_score_bucket,
            most_severe_violation_type=violation_history.most_severe_violation_type,
            most_severe_violation_type_subtype=violation_history.most_severe_violation_type_subtype,
            most_severe_response_decision=violation_history.most_severe_response_decision,
            most_recent_response_decision=most_recent_response_decision,
            response_count=violation_history.response_count,
            violation_history_description=violation_history.violation_history_description,
            violation_type_frequency_counter=violation_history.violation_type_frequency_counter,
            supervising_officer_external_id=commitment_details.supervising_officer_external_id,
            supervising_district_external_id=deprecated_supervising_district_external_id,
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
        incarceration_delegate: StateSpecificIncarcerationDelegate,
        supervision_delegate: StateSpecificSupervisionDelegate,
        commitments_from_supervision: Dict[
            date, IncarcerationCommitmentFromSupervisionAdmissionEvent
        ],
        county_of_residence: Optional[str],
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
                incarceration_period, supervision_period_index, supervision_delegate
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
                included_in_state_population=incarceration_delegate.is_period_included_in_state_population(
                    incarceration_period
                ),
                event_date=release_date,
                facility=incarceration_period.facility,
                release_reason=release_reason,
                release_reason_raw_text=incarceration_period.release_reason_raw_text,
                purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
                county_of_residence=county_of_residence,
                supervision_type_at_release=supervision_type_at_release,
                admission_reason=original_admission_reason,
                total_days_incarcerated=total_days_incarcerated,
                commitment_from_supervision_supervision_type=commitment_from_supervision_supervision_type,
            )

        return None
