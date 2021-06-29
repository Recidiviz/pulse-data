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
"""Identifies instances of admission and release from incarceration."""
import logging
from datetime import date
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.incarceration.incarceration_event import (
    IncarcerationAdmissionEvent,
    IncarcerationCommitmentFromSupervisionAdmissionEvent,
    IncarcerationEvent,
    IncarcerationReleaseEvent,
    IncarcerationStandardAdmissionEvent,
    IncarcerationStayEvent,
)
from recidiviz.calculator.pipeline.utils import assessment_utils
from recidiviz.calculator.pipeline.utils.commitment_from_supervision_utils import (
    default_violation_history_window_pre_commitment_from_supervision,
    get_commitment_from_supervision_details,
)
from recidiviz.calculator.pipeline.utils.entity_pre_processing_utils import (
    pre_processing_managers_for_calculations,
)
from recidiviz.calculator.pipeline.utils.execution_utils import (
    extract_county_of_residence_from_rows,
    list_of_dicts_to_dict_with_keys,
)
from recidiviz.calculator.pipeline.utils.incarceration_period_pre_processing_manager import (
    IncarcerationPreProcessingManager,
)
from recidiviz.calculator.pipeline.utils.pre_processed_incarceration_period_index import (
    PreProcessedIncarcerationPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.pre_processed_supervision_period_index import (
    PreProcessedSupervisionPeriodIndex,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import (
    get_commitment_from_supervision_supervision_type,
    get_post_incarceration_supervision_type,
    get_pre_incarceration_supervision_type,
    include_decisions_on_follow_up_responses_for_most_severe_response,
    pre_commitment_supervision_period_if_commitment,
    state_specific_incarceration_admission_reason_override,
    state_specific_violation_history_window_pre_commitment_from_supervision,
    state_specific_violation_response_pre_processing_function,
    state_specific_violation_responses_for_violation_history,
)
from recidiviz.calculator.pipeline.utils.supervision_period_pre_processing_manager import (
    SupervisionPreProcessingManager,
)
from recidiviz.calculator.pipeline.utils.violation_response_utils import (
    get_most_severe_response_decision,
    prepare_violation_responses_for_calculations,
    responses_on_most_recent_response_date,
    violation_responses_in_window,
)
from recidiviz.calculator.pipeline.utils.violation_utils import (
    get_violation_and_response_history,
)
from recidiviz.common.constants.state.state_assessment import StateAssessmentClass
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.entity_utils import get_single_state_code
from recidiviz.persistence.entity.state.entities import (
    PeriodType,
    StateAssessment,
    StateCharge,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateSentenceGroup,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolationResponse,
)


def find_incarceration_events(
    sentence_groups: List[StateSentenceGroup],
    assessments: List[StateAssessment],
    violation_responses: List[StateSupervisionViolationResponse],
    incarceration_period_judicial_district_association: List[Dict[str, Any]],
    persons_to_recent_county_of_residence: List[Dict[str, Any]],
    supervision_period_to_agent_association: List[Dict[str, Any]],
) -> List[IncarcerationEvent]:
    """Finds instances of admission or release from incarceration.
    Transforms the person's StateIncarcerationPeriods, which are connected to their StateSentenceGroups, into
    IncarcerationAdmissionEvents, IncarcerationStayEvents, and IncarcerationReleaseEvents, representing admissions,
    stays in, and releases from incarceration in a state prison.
    Args:
        - sentence_groups: All of the person's StateSentenceGroups
        - incarceration_period_judicial_district_association: A list of dictionaries with information connecting
            StateIncarcerationPeriod ids to the judicial district responsible for the period of incarceration
        - persons_to_recent_county_of_residence: Reference table rows containing the county that the incarcerated person
            lives in (prior to incarceration).
    Returns:
        A list of IncarcerationEvents for the person.
    """
    incarceration_events: List[IncarcerationEvent] = []
    incarceration_sentences: List[StateIncarcerationSentence] = []
    supervision_sentences: List[StateSupervisionSentence] = []
    for sentence_group in sentence_groups:
        incarceration_sentences.extend(sentence_group.incarceration_sentences)
        supervision_sentences.extend(sentence_group.supervision_sentences)
    (
        incarceration_periods,
        supervision_periods,
    ) = get_unique_periods_from_sentence_groups_and_add_backedges(sentence_groups)

    if not incarceration_periods:
        return incarceration_events

    county_of_residence: Optional[str] = extract_county_of_residence_from_rows(
        persons_to_recent_county_of_residence
    )

    state_code: str = get_single_state_code(incarceration_periods)

    # Convert the list of dictionaries into one dictionary where the keys are the
    # incarceration_period_id values
    incarceration_period_to_judicial_district: Dict[
        Any, Dict[str, Any]
    ] = list_of_dicts_to_dict_with_keys(
        incarceration_period_judicial_district_association,
        key=StateIncarcerationPeriod.get_class_id_name(),
    )

    supervision_period_to_agent_associations = list_of_dicts_to_dict_with_keys(
        supervision_period_to_agent_association,
        StateSupervisionPeriod.get_class_id_name(),
    )

    (
        ip_pre_processing_manager,
        sp_pre_processing_manager,
    ) = pre_processing_managers_for_calculations(
        state_code=state_code,
        incarceration_periods=incarceration_periods,
        supervision_periods=supervision_periods,
    )

    if not ip_pre_processing_manager or not sp_pre_processing_manager:
        raise ValueError("Expected both pre-processed IPs and SPs for this pipeline.")

    incarceration_events.extend(
        find_all_stay_events(
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=supervision_sentences,
            ip_pre_processing_manager=ip_pre_processing_manager,
            incarceration_period_to_judicial_district=incarceration_period_to_judicial_district,
            county_of_residence=county_of_residence,
        )
    )

    incarceration_events.extend(
        find_all_admission_release_events(
            state_code=state_code,
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=supervision_sentences,
            ip_pre_processing_manager=ip_pre_processing_manager,
            sp_pre_processing_manager=sp_pre_processing_manager,
            assessments=assessments,
            violation_responses=violation_responses,
            supervision_period_to_agent_associations=supervision_period_to_agent_associations,
            county_of_residence=county_of_residence,
        )
    )

    return incarceration_events


def find_all_admission_release_events(
    state_code: str,
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    ip_pre_processing_manager: IncarcerationPreProcessingManager,
    sp_pre_processing_manager: SupervisionPreProcessingManager,
    assessments: List[StateAssessment],
    violation_responses: List[StateSupervisionViolationResponse],
    supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
    county_of_residence: Optional[str],
) -> List[Union[IncarcerationAdmissionEvent, IncarcerationReleaseEvent]]:
    """Given the |original_incarceration_periods| generates and returns all IncarcerationAdmissionEvents and
    IncarcerationReleaseEvents.
    """
    incarceration_events: List[
        Union[IncarcerationAdmissionEvent, IncarcerationReleaseEvent]
    ] = []

    incarceration_period_index_for_admissions = ip_pre_processing_manager.pre_processed_incarceration_period_index_for_calculations(
        collapse_transfers=True,
        overwrite_facility_information_in_transfers=False,
    )

    supervision_period_index = (
        sp_pre_processing_manager.pre_processed_supervision_period_index_for_calculations()
    )

    sorted_violation_responses = prepare_violation_responses_for_calculations(
        violation_responses=violation_responses,
        pre_processing_function=state_specific_violation_response_pre_processing_function(
            state_code=state_code
        ),
    )

    for (
        incarceration_period
    ) in incarceration_period_index_for_admissions.incarceration_periods:
        admission_event = admission_event_for_period(
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=supervision_sentences,
            incarceration_period=incarceration_period,
            incarceration_period_index=incarceration_period_index_for_admissions,
            supervision_period_index=supervision_period_index,
            assessments=assessments,
            sorted_violation_responses=sorted_violation_responses,
            supervision_period_to_agent_associations=supervision_period_to_agent_associations,
            county_of_residence=county_of_residence,
        )

        if admission_event:
            incarceration_events.append(admission_event)

    incarceration_period_index_for_releases = ip_pre_processing_manager.pre_processed_incarceration_period_index_for_calculations(
        collapse_transfers=True,
        overwrite_facility_information_in_transfers=True,
    )

    for (
        incarceration_period
    ) in incarceration_period_index_for_releases.incarceration_periods:
        release_event = release_event_for_period(
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=supervision_sentences,
            incarceration_period=incarceration_period,
            incarceration_period_index=incarceration_period_index_for_releases,
            county_of_residence=county_of_residence,
        )

        if release_event:
            incarceration_events.append(release_event)

    return incarceration_events


def find_all_stay_events(
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    ip_pre_processing_manager: IncarcerationPreProcessingManager,
    incarceration_period_to_judicial_district: Dict[int, Dict[Any, Any]],
    county_of_residence: Optional[str],
) -> List[IncarcerationStayEvent]:
    """Given the |original_incarceration_periods| generates and returns all IncarcerationStayEvents based on the
    final day relevant months.
    """
    incarceration_stay_events: List[IncarcerationStayEvent] = []

    incarceration_period_index = ip_pre_processing_manager.pre_processed_incarceration_period_index_for_calculations(
        collapse_transfers=False,
        overwrite_facility_information_in_transfers=False,
    )

    for incarceration_period in incarceration_period_index.incarceration_periods:
        period_stay_events: List[IncarcerationStayEvent] = find_incarceration_stays(
            incarceration_sentences,
            supervision_sentences,
            incarceration_period,
            incarceration_period_index,
            incarceration_period_to_judicial_district,
            county_of_residence,
        )

        if period_stay_events:
            incarceration_stay_events.extend(period_stay_events)

    return incarceration_stay_events


def find_incarceration_stays(
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    incarceration_period: StateIncarcerationPeriod,
    incarceration_period_index: PreProcessedIncarcerationPeriodIndex,
    incarceration_period_to_judicial_district: Dict[int, Dict[Any, Any]],
    county_of_residence: Optional[str],
) -> List[IncarcerationStayEvent]:
    """Finds all days for which this person was incarcerated."""
    incarceration_stay_events: List[IncarcerationStayEvent] = []

    admission_date: Optional[date] = incarceration_period.admission_date
    release_date: Optional[date] = incarceration_period.release_date

    if release_date is None:
        if incarceration_period.status != StateIncarcerationPeriodStatus.IN_CUSTODY:
            # This should not happen after validation.
            raise ValueError(
                "Unexpected missing release date. _infer_missing_dates_and_statuses is not properly"
                " setting missing dates."
            )

        # This person is in custody for this period. Set the release date for tomorrow.
        release_date = date.today() + relativedelta(days=1)

    if admission_date is None:
        return incarceration_stay_events

    supervision_type_at_admission: Optional[
        StateSupervisionPeriodSupervisionType
    ] = get_pre_incarceration_supervision_type(
        incarceration_sentences, supervision_sentences, incarceration_period
    )

    sentence_group: StateSentenceGroup = _get_sentence_group_for_incarceration_period(
        incarceration_period
    )
    judicial_district_code: Optional[str] = _get_judicial_district_code(
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
        int, Tuple[StateIncarcerationPeriodAdmissionReason, Optional[str]]
    ] = incarceration_period_index.original_admission_reasons_by_period_id

    (
        original_admission_reason,
        original_admission_reason_raw_text,
    ) = original_admission_reasons_by_period_id[incarceration_period_id]

    original_admission_reason = state_specific_incarceration_admission_reason_override(
        incarceration_period,
        original_admission_reason,
        supervision_type_at_admission,
    )

    while stay_date < release_date:
        most_serious_charge: Optional[
            StateCharge
        ] = find_most_serious_prior_charge_in_sentence_group(sentence_group, stay_date)
        most_serious_offense_ncic_code: Optional[str] = (
            most_serious_charge.ncic_code if most_serious_charge else None
        )
        most_serious_offense_statute: Optional[str] = (
            most_serious_charge.statute if most_serious_charge else None
        )

        incarceration_stay_events.append(
            IncarcerationStayEvent(
                state_code=incarceration_period.state_code,
                event_date=stay_date,
                facility=incarceration_period.facility,
                county_of_residence=county_of_residence,
                most_serious_offense_ncic_code=most_serious_offense_ncic_code,
                most_serious_offense_statute=most_serious_offense_statute,
                admission_reason=original_admission_reason,
                admission_reason_raw_text=original_admission_reason_raw_text,
                judicial_district_code=judicial_district_code,
                specialized_purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
            )
        )

        stay_date = stay_date + relativedelta(days=1)

    return incarceration_stay_events


def _get_judicial_district_code(
    incarceration_period: StateIncarcerationPeriod,
    incarceration_period_to_judicial_district: Dict[int, Dict[Any, Any]],
) -> Optional[str]:
    """Retrieves the judicial_district_code corresponding to the incarceration period, if one exists."""
    incarceration_period_id: Optional[
        int
    ] = incarceration_period.incarceration_period_id

    if incarceration_period_id is None:
        raise ValueError("Unexpected unset incarceration_period_id.")

    ip_info: Optional[Dict[Any, Any]] = incarceration_period_to_judicial_district.get(
        incarceration_period_id
    )

    if ip_info is not None:
        return ip_info.get("judicial_district_code")

    return None


def _get_sentence_group_for_incarceration_period(
    incarceration_period: StateIncarcerationPeriod,
) -> StateSentenceGroup:
    sentence_groups: List[StateSentenceGroup] = [
        incarceration_sentence.sentence_group
        for incarceration_sentence in incarceration_period.incarceration_sentences
        if incarceration_sentence.sentence_group
    ]

    sentence_groups.extend(
        [
            supervision_sentence.sentence_group
            for supervision_sentence in incarceration_period.supervision_sentences
            if supervision_sentence.sentence_group
        ]
    )

    sentence_group_ids: Set[Optional[int]] = {
        sentence_group.sentence_group_id for sentence_group in sentence_groups
    }

    if len(sentence_group_ids) != 1:
        raise ValueError(
            f"Found unexpected number of sentence groups attached to incarceration_period "
            f"[{incarceration_period.incarceration_period_id}]. Ids: [{str(sentence_group_ids)}]."
        )

    return sentence_groups[0]


def find_most_serious_prior_charge_in_sentence_group(
    sentence_group: StateSentenceGroup, sentence_start_upper_bound: date
) -> Optional[StateCharge]:
    """Finds the most serious offense associated with a sentence that started prior to the cutoff date and is within the
    same sentence group as the incarceration period.
    StateCharges have an optional `ncic_code` field that contains the identifying NCIC code for the offense, as well as
    a `statute` field that describes the offense. NCIC codes decrease in severity as the code increases. E.g. '1010' is
    a more serious offense than '5599'. Therefore, this returns the statute associated with the lowest ranked NCIC code
    attached to the charges in the sentence groups that this incarceration period is attached to. Although most NCIC
    codes are numbers, some may contain characters such as the letter 'A', so the codes are sorted alphabetically.
    """

    relevant_charges: List[StateCharge] = []

    def is_relevant_charge(
        sentence_start_date: Optional[date], charge: StateCharge
    ) -> bool:
        return (
            charge.ncic_code is not None
            and sentence_start_date is not None
            and sentence_start_date < sentence_start_upper_bound
        )

    for incarceration_sentence in sentence_group.incarceration_sentences:
        relevant_charges_in_sentence = [
            charge
            for charge in incarceration_sentence.charges
            if is_relevant_charge(incarceration_sentence.start_date, charge)
        ]
        relevant_charges.extend(relevant_charges_in_sentence)

    for supervision_sentence in sentence_group.supervision_sentences:
        relevant_charges_in_sentence = [
            charge
            for charge in supervision_sentence.charges
            if is_relevant_charge(supervision_sentence.start_date, charge)
        ]
        relevant_charges.extend(relevant_charges_in_sentence)

    if relevant_charges:
        return min(
            relevant_charges,
            key=lambda b: b.ncic_code if b.ncic_code is not None else "",
        )

    return None


def admission_event_for_period(
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    incarceration_period: StateIncarcerationPeriod,
    incarceration_period_index: PreProcessedIncarcerationPeriodIndex,
    supervision_period_index: PreProcessedSupervisionPeriodIndex,
    assessments: List[StateAssessment],
    sorted_violation_responses: List[StateSupervisionViolationResponse],
    supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
    county_of_residence: Optional[str],
) -> Optional[IncarcerationAdmissionEvent]:
    """Returns an IncarcerationAdmissionEvent if this incarceration period represents an
    admission to incarceration."""

    admission_date: Optional[date] = incarceration_period.admission_date
    admission_reason: Optional[
        StateIncarcerationPeriodAdmissionReason
    ] = incarceration_period.admission_reason
    supervision_type_at_admission: Optional[
        StateSupervisionPeriodSupervisionType
    ] = get_pre_incarceration_supervision_type(
        incarceration_sentences, supervision_sentences, incarceration_period
    )

    if admission_date and admission_reason:
        admission_reason = state_specific_incarceration_admission_reason_override(
            incarceration_period,
            admission_reason,
            supervision_type_at_admission,
        )

        (
            admission_is_commitment_from_supervision,
            pre_commitment_supervision_period,
        ) = pre_commitment_supervision_period_if_commitment(
            state_code=incarceration_period.state_code,
            incarceration_period=incarceration_period,
            supervision_period_index=supervision_period_index,
            incarceration_period_index=incarceration_period_index,
        )

        if admission_is_commitment_from_supervision:
            return _commitment_from_supervision_event_for_period(
                incarceration_sentences=incarceration_sentences,
                supervision_sentences=supervision_sentences,
                incarceration_period=incarceration_period,
                pre_commitment_supervision_period=pre_commitment_supervision_period,
                assessments=assessments,
                sorted_violation_responses=sorted_violation_responses,
                supervision_period_to_agent_associations=supervision_period_to_agent_associations,
                county_of_residence=county_of_residence,
            )

        return IncarcerationStandardAdmissionEvent(
            state_code=incarceration_period.state_code,
            event_date=admission_date,
            facility=incarceration_period.facility,
            admission_reason=admission_reason,
            admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
            specialized_purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
            county_of_residence=county_of_residence,
        )

    return None


def _commitment_from_supervision_event_for_period(
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    incarceration_period: StateIncarcerationPeriod,
    pre_commitment_supervision_period: Optional[StateSupervisionPeriod],
    assessments: List[StateAssessment],
    sorted_violation_responses: List[StateSupervisionViolationResponse],
    supervision_period_to_agent_associations: Dict[int, Dict[Any, Any]],
    county_of_residence: Optional[str],
) -> IncarcerationCommitmentFromSupervisionAdmissionEvent:
    """
    Returns the IncarcerationCommitmentFromSupervisionAdmissionEvent corresponding to
    the admission to the |incarceration_period| that qualifies as a commitment from
    supervision admission. Includes details about the period of supervision that
    preceded the admission.
    """
    admission_date = incarceration_period.admission_date
    admission_reason = incarceration_period.admission_reason
    state_code = incarceration_period.state_code

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
        state_code=incarceration_period.state_code,
    )

    violation_responses_for_history = (
        state_specific_violation_responses_for_violation_history(
            state_code=state_code,
            violation_responses=sorted_violation_responses,
            include_follow_up_responses=False,
        )
    )

    violation_history_window = state_specific_violation_history_window_pre_commitment_from_supervision(
        state_code=state_code,
        admission_date=admission_date,
        sorted_and_filtered_violation_responses=violation_responses_for_history,
        default_violation_history_window_function=default_violation_history_window_pre_commitment_from_supervision,
    )

    # Get details about the violation and response history leading up to the
    # admission to incarceration
    violation_history = get_violation_and_response_history(
        upper_bound_exclusive_date=violation_history_window.upper_bound_exclusive_date,
        lower_bound_inclusive_date_override=violation_history_window.lower_bound_inclusive_date,
        violation_responses_for_history=violation_responses_for_history,
        incarceration_period=incarceration_period,
    )

    responses_for_decision_evaluation = violation_responses_for_history

    if include_decisions_on_follow_up_responses_for_most_severe_response(
        incarceration_period.state_code
    ):
        # Get a new state-specific list of violation responses that includes follow-up
        # responses
        responses_for_decision_evaluation = (
            state_specific_violation_responses_for_violation_history(
                state_code=state_code,
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
        pre_commitment_supervision_period=pre_commitment_supervision_period,
        violation_responses=sorted_violation_responses,
        supervision_period_to_agent_associations=supervision_period_to_agent_associations,
    )

    supervision_type = get_commitment_from_supervision_supervision_type(
        incarceration_sentences=incarceration_sentences,
        supervision_sentences=supervision_sentences,
        incarceration_period=incarceration_period,
        previous_supervision_period=pre_commitment_supervision_period,
    )

    deprecated_supervising_district_external_id = (
        commitment_details.level_2_supervision_location_external_id
        or commitment_details.level_1_supervision_location_external_id
    )

    return IncarcerationCommitmentFromSupervisionAdmissionEvent(
        state_code=incarceration_period.state_code,
        event_date=admission_date,
        facility=incarceration_period.facility,
        admission_reason=admission_reason,
        admission_reason_raw_text=incarceration_period.admission_reason_raw_text,
        supervision_type=supervision_type,
        specialized_purpose_for_incarceration=commitment_details.purpose_for_incarceration,
        specialized_purpose_for_incarceration_subtype=commitment_details.purpose_for_incarceration_subtype,
        county_of_residence=county_of_residence,
        case_type=commitment_details.case_type,
        supervision_level=commitment_details.supervision_level,
        supervision_level_raw_text=commitment_details.supervision_level_raw_text,
        assessment_score=assessment_score,
        assessment_level=assessment_level,
        assessment_type=assessment_type,
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


def release_event_for_period(
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    incarceration_period: StateIncarcerationPeriod,
    incarceration_period_index: PreProcessedIncarcerationPeriodIndex,
    county_of_residence: Optional[str],
) -> Optional[IncarcerationReleaseEvent]:
    """Returns an IncarcerationReleaseEvent if this incarceration period represents an release from incarceration."""
    release_date: Optional[date] = incarceration_period.release_date
    release_reason: Optional[
        StateIncarcerationPeriodReleaseReason
    ] = incarceration_period.release_reason
    supervision_type_at_admission: Optional[
        StateSupervisionPeriodSupervisionType
    ] = get_pre_incarceration_supervision_type(
        incarceration_sentences, supervision_sentences, incarceration_period
    )

    incarceration_period_id: Optional[
        int
    ] = incarceration_period.incarceration_period_id

    if not incarceration_period_id:
        raise ValueError(
            "Unexpected incarceration period without an incarceration_period_id."
        )

    original_admission_reasons_by_period_id: Dict[
        int, Tuple[StateIncarcerationPeriodAdmissionReason, Optional[str]]
    ] = incarceration_period_index.original_admission_reasons_by_period_id

    admission_reason, _ = original_admission_reasons_by_period_id[
        incarceration_period_id
    ]

    if incarceration_period.admission_reason:
        admission_reason = state_specific_incarceration_admission_reason_override(
            incarceration_period, admission_reason, supervision_type_at_admission
        )

    if release_date and release_reason:
        supervision_type_at_release: Optional[
            StateSupervisionPeriodSupervisionType
        ] = get_post_incarceration_supervision_type(
            incarceration_sentences, supervision_sentences, incarceration_period
        )

        total_days_incarcerated: Optional[int] = None

        if incarceration_period.admission_date:
            total_days_incarcerated = (
                release_date - incarceration_period.admission_date
            ).days

            if total_days_incarcerated < 0:
                logging.warning(
                    "release_date before admission_date on incarceration period: %s",
                    incarceration_period,
                )
                total_days_incarcerated = 0

        return IncarcerationReleaseEvent(
            state_code=incarceration_period.state_code,
            event_date=release_date,
            facility=incarceration_period.facility,
            release_reason=release_reason,
            release_reason_raw_text=incarceration_period.release_reason_raw_text,
            purpose_for_incarceration=incarceration_period.specialized_purpose_for_incarceration,
            county_of_residence=county_of_residence,
            supervision_type_at_release=supervision_type_at_release,
            admission_reason=admission_reason,
            total_days_incarcerated=total_days_incarcerated,
        )

    return None


def get_unique_periods_from_sentence_groups_and_add_backedges(
    sentence_groups: List[StateSentenceGroup],
) -> Tuple[List[StateIncarcerationPeriod], List[StateSupervisionPeriod]]:
    """Returns a list of unique StateIncarcerationPeriods and StateSupervisionSentences hanging off of the
    StateSentenceGroups. Additionally adds sentence backedges to all StateIncarcerationPeriods/StateSupervisionPeriods.
    """

    incarceration_periods_and_sentences: List[
        Tuple[
            Union[StateSupervisionSentence, StateIncarcerationSentence],
            StateIncarcerationPeriod,
        ]
    ] = []
    supervision_periods_and_sentences: List[
        Tuple[
            Union[StateSupervisionSentence, StateIncarcerationSentence],
            StateSupervisionPeriod,
        ]
    ] = []

    for sentence_group in sentence_groups:
        for incarceration_sentence in sentence_group.incarceration_sentences:
            for incarceration_period in incarceration_sentence.incarceration_periods:
                incarceration_periods_and_sentences.append(
                    (incarceration_sentence, incarceration_period)
                )
            for supervision_period in incarceration_sentence.supervision_periods:
                supervision_periods_and_sentences.append(
                    (incarceration_sentence, supervision_period)
                )
        for supervision_sentence in sentence_group.supervision_sentences:
            for incarceration_period in supervision_sentence.incarceration_periods:
                incarceration_periods_and_sentences.append(
                    (supervision_sentence, incarceration_period)
                )
            for supervision_period in supervision_sentence.supervision_periods:
                supervision_periods_and_sentences.append(
                    (supervision_sentence, supervision_period)
                )

    unique_incarceration_periods = _set_backedges_and_return_unique_periods(
        incarceration_periods_and_sentences
    )
    unique_supervision_periods = _set_backedges_and_return_unique_periods(
        supervision_periods_and_sentences
    )

    return unique_incarceration_periods, unique_supervision_periods


def _set_backedges_and_return_unique_periods(
    sentences_and_periods: List[
        Tuple[Union[StateSupervisionSentence, StateIncarcerationSentence], PeriodType]
    ]
) -> List[PeriodType]:
    period_ids: Set[int] = set()

    unique_periods = []
    for sentence, period in sentences_and_periods:
        # TODO(#2888): Remove this once these bi-directional relationships are hydrated properly
        # Setting this manually because this direction of hydration doesn't happen in the hydration steps
        if isinstance(sentence, StateSupervisionSentence):
            period.supervision_sentences.append(sentence)
        if isinstance(sentence, StateIncarcerationSentence):
            period.incarceration_sentences.append(sentence)

        period_id = period.get_id()
        if not period_id:
            raise ValueError("No period id found for incarceration period.")
        if period_id and period_id not in period_ids:
            period_ids.add(period_id)
            unique_periods.append(period)

    return unique_periods
