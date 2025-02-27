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
"""Utils for the normalization of state entities for calculations."""
import datetime
from typing import Dict, List, Optional, Tuple, Type

from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateIncarcerationSentence,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionSentence,
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.assessment_normalization_manager import (
    AssessmentNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    IncarcerationPeriodNormalizationManager,
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.program_assignment_normalization_manager import (
    ProgramAssignmentNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.sentence_normalization_manager import (
    SentenceNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.staff_role_period_normalization_manager import (
    StaffRolePeriodNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_contact_normalization_manager import (
    SupervisionContactNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
    SupervisionPeriodNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_violation_responses_normalization_manager import (
    ViolationResponseNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.pipelines.utils.period_utils import (
    find_earliest_date_of_period_ending_in_death,
)

# All EntityNormalizationManagers
NORMALIZATION_MANAGERS: List[Type[EntityNormalizationManager]] = [
    IncarcerationPeriodNormalizationManager,
    ProgramAssignmentNormalizationManager,
    SupervisionPeriodNormalizationManager,
    ViolationResponseNormalizationManager,
    AssessmentNormalizationManager,
    SentenceNormalizationManager,
    SupervisionContactNormalizationManager,
    StaffRolePeriodNormalizationManager,
]


# TODO(#10084) Combine incarceration and supervision period normalization
# TODO(#25800) Instantiate incarceration and supervision delegates with their state-specific data
def normalized_periods_for_calculations(
    person_id: int,
    ip_normalization_delegate: StateSpecificIncarcerationNormalizationDelegate,
    sp_normalization_delegate: StateSpecificSupervisionNormalizationDelegate,
    incarceration_periods: List[StateIncarcerationPeriod],
    supervision_periods: List[StateSupervisionPeriod],
    normalized_violation_responses: List[NormalizedStateSupervisionViolationResponse],
    field_index: CoreEntityFieldIndex,
    incarceration_sentences: List[NormalizedStateIncarcerationSentence],
    supervision_sentences: List[NormalizedStateSupervisionSentence],
    staff_external_id_to_staff_id: Dict[Tuple[str, str], int],
) -> Tuple[
    Tuple[List[StateIncarcerationPeriod], AdditionalAttributesMap],
    Tuple[List[StateSupervisionPeriod], AdditionalAttributesMap],
]:
    """Helper for returning the normalized incarceration and supervision periods for
    calculations.

    DISCLAIMER: IP normalization may rely on normalized StateSupervisionPeriod
    entities for some states. Tread carefully if you are implementing any changes to
    SP normalization that may create circular dependencies between these processes.
    """

    # The normalization functions need to know if this person has any periods that
    # ended because of death to handle any open periods or periods that extend past
    # their death date accordingly.
    earliest_death_date: Optional[
        datetime.date
    ] = find_earliest_date_of_period_ending_in_death(
        periods=supervision_periods + incarceration_periods
    )

    sp_normalization_manager = SupervisionPeriodNormalizationManager(
        person_id=person_id,
        supervision_periods=supervision_periods,
        delegate=sp_normalization_delegate,
        earliest_death_date=earliest_death_date,
        incarceration_sentences=incarceration_sentences,
        supervision_sentences=supervision_sentences,
        staff_external_id_to_staff_id=staff_external_id_to_staff_id,
    )

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
        field_index=field_index,
    )

    supervision_period_index = NormalizedSupervisionPeriodIndex(
        sorted_supervision_periods=normalized_sps
    )

    ip_normalization_manager = IncarcerationPeriodNormalizationManager(
        person_id=person_id,
        incarceration_periods=incarceration_periods,
        normalization_delegate=ip_normalization_delegate,
        normalized_supervision_period_index=supervision_period_index,
        normalized_violation_responses=normalized_violation_responses,
        incarceration_sentences=incarceration_sentences,
        field_index=field_index,
        earliest_death_date=earliest_death_date,
    )

    (
        processed_ips,
        additional_ip_attributes,
    ) = (
        ip_normalization_manager.normalized_incarceration_periods_and_additional_attributes()
    )

    return (
        (processed_ips, additional_ip_attributes),
        (processed_sps, additional_sp_attributes),
    )


def normalized_violation_responses_from_processed_versions(
    processed_violation_responses: List[StateSupervisionViolationResponse],
    additional_vr_attributes: AdditionalAttributesMap,
    field_index: CoreEntityFieldIndex,
) -> List[NormalizedStateSupervisionViolationResponse]:
    """Converts the entity trees connected to the |processed_violation_responses|
    into their Normalized versions.

    First, identifies the list of distinct StateSupervisionViolations in the list of
    StateSupervisionViolationResponse entity trees. Then, converts those distinct
    entity trees to the Normalized versions. Finally, assembles and returns the list of
    distinct NormalizedStateSupervisionViolationResponses.
    """
    distinct_processed_violations: List[StateSupervisionViolation] = []

    # We must convert the entity tree from the StateSupervisionViolation roots,
    # otherwise we will drop the StateSupervisionViolations that are attached to the
    # responses.
    for response in processed_violation_responses:
        if not response.supervision_violation:
            raise ValueError(
                "Found empty supervision_violation on response: " f"{response}."
            )

        if response.supervision_violation not in distinct_processed_violations:
            distinct_processed_violations.append(response.supervision_violation)

    normalized_violations = convert_entity_trees_to_normalized_versions(
        root_entities=distinct_processed_violations,
        normalized_entity_class=NormalizedStateSupervisionViolation,
        additional_attributes_map=additional_vr_attributes,
        field_index=field_index,
    )

    distinct_normalized_violation_responses: List[
        NormalizedStateSupervisionViolationResponse
    ] = []

    for normalized_violation in normalized_violations:
        for normalized_response in normalized_violation.supervision_violation_responses:
            if not isinstance(
                normalized_response, NormalizedStateSupervisionViolationResponse
            ):
                raise ValueError(
                    "Found supervision_violation_responses entry that is "
                    "not of type "
                    "NormalizedStateSupervisionViolationResponse. Type "
                    f"is: {type(normalized_response)}."
                )

            if normalized_response not in distinct_normalized_violation_responses:
                distinct_normalized_violation_responses.append(normalized_response)

    return distinct_normalized_violation_responses
