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
"""Contains the logic for a ViolationResponseNormalizationManager that manages the
normalization of StateSupervisionViolationResponse entities in the calculation
pipelines."""
import datetime
from collections import defaultdict
from copy import deepcopy
from typing import Any, Dict, List, Optional, Set, Tuple, Type

from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import deep_entity_update
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
    copy_entities_and_add_unique_ids,
    get_shared_additional_attributes_map_for_entities,
    merge_additional_attributes_maps,
    update_entity_with_globally_unique_id,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolatedConditionEntry,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.persistence.entity.state.violation_utils import collect_unique_violations
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.entity_normalization_manager import (
    EntityNormalizationManager,
)
from recidiviz.pipelines.ingest.state.normalization.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
)
from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.utils.types import assert_type


class StateSpecificViolationResponseNormalizationDelegate(StateSpecificDelegate):
    """Interface for state-specific decisions involved in normalization violation
    responses for calculations."""

    # TODO(#8781):  We will eventually want to do some variation of this logic for
    #  all states, which would mean that we don't have a
    #  should_de_duplicate_responses_by_date() function on the delegate
    def should_de_duplicate_responses_by_date(self) -> bool:
        """Whether or not responses should be de-duplicated by response date for the
        state. The default behavior is not to perform any de-duplication of
        StateSupervisionViolationResponse entities by response date."""
        return False

    # pylint: disable=unused-argument
    def get_additional_violation_types_for_response(
        self,
        person_id: int,
        response: StateSupervisionViolationResponse,
        violation_response_index: int,
        sorted_violation_responses: Optional[List[StateSupervisionViolationResponse]],
    ) -> List[StateSupervisionViolationTypeEntry]:
        """Returns the list of additional violation types that need to be added to a
        StateSupervisionViolationResponse's list of supervision_violation_types. By
        default, there are no additional types to be returned."""
        return []

    def update_condition(
        self,
        response: StateSupervisionViolationResponse,  # pylint: disable=unused-argument
        condition_entry: StateSupervisionViolatedConditionEntry,
    ) -> StateSupervisionViolatedConditionEntry:
        """Updates the |condition_entry| on the |response|, if necessary, and returns
        the updated |condition_entry|.

        By default, returns the original |condition_entry| without updates."""
        return condition_entry


class ViolationResponseNormalizationManager(EntityNormalizationManager):
    """Interface for generalized and state-specific normalization of
    StateSupervisionViolationResponses for use in calculations."""

    def __init__(
        self,
        person_id: int,
        violation_responses: List[StateSupervisionViolationResponse],
        delegate: StateSpecificViolationResponseNormalizationDelegate,
        staff_external_id_to_staff_id: Dict[Tuple[str, str], int],
    ):
        self.person_id = person_id
        self._violation_responses = deepcopy(violation_responses)
        self.delegate = delegate
        self.staff_external_id_to_staff_id = staff_external_id_to_staff_id
        self._normalized_violation_responses_and_additional_attributes: Optional[
            Tuple[List[StateSupervisionViolationResponse], AdditionalAttributesMap]
        ] = None

    @staticmethod
    def normalized_entity_classes() -> List[Type[Entity]]:
        return [
            StateSupervisionViolationResponse,
            StateSupervisionViolation,
            StateSupervisionViolationTypeEntry,
            StateSupervisionViolatedConditionEntry,
            StateSupervisionViolationResponseDecisionEntry,
        ]

    def get_normalized_violations(
        self,
    ) -> list[NormalizedStateSupervisionViolation]:
        (
            processed_violation_responses,
            additional_vr_attributes,
        ) = self.normalized_violation_responses_for_calculations()

        normalized_violation_responses = (
            normalized_violation_responses_from_processed_versions(
                processed_violation_responses=processed_violation_responses,
                additional_vr_attributes=additional_vr_attributes,
            )
        )

        return collect_unique_violations(normalized_violation_responses)

    def normalized_violation_responses_for_calculations(
        self,
    ) -> Tuple[List[StateSupervisionViolationResponse], AdditionalAttributesMap]:
        """Performs normalization on violation responses. Filters out draft
        responses or those with null dates, sorts responses by `response_date`,
        updates missing violation data (if needed by the state), de-duplicates
        responses by date (if the state delegate says we should), and returns the
        list of sorted, normalized StateSupervisionViolationResponses."""

        if not self._normalized_violation_responses_and_additional_attributes:
            # Make a deep copy of the original violation responses to preprocess
            responses_for_normalization = deepcopy(self._violation_responses)

            filtered_responses = self._drop_responses_from_calculations(
                responses_for_normalization
            )
            sorted_responses = self._sorted_violation_responses(filtered_responses)
            updated_responses = self._update_violations_on_responses(sorted_responses)

            if self.delegate.should_de_duplicate_responses_by_date():
                updated_responses = self._de_duplicate_responses_by_date(
                    updated_responses
                )

            # Validate VRs
            self.validate_vr_invariants(updated_responses)

            self._normalized_violation_responses_and_additional_attributes = (
                updated_responses,
                self.additional_attributes_map_for_normalized_vrs(
                    violation_responses=updated_responses
                ),
            )

        return self._normalized_violation_responses_and_additional_attributes

    def _drop_responses_from_calculations(
        self,
        violation_responses: List[StateSupervisionViolationResponse],
    ) -> List[StateSupervisionViolationResponse]:
        """Filters out responses with null dates or that are drafts"""
        filtered_responses: List[StateSupervisionViolationResponse] = []

        for response in violation_responses:
            if response.response_date is not None and not response.is_draft:
                filtered_responses.append(response)

        for response in violation_responses:
            if response not in filtered_responses:
                if not response.supervision_violation:
                    raise ValueError(
                        "Violation response missing violation: " f"{response}."
                    )

                # Remove this response from its associated violation since it is
                # being dropped entirely
                response.supervision_violation.supervision_violation_responses.remove(
                    response
                )

        return filtered_responses

    def _sorted_violation_responses(
        self,
        violation_responses: List[StateSupervisionViolationResponse],
    ) -> List[StateSupervisionViolationResponse]:
        """Sorts the list of |violation_responses|."""
        # All responses will have a response_date at this point, but date.min helps
        # to satisfy mypy
        violation_responses.sort(
            key=lambda b: (
                b.response_date or datetime.date.min,
                # For cases where there are multiple responses on the same day, sort by
                # external id for determinism
                b.external_id,
            )
        )

        return violation_responses

    def _de_duplicate_responses_by_date(
        self,
        violation_responses: List[StateSupervisionViolationResponse],
    ) -> List[StateSupervisionViolationResponse]:
        """Some states may erroneously have multiple violation responses with the
        same `response_date` that require de-duplication. To avoid over-counting
        violation responses for these states, we collapse all violation types listed
        on the violations for responses with the same response_date.
        """
        bucketed_responses_by_date: Dict[
            datetime.date, List[StateSupervisionViolationResponse]
        ] = defaultdict(list)
        for response in violation_responses:
            if not response.response_date:
                continue
            bucketed_responses_by_date[response.response_date].append(response)

        deduped_responses_by_date: Dict[
            datetime.date, StateSupervisionViolationResponse
        ] = {}
        for response_date, responses in bucketed_responses_by_date.items():
            sorted_responses = sorted(responses, key=lambda vr: vr.external_id)
            base_response_to_update = sorted_responses[0]
            seen_violation_types: Set[StateSupervisionViolationType] = set()
            deduped_violation_type_entries: List[
                StateSupervisionViolationTypeEntry
            ] = []
            for response in sorted_responses:
                if response.supervision_violation is None:
                    # If this response has no violation then there is no information
                    # we need from it
                    continue

                for (
                    violation_type_entry
                ) in response.supervision_violation.supervision_violation_types:
                    if violation_type_entry.violation_type is None:
                        continue
                    if violation_type_entry.violation_type not in seen_violation_types:
                        deduped_violation_type_entries.append(violation_type_entry)
                        seen_violation_types.add(violation_type_entry.violation_type)

            base_violation = StateSupervisionViolation(
                state_code=base_response_to_update.state_code,
                external_id=assert_type(
                    base_response_to_update.supervision_violation,
                    StateSupervisionViolation,
                ).external_id,
            )

            new_violation_type_entries = copy_entities_and_add_unique_ids(
                person_id=self.person_id, entities=deduped_violation_type_entries
            )

            update_entity_with_globally_unique_id(
                root_entity_id=self.person_id, entity=base_violation
            )

            updated_response = deep_entity_update(
                base_response_to_update,
                supervision_violation=deep_entity_update(
                    base_violation,
                    supervision_violation_types=new_violation_type_entries,
                ),
            )

            deduped_responses_by_date[response_date] = updated_response

        return list(deduped_responses_by_date.values())

    def _update_violations_on_responses(
        self,
        violation_responses: List[StateSupervisionViolationResponse],
    ) -> List[StateSupervisionViolationResponse]:
        """For the violation on each response, adds additional violation types to the
        list of supervision_violation_types and performs necessary updates to the
        violated conditions. The state-specific delegates determine whether or not
        additional violation types need to be added, as well as the necessary updates
        needed to be performed on StateSupervisionViolatedConditionEntry entities
        associated with a given StateSupervisionViolationResponse.

        By default, no additional violation types are added and no updates are made
        to the violated conditions."""

        updated_responses = []
        for index, response in enumerate(violation_responses):
            updated_supervision_violation: Optional[StateSupervisionViolation] = None

            if response.supervision_violation:
                additional_types = (
                    self.delegate.get_additional_violation_types_for_response(
                        self.person_id, response, index, violation_responses
                    )
                )

                supervision_violation = response.supervision_violation

                updated_violation_types = (
                    supervision_violation.supervision_violation_types
                )
                updated_violation_types.extend(additional_types)

                updated_conditions: List[StateSupervisionViolatedConditionEntry] = []

                for condition in supervision_violation.supervision_violated_conditions:
                    updated_conditions.append(
                        self.delegate.update_condition(response, condition)
                    )

                updated_supervision_violation = deep_entity_update(
                    supervision_violation,
                    supervision_violation_types=updated_violation_types,
                    supervision_violated_conditions=updated_conditions,
                )

            updated_response = deep_entity_update(
                response, supervision_violation=updated_supervision_violation
            )

            updated_responses.append(updated_response)
        return updated_responses

    def additional_attributes_map_for_normalized_vrs(
        self,
        violation_responses: List[StateSupervisionViolationResponse],
    ) -> AdditionalAttributesMap:
        """Get additional attributes for each StateProgramAssignment."""
        shared_additional_attributes_map = (
            get_shared_additional_attributes_map_for_entities(
                entities=violation_responses
            )
        )
        violation_response_additional_attributes_map: Dict[
            str, Dict[int, Dict[str, Any]]
        ] = {StateSupervisionViolationResponse.__name__: {}}

        for violation_response in violation_responses:
            if not violation_response.supervision_violation_response_id:
                raise ValueError(
                    "Expected non-null supervision_violation_response_id values"
                    f"at this point. Found {violation_response}."
                )
            deciding_staff_id = None
            if violation_response.deciding_staff_external_id:
                if not violation_response.deciding_staff_external_id_type:
                    raise ValueError(
                        f"Found no deciding_staff_external_id_type for deciding_staff_external_id "
                        f"{violation_response.deciding_staff_external_id} on person "
                        f"{violation_response.person}"
                    )
                deciding_staff_id = self.staff_external_id_to_staff_id[
                    (
                        violation_response.deciding_staff_external_id,
                        violation_response.deciding_staff_external_id_type,
                    )
                ]
            violation_response_additional_attributes_map[
                StateSupervisionViolationResponse.__name__
            ][violation_response.supervision_violation_response_id] = {
                "deciding_staff_id": deciding_staff_id,
            }
        return merge_additional_attributes_maps(
            [
                shared_additional_attributes_map,
                violation_response_additional_attributes_map,
            ]
        )

    @staticmethod
    def validate_vr_invariants(
        violation_responses: List[StateSupervisionViolationResponse],
    ) -> None:
        """Validates that no violation responses violate standards that we can expect
        to be met for all periods in all states at the end of violation response
        normalization.

        Asserts that each response in |violation_responses| has a unique id,
        and confirms that all entity trees are of a valid configuration.
        """
        distinct_response_ids = [
            violation_response.supervision_violation_response_id
            for violation_response in violation_responses
        ]

        if len(distinct_response_ids) != len(set(distinct_response_ids)):
            raise ValueError(
                "Finalized list of StateSupervisionViolationResponse "
                "contains duplicate supervision_violation_response_id "
                f"values: {[distinct_response_ids]}."
            )

        for violation_response in violation_responses:
            if not violation_response.supervision_violation:
                raise ValueError(
                    f"Violation response missing violation: {violation_response}."
                )

            violation = violation_response.supervision_violation

            for associated_response in violation.supervision_violation_responses:
                if associated_response.supervision_violation is not violation:
                    raise ValueError(
                        "Violation response normalization resulted in "
                        "an invalid entity tree, where a child "
                        "StateSupervisionViolationResponse is not "
                        "pointing to its parent "
                        "StateSupervisionViolation: "
                        f"{violation}."
                    )

                if (
                    associated_response.supervision_violation_response_id
                    not in distinct_response_ids
                ):
                    raise ValueError(
                        "Violation response normalization resulted in an invalid "
                        "entity tree, where a StateSupervisionViolationResponse was "
                        "dropped from the list during normalization, but was not "
                        "disconnected from its parent violation: "
                        f"{associated_response}."
                    )


def normalized_violation_responses_from_processed_versions(
    processed_violation_responses: List[StateSupervisionViolationResponse],
    additional_vr_attributes: AdditionalAttributesMap,
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
