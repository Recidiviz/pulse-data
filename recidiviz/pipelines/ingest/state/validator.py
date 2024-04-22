# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Utility classes for validating state entities and entity trees."""

from collections import defaultdict
from typing import Dict, Iterable, List, Type

from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    get_all_entities_from_tree,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.persistence_utils import RootEntityT
from recidiviz.pipelines.ingest.state.constants import EntityKey, Error
from recidiviz.utils.types import assert_type


def state_allows_multiple_ids_same_type_for_state_person(state_code: str) -> bool:
    if state_code.upper() in (
        "US_ND",
        "US_PA",
        "US_MI",
        "US_OR",
    ):  # TODO(#18005): Edit to allow multiple id for OR id_number but not Record_key
        return True

    # By default, states don't allow multiple different ids of the same type
    return False


def state_allows_multiple_ids_same_type_for_state_staff(state_code: str) -> bool:
    if state_code.upper() in ("US_MI", "US_IX", "US_CA"):
        return True

    # By default, states don't allow multiple different ids of the same type
    return False


def _external_id_checks(root_entity: RootEntityT) -> Iterable[Error]:
    """This function yields error messages relating to the external IDs of a root entity. Namely,
    - If there is not an external ID.
    - If there are multiple IDs of the same type in a state that doesn't allow it.
    """

    if len(root_entity.external_ids) == 0:
        yield (
            f"Found [{type(root_entity).__name__}] with id [{root_entity.get_id()}] missing an "
            f"external_id: {root_entity}"
        )

    if isinstance(root_entity, state_entities.StatePerson):
        allows_multiple_ids_same_type = (
            state_allows_multiple_ids_same_type_for_state_person(root_entity.state_code)
        )
    elif isinstance(root_entity, state_entities.StateStaff):
        allows_multiple_ids_same_type = (
            state_allows_multiple_ids_same_type_for_state_staff(root_entity.state_code)
        )
    else:
        raise ValueError("Found RootEntity that is not StatePerson or StateStaff")

    if not allows_multiple_ids_same_type:
        external_id_types = set()
        for external_id in root_entity.external_ids:
            if external_id.id_type in external_id_types:
                yield (
                    f"Duplicate external id types for [{type(root_entity).__name__}] with id "
                    f"[{root_entity.get_id()}]: {external_id.id_type}"
                )
            external_id_types.add(external_id.id_type)


def _unique_constraint_check(
    root_entity: RootEntityT, field_index: CoreEntityFieldIndex
) -> Iterable[Error]:
    """Checks that all child entities match entity_tree_unique_constraints.
    If not, this function yields an error message for each child entity and constraint
    that fails. The message shows a pii-limited view of the first three entities that
    fail the checks.
    """
    child_entities = get_all_entities_from_tree(root_entity, field_index=field_index)

    entities_by_cls: Dict[Type[Entity], List[Entity]] = defaultdict(list)
    for child in child_entities:
        entities_by_cls[type(child)].append(child)

    for entity_cls, entity_objects in entities_by_cls.items():
        for constraint in entity_cls.entity_tree_unique_constraints():
            grouped_entities: Dict[str, List[Entity]] = defaultdict(list)

            for entity in entity_objects:
                unique_fields = ", ".join(
                    f"{field}={getattr(entity, field)}" for field in constraint.fields
                )
                grouped_entities[unique_fields].append(entity)

            for unique_key in grouped_entities:
                if (n_entities := len(grouped_entities[unique_key])) == 1:
                    continue
                error_msg = f"Found [{n_entities}] {entity_cls.get_entity_name()} entities with ({unique_key})"

                entities_to_show = min(3, n_entities)
                entities_str = "\n  * ".join(
                    e.limited_pii_repr()
                    for e in grouped_entities[unique_key][:entities_to_show]
                )
                error_msg += (
                    f". First {entities_to_show} entities found:\n  * {entities_str}"
                )
                yield error_msg


def _sentencing_entities_checks(
    state_person: state_entities.StatePerson,
) -> Iterable[Error]:
    """Yields errors for entities related to the sentencing schema, namely:
    - If a StateSentence does not have a sentence_type or imposed_date (from partially hydrated entities).
    - If StateSentenceStatusSnapshot entities with a REVOKED SentenceStatus stem from a sentence that
      do not have a PAROLE or PROBATION sentence_type.
    """
    for sentence in state_person.sentences:
        if not sentence.imposed_date:
            yield f"Found sentence {sentence.limited_pii_repr()} with no imposed_date."
        if not sentence.sentence_type:
            yield f"Found sentence {sentence.limited_pii_repr()} with no StateSentenceType."
        elif sentence.sentence_type in {
            StateSentenceType.PAROLE,
            StateSentenceType.PROBATION,
            StateSentenceType.TREATMENT,
        }:
            continue
        for status in sentence.sentence_status_snapshots:
            if status.status == StateSentenceStatus.REVOKED:
                yield (
                    f"Found person {state_person.limited_pii_repr()} with REVOKED status on {sentence.sentence_type} sentence."
                    " REVOKED statuses are only allowed on PROBATION and PAROLE type sentences."
                )


def validate_root_entity(
    root_entity: RootEntityT, field_index: CoreEntityFieldIndex
) -> List[Error]:
    """The assumed input is a root entity with hydrated children entities attached to it.
    This function checks if the root entity does not violate any entity tree specific
    checks. This function returns a list of errors, where each error corresponds to
    each check that is failed.
    """
    error_messages: List[Error] = []

    # Yields errors if incorrect number of external IDs
    error_messages.extend(_external_id_checks(root_entity))

    # Yields errors if global_unique_constraints fail
    error_messages.extend(_unique_constraint_check(root_entity, field_index))

    # TODO(#27113) Check sequence_num on LedgerEntity objects
    if isinstance(root_entity, state_entities.StatePerson):
        error_messages.extend(_sentencing_entities_checks(root_entity))

    return error_messages


def get_entity_key(entity: Entity) -> EntityKey:
    return (
        assert_type(entity.get_id(), int),
        assert_type(entity.get_entity_name(), str),
    )
