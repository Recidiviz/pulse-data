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
from typing import Dict, Iterable, List, Optional, Sequence, Type

from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import DurationMixin
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import get_all_entities_from_tree
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state.entity_field_validators import (
    ParsingOptionalOnlyValidator,
)
from recidiviz.persistence.entity.state.state_entity_mixins import LedgerEntityMixin
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

    if state_code.upper() in ("US_MI", "US_IX", "US_CA", "US_TN", "US_ND", "US_ME"):

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
    entities_by_cls: Dict[Type[Entity], List[Entity]]
) -> Iterable[Error]:
    """Checks that all child entities match entity_tree_unique_constraints.
    If not, this function yields an error message for each child entity and constraint
    that fails. The message shows a pii-limited view of the first three entities that
    fail the checks.
    """
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


def _sentence_group_checks(
    state_person: state_entities.StatePerson,
) -> Iterable[Error]:
    """Yields errors related to StateSentenceGroup and StateSentenceGroupLength:
    - If this person has StateSentenceGroup entities, then the external ID must be associated with a sentence.
    """
    sentences_by_group = defaultdict(list)
    for sentence in state_person.sentences:
        sentences_by_group[sentence.sentence_group_external_id].append(sentence)

    # If we've hydrated any StateSentenceGroup entities, we check that
    # every StateSentence.sentence_group_external_id exists as an external_id
    # of a StateSentenceGroup
    if ids_from_groups := {sg.external_id for sg in state_person.sentence_groups}:
        for sgid in set(sentences_by_group.keys()).difference(ids_from_groups):
            sentence_ext_ids = [s.external_id for s in sentences_by_group[sgid]]
            yield f"Found {sentence_ext_ids=} referencing non-existent StateSentenceGroup {sgid}."

    # Every StateSentenceGroup should have at least one associated StateSentence
    # If all sentences do not have parole possible, then all group level projected parole dates should be None
    for sg in state_person.sentence_groups:
        sentences = sentences_by_group.get(sg.external_id)
        if not sentences:
            yield f"Found StateSentenceGroup {sg.external_id} without an associated sentence."
        elif {s.parole_possible for s in sentences} == {False}:
            for length in sg.sentence_group_lengths:
                if length.parole_eligibility_date_external is not None:
                    yield f"{sg.limited_pii_repr()} has parole eligibility date, but none of its sentences allow parole."
                if length.projected_parole_release_date_external is not None:
                    yield f"{sg.limited_pii_repr()} has projected parole release date, but none of its sentences allow parole."

        if err := ledger_entity_checks(
            state_person,
            state_entities.StateSentenceGroupLength,
            sg.sentence_group_lengths,
        ):
            yield err


def _sentencing_entities_checks(
    state_person: state_entities.StatePerson,
) -> Iterable[Error]:
    """Yields errors for entities related to the sentencing schema, namely:
    - If a StateSentence does not have a sentence_type or imposed_date (from partially hydrated entities).
    - If StateSentenceStatusSnapshot entities with a REVOKED SentenceStatus stem from a sentence that
      do not have a PAROLE or PROBATION sentence_type.
    """

    # TODO(#29961) We need to update the test fixtures for sentence group checks.
    if state_person.state_code != StateCode.US_AZ.value:
        yield from _sentence_group_checks(state_person)

    external_ids = set(s.external_id for s in state_person.sentences)

    for sentence in state_person.sentences:
        if (
            not sentence.imposed_date
            and sentence.sentencing_authority != StateSentencingAuthority.OTHER_STATE
        ):
            yield f"Found sentence {sentence.limited_pii_repr()} with no imposed_date."

        # TODO(#29457) Ensure test fixture data and ingest views allow this check to happen in AZ
        # TODO(#30060) Ensure test fixture data and ingest views allow this check to happen in IX
        # We can then remove the state code check from this statement.
        if (
            not any(sentence.charges)
            and state_person.state_code != StateCode.US_AZ.value
            and state_person.state_code != StateCode.US_IX.value
        ):
            yield f"Found sentence {sentence.limited_pii_repr()} with no charges."
        if sentence.parole_possible is False:
            if any(
                length.parole_eligibility_date_external is not None
                or length.projected_parole_release_date_external is not None
                for length in sentence.sentence_lengths
            ):
                yield (
                    f"Sentence {sentence.limited_pii_repr()} has parole projected dates, "
                    "despite denoting that parole is not possible."
                )

        # If this sentence has consecutive sentences before it, check
        # that they exist for this person.
        if (
            sentence.parent_sentence_external_id_array is not None
            # TODO(#32140) Update state_sentence view in US_IX so that all consecutive sentence exist.
            and state_person.state_code != StateCode.US_IX.value
        ):
            for p_id in sentence.parent_sentence_external_id_array.split(","):
                if p_id not in external_ids:
                    yield (
                        f"{sentence.limited_pii_repr()} denotes parent sentence {p_id}, "
                        f"but {state_person.limited_pii_repr()} does not have a sentence "
                        "with that external ID."
                    )

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
        if sentence.sentence_lengths:
            if err := ledger_entity_checks(
                state_person,
                state_entities.StateSentenceLength,
                sentence.sentence_lengths,
            ):
                yield err
        if sentence.sentence_status_snapshots:
            if err := ledger_entity_checks(
                state_person,
                state_entities.StateSentenceStatusSnapshot,
                sentence.sentence_status_snapshots,
            ):
                yield err
            # TODO(#10389): Add a validation here that throws if a COMPLETED status (or
            #  status that means completion like COMMUTED) is followed by a
            #  non-completion status.


def ledger_entity_checks(
    root_entiy: RootEntityT,
    entity_cls: Type[LedgerEntityMixin],
    ledger_objects: Sequence[LedgerEntityMixin],
) -> Optional[Error]:
    """Yields error messages related to LedgerEntity checks:
    - sequence_num must be all None or all not-None
    - partition_key must be unique if sequence_num are all None
    - sequence_num must be unique
    - sequence_num ledger datetime ordering must be consistent
    """
    preamble = (
        f"Found {root_entiy.limited_pii_repr()} having {entity_cls.__name__} with "
    )
    # If sequence_num are all None, ensure the partition key is unique.
    # Unique partition keys are usually from unique ledger datetimes.
    if all(eo.sequence_num is None for eo in ledger_objects):
        if len(ledger_objects) != len({eo.partition_key for eo in ledger_objects}):
            return (
                preamble + "invalid datetime/sequence_num hydration."
                " If sequence_num is None, then the ledger's partition_key must be unique across hydrated entities."
            )
        return None
    # If there are any sequence_num that are not None, then they ALL must be not None.
    if any(eo.sequence_num is None for eo in ledger_objects):
        return (
            preamble + " inconsistent sequence_num hydration."
            " sequence_num should be None for ALL hydrated entities or NO hydrated entities of the same type."
        )
    # If ALL sequence_num are not None, then they must be unique.
    if len({eo.sequence_num for eo in ledger_objects}) != len(ledger_objects):
        return preamble + "DUPLICATE sequence_num hydration."
    return None


def duration_entity_checks(
    root_entity: RootEntityT,
    entity_cls: Type[DurationMixin],
    duration_objects: Sequence[DurationMixin],
) -> Optional[Error]:
    """Yields error messages related to DurationMixin checks:
    - start_date_inclusive must not be None
    """
    for obj in duration_objects:
        if not obj.start_date_inclusive:
            return (
                f"Found {root_entity.limited_pii_repr()} having a "
                f"{entity_cls.__name__} with a null start date."
            )
    return None


def validate_root_entity(root_entity: RootEntityT) -> List[Error]:
    """The assumed input is a root entity with hydrated children entities attached to it.
    This function checks if the root entity does not violate any entity tree specific
    checks. This function returns a list of errors, where each error corresponds to
    each check that is failed.
    """
    error_messages: List[Error] = []

    # Yields errors if incorrect number of external IDs
    error_messages.extend(_external_id_checks(root_entity))

    entities_by_cls: Dict[Type[Entity], List[Entity]] = defaultdict(list)
    for child in get_all_entities_from_tree(root_entity):
        entities_by_cls[type(child)].append(child)

    # Yields errors if global_unique_constraints fail
    error_messages.extend(_unique_constraint_check(entities_by_cls))

    for entity_cls, entities in entities_by_cls.items():
        for field_name, field_info in attribute_field_type_reference_for_class(
            entity_cls
        ).items():
            validator = field_info.attribute.validator
            if isinstance(validator, ParsingOptionalOnlyValidator):
                for entity in entities:
                    if entity.get_field(field_name) is not None:
                        continue
                    error_messages.append(
                        f"Found entity [{entity.limited_pii_repr()}] with null "
                        f"[{field_name}]. The [{field_name}] field must be set by "
                        f"the time we reach the validations step."
                    )

    if isinstance(root_entity, state_entities.StatePerson):
        error_messages.extend(_sentencing_entities_checks(root_entity))

        if err := duration_entity_checks(
            root_entity,
            state_entities.StateIncarcerationPeriod,
            root_entity.incarceration_periods,
        ):
            error_messages.append(err)

        if err := duration_entity_checks(
            root_entity,
            state_entities.StateSupervisionPeriod,
            root_entity.supervision_periods,
        ):
            error_messages.append(err)

        # Ensure StateTaskDeadline passes ledger checks
        if root_entity.task_deadlines:
            if err := ledger_entity_checks(
                root_entity,
                state_entities.StateTaskDeadline,
                root_entity.task_deadlines,
            ):
                error_messages.append(err)

    return error_messages


def get_entity_key(entity: Entity) -> EntityKey:
    return (
        assert_type(entity.get_id(), int),
        assert_type(entity.get_entity_name(), str),
    )
