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

import networkx

from recidiviz.common.attr_mixins import attribute_field_type_reference_for_class
from recidiviz.common.constants.state.state_charge import StateChargeV2Status
from recidiviz.common.constants.state.state_person_address_period import (
    StatePersonAddressType,
)
from recidiviz.common.constants.state.state_person_staff_relationship_period import (
    StatePersonStaffRelationshipType,
)
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceStatus,
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import (
    CriticalRangesBuilder,
    DurationMixin,
    PotentiallyOpenDateRange,
    get_overlapping_date_ranges,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity,
)
from recidiviz.persistence.entity.entity_utils import get_all_entities_from_tree
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.entity_field_validators import (
    ParsingOptionalOnlyValidator,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    EntityBackedgeValidator,
    NormalizedStatePersonStaffRelationshipPeriod,
)
from recidiviz.persistence.entity.state.state_entity_mixins import LedgerEntityMixin
from recidiviz.persistence.persistence_utils import NormalizedRootEntityT, RootEntityT
from recidiviz.pipelines.ingest.state.constants import EntityKey, Error
from recidiviz.pipelines.ingest.state.multiple_external_id_helpers import (
    person_external_id_types_with_allowed_multiples_per_person,
    staff_external_id_types_with_allowed_multiples_per_person,
)
from recidiviz.utils.types import assert_type

STATES_EXEMPT_FROM_ADDRESS_PERIOD_CHECKS = {
    # TODO(#37678): Remove overlapping periods
    StateCode.US_UT.value,
    # TODO(#37679): Remove overlapping periods
    StateCode.US_ND.value,
}


def _external_id_checks(
    root_entity: RootEntityT | NormalizedRootEntityT,
) -> Iterable[Error]:
    """This function yields error messages relating to the external IDs of a root entity. Namely,
    - If there is not an external ID.
    - If there are multiple IDs of the same type in a state that doesn't allow it.
    """

    if len(root_entity.external_ids) == 0:
        yield (
            f"Found [{type(root_entity).__name__}] with id [{root_entity.get_id()}] missing an "
            f"external_id: {root_entity}"
        )

    if isinstance(
        root_entity,
        (state_entities.StatePerson, normalized_entities.NormalizedStatePerson),
    ):
        allowed_types_with_multiples = (
            person_external_id_types_with_allowed_multiples_per_person(
                StateCode(root_entity.state_code)
            )
        )
    elif isinstance(
        root_entity,
        (state_entities.StateStaff, normalized_entities.NormalizedStateStaff),
    ):
        allowed_types_with_multiples = (
            staff_external_id_types_with_allowed_multiples_per_person(
                StateCode(root_entity.state_code)
            )
        )
    else:
        raise ValueError("Found RootEntity that is not StatePerson or StateStaff")

    external_id_types = set()
    for external_id in root_entity.external_ids:
        if (
            external_id.id_type in external_id_types
            and external_id.id_type not in allowed_types_with_multiples
        ):
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
                error_msg = f"Found [{n_entities}] {entity_cls.__name__} entities with ({unique_key})"

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
        if sentence.sentence_group_external_id:
            sentences_by_group[sentence.sentence_group_external_id].append(sentence)

    # All StateSentenceGroup.external_id values should match against
    # a StateSentence.sentence_group_external_id value
    # If you get this error and StateSentenceGroup is not hydrated,
    # sentence_group_external_id can be null until
    # StateSentenceGroup is hydrated.
    ids_from_groups = {sg.external_id for sg in state_person.sentence_groups}
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


def _check_sentence_status_snapshots(
    state_person: state_entities.StatePerson,
    sentence: state_entities.StateSentence,
) -> Iterable[Error]:
    """
    Yields errors if StateSentenceStatusSnapshots:
      - do not conform to the LedgerEntityMixin protocol
      - are REVOKED on StateSentenceType that can't be revoked.
    """
    if err := ledger_entity_checks(
        state_person,
        state_entities.StateSentenceStatusSnapshot,
        sentence.sentence_status_snapshots,
    ):
        yield err
    for snapshot in sentence.sentence_status_snapshots:
        if (
            snapshot.status == StateSentenceStatus.REVOKED
            and sentence.sentence_type
            not in {
                StateSentenceType.PAROLE,
                StateSentenceType.PROBATION,
                StateSentenceType.TREATMENT,
            }
        ):
            yield (
                f"Found person {state_person.limited_pii_repr()} with REVOKED status on {sentence.sentence_type} sentence."
                " REVOKED statuses are only allowed on PROBATION and PAROLE type sentences."
            )


def _check_sentence_lengths(
    state_person: state_entities.StatePerson,
    sentence: state_entities.StateSentence,
) -> Iterable[Error]:
    """
    Yields errors if StateSentenceLength entities:
      - do not conform to the LedgerEntityMixin protocol
      - have parole projected dates on a sentence without possible parole
    """
    if err := ledger_entity_checks(
        state_person,
        state_entities.StateSentenceLength,
        sentence.sentence_lengths,
    ):
        yield err
    if sentence.parole_possible is False and any(
        length.parole_eligibility_date_external is not None
        or length.projected_parole_release_date_external is not None
        for length in sentence.sentence_lengths
    ):
        yield (
            f"Sentence {sentence.limited_pii_repr()} has parole projected dates, "
            "despite denoting that parole is not possible."
        )


def _consecutive_sentences_check(
    state_person: state_entities.StatePerson,
    sentences_by_external_id: dict[str, state_entities.StateSentence],
) -> Iterable[Error]:
    """
    Checks that all consecutive sentences exist and that there are no cycles
    in the graph of consecutive sentences for a person.
    """
    G: networkx.DiGraph = networkx.DiGraph()
    for external_id, sentence in sentences_by_external_id.items():
        G.add_node(external_id)
        if not sentence.parent_sentence_external_id_array:
            continue
        for parent_id in sentence.parent_sentence_external_id_array.split(","):
            if parent_id not in sentences_by_external_id:
                yield Error(
                    f"Found sentence {sentence.limited_pii_repr()} with parent "
                    f"sentence external ID {parent_id}, but no sentence with that "
                    "external ID exists."
                )
                continue
            G.add_edge(sentence.external_id, parent_id)
    try:
        if cycle := networkx.find_cycle(G, orientation="original"):
            cycle_detail_str = "; ".join(
                f"{child} -> as child of -> {parent}" for child, parent, _ in cycle
            )
            yield Error(
                f"{state_person.limited_pii_repr()} has an invalid set of consecutive sentences that form a cycle: {cycle_detail_str}. "
                "Did you intend to hydrate these a concurrent sentences?"
            )
    except networkx.NetworkXNoCycle:
        pass


def _sentencing_entities_checks(
    state_person: state_entities.StatePerson,
) -> Iterable[Error]:
    """Yields errors for entities related to the sentencing schema, namely:
    - If a StateSentence does not have a sentence_type or imposed_date (from partially hydrated entities).
    - If StateSentenceStatusSnapshot entities with a REVOKED SentenceStatus stem from a sentence that
      do not have a PAROLE or PROBATION sentence_type.
    """

    yield from _sentence_group_checks(state_person)

    sentences_by_external_id = {s.external_id: s for s in state_person.sentences}

    yield from _consecutive_sentences_check(state_person, sentences_by_external_id)

    for sentence in state_person.sentences:
        if (
            not sentence.imposed_date
            and sentence.sentencing_authority != StateSentencingAuthority.OTHER_STATE
        ):
            yield f"Found sentence {sentence.limited_pii_repr()} with no imposed_date."

        if not any(
            charge.status
            # States that we assume provide convicted charges can be hydrated with
            # PRESENT WITHOUT INFO, our de-facto default.
            in (StateChargeV2Status.CONVICTED, StateChargeV2Status.PRESENT_WITHOUT_INFO)
            for charge in sentence.charges
        ):
            yield f"Found sentence {sentence.limited_pii_repr()} with no CONVICTED charges."

        if sentence.sentence_lengths:
            yield from _check_sentence_lengths(state_person, sentence)
        if sentence.sentence_status_snapshots:
            yield from _check_sentence_status_snapshots(state_person, sentence)


def ledger_entity_checks(
    root_entity: RootEntityT | NormalizedRootEntityT,
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
        f"Found {root_entity.limited_pii_repr()} having {entity_cls.__name__} with "
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
    root_entity: RootEntityT | NormalizedRootEntityT,
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


def _person_address_period_checks(
    person: state_entities.StatePerson,
) -> Iterable[Error]:
    """
    This function checks that:
      - Addresses within a person's address periods have a distinct type.
      - Periods within a person's address periods *of the same type* do not overlap.
    For example in our state of OZ:
      - "14 Yellow Brick Road" cannot be both PHYSICAL_RESIDENCE and MAILING
      - "14 Yellow Brick Road" and "Room 205, Shiz University" cannot
        both be the PHYSICAL_RESIDENCE address for a person *at the same time*

    If these assumptions are too strict for your state (e.g. we need an address
    to be both PHYSICAL_RESIDENCE and MAILING), ping #platform-team to discuss!
    """
    by_address: dict[str, StatePersonAddressType] = {}
    by_type: dict[StatePersonAddressType, list[PotentiallyOpenDateRange]] = defaultdict(
        list
    )
    for period in person.address_periods:
        if (
            addr_type := by_address.get(period.full_address)
        ) is not None and addr_type != period.address_type:
            yield (
                f"Found {person.limited_pii_repr()} with StateAddressPeriod address "
                f"used with multiple StatePersonAddressType enums. "
                f"If this assumption is too strict for your state (e.g. we need an "
                f"address to be both PHYSICAL_RESIDENCE and MAILING), ping "
                "#platform-team to discuss!"
            )
        by_address[period.full_address] = period.address_type
        by_type[period.address_type].append(period.date_range)

    for period_type, date_ranges in by_type.items():
        if overlapping_ranges := get_overlapping_date_ranges(date_ranges):
            yield (
                f"Found {person.limited_pii_repr()} with address periods of type {period_type} that overlap.\n"
                f"Date Ranges: {sorted(overlapping_ranges, key= lambda dr: dr.lower_bound_inclusive_date)} "
                "If this assumption is too strict for your state (e.g. we need multiple MAILING addresses), ping #platform-team to discuss!"
            )


def _normalized_person_staff_relationship_period_checks(
    person: normalized_entities.NormalizedStatePerson,
) -> Iterable[Error]:
    """
    This function checks that, for any range of time:
      - There is not more than one overlapping
        NormalizedStatePersonStaffRelationshipPeriod with the same relationship_type and
        relationship_priority.
      - There is not more than one overlapping
        NormalizedStatePersonStaffRelationshipPeriod with the same relationship_type
        and staff_id.

    All relationship periods with relationship_type INTERNAL_UNKNOWN are excluded from
    the above checks.

    If these assumptions are too strict for your state, ping #platform-team to
    discuss!
    """
    critical_range_builder = CriticalRangesBuilder(person.staff_relationship_periods)

    for critical_range in critical_range_builder.get_sorted_critical_ranges():
        periods_overlapping_range = (
            critical_range_builder.get_objects_overlapping_with_critical_range(
                critical_range,
                NormalizedStatePersonStaffRelationshipPeriod,
            )
        )

        periods_by_relationship_type_and_priority: dict[
            tuple[StatePersonStaffRelationshipType, int],
            list[NormalizedStatePersonStaffRelationshipPeriod],
        ] = defaultdict(list)
        periods_by_staff_id_and_relationship_type: dict[
            tuple[int, StatePersonStaffRelationshipType],
            list[NormalizedStatePersonStaffRelationshipPeriod],
        ] = defaultdict(list)
        for period in periods_overlapping_range:
            if (
                period.relationship_type
                == StatePersonStaffRelationshipType.INTERNAL_UNKNOWN
            ):
                continue
            periods_by_relationship_type_and_priority[
                (period.relationship_type, period.relationship_priority)
            ].append(period)

            periods_by_staff_id_and_relationship_type[
                (period.associated_staff_id, period.relationship_type)
            ].append(period)

        for (
            staff_id,
            relationship_type,
        ), period_list in periods_by_staff_id_and_relationship_type.items():
            if len(period_list) <= 1:
                continue

            yield (
                f"Found multiple ({len(period_list)}) "
                f"NormalizedStatePersonStaffRelationshipPeriod with relationship_type "
                f"[{relationship_type}] and staff_id [{staff_id}] on person "
                f"[{person.limited_pii_repr()}] for date range [{critical_range}]. "
                f"Overlapping periods for the same staff member and relationship type "
                f"should be collapsed / deduplicated."
            )

        # TODO(#38347): Add exemption mechanism for states that allow multiple case
        #  managers with no defined priority. Make the exemption role- and
        #  state-specific.
        for (
            relationship_type,
            priority,
        ), period_list in periods_by_relationship_type_and_priority.items():
            if len(period_list) <= 1:
                continue

            yield (
                f"Found multiple ({len(period_list)}) "
                f"NormalizedStatePersonStaffRelationshipPeriod with relationship_type "
                f"[{relationship_type}] and priority [{priority}] on person "
                f"[{person.limited_pii_repr()}] for date range [{critical_range}]. If "
                f"two staff have the same type of relationship with a person during a "
                f"period of time, we must be able to prioritize that relationship."
            )


def _normalized_person_external_id_checks(
    person: normalized_entities.NormalizedStatePerson,
) -> Iterable[Error]:
    """This function checks that:
    - For each group of NormalizedStatePersonExternalId with the same id_type,
    exactly one has is_current_display_id_for_type=True.
    """
    ids_by_type: dict[
        str, list[normalized_entities.NormalizedStatePersonExternalId]
    ] = defaultdict(list)
    for pei in person.external_ids:
        ids_by_type[pei.id_type].append(pei)

    for id_type, external_ids_of_type in ids_by_type.items():
        display_external_ids = [
            pei for pei in external_ids_of_type if pei.is_current_display_id_for_type
        ]
        if len(display_external_ids) == 0:
            yield (
                f"Found no NormalizedStatePersonExternalId on person "
                f"[{person.limited_pii_repr()}] with type [{id_type}] that are "
                f"designated as is_current_display_id_for_type=True. If a person has "
                f"any ids of a given id_type, exactly one must be set as the display "
                f"id."
            )
        if len(display_external_ids) > 1:
            yield (
                f"Found multiple ({len(display_external_ids)}) "
                f"NormalizedStatePersonExternalId on person "
                f"[{person.limited_pii_repr()}] with type [{id_type}] that are "
                f"designated as is_current_display_id_for_type=True. If a person has "
                f"any ids of a given id_type, exactly one must be set as the display "
                f"id."
            )


def validate_root_entity(
    root_entity: RootEntityT | NormalizedRootEntityT,
) -> List[Error]:
    """The assumed input is a root entity with hydrated children entities attached to it.
    This function checks if the root entity does not violate any entity tree specific
    checks. This function returns a list of errors, where each error corresponds to
    each check that is failed.
    """
    entities_module_context = entities_module_context_for_entity(root_entity)
    error_messages: List[Error] = []

    # Yields errors if incorrect number of external IDs
    error_messages.extend(_external_id_checks(root_entity))

    entities_by_cls: Dict[Type[Entity], List[Entity]] = defaultdict(list)
    for child in get_all_entities_from_tree(root_entity, entities_module_context):
        entities_by_cls[type(child)].append(child)

    # Yields errors if global_unique_constraints fail
    error_messages.extend(_unique_constraint_check(entities_by_cls))

    for entity_cls, entities in entities_by_cls.items():
        class_reference = attribute_field_type_reference_for_class(entity_cls)
        for field_name in class_reference.fields:
            field_info = class_reference.get_field_info(field_name)
            validator = field_info.attribute.validator
            if isinstance(
                validator, (ParsingOptionalOnlyValidator, EntityBackedgeValidator)
            ):
                for entity in entities:
                    if entity.get_field(field_name) is not None:
                        continue
                    if (
                        isinstance(validator, EntityBackedgeValidator)
                        and validator.allow_nulls()
                    ):
                        continue
                    error_messages.append(
                        f"Found entity [{entity.limited_pii_repr()}] with null "
                        f"[{field_name}]. The [{field_name}] field must be set by "
                        f"the time we reach the validations step."
                    )

    if isinstance(root_entity, state_entities.StatePerson):
        error_messages.extend(_get_state_person_specific_errors(root_entity))
    if isinstance(root_entity, normalized_entities.NormalizedStatePerson):
        error_messages.extend(_get_normalized_state_person_specific_errors(root_entity))

    return error_messages


def _get_state_person_specific_errors(
    root_entity: state_entities.StatePerson,
) -> List[str]:
    """Yields errors for entities related to StatePerson objects."""
    error_messages: list[str] = []
    error_messages.extend(_sentencing_entities_checks(root_entity))

    if root_entity.state_code not in STATES_EXEMPT_FROM_ADDRESS_PERIOD_CHECKS:
        error_messages.extend(_person_address_period_checks(root_entity))

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


def _legacy_sentencing_entities_checks(
    state_person: normalized_entities.NormalizedStatePerson,
) -> Iterable[Error]:
    """Yields errors for entities related to the legacy sentencing schema, namely:
    - If there are NormalizedStateEarlyDischarge objects with no backedges set
    """

    early_discharges: list[normalized_entities.NormalizedStateEarlyDischarge] = [
        *[
            ed
            for s in state_person.incarceration_sentences
            for ed in s.early_discharges
        ],
        *[ed for s in state_person.supervision_sentences for ed in s.early_discharges],
    ]
    for early_discharge in early_discharges:
        if (
            early_discharge.incarceration_sentence is None
            and early_discharge.supervision_sentence is None
        ):
            yield (
                f"Found entity {early_discharge.limited_pii_repr()} with neither one "
                f"of incarceration_sentence or supervision_sentence backedges set."
            )


def _get_normalized_state_person_specific_errors(
    root_entity: normalized_entities.NormalizedStatePerson,
) -> List[str]:
    assert_type(root_entity, normalized_entities.NormalizedStatePerson)
    error_messages: list[str] = []
    error_messages.extend(_normalized_person_external_id_checks(root_entity))
    error_messages.extend(_legacy_sentencing_entities_checks(root_entity))
    error_messages.extend(
        _normalized_person_staff_relationship_period_checks(root_entity)
    )

    # TODO(#26136): Add validation logic for normalized sentence entities here
    return error_messages


def get_entity_key(entity: Entity) -> EntityKey:
    return (
        assert_type(entity.get_id(), int),
        assert_type(entity.get_entity_name(), str),
    )
