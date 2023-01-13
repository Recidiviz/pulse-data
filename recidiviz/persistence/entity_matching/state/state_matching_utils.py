# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
# ============================================================================
"""State specific utils for entity matching. Utils in this file are generic to any DatabaseEntity."""
from enum import Enum
from typing import List, Set, Type, cast

from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_entity_converter.schema_to_entity_class_mapper import (
    SchemaToEntityClassMapper,
)
from recidiviz.persistence.entity.base_entity import (
    EnumEntity,
    ExternalIdEntity,
    HasExternalIdEntity,
    HasMultipleExternalIdsEntity,
)
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    EntityFieldType,
    SchemaEdgeDirectionChecker,
    get_explicilty_set_flat_fields,
    is_placeholder,
    is_reference_only_entity,
)
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity_matching.entity_matching_types import EntityTree
from recidiviz.persistence.errors import EntityMatchingError
from recidiviz.persistence.persistence_utils import SchemaRootEntityT


def is_match(
    ingested_entity: EntityTree,
    db_entity: EntityTree,
    field_index: CoreEntityFieldIndex,
) -> bool:
    """Returns true if the provided |ingested_entity| matches the provided
    |db_entity|. Otherwise returns False.
    """
    return _is_match(
        ingested_entity=ingested_entity.entity,
        db_entity=db_entity.entity,
        field_index=field_index,
    )


def _is_match(
    *,
    ingested_entity: DatabaseEntity,
    db_entity: DatabaseEntity,
    field_index: CoreEntityFieldIndex,
) -> bool:
    """Returns true if the provided |ingested_entity| matches the provided
    |db_entity|. Otherwise returns False.
    """
    # We should never be entity-matching objects from different states.
    if ingested_entity.get_field("state_code") != db_entity.get_field("state_code"):
        raise ValueError(
            f"Attempting to match entity from state "
            f"[{ingested_entity.get_field('state_code')}] with entity from state "
            f"[{db_entity.get_field('state_code')}]"
        )

    if not ingested_entity:
        raise ValueError("Found null ingested_entity.")
    if not db_entity:
        raise ValueError("Found null db_entity.")

    if ingested_entity.__class__ != db_entity.__class__:
        raise ValueError(
            f"is_match received entities of two different classes: ingested entity "
            f"[{ingested_entity}] and db_entity [{db_entity}]"
        )

    if not isinstance(ingested_entity, DatabaseEntity):
        raise ValueError(
            f"Unexpected type for ingested entity [{ingested_entity}]: [{type(ingested_entity)}]"
        )
    if not isinstance(db_entity, DatabaseEntity):
        raise ValueError(
            f"Unexpected type for db entity [{db_entity}]: [{type(db_entity)}]"
        )

    class_mapper = SchemaToEntityClassMapper.get(
        schema_module=schema, entities_module=entities
    )
    ingested_entity_cls = class_mapper.entity_cls_for_schema_cls(
        ingested_entity.__class__
    )

    if issubclass(ingested_entity_cls, HasMultipleExternalIdsEntity):
        for ingested_external_id in ingested_entity.get_field("external_ids"):
            for db_external_id in db_entity.get_field("external_ids"):
                if _is_match(
                    ingested_entity=ingested_external_id,
                    db_entity=db_external_id,
                    field_index=field_index,
                ):
                    return True
        return False

    # TODO(#2671): Update all person attributes below to use complete entity
    # equality instead of just comparing individual fields.
    if issubclass(ingested_entity_cls, ExternalIdEntity):
        return ingested_entity.get_field("external_id") == db_entity.get_field(
            "external_id"
        ) and ingested_entity.get_field("id_type") == db_entity.get_field("id_type")

    # As person has already been matched, assume that any of these 'person
    # attribute' entities are matches if specific attributes match.
    if isinstance(ingested_entity, schema.StatePersonAlias):
        db_entity = cast(schema.StatePersonAlias, db_entity)
        return ingested_entity.full_name == db_entity.full_name
    if isinstance(ingested_entity, schema.StatePersonRace):
        db_entity = cast(schema.StatePersonRace, db_entity)
        return ingested_entity.race == db_entity.race
    if isinstance(ingested_entity, schema.StatePersonEthnicity):
        db_entity = cast(schema.StatePersonEthnicity, db_entity)
        return ingested_entity.ethnicity == db_entity.ethnicity

    # For entities with no external id, rely on matching based on other flat fields.
    if not issubclass(ingested_entity_cls, HasExternalIdEntity):
        return _base_entity_match(ingested_entity, db_entity, field_index=field_index)

    # Placeholders entities are considered equal
    if (
        ingested_entity.get_external_id() is None
        and db_entity.get_external_id() is None
    ):
        return is_placeholder(ingested_entity, field_index) and is_placeholder(
            db_entity, field_index
        )
    return ingested_entity.get_external_id() == db_entity.get_external_id()


def nonnull_fields_entity_match(
    ingested_entity: EntityTree,
    db_entity: EntityTree,
    field_index: CoreEntityFieldIndex,
) -> bool:
    """
    Matching logic for comparing entities that might not have external ids, by
    comparing all flat fields in the given entities. Should only be used for
    entities that we know might not have external_ids based on the ingested
    state data.
    """
    a = ingested_entity.entity
    b = db_entity.entity
    return _base_entity_match(a, b, allow_null_mismatch=True, field_index=field_index)


def _base_entity_match(
    a: DatabaseEntity,
    b: DatabaseEntity,
    field_index: CoreEntityFieldIndex,
    allow_null_mismatch: bool = False,
) -> bool:
    """Returns whether two objects of the same type are an entity match.

    Args:
        a: The first entity to match
        b: The second entity to match
        allow_null_mismatch: Allow for two objects to still match if one has a null value in a field where the other's
            is nonnull.
    """

    # Placeholders never match
    if is_placeholder(a, field_index) or is_placeholder(b, field_index):
        return False

    # Compare external ids if one is present
    if a.get_external_id() or b.get_external_id():
        return a.get_external_id() == b.get_external_id()

    # Compare all flat fields of the two entities
    all_set_flat_field_names = field_index.get_fields_with_non_empty_values(
        a, EntityFieldType.FLAT_FIELD
    ) | field_index.get_fields_with_non_empty_values(b, EntityFieldType.FLAT_FIELD)
    for field_name in all_set_flat_field_names:
        # Skip primary key
        if field_name == a.get_class_id_name():
            continue
        a_field = a.get_field(field_name)
        b_field = b.get_field(field_name)

        if allow_null_mismatch and (a_field is None or b_field is None):
            # Do not disqualify a match if one of the fields is null
            continue

        if a_field != b_field:
            return False

    return True


def generate_child_entity_trees(
    child_field_name: str, entity_trees: List[EntityTree]
) -> List[EntityTree]:
    """Generates a new EntityTree object for each found child of the provided
    |entity_trees| with the field name |child_field_name|.
    """
    child_trees = []
    for entity_tree in entity_trees:
        child_field = entity_tree.entity.get_field(child_field_name)

        # If child_field is unset, skip
        if not child_field:
            continue
        if isinstance(child_field, list):
            children = child_field
        else:
            children = [child_field]
        child_trees.extend(entity_tree.generate_child_trees(children))
    return child_trees


def is_multiple_id_entity(db_entity_cls: Type[DatabaseEntity]) -> bool:
    """Returns True if the given entity can have multiple external ids."""
    class_mapper = SchemaToEntityClassMapper.get(
        schema_module=schema, entities_module=entities
    )
    entity_cls = class_mapper.entity_cls_for_schema_cls(db_entity_cls)
    return issubclass(entity_cls, HasMultipleExternalIdsEntity)


def remove_child_from_entity(
    *, entity: DatabaseEntity, child_field_name: str, child_to_remove: DatabaseEntity
) -> None:
    """If present, removes the |child_to_remove| from the |child_field_name|
    field on the |entity|.
    """

    child_field = entity.get_field(child_field_name)

    if isinstance(child_field, list):
        if child_to_remove in child_field:
            child_field.remove(child_to_remove)
    elif isinstance(child_field, DatabaseEntity):
        if child_field == child_to_remove:
            child_field = None
    entity.set_field(child_field_name, child_field)


def add_child_to_entity(
    *, entity: DatabaseEntity, child_field_name: str, child_to_add: DatabaseEntity
) -> None:
    """Adds the |child_to_add| to the |child_field_name| field on the
    |entity|.
    """

    child_field = entity.get_field(child_field_name)

    if isinstance(child_field, list):
        if child_to_add not in child_field:
            child_field.append(child_to_add)
    else:
        if child_field and child_field != child_to_add:
            raise EntityMatchingError(
                f"Attempting to add child [{child_to_add}] to entity [{entity}], but field [{child_field_name}] "
                f"already had different value [{child_field}]",
                entity.get_entity_name(),
            )
        child_field = child_to_add
        entity.set_field(child_field_name, child_field)


def can_atomically_merge_entity(
    new_entity: DatabaseEntity, field_index: CoreEntityFieldIndex
) -> bool:
    """If True, then when merging this entity onto an existing database entity, all the
    values from this new entity should be taken wholesale, even if some of those values
    are null.
    """

    # We often get data about a multi-id entity from multiple sources - do not overwrite
    # all information with new updates to that entity.
    new_entity_cls = SchemaToEntityClassMapper.get(
        entities_module=entities, schema_module=schema
    ).entity_cls_for_schema_cls(new_entity.__class__)
    if issubclass(new_entity_cls, HasMultipleExternalIdsEntity):
        return False

    # Entities that only represent external id information should always be atomically
    # merged.
    if issubclass(new_entity_cls, ExternalIdEntity):
        return True

    # If this entity only contains external id information - do not merge atomically
    # onto database.
    if is_reference_only_entity(new_entity, field_index) or is_placeholder(
        new_entity, field_index
    ):
        return False

    # Enum-like objects with no external id should always be merged atomically.
    if not issubclass(new_entity_cls, HasExternalIdEntity):
        return True

    # Entities were added to this list by default, with exemptions (below) selected
    # explicitly where issues arose because we were writing to them from multiple
    # sources.
    if isinstance(
        new_entity,
        (
            schema.StateAssessment,
            schema.StateCharge,
            schema.StateDrugScreen,
            schema.StateEmploymentPeriod,
            schema.StateIncarcerationIncident,
            schema.StateIncarcerationIncidentOutcome,
            schema.StateIncarcerationPeriod,
            schema.StateProgramAssignment,
            schema.StateSupervisionContact,
            schema.StateSupervisionCaseTypeEntry,
            schema.StateSupervisionPeriod,
            schema.StateSupervisionSentence,
            schema.StateSupervisionViolation,
            schema.StateSupervisionViolationResponse,
        ),
    ):
        return True

    if isinstance(
        new_entity,
        (
            schema.StateAgent,
            # We update this entity from multiple views in ND
            schema.StateIncarcerationSentence,
            # We update this entity from four views in US_ID:
            # early_discharge_supervision_sentence
            # early_discharge_incarceration_sentence
            # early_discharge_supervision_sentence_deleted_rows
            # early_discharge_incarceration_sentence_deleted_rows
            schema.StateEarlyDischarge,
        ),
    ):
        return False

    raise ValueError(
        f"Cannot determine whether we can merge entity [{new_entity}] atomically "
        f"onto the existing database match."
    )


# TODO(#2244): Create general approach for required fields/default values
def merge_flat_fields(
    *,
    new_entity: DatabaseEntity,
    old_entity: DatabaseEntity,
    field_index: CoreEntityFieldIndex,
) -> DatabaseEntity:
    """Merges all set non-relationship fields on the |new_entity| onto the |old_entity|.
    Returns the newly merged entity."""

    if can_atomically_merge_entity(new_entity, field_index):
        fields = field_index.get_all_core_entity_fields(
            new_entity, EntityFieldType.FLAT_FIELD
        )
    else:
        fields = get_explicilty_set_flat_fields(new_entity, field_index)

        # If an enum field is updated, always update the corresponding raw text field
        # (and vice versa), even if one of the values is null.
        new_fields = set()
        for field_name in fields:
            if isinstance(new_entity.get_field(field_name), Enum):
                new_fields.add(f"{field_name}{EnumEntity.RAW_TEXT_FIELD_SUFFIX}")
            if field_name.endswith(EnumEntity.RAW_TEXT_FIELD_SUFFIX):
                new_fields.add(field_name[: -len(EnumEntity.RAW_TEXT_FIELD_SUFFIX)])
        fields.update(new_fields)

    for child_field_name in fields:
        if child_field_name == old_entity.get_class_id_name():
            continue

        old_entity.set_field(child_field_name, new_entity.get_field(child_field_name))

    return old_entity


def get_all_root_entity_external_ids(
    root_entities: List[SchemaRootEntityT],
) -> Set[str]:
    """Returns the external ids all the provided |root_entities|, regardless of id_type."""
    ids: Set[str] = set()
    for root_entity in root_entities:
        external_ids = get_root_entity_external_ids(root_entity)
        ids.update(external_ids)
    return ids


def get_root_entity_external_ids(db_root_entity: SchemaRootEntityT) -> List[str]:
    external_ids = []
    if not db_root_entity.external_ids:
        raise EntityMatchingError(
            f"Expected external_ids to be non-empty for [{db_root_entity}]",
            db_root_entity.get_class_id_name(),
        )
    for external_id in db_root_entity.external_ids:
        external_ids.append(external_id.external_id)

    return external_ids


def get_multiparent_classes() -> List[Type[DatabaseEntity]]:
    cls_list: List[Type[DatabaseEntity]] = [
        schema.StateCharge,
        schema.StateAgent,
    ]
    direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
    direction_checker.assert_sorted(cls_list)
    return cls_list


def db_id_or_object_id(entity: DatabaseEntity) -> int:
    """If present, returns the primary key field from the provided |entity|,
    otherwise provides the object id.
    """
    return entity.get_id() if entity.get_id() else id(entity)
