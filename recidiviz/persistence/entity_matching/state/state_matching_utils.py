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
"""State schema specific utils for match database entities with ingested
entities."""
from typing import List, cast

from recidiviz.persistence.entity.base_entity import Entity, ExternalIdEntity
from recidiviz.persistence.entity.entity_utils import \
    EntityFieldType, get_set_entity_field_names
from recidiviz.persistence.entity.state.entities import StatePerson, \
    StatePersonExternalId, StatePersonAlias, StatePersonRace, \
    StatePersonEthnicity
from recidiviz.persistence.errors import EntityMatchingError


# TODO(1868): update with more robust matching code
def is_match(*, ingested_entity: Entity, db_entity: Entity) -> bool:
    """Returns true if the provided |ingested_entity| matches the provided
    |db_entity|. Otherwise returns False.
    """
    if not ingested_entity or not db_entity:
        return ingested_entity == db_entity

    if ingested_entity.__class__ != db_entity.__class__:
        raise EntityMatchingError(
            f"is_match received entities of two different classes: "
            f"ingested entity {ingested_entity.__class__.__name__} and "
            f"db_entity {db_entity.__class__.__name__}",
            ingested_entity.get_entity_name())

    if isinstance(ingested_entity, StatePerson):
        db_entity = cast(StatePerson, db_entity)
        for ingested_external_id in ingested_entity.external_ids:
            for db_external_id in db_entity.external_ids:
                if is_match(ingested_entity=ingested_external_id,
                            db_entity=db_external_id):
                    return True
        return False

    if isinstance(ingested_entity, StatePersonExternalId):
        db_entity = cast(StatePersonExternalId, db_entity)
        return ingested_entity.state_code == db_entity.state_code \
               and ingested_entity.external_id == db_entity.external_id

    # As person has already been matched, assume that any of these 'person
    # attribute' entities are matches if their state_codes align.j
    if isinstance(ingested_entity, StatePersonAlias):
        db_entity = cast(StatePersonAlias, db_entity)
        return ingested_entity.state_code == db_entity.state_code
    if isinstance(ingested_entity, StatePersonRace):
        db_entity = cast(StatePersonRace, db_entity)
        return ingested_entity.state_code == db_entity.state_code
    if isinstance(ingested_entity, StatePersonEthnicity):
        db_entity = cast(StatePersonEthnicity, db_entity)
        return ingested_entity.state_code == db_entity.state_code

    db_entity = cast(ExternalIdEntity, db_entity)
    ingested_entity = cast(ExternalIdEntity, ingested_entity)
    return ingested_entity.external_id == db_entity.external_id


def merge_flat_fields(*, ingested_entity: Entity, db_entity: Entity) -> Entity:
    """Merges all set non-relationship fields on the |ingested_entity| onto the
    |db_entity|. Returns the newly merged entity.
    """
    for child_field_name in get_set_entity_field_names(
            ingested_entity, EntityFieldType.FLAT_FIELD):
        setattr(db_entity, child_field_name,
                getattr(ingested_entity, child_field_name))

    return db_entity


def remove_back_edges(entity: Entity):
    """ Removes all backedges from the provided |entity| and all of its
    children.
    """
    for child_field_name in get_set_entity_field_names(
            entity, EntityFieldType.BACK_EDGE):
        child_field = getattr(entity, child_field_name)
        if isinstance(child_field, list):
            setattr(entity, child_field_name, [])
        else:
            setattr(entity, child_field_name, None)

    for child_field_name in get_set_entity_field_names(
            entity, EntityFieldType.FORWARD_EDGE):
        child_field = getattr(entity, child_field_name)
        if isinstance(child_field, list):
            for child in child_field:
                remove_back_edges(child)
        else:
            remove_back_edges(child_field)


def add_person_to_entity_graph(persons: List[StatePerson]):
    """Adds a person back edge to children of each provided person in
    |persons|.
    """
    for person in persons:
        _add_person_to_entity_graph_helper(person, person)


def _add_person_to_entity_graph_helper(person: StatePerson, entity: Entity):
    _set_person_on_entity(person, entity)

    for child_field_name in get_set_entity_field_names(
            entity, EntityFieldType.FORWARD_EDGE):
        child_field = getattr(entity, child_field_name)
        if isinstance(child_field, list):
            for child in child_field:
                _add_person_to_entity_graph_helper(person, child)
        else:
            _add_person_to_entity_graph_helper(person, child_field)


def _set_person_on_entity(person: StatePerson, entity: Entity):
    if hasattr(entity, 'person'):
        setattr(entity, 'person', person)
