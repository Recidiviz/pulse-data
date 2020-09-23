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
# =============================================================================
"""Contains utils for match database entities with ingested entities."""

from typing import Callable, Sequence, List, cast, Optional, TypeVar

from recidiviz.persistence.entity.core_entity import CoreEntity
from recidiviz.persistence.entity_matching.entity_matching_types import \
    EntityTree
from recidiviz.persistence.errors import MatchedMultipleDatabaseEntitiesError

MatchObject = TypeVar('MatchObject', CoreEntity, EntityTree)


# TODO(#2022): Update all utils methods to accept MatchObject types
def get_only_match(
        ingested_entity_obj: MatchObject,
        db_entities: Sequence[MatchObject],
        matcher: Callable) -> Optional[MatchObject]:
    """
       Finds the entity in |db_entites| that matches the |ingested_entity|.
       Args:
           ingested_entity_obj: an entity ingested from source (usually website)
           db_entities: List of entities from our db that are potential matches
               for the |ingested_entity|
           matcher:
               (db_entity, ingested_entity) -> (bool)
       Returns:
           The entity from |db_entities| that matches the |ingested_entity|,
           or None if no match is found.
       Raises:
           EntityMatchingError: if more than one unique match is found.
       """
    matches = get_all_matches(ingested_entity_obj, db_entities, matcher)
    if len(matches) > 1:
        if isinstance(ingested_entity_obj, EntityTree):
            ingested_entity: CoreEntity = ingested_entity_obj.entity
            tree_matches = cast(List[EntityTree], matches)
            matched_entities: List[CoreEntity] = [m.entity for m in tree_matches]
        elif isinstance(ingested_entity_obj, CoreEntity):
            ingested_entity = ingested_entity_obj
            matched_entities = matches
        else:
            raise ValueError(f"Unexpected type [{type(ingested_entity_obj)}]")

        all_same = all(m == matched_entities[0] for m in matched_entities)
        if not all_same:
            raise MatchedMultipleDatabaseEntitiesError(ingested_entity, matched_entities)
    return matches[0] if matches else None


def get_all_matches(
        ingested_entity: MatchObject,
        db_entities: Sequence[MatchObject],
        matcher: Callable
) -> List[MatchObject]:
    """
    Finds all |db_entities| that match the provided |ingested_entity| based
    on the |matcher| function
    """
    return [db_entity for db_entity in db_entities
            if matcher(db_entity=db_entity, ingested_entity=ingested_entity)]
