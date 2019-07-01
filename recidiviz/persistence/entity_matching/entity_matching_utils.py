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

import datetime
import logging
from typing import Optional, Set, Callable, Sequence, Dict, Any, Iterable, \
    Union, List, cast

import deepdiff

from recidiviz.persistence.entity.base_entity import Entity, ExternalIdEntity
from recidiviz.persistence.entity.entities import EntityPersonType
from recidiviz.persistence.entity_matching.state.state_matching_utils import \
    EntityTree
from recidiviz.persistence.errors import MatchedMultipleDatabaseEntitiesError


# TODO(2022): Update all utils methods to accept EntityTree and Entity types
def diff_count(entity_a: Entity, entity_b: Entity) -> int:
    """Counts the number of differences between two entities, including
    their descendants."""
    ddiff = deepdiff.DeepDiff(entity_a, entity_b,
                              ignore_order=True, report_repetition=True)
    diff_types = ('values_changed', 'type_changes', 'iterable_item_added',
                  'iterable_item_removed', 'repetition_change')
    if not any(diff_type in ddiff for diff_type in diff_types):
        logging.warning("DeepDiff did not return any of the expected diff "
                        "report fields. Maybe the API changed?\nDiff output:%s",
                        ddiff)

    return sum(len(diffs) for diff_type, diffs in ddiff.items()
               if diff_type in diff_types)


def is_birthdate_match(a: EntityPersonType, b: EntityPersonType) -> bool:
    if a.birthdate_inferred_from_age and b.birthdate_inferred_from_age:
        return _is_inferred_birthdate_match(a.birthdate, b.birthdate)

    if not a.birthdate_inferred_from_age \
            and not b.birthdate_inferred_from_age:
        return a.birthdate == b.birthdate

    return False


def _is_inferred_birthdate_match(
        a: Optional[datetime.date],
        b: Optional[datetime.date]) -> bool:
    if not a or not b:
        return False
    return abs(a.year - b.year) <= 1


def is_match(
        db_entity: Optional[ExternalIdEntity],
        ingested_entity: Optional[ExternalIdEntity],
        match_fields: Set[str]) -> bool:
    if not db_entity or not ingested_entity:
        return db_entity == ingested_entity

    if db_entity.external_id or ingested_entity.external_id:
        return db_entity.external_id == ingested_entity.external_id

    return all(getattr(db_entity, field) == getattr(ingested_entity, field)
               for field in match_fields)


def get_next_available_match(
        ingested_entity: Entity,
        db_entities: Sequence[Entity],
        db_entities_matched_by_id: Dict[int, Any],
        matcher: Callable):
    """
    Finds all |db_entities| that match the provided |ingested_entity| based on
    the |matcher| function, and returns the first of these matches that has not
    already been matched (based on the provided |db_entities_matched_by_id|.
    """
    for db_entity in db_entities:
        if not db_entity.get_id() in db_entities_matched_by_id and \
                matcher(db_entity=db_entity, ingested_entity=ingested_entity):
            return db_entity
    return None


def get_best_match(
        ingest_entity: Entity,
        db_entities: Sequence[Entity],
        matcher: Callable,
        matched_db_ids: Iterable[int]) -> Optional[Entity]:
    """
    Selects the database entity that most closely matches the ingest entity,
    if a match exists. The steps are as follows:
        - Use |matcher| to select a list of candidate matches
        - Disqualify previously matched entities in |matched_db_ids|
        - Select the candidate match that differs minimally from the ingested
          entity.
    """
    matches = get_all_matches(ingest_entity, db_entities, matcher)
    matches = [m for m in matches if m.get_id() not in matched_db_ids]
    if not matches:
        return None
    if len(matches) == 1:
        return matches[0]
    logging.info(
        "Using diff_count to pick best match: Multiple matches found for a "
        "single ingested person.\nIngested entity: %s\nDatabase matches:%s",
        ingest_entity, matches)
    return min(matches,
               key=lambda db_entity: diff_count(ingest_entity, db_entity))


def get_only_match(
        ingested_entity: Union[Entity, EntityTree],
        db_entities: Sequence[Union[Entity, EntityTree]], matcher: Callable):
    """
       Finds the entity in |db_entites| that matches the |ingested_entity|.
       Args:
           ingested_entity: an entity ingested from source (usually website)
           db_entities: List of entities from our db that are potential matches
               for the |ingested_entity|
           matcher:
               (db_entity, ingested_entity) -> (bool)
       Returns:
           The entity from |db_entities| that matches the |ingested_entity|,
           or None if no match is found.
       Raises:
           EntityMatchingError: if more than one match is found.
       """
    matches = get_all_matches(ingested_entity, db_entities, matcher)
    if len(matches) > 1:
        if isinstance(ingested_entity, EntityTree):
            matches = cast(List[EntityTree], matches)
            matched_entities = [m.entity for m in matches]
            raise MatchedMultipleDatabaseEntitiesError(
                ingested_entity.entity, matched_entities)
        raise MatchedMultipleDatabaseEntitiesError(ingested_entity, matches)
    return matches[0] if matches else None


def get_all_matches(
        ingested_entity: Union[Entity, EntityTree],
        db_entities: Sequence[Union[Entity, EntityTree]], matcher: Callable):
    """
    Finds all |db_entities| that match the provided |ingested_entity| based
    on the |matcher| function
    """
    return [db_entity for db_entity in db_entities
            if matcher(db_entity=db_entity, ingested_entity=ingested_entity)]


def generate_id_from_obj(obj) -> str:
    return str(id(obj)) + '_ENTITY_MATCHING_GENERATED'
