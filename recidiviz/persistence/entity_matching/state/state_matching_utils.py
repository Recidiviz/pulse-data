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
import datetime
from typing import List, cast, Any, Optional, Tuple, Union

import attr

from recidiviz.common.constants import enum_canonical_strings
from recidiviz.persistence.entity.base_entity import Entity, ExternalIdEntity
from recidiviz.persistence.entity.entity_utils import \
    EntityFieldType, get_set_entity_field_names
from recidiviz.persistence.entity.state.entities import StatePerson, \
    StatePersonExternalId, StatePersonAlias, StatePersonRace, \
    StatePersonEthnicity, StateIncarcerationPeriod, \
    StateIncarcerationIncident, StateSentenceGroup
from recidiviz.persistence.errors import EntityMatchingError


class EntityTree:
    """Object that contains an entity and the list of ancestors traversed to get
    to this entity from the root Person node."""

    def __init__(self, entity: Entity, ancestor_chain: List[Entity]):
        if not entity:
            raise EntityMatchingError(
                "When creating EntityTree object, entity field must be set",
                'entity_tree')

        # The final child in this EntityTree.
        self.entity = entity

        # The list of ancestors for the entity above. This list is ordered from
        # furthest to closest ancestor.
        self.ancestor_chain = ancestor_chain[:]

    def generate_parent_tree(self) -> 'EntityTree':
        """Returns an EntityTree object for the direct parent of this
        EntityTree.
        """
        return EntityTree(entity=self.ancestor_chain[-1],
                          ancestor_chain=self.ancestor_chain[:-1])

    def generate_child_trees(self, children: List[Entity]) \
            -> List['EntityTree']:
        """For each of the provided |children| creates a new EntityTree object
        by adding the child to this EntityTree. Returns these new EntityTrees.
        """
        result = []
        for child in children:
            result.append(EntityTree(
                entity=child,
                ancestor_chain=self.ancestor_chain + [self.entity]))
        return result

    def __eq__(self, other):
        return self.entity == other.entity \
               and self.ancestor_chain == other.ancestor_chain


class IndividualMatchResult:
    """Object that represents the result of a match attempt for an
    ingested_entity_tree."""

    def __init__(self, ingested_entity_tree: EntityTree,
                 merged_entity_trees: List[EntityTree]):
        if not ingested_entity_tree:
            raise EntityMatchingError(
                "When creating IndividualMatchResult object, "
                "ingested_entity_tree field must be set",
                'individual_match_result')

        # The initial EntityTree to be matched to DB EntityTrees.
        self.ingested_entity_tree = ingested_entity_tree

        # If matching was successful, these are results of merging the
        # ingested_entity_tree with any of it's DB matches.
        self.merged_entity_trees = merged_entity_trees


class MatchResults:
    """Object that represents the results of a match attempt for a group of
    ingested and database EntityTree objects"""

    def __init__(self, individual_match_results: List[IndividualMatchResult],
                 unmatched_db_entities: List[Entity]):
        if not individual_match_results:
            raise EntityMatchingError(
                "When creating MatchResults object, individual_match_results "
                "field must be set/non empty", 'individual_match_result')

        # Results for each individual ingested EntityTree.
        self.individual_match_results = individual_match_results

        # List of db entities that were unmatched.
        self.unmatched_db_entities = unmatched_db_entities


def is_match(*, ingested_entity: EntityTree, db_entity: EntityTree) -> bool:
    """Returns true if the provided |ingested_entity| matches the provided
    |db_entity|. Otherwise returns False.
    """
    return _is_match(ingested_entity=ingested_entity.entity,
                     db_entity=db_entity.entity)


def _is_match(*, ingested_entity: Entity, db_entity: Entity) -> bool:
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
                if _is_match(ingested_entity=ingested_external_id,
                             db_entity=db_external_id):
                    return True
        return False

    if isinstance(ingested_entity, StatePersonExternalId):
        db_entity = cast(StatePersonExternalId, db_entity)
        return ingested_entity.state_code == db_entity.state_code \
               and ingested_entity.external_id == db_entity.external_id \
               and ingested_entity.id_type == db_entity.id_type

    # As person has already been matched, assume that any of these 'person
    # attribute' entities are matches if their state_codes align.
    if isinstance(ingested_entity, StatePersonAlias):
        db_entity = cast(StatePersonAlias, db_entity)
        return ingested_entity.state_code == db_entity.state_code \
                and ingested_entity.full_name == db_entity.full_name
    if isinstance(ingested_entity, StatePersonRace):
        db_entity = cast(StatePersonRace, db_entity)
        return ingested_entity.state_code == db_entity.state_code \
                and ingested_entity.race == db_entity.race
    if isinstance(ingested_entity, StatePersonEthnicity):
        db_entity = cast(StatePersonEthnicity, db_entity)
        return ingested_entity.state_code == db_entity.state_code \
                and ingested_entity.ethnicity == db_entity.ethnicity

    db_entity = cast(ExternalIdEntity, db_entity)
    ingested_entity = cast(ExternalIdEntity, ingested_entity)
    return ingested_entity.external_id == db_entity.external_id


def remove_back_edges(entity: Entity):
    """ Removes all backedges from the provided |entity| and all of its
    children.
    """
    for child_field_name in get_set_entity_field_names(
            entity, EntityFieldType.BACK_EDGE):
        child_field = get_field(entity, child_field_name)
        if isinstance(child_field, list):
            set_field(entity, child_field_name, [])
        else:
            set_field(entity, child_field_name, None)

    for child_field_name in get_set_entity_field_names(
            entity, EntityFieldType.FORWARD_EDGE):
        child_field = get_field(entity, child_field_name)
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
        child_field = get_field(entity, child_field_name)
        if isinstance(child_field, list):
            for child in child_field:
                _add_person_to_entity_graph_helper(person, child)
        else:
            _add_person_to_entity_graph_helper(person, child_field)


def _set_person_on_entity(person: StatePerson, entity: Entity):
    if hasattr(entity, 'person'):
        set_field(entity, 'person', person)


def has_default_status(entity: Entity) -> bool:
    if hasattr(entity, 'status'):
        status = get_field(entity, 'status')
        return status \
               and status.value == enum_canonical_strings.present_without_info
    return False


def is_placeholder(entity: Entity) -> bool:
    """Determines if the provided entity is a placeholder. Conceptually, a
    placeholder is an object that we have no information about, but have
    inferred its existence based on other objects we do have information about.
    Generally, an entity is a placeholder if all of the optional flat fields are
    empty or set to a default value.
    """

    # Although these are not flat fields, they represent characteristics of a
    # person. If present, we do have information about the provided person, and
    # therefore it is not a placeholder.
    if isinstance(entity, StatePerson):
        entity = cast(StatePerson, entity)
        if any([entity.external_ids, entity.races, entity.aliases,
                entity.ethnicities]):
            return False

    copy = attr.evolve(entity)

    # Clear id
    copy.clear_id()

    # Clear state code as it's non nullable
    if hasattr(copy, 'state_code'):
        set_field(copy, 'state_code', None)

    # Clear status if set to default value
    if has_default_status(entity):
        set_field(copy, 'status', None)

    set_flat_fields = get_set_entity_field_names(
        copy, EntityFieldType.FLAT_FIELD)
    return not bool(set_flat_fields)


def get_field(entity: Entity, field_name: str):
    if not hasattr(entity, field_name):
        raise EntityMatchingError(
            f"Expected entity {entity} to have field {field_name}, but it did "
            f"not.", entity.get_entity_name())
    return getattr(entity, field_name)


def set_field(entity: Entity, field_name: str, value: Any):
    if not hasattr(entity, field_name):
        raise EntityMatchingError(
            f"Expected entity {entity} to have field {field_name}, but it did "
            f"not.", entity.get_entity_name())
    return setattr(entity, field_name, value)


def generate_child_entity_trees(
        child_field_name: str, entity_trees: List[EntityTree]) \
        -> List[EntityTree]:
    """Generates a new EntityTree object for each found child of the provided
    |entity_trees| with the field name |child_field_name|.
    """
    child_trees = []
    for entity_tree in entity_trees:
        child_field = get_field(entity_tree.entity, child_field_name)

        # If child_field is unset, skip
        if not child_field:
            continue
        if isinstance(child_field, list):
            children = child_field
        else:
            children = [child_field]
        child_trees.extend(entity_tree.generate_child_trees(children))
    return child_trees


def remove_child_from_entity(
        *, entity: Entity, child_field_name: str, child_to_remove: Entity):
    """If present, removes the |child_to_remove| from the |child_field_name|
    field on the |entity|.
    """
    child_field = get_field(entity, child_field_name)

    if isinstance(child_field, list):
        if child_to_remove in child_field:
            child_field.remove(child_to_remove)
    elif isinstance(child_field, Entity):
        if child_field == child_to_remove:
            child_field = None
    set_field(entity, child_field_name, child_field)


def add_child_to_entity(
        *, entity: Entity, child_field_name: str, child_to_add: Entity):
    """Adds the |child_to_add| to the |child_field_name| field on the
    |entity|.
    """
    child_field = get_field(entity, child_field_name)

    if isinstance(child_field, list):
        if child_to_add not in child_field:
            child_field.append(child_to_add)
    else:
        if child_field and child_field != child_to_add:
            raise EntityMatchingError(
                f"Attempting to add child {child_to_add} to entity {entity}, "
                f"but {child_field_name} already had different value "
                f"{child_field}", entity.get_entity_name())
        child_field = child_to_add
    set_field(entity, child_field_name, child_field)


# TODO(2037): Move the following into North Dakota specific file.
def merge_incarceration_periods(ingested_persons: List[StatePerson]):
    """Merges any incomplete StateIncarcerationPeriods in the provided
    |ingested_persons|.
    """
    for person in ingested_persons:
        for sentence_group in person.sentence_groups:
            for incarceration_sentence in \
                    sentence_group.incarceration_sentences:
                incarceration_sentence.incarceration_periods = \
                    _merge_incarceration_periods_helper(
                        incarceration_sentence.incarceration_periods)


def _merge_incarceration_periods_helper(
        incomplete_incarceration_periods: List[StateIncarcerationPeriod]) \
        -> List[StateIncarcerationPeriod]:
    """Using the provided |incomplete_incarceration_periods|, attempts to merge
    consecutive admission and release periods from the same facility.

    Returns a list containing all merged incarceration periods as well as all
    incarceration periods that could not be merged, all ordered chronologically
    (based on the movement sequence number provided directly from ND).
    """

    placeholder_periods = [
        p for p in incomplete_incarceration_periods if is_placeholder(p)]
    non_placeholder_periods = [
        p for p in incomplete_incarceration_periods if not is_placeholder(p)]

    # Within any IncarcerationSentence, IncarcerationPeriod external_ids are all
    # equivalent, except for their suffixes. Each suffix is based on the
    # ND-provided movement sequence number. Therefore, sorting by external_ids
    # is equivalent to sorting by this movement sequence number.
    sorted_periods = sorted(non_placeholder_periods, key=_get_sequence_no)
    merged_periods = []
    last_period = None
    for period in sorted_periods:
        if not last_period:
            last_period = period
            continue
        if is_incomplete_incarceration_period_match(last_period, period):
            merged_periods.append(
                _merge_incomplete_periods(period, last_period))
            last_period = None
        else:
            merged_periods.append(last_period)
            last_period = period

    if last_period:
        merged_periods.append(last_period)
    merged_periods.extend(placeholder_periods)
    return merged_periods


_INCARCERATION_PERIOD_ID_DELIMITER = '|'


def _merge_incomplete_periods(
        a: StateIncarcerationPeriod, b: StateIncarcerationPeriod) \
        -> StateIncarcerationPeriod:
    if bool(a.admission_date) and bool(b.release_date):
        admission_period, release_period = a, b
    elif bool(a.release_date) and bool(b.admission_date):
        admission_period, release_period = b, a
    else:
        raise EntityMatchingError(
            f"Expected one admission period and one release period when "
            f"merging, instead found periods: {a}, {b}", a.get_entity_name())

    merged_period = attr.evolve(admission_period)
    admission_external_id = admission_period.external_id or ''
    release_external_id = release_period.external_id or ''
    new_external_id = admission_external_id \
                      + _INCARCERATION_PERIOD_ID_DELIMITER \
                      + release_external_id
    _default_merge_flat_fields(new_entity=release_period,
                               old_entity=merged_period)

    merged_period.external_id = new_external_id
    return merged_period


def is_incomplete_incarceration_period_match(
        ingested_entity: Union[EntityTree, Entity],
        db_entity: Union[EntityTree, Entity]) -> bool:
    """Given two incomplete StateIncarcerationPeriods, determines if they
    should be considered the same StateIncarcerationPeriod.
    """

    a, b = ingested_entity, db_entity
    if isinstance(ingested_entity, EntityTree):
        db_entity = cast(EntityTree, db_entity)
        a, b = ingested_entity.entity, db_entity.entity
    a = cast(StateIncarcerationPeriod, a)
    b = cast(StateIncarcerationPeriod, b)

    a_seq_no = _get_sequence_no(a)
    b_seq_no = _get_sequence_no(b)

    # Only match incomplete periods if they are adjacent based on seq no.
    if abs(a_seq_no - b_seq_no) != 1:
        return False

    # Check that the first period is an admission and second a release
    if a_seq_no < b_seq_no:
        first, second = a, b
    else:
        first, second = b, a
    if not first.admission_date or not second.release_date:
        return False

    # Must have same facility
    if a.facility != b.facility:
        return False

    return True


def _get_sequence_no(period: StateIncarcerationPeriod) -> int:
    """Extracts the ND specific Movement Sequence Number from the external id
    of the provided |period|.
    """
    try:
        external_id = cast(str, period.external_id)
        sequence_no = int(external_id.split('-')[-1])
    except Exception:
        raise EntityMatchingError(
            f"Could not parse sequence number from external_id "
            f"{period.external_id}", period.get_entity_name())
    return sequence_no


def is_incarceration_period_complete(period: StateIncarcerationPeriod) -> bool:
    """Returns True if the period is considered complete (has both an admission
    and release date).
    """
    return all([period.admission_date, period.release_date])


def merge_flat_fields(new_entity: Entity, old_entity: Entity):
    """Merges appropriate non-relationship fields on the |new_entity| onto the
    |old_entity|. Returns the newly merged entity."""

    # Special merge logic if we are merging 2 different incarceration periods.
    if isinstance(new_entity, StateIncarcerationPeriod):
        old_entity = cast(StateIncarcerationPeriod, old_entity)
        if new_entity.external_id != old_entity.external_id:
            return _merge_incomplete_periods(new_entity, old_entity)

    return _default_merge_flat_fields(
        new_entity=new_entity, old_entity=old_entity)


def _default_merge_flat_fields(*, new_entity: Entity, old_entity: Entity) \
        -> Entity:
    """Merges all set non-relationship fields on the |new_entity| onto the
    |old_entity|. Returns the newly merged entity.
    """
    for child_field_name in get_set_entity_field_names(
            new_entity, EntityFieldType.FLAT_FIELD):
        # Do not overwrite with default status
        if child_field_name == 'status' and has_default_status(new_entity):
            continue

        set_field(old_entity, child_field_name,
                  get_field(new_entity, child_field_name))

    return old_entity


def move_incidents_onto_periods(merged_persons: List[StatePerson]):
    """Moves all StateIncarcerationIncidents that have placeholder
    StateIncarcerationPeriod parents onto non-placeholder
    StateIncarcerationPeriods if appropriate.
    """
    for person in merged_persons:
        for sentence_group in person.sentence_groups:
            placeholder_periods, non_placeholder_periods = \
                _get_periods_in_sentence_group(sentence_group)
            _move_incidents_onto_periods_helper(
                placeholder_periods=placeholder_periods,
                non_placeholder_periods=non_placeholder_periods)


def _get_periods_in_sentence_group(sentence_group: StateSentenceGroup) \
        -> Tuple[List[StateIncarcerationPeriod],
                 List[StateIncarcerationPeriod]]:
    """Finds all placeholder and non-placeholder StateIncarcerationPeriods in
    the provided |sentence_group|, and returns the two lists in a tuple.
    """
    placeholder_periods = []
    non_placeholder_periods = []

    for incarceration_sentence in \
            sentence_group.incarceration_sentences:
        for incarceration_period in \
                incarceration_sentence.incarceration_periods:
            if is_placeholder(incarceration_period):
                placeholder_periods.append(incarceration_period)
            else:
                non_placeholder_periods.append(incarceration_period)
    return placeholder_periods, non_placeholder_periods


def _move_incidents_onto_periods_helper(
        *,
        placeholder_periods: List[StateIncarcerationPeriod],
        non_placeholder_periods: List[StateIncarcerationPeriod]):
    """Moves all StateIncarcerationIncidents on any of the provided
    |placeholder_periods| onto periods in |non_placeholder_periods|, if a
    matching non-placeholder period exists.
    """
    for placeholder_period in placeholder_periods:
        incidents_to_remove = []
        for incident in placeholder_period.incarceration_incidents:
            match = _find_matching_period(
                incident, non_placeholder_periods)
            if match:
                add_child_to_entity(
                    entity=match,
                    child_field_name='incarceration_incidents',
                    child_to_add=incident)
                incidents_to_remove.append(incident)

        # Remove incidents from placeholder parent after looping through all
        # incidents.
        for incident in incidents_to_remove:
            remove_child_from_entity(
                entity=placeholder_period,
                child_field_name='incarceration_incidents',
                child_to_remove=incident)


def _find_matching_period(
        incident: StateIncarcerationIncident,
        potential_periods: List[StateIncarcerationPeriod]) -> \
        Optional[StateIncarcerationPeriod]:
    """Given the |incident|, finds a matching StateIncarcerationPeriod from
    the provided |periods|, if one exists.
    """
    incident_date = incident.incident_date
    if not incident_date:
        return None

    for potential_period in potential_periods:
        admission_date = potential_period.admission_date
        release_date = potential_period.release_date

        # Only match to periods with admission_dates
        if not admission_date:
            continue

        # If no release date, we assume the person is still in custody.
        if not release_date:
            release_date = datetime.date.max

        if admission_date <= incident_date <= release_date \
                and incident.facility == potential_period.facility:
            return potential_period
    return None
