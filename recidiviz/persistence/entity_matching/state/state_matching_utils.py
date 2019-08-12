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
from typing import List, cast, Optional, Tuple, Union, Set, Type

import attr

from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType
from recidiviz.persistence.entity.base_entity import Entity, ExternalIdEntity
from recidiviz.persistence.entity.entity_utils import \
    EntityFieldType, get_set_entity_field_names, get_field, set_field, \
    get_field_as_list, is_placeholder, has_default_status
from recidiviz.persistence.entity.state.entities import StatePerson, \
    StatePersonExternalId, StatePersonAlias, StatePersonRace, \
    StatePersonEthnicity, StateIncarcerationPeriod, \
    StateIncarcerationIncident, StateSentenceGroup, \
    StateSupervisionViolationResponse
from recidiviz.persistence.entity_matching import entity_matching_utils
from recidiviz.persistence.entity_matching.entity_matching_types import \
    EntityTree
from recidiviz.persistence.errors import EntityMatchingError


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

    # Placeholders entities are considered equal
    if ingested_entity.external_id is None and db_entity.external_id is None:
        return is_placeholder(ingested_entity) and is_placeholder(db_entity)
    return ingested_entity.external_id == db_entity.external_id


# TODO(2037): Move the following into North Dakota specific file.
def base_entity_match(
        ingested_entity: EntityTree, db_entity: EntityTree) -> bool:
    """
    Matching logic for comparing entities that might not have external ids, by
    comparing all flat fields in the given entities. Should only be used for
    entities that we know might not have external_ids based on the ingested
    state data.
    """
    a = cast(ExternalIdEntity, ingested_entity.entity)
    b = cast(ExternalIdEntity, db_entity.entity)

    # Placeholders never match
    if is_placeholder(a) or is_placeholder(b):
        return False

    # Compare external ids if one is present
    if a.external_id or b.external_id:
        return a.external_id == b.external_id

    # Compare all flat fields of the two entities
    all_set_flat_field_names = \
        get_set_entity_field_names(a, EntityFieldType.FLAT_FIELD) | \
        get_set_entity_field_names(b, EntityFieldType.FLAT_FIELD)
    for field_name in all_set_flat_field_names:
        # Skip primary key
        if field_name == a.get_class_id_name():
            continue
        a_field = get_field(a, field_name)
        b_field = get_field(b, field_name)
        if a_field != b_field:
            return False

    return True


def nd_get_incomplete_incarceration_period_match(
        ingested_entity_tree: EntityTree, db_entity_trees: List[EntityTree]) \
        -> Optional[EntityTree]:
    """For the ingested StateIncarcerationPeriod in the provided
    |ingested_entity_tree|, attempts to find a matching incomplete
    StateIncarcerationPeriod in the provided |db_entity_trees|.

    Returns the match if one is found, otherwise returns None.
    """

    # If the period is complete, it cannot match to an incomplete period.
    ingested_period = cast(
        StateIncarcerationPeriod, ingested_entity_tree.entity)
    if is_incarceration_period_complete(ingested_period):
        return None

    incomplete_db_trees = []
    for db_tree in db_entity_trees:
        db_period = cast(StateIncarcerationPeriod, db_tree.entity)
        if not is_incarceration_period_complete(db_period):
            incomplete_db_trees.append(db_tree)

    return entity_matching_utils.get_only_match(
        ingested_entity_tree, incomplete_db_trees,
        is_incomplete_incarceration_period_match)


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

    # Cannot match with a placeholder StateIncarcerationPeriod
    if is_placeholder(a) or is_placeholder(b):
        return False

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


def _get_all_entities_of_type(root: Entity, cls: Type):
    """Given a |root| entity, returns a list of all unique entities of type
    |cls| that exist in the |root| graph.
    """
    seen: Set[int] = set()
    entities_of_type: List[Entity] = []
    _get_all_entities_of_type_helper(root, cls, seen, entities_of_type)
    return entities_of_type


def _get_all_entities_of_type_helper(
        root: Entity, cls: Type, seen: Set[int],
        entities_of_type: List[Entity]):
    if isinstance(root, cls):
        if id(root) not in seen:
            root = cast(Entity, root)
            entities_of_type.append(root)
        return

    for field_name in get_set_entity_field_names(
            root, EntityFieldType.FORWARD_EDGE):
        for field in get_field_as_list(root, field_name):
            _get_all_entities_of_type_helper(field, cls, seen, entities_of_type)


def admitted_for_revocation(ip: StateIncarcerationPeriod) -> bool:
    """Determines if the provided |ip| began because of a revocation."""
    if not ip.admission_reason:
        return False
    revocation_types = [
        StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
        StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION]
    non_revocation_types = [
        StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR,
        StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN,
        StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ERRONEOUS_RELEASE,
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE,
        StateIncarcerationPeriodAdmissionReason.TRANSFER]
    if ip.admission_reason in revocation_types:
        return True
    if ip.admission_reason in non_revocation_types:
        return False
    raise EntityMatchingError(
        f"Unexpected StateIncarcerationPeriodAdmissionReason "
        f"{ip.admission_reason}.", ip.get_entity_name())


def revoked_to_prison(svr: StateSupervisionViolationResponse) -> bool:
    """Determines if the provided |svr| resulted in a revocation."""
    if not svr.revocation_type:
        return False
    reincarceration_types = [
        StateSupervisionViolationResponseRevocationType.REINCARCERATION,
        StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
        StateSupervisionViolationResponseRevocationType.TREATMENT_IN_PRISON]
    non_reincarceration_types = [
        StateSupervisionViolationResponseRevocationType.RETURN_TO_SUPERVISION]
    if svr.revocation_type in reincarceration_types:
        return True
    if svr.revocation_type in non_reincarceration_types:
        return False
    raise EntityMatchingError(
        f"Unexpected StateSupervisionViolationRevocationType "
        f"{svr.revocation_type}.", svr.get_entity_name())


def _get_closest_response(
        ip: StateIncarcerationPeriod,
        sorted_responses: List[StateSupervisionViolationResponse]
        ) -> Optional[StateSupervisionViolationResponse]:
    """Returns the most recent StateSupervisionViolationResponse when compared
    to the beginning of the provided |incarceration_period|.

    Assumes the provided |sorted_responses| are sorted chronologically by
    response_date.
    """
    closest_response = None
    admission_date = cast(datetime.date, ip.admission_date)
    for response in sorted_responses:
        response_date = cast(datetime.date, response.response_date)
        if response_date > admission_date:
            break
        closest_response = response

    return closest_response


def associate_revocation_svrs_with_ips(merged_persons: List[StatePerson]):
    """
    For each person in the provided |merged_persons|, attempts to associate
    StateSupervisionViolationResponses that result in revocation with their
    corresponding StateIncarcerationPeriod.
    """
    for person in merged_persons:
        svrs = _get_all_entities_of_type(
            person, StateSupervisionViolationResponse)
        ips = _get_all_entities_of_type(person, StateIncarcerationPeriod)

        revocation_svrs = []
        for svr in svrs:
            svr = cast(StateSupervisionViolationResponse, svr)
            if revoked_to_prison(svr) and svr.response_date:
                revocation_svrs.append(svr)
        revocation_ips = []
        for ip in ips:
            ip = cast(StateIncarcerationPeriod, ip)
            if admitted_for_revocation(ip) and ip.admission_date:
                revocation_ips.append(ip)

        if not revocation_svrs or not revocation_ips:
            continue

        sorted_revocation_svrs = sorted(
            revocation_svrs, key=lambda x: x.response_date)
        sorted_revocation_ips = sorted(
            revocation_ips, key=lambda x: x.admission_date)

        seen: Set[int] = set()
        for ip in sorted_revocation_ips:
            closest_svr = _get_closest_response(ip, sorted_revocation_svrs)
            svr_id = id(closest_svr)

            if closest_svr and svr_id not in seen:
                seen.add(svr_id)
                ip.source_supervision_violation_response = closest_svr


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


def get_total_entities_of_cls(persons: List[StatePerson], cls: Type) -> int:
    """Counts the total number of unique objects of type |cls| in the entity
    graphs passed in by |persons|.
    """
    seen_roots: Set[int] = set()
    for person in persons:
        _get_total_entities_of_cls(person, cls, seen_roots)
    return len(seen_roots)


def _get_total_entities_of_cls(
        entity: Entity, cls: Type, seen: Set[int]):
    if isinstance(entity, cls) and id(entity) not in seen:
        seen.add(id(entity))
        return
    for child_field_name in get_set_entity_field_names(
            entity, EntityFieldType.FORWARD_EDGE):
        for child_field in get_field_as_list(entity, child_field_name):
            _get_total_entities_of_cls(child_field, cls, seen)


def get_root_entity_cls(ingested_persons: List[StatePerson]) -> Type:
    """
    Attempts to find the highest entity class within the |ingested_persons| for
    which objects are not placeholders. Returns the class if found, otherwise
    raises.

    Note: This should only be used with persons ingested from a region directly
    (and not with persons post entity matching), as this function uses DFS to
    find the root entity cls. This therefore assumes that a) the passed in
    StatePersons are trees and not DAGs (one parent per entity) and b) that the
    structure of the passed in graph is symmetrical.
    """
    root_cls = None
    if ingested_persons:
        root_cls = _get_root_entity_helper(ingested_persons[0])
    if root_cls is None:
        raise EntityMatchingError(
            "Could not find root class for ingested persons", 'state_person')
    return root_cls


def _get_root_entity_helper(entity: Entity) -> Optional[Type]:
    if not is_placeholder(entity):
        return entity.__class__

    for field_name in get_set_entity_field_names(
            entity, EntityFieldType.FORWARD_EDGE):
        field = get_field_as_list(entity, field_name)[0]
        result = _get_root_entity_helper(field)
        if result is not None:
            return result
    return None
