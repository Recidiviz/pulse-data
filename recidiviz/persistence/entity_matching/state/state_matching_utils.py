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
import logging
from typing import List, cast, Optional, Tuple, Union, Set, Type

from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import dao
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity.entity_utils import \
    EntityFieldType, is_placeholder, \
    get_set_entity_field_names
from recidiviz.common.common_utils import check_all_objs_have_type
from recidiviz.persistence.entity_matching.entity_matching_types import \
    EntityTree
from recidiviz.persistence.errors import EntityMatchingError


def is_match(ingested_entity: EntityTree,
             db_entity: EntityTree) -> bool:
    """Returns true if the provided |ingested_entity| matches the provided
    |db_entity|. Otherwise returns False.
    """
    return _is_match(ingested_entity=ingested_entity.entity,
                     db_entity=db_entity.entity)


def _is_match(*,
              ingested_entity: DatabaseEntity,
              db_entity: DatabaseEntity) -> bool:
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

    if not isinstance(ingested_entity, DatabaseEntity):
        raise EntityMatchingError(
            f"Unexpected type for ingested entity[{type(ingested_entity)}]",
            'unknown')
    if not isinstance(db_entity, DatabaseEntity):
        raise EntityMatchingError(
            f"Unexpected type for db entity[{type(db_entity)}]",
            'unknown')

    if isinstance(ingested_entity, schema.StatePerson):
        db_entity = cast(schema.StatePerson, db_entity)
        for ingested_external_id in ingested_entity.external_ids:
            for db_external_id in db_entity.external_ids:
                if _is_match(ingested_entity=ingested_external_id,
                             db_entity=db_external_id):
                    return True
        return False

    if isinstance(ingested_entity, schema.StatePersonExternalId):
        db_entity = cast(schema.StatePersonExternalId, db_entity)
        return ingested_entity.state_code == db_entity.state_code \
               and ingested_entity.external_id == db_entity.external_id \
               and ingested_entity.id_type == db_entity.id_type

    # As person has already been matched, assume that any of these 'person
    # attribute' entities are matches if their state_codes align.
    if isinstance(ingested_entity, schema.StatePersonAlias):
        db_entity = cast(schema.StatePersonAlias, db_entity)
        return ingested_entity.state_code == db_entity.state_code \
               and ingested_entity.full_name == db_entity.full_name
    if isinstance(ingested_entity, schema.StatePersonRace):
        db_entity = cast(schema.StatePersonRace, db_entity)
        return ingested_entity.state_code == db_entity.state_code \
               and ingested_entity.race == db_entity.race
    if isinstance(ingested_entity, schema.StatePersonEthnicity):
        db_entity = cast(schema.StatePersonEthnicity, db_entity)
        return ingested_entity.state_code == db_entity.state_code \
               and ingested_entity.ethnicity == db_entity.ethnicity

    # Placeholders entities are considered equal
    if ingested_entity.get_external_id() is None \
            and db_entity.get_external_id() is None:
        return is_placeholder(ingested_entity) and is_placeholder(db_entity)
    return ingested_entity.get_external_id() == db_entity.get_external_id()


# TODO(2037): Move the following into North Dakota specific file.
def base_entity_match(
        ingested_entity: EntityTree,
        db_entity: EntityTree) -> bool:
    """
    Matching logic for comparing entities that might not have external ids, by
    comparing all flat fields in the given entities. Should only be used for
    entities that we know might not have external_ids based on the ingested
    state data.
    """
    a = ingested_entity.entity
    b = db_entity.entity

    # Placeholders never match
    if is_placeholder(a) or is_placeholder(b):
        return False

    # Compare external ids if one is present
    if a.get_external_id() or b.get_external_id():
        return a.get_external_id() == b.get_external_id()

    # Compare all flat fields of the two entities
    all_set_flat_field_names = \
        get_set_entity_field_names(a, EntityFieldType.FLAT_FIELD) | \
        get_set_entity_field_names(b, EntityFieldType.FLAT_FIELD)
    for field_name in all_set_flat_field_names:
        # Skip primary key
        if field_name == a.get_class_id_name():
            continue
        if field_name.endswith('_id'):
            continue
        a_field = a.get_field(field_name)
        b_field = b.get_field(field_name)
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


def remove_child_from_entity(
        *,
        entity: DatabaseEntity,
        child_field_name: str,
        child_to_remove: DatabaseEntity):
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
        *,
        entity: DatabaseEntity,
        child_field_name: str,
        child_to_add: DatabaseEntity):
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
                f"Attempting to add child {child_to_add} to entity {entity}, "
                f"but {child_field_name} already had different value "
                f"{child_field}", entity.get_entity_name())
        child_field = child_to_add
        entity.set_field(child_field_name, child_field)


# TODO(2037): Move the following into North Dakota specific file.
def merge_incarceration_periods(ingested_persons: List[schema.StatePerson]):
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
        incomplete_incarceration_periods: List[schema.StateIncarcerationPeriod]
) -> List[schema.StateIncarcerationPeriod]:
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
    # ND-provided movement sequence number. We sort directly by that number.
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
        new: schema.StateIncarcerationPeriod,
        old: schema.StateIncarcerationPeriod
) -> schema.StateIncarcerationPeriod:
    """Merges two incarceration periods with information about
    admission and release into one period. Assumes the status of
    the release event is the most relevant, up-to-date status.

    Args:
        new: The out-of-session period (i.e. new to this ingest run).
        old: The in-session period (i.e. pulled out of the DB), if there is one.
    """

    # Complete match, perform normal merge.
    if new.external_id == old.external_id:
        _default_merge_flat_fields(new_entity=new, old_entity=old)
        return old

    # Determine updated external_id
    new_complete = is_incarceration_period_complete(new)
    old_complete = is_incarceration_period_complete(old)
    if new_complete != old_complete:
        updated_external_id = new.external_id \
            if new_complete else old.external_id
    else:
        admission_period, release_period = \
            (new, old) if new.admission_date else (old, new)
        updated_external_id = admission_period.external_id \
                      + _INCARCERATION_PERIOD_ID_DELIMITER \
                      + release_period.external_id

    # Keep the new status if the new period is a release period
    updated_status = new.status if new.release_date else old.status
    updated_status_raw_text = new.status_raw_text \
        if new.release_date else old.status_raw_text

    # Copy all fields from new onto old
    new_fields = \
        get_set_entity_field_names(new, EntityFieldType.FLAT_FIELD)
    for child_field_name in new_fields:
        old.set_field(child_field_name, new.get_field(child_field_name))

    # Always update the external id and status
    old.external_id = updated_external_id
    old.status = updated_status
    old.status_raw_text = updated_status_raw_text

    return old


def nd_is_incarceration_period_match(
        ingested_entity: Union[EntityTree, StateBase],
        db_entity: Union[EntityTree, StateBase]) -> bool:
    """
    Determines if the provided |ingested_entity| matches the |db_entity| based
    on ND specific StateIncarcerationPeriod matching.
    """
    if isinstance(ingested_entity, EntityTree):
        db_entity = cast(EntityTree, db_entity.entity)
        ingested_entity = ingested_entity.entity

    ingested_entity = cast(schema.StateIncarcerationPeriod, ingested_entity)
    db_entity = cast(schema.StateIncarcerationPeriod, db_entity)

    ingested_complete = is_incarceration_period_complete(ingested_entity)
    db_complete = is_incarceration_period_complete(db_entity)
    if not ingested_complete and not db_complete:
        return is_incomplete_incarceration_period_match(
            ingested_entity, db_entity)
    if ingested_complete and db_complete:
        return ingested_entity.external_id == db_entity.external_id

    # Only one of the two is complete
    complete, incomplete = (ingested_entity, db_entity) \
        if ingested_complete else (db_entity, ingested_entity)

    complete_external_ids = complete.external_id.split(
        _INCARCERATION_PERIOD_ID_DELIMITER)
    incomplete_external_id = incomplete.external_id

    if len(complete_external_ids) != 2:
        raise EntityMatchingError(
            f"Could not split external id of complete incarceration period "
            f"{complete.external_id} as expected", "state_incarceration_period")

    return incomplete_external_id in complete_external_ids


def is_incomplete_incarceration_period_match(
        ingested_entity: schema.StateIncarcerationPeriod,
        db_entity: schema.StateIncarcerationPeriod) -> bool:
    """Given two incomplete StateIncarcerationPeriods, determines if they
    should be considered the same StateIncarcerationPeriod.
    """
    # Cannot match with a placeholder StateIncarcerationPeriod
    if is_placeholder(ingested_entity) or is_placeholder(db_entity):
        return False

    ingested_seq_no = _get_sequence_no(ingested_entity)
    db_seq_no = _get_sequence_no(db_entity)

    # Only match incomplete periods if they are adjacent based on seq no.
    if abs(ingested_seq_no - db_seq_no) != 1:
        return False

    # Check that the first period is an admission and second a release
    if ingested_seq_no < db_seq_no:
        first, second = ingested_entity, db_entity
    else:
        first, second = db_entity, ingested_entity
    if not first.admission_date or not second.release_date:
        return False

    # Must have same facility
    if ingested_entity.facility != db_entity.facility:
        return False

    return True


def _get_sequence_no(period: schema.StateIncarcerationPeriod) -> int:
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


def is_incarceration_period_complete(
        period: schema.StateIncarcerationPeriod) -> bool:
    """Returns True if the period is considered complete (has both an admission
    and release date).
    """
    return all([period.admission_date, period.release_date])


def merge_flat_fields(new_entity: DatabaseEntity, old_entity: DatabaseEntity):
    """Merges appropriate non-relationship fields on the |new_entity| onto the
    |old_entity|. Returns the newly merged entity."""

    # Special merge logic if we are merging 2 different incarceration periods.
    if isinstance(new_entity, schema.StateIncarcerationPeriod):
        old_entity = cast(schema.StateIncarcerationPeriod, old_entity)
        return _merge_incomplete_periods(new_entity, old_entity)

    return _default_merge_flat_fields(
        new_entity=new_entity, old_entity=old_entity)


def _default_merge_flat_fields(
        *, new_entity: DatabaseEntity, old_entity: DatabaseEntity
) -> DatabaseEntity:
    """Merges all set non-relationship fields on the |new_entity| onto the
    |old_entity|. Returns the newly merged entity.
    """
    for child_field_name in get_set_entity_field_names(
            new_entity, EntityFieldType.FLAT_FIELD):
        # Do not overwrite with default status
        if child_field_name == 'status' and new_entity.has_default_status():
            continue

        old_entity.set_field(child_field_name,
                             new_entity.get_field(child_field_name))

    return old_entity


def admitted_for_revocation(ip: schema.StateIncarcerationPeriod) -> bool:
    """Determines if the provided |ip| began because of a revocation."""
    if not ip.admission_reason:
        return False
    revocation_types = [
        StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION.value,
        StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION.value]
    non_revocation_types = [
        StateIncarcerationPeriodAdmissionReason.ADMITTED_IN_ERROR.value,
        StateIncarcerationPeriodAdmissionReason.EXTERNAL_UNKNOWN.value,
        StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION.value,
        StateIncarcerationPeriodAdmissionReason.
        RETURN_FROM_ERRONEOUS_RELEASE.value,
        StateIncarcerationPeriodAdmissionReason.RETURN_FROM_ESCAPE.value,
        StateIncarcerationPeriodAdmissionReason.TRANSFER.value]
    if ip.admission_reason in revocation_types:
        return True
    if ip.admission_reason in non_revocation_types:
        return False
    raise EntityMatchingError(
        f"Unexpected StateIncarcerationPeriodAdmissionReason "
        f"{ip.admission_reason}.", ip.get_entity_name())


def revoked_to_prison(svr: schema.StateSupervisionViolationResponse) -> bool:
    """Determines if the provided |svr| resulted in a revocation."""
    if not svr.revocation_type:
        return False
    reincarceration_types = [
        StateSupervisionViolationResponseRevocationType.REINCARCERATION.value,
        StateSupervisionViolationResponseRevocationType.
        SHOCK_INCARCERATION.value,
        StateSupervisionViolationResponseRevocationType.
        TREATMENT_IN_PRISON.value]
    non_reincarceration_types = [
        StateSupervisionViolationResponseRevocationType.
        RETURN_TO_SUPERVISION.value]
    if svr.revocation_type in reincarceration_types:
        return True
    if svr.revocation_type in non_reincarceration_types:
        return False
    raise EntityMatchingError(
        f"Unexpected StateSupervisionViolationRevocationType "
        f"{svr.revocation_type}.", svr.get_entity_name())


def _get_closest_response(
        ip: schema.StateIncarcerationPeriod,
        sorted_responses: List[schema.StateSupervisionViolationResponse]
        ) -> Optional[schema.StateSupervisionViolationResponse]:
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


def associate_revocation_svrs_with_ips(
        merged_persons: List[schema.StatePerson]):
    """
    For each person in the provided |merged_persons|, attempts to associate
    StateSupervisionViolationResponses that result in revocation with their
    corresponding StateIncarcerationPeriod.
    """
    for person in merged_persons:
        svrs = _get_all_entities_of_cls(
            [person], schema.StateSupervisionViolationResponse)
        ips = _get_all_entities_of_cls(
            [person], schema.StateIncarcerationPeriod)

        revocation_svrs: List[schema.StateSupervisionViolationResponse] = []
        for svr in svrs:
            svr = cast(schema.StateSupervisionViolationResponse, svr)
            if revoked_to_prison(svr) and svr.response_date:
                revocation_svrs.append(svr)
        revocation_ips: List[schema.StateIncarcerationPeriod] = []
        for ip in ips:
            ip = cast(schema.StateIncarcerationPeriod, ip)
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


def move_incidents_onto_periods(merged_persons: List[schema.StatePerson]):
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


def _get_periods_in_sentence_group(
        sentence_group: schema.StateSentenceGroup
) -> Tuple[List[schema.StateIncarcerationPeriod],
           List[schema.StateIncarcerationPeriod]]:
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
        placeholder_periods: List[schema.StateIncarcerationPeriod],
        non_placeholder_periods: List[schema.StateIncarcerationPeriod]):
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
                incidents_to_remove.append((match, incident))

        # Remove incidents from placeholder parent after looping through all
        # incidents.
        for match_period, incident in incidents_to_remove:
            add_child_to_entity(
                entity=match_period,
                child_field_name='incarceration_incidents',
                child_to_add=incident)
            remove_child_from_entity(
                entity=placeholder_period,
                child_field_name='incarceration_incidents',
                child_to_remove=incident)


def _find_matching_period(
        incident: schema.StateIncarcerationIncident,
        potential_periods: List[schema.StateIncarcerationPeriod]) -> \
        Optional[schema.StateIncarcerationPeriod]:
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


def get_total_entities_of_cls(
        persons: List[schema.StatePerson], cls: Type) -> int:
    """Counts the total number of unique objects of type |cls| in the entity
    graphs passed in by |persons|.
    """
    check_all_objs_have_type(persons, schema.StatePerson)
    return len(_get_all_entities_of_cls(persons, cls))


def get_external_ids_of_cls(persons: List[schema.StatePerson],
                            cls: Type[DatabaseEntity]) -> Set[str]:
    """Returns the external ids of all entities of type |cls| found in the
    provided |persons| trees.
    """
    check_all_objs_have_type(persons, schema.StatePerson)

    ids: Set[str] = set()
    entities = _get_all_entities_of_cls(persons, cls)
    for entity in entities:
        external_ids = get_external_ids_from_entity(entity)
        if not external_ids:
            raise EntityMatchingError(
                f'Expected all external_ids to be present in cls '
                f'[{cls.__name__}]', cls.__name__)
        ids.update(external_ids)
    return ids


def get_external_ids_from_entity(entity: DatabaseEntity):
    external_ids = []
    if isinstance(entity, schema.StatePerson):
        for external_id in entity.external_ids:
            if external_id:
                external_ids.append(external_id.external_id)
    else:
        if entity.get_external_id():
            external_ids.append(entity.get_external_id())
    return external_ids


def _get_all_entities_of_cls(
        persons: List[schema.StatePerson],
        cls: Type[DatabaseEntity]):
    """Returns all entities found in the provided |persons| trees of type |cls|.
    """
    seen_entities: List[DatabaseEntity] = []
    for tree in get_all_entity_trees_of_cls(persons, cls):
        seen_entities.append(tree.entity)
    return seen_entities


def get_all_entity_trees_of_cls(
        persons: List[schema.StatePerson],
        cls: Type[DatabaseEntity]) -> List[EntityTree]:
    """
    Finds all unique entities of type |cls| in the provided |persons|, and
    returns their corresponding EntityTrees.
    """
    seen_ids: Set[int] = set()
    seen_trees: List[EntityTree] = []
    for person in persons:
        tree = EntityTree(entity=person, ancestor_chain=[])
        _get_all_entity_trees_of_cls_helper(tree, cls, seen_ids, seen_trees)
    return seen_trees


def _get_all_entity_trees_of_cls_helper(
        tree: EntityTree,
        cls: Type[DatabaseEntity],
        seen_ids: Set[int],
        seen_trees: List[EntityTree]):
    """
    Finds all objects in the provided |tree| graph which have the type |cls|.
    When an object of type |cls| is found, updates the provided |seen_ids| and
    |seen_trees| with the object's id and EntityTree respectively.
    """
    entity = tree.entity
    if isinstance(entity, cls) and id(entity) not in seen_ids:
        seen_ids.add(id(entity))
        seen_trees.append(tree)
        return
    for child_field_name in get_set_entity_field_names(
            entity, EntityFieldType.FORWARD_EDGE):
        child_trees = tree.generate_child_trees(
            entity.get_field_as_list(child_field_name))
        for child_tree in child_trees:
            _get_all_entity_trees_of_cls_helper(
                child_tree, cls, seen_ids, seen_trees)


def get_root_entity_cls(
        ingested_persons: List[schema.StatePerson]) -> Type[DatabaseEntity]:
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
    check_all_objs_have_type(ingested_persons, schema.StatePerson)

    root_cls = None
    if ingested_persons:
        root_cls = _get_root_entity_helper(ingested_persons[0])
    if root_cls is None:
        raise EntityMatchingError(
            "Could not find root class for ingested persons", 'state_person')
    return root_cls


def _get_root_entity_helper(
        entity: DatabaseEntity) -> Optional[Type[DatabaseEntity]]:
    if not is_placeholder(entity):
        return entity.__class__

    for field_name in get_set_entity_field_names(
            entity, EntityFieldType.FORWARD_EDGE):
        field = entity.get_field_as_list(field_name)[0]
        result = _get_root_entity_helper(field)
        if result is not None:
            return result
    return None


def _read_persons(
        session: Session,
        region: str,
        ingested_people: List[schema.StatePerson]
) -> List[schema.StatePerson]:
    """Looks up all people necessary for entity matching based on the provided
    |region| and |ingested_people|.
    """
    check_all_objs_have_type(ingested_people, schema.StatePerson)

    if region.upper() == 'US_ND':
        db_people = _nd_read_people(session, region, ingested_people)
    else:
        # TODO(1868): more specific query
        # Do not populate back edges before entity matching. All entities in the
        # state schema have edges both to their children and their parents. We
        # remove these for simplicity as entity matching does not depend on
        # these parent references (back edges). Back edges are regenerated
        # as a part of the conversion process from Entity -> Schema object.
        # If we did not remove these back edges, any time an entity relationship
        # changes, we would have to update edges both on the parent and child,
        # instead of just on the parent.
        db_people = dao.read_people(session)
    logging.info("Read [%d] people from DB in region [%s]",
                 len(db_people), region)
    return db_people


def _nd_read_people(
        session: Session,
        region: str,
        ingested_people: List[schema.StatePerson]
) -> List[schema.StatePerson]:
    """ND specific code that looks up all people necessary for entity matching
    based on the provided |region| and |ingested_people|.
    """
    root_entity_cls = get_root_entity_cls(ingested_people)
    if root_entity_cls not in (schema.StatePerson, schema.StateSentenceGroup):
        raise EntityMatchingError(
            f'For region [{region}] found unexpected root_entity_cls: '
            f'[{root_entity_cls.__name__}]', 'root_entity_cls')
    root_external_ids = get_external_ids_of_cls(
        ingested_people, root_entity_cls)
    logging.info("[Entity Matching] Reading [%s] external ids of class [%s]",
                 len(root_external_ids), root_entity_cls.__name__)
    return dao.read_people_by_cls_external_ids(
        session, region, root_entity_cls, root_external_ids)
