# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
"""Contains logic to match database entities with ingested entities."""

import logging
from collections import defaultdict
from typing import List, Dict, Tuple, Set

from recidiviz import Session
from recidiviz.common import common_utils
from recidiviz.common.buildable_attr import BuildableAttr
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.persistence.database import database
from recidiviz.persistence import entity_matching_utils as utils, entities


class EntityMatchingError(Exception):
    """Raised when an error with entity matching is encountered."""


def match_entities(
        session: Session, region: str, ingested_people: List[entities.Person]):
    """
    Finds all people in the given |region| and database |session| and attempts
    to match them to the |ingested_people|. For any ingested person, if a
    matching person exists in the database, the primary key is updated on the
    ingested person.
    TODO: document how matches are determined in docstring or README.
    Args:
        session: (Session)
        region: (str)
        ingested_people: List[entities.Person]
    """
    if _has_external_ids(ingested_people):
        db_people = database.read_people_by_external_ids(session, region,
                                                         ingested_people)
    else:
        db_people = database.read_people_with_open_bookings(session, region,
                                                            ingested_people)

    for db_person in db_people:
        ingested_person = _get_only_match(db_person, ingested_people,
                                          utils.is_person_match)
        if ingested_person:
            logging.info('Successfully matched person with ID %s',
                         db_person.person_id)
            # If the match was previously matched to a different database
            # person, raise an error.
            if ingested_person.person_id:
                raise EntityMatchingError('matched ingested person to more '
                                          'than one database entity')
            ingested_person.person_id = db_person.person_id
            match_bookings(db_person=db_person,
                           ingested_person=ingested_person)


def _has_external_ids(ingested_people: List[entities.Person]) -> bool:
    for ingested_person in ingested_people:
        if ingested_person.external_id:
            return True
    return False


def match_bookings(
        *, db_person: entities.Person, ingested_person: entities.Person):
    """
    Attempts to match all bookings on the |ingested_person| with bookings on
    the |db_person|. For any ingested booking, if a matching booking exists on
    |db_person|, the primary key is updated on the ingested booking.
    Args:
        ingested_person: (entities.Person)
        db_person: (entities.Person)
    """
    for db_booking in db_person.bookings:
        ingested_booking = _get_only_match(db_booking, ingested_person.bookings,
                                           utils.is_booking_match)
        if ingested_booking:
            logging.info('Successfully matched booking with ID %s',
                         db_booking.booking_id)
            # If the match was previously matched to a different database
            # booking, raise an error.
            if ingested_booking.booking_id:
                raise EntityMatchingError('matched ingested booking to more '
                                          'than one database entity')
            ingested_booking.booking_id = db_booking.booking_id

            if (db_booking.admission_date_inferred and
                    ingested_booking.admission_date_inferred):
                ingested_booking.admission_date = db_booking.admission_date
                ingested_booking.admission_date_inferred = True

            match_arrest(db_booking=db_booking,
                         ingested_booking=ingested_booking)
            match_holds(db_booking=db_booking,
                        ingested_booking=ingested_booking)
            match_charges(db_booking=db_booking,
                          ingested_booking=ingested_booking)
            match_bonds(db_booking=db_booking,
                        ingested_booking=ingested_booking)
            match_sentences(db_booking=db_booking,
                            ingested_booking=ingested_booking)

        else:
            ingested_person.bookings.append(db_booking)


def match_bonds(*, db_booking: entities.Booking,
                ingested_booking: entities.Booking):
    _match_from_charges(db_booking=db_booking,
                        ingested_booking=ingested_booking, name='bond')


def match_sentences(*, db_booking: entities.Booking,
                    ingested_booking: entities.Booking):
    _match_from_charges(db_booking=db_booking,
                        ingested_booking=ingested_booking, name='sentence')


def _build_maps_from_charges(charges: List[entities.Charge], obj_name: str) -> \
        Tuple[Dict[str, BuildableAttr], Dict[str, Set[BuildableAttr]]]:
    """Helper function that returns a pair of maps describing the relationships
    between charges and their children. The |object_map| maps ids, which may be
    temporary generated ids, to the objects they refer to. The
    |object_relationships| map maps those ids to the set of charges that the
    object is a child of. This is part of determining entity equality,
    e.g. two bonds are equal only if the same set of charges has the bond as its
    child."""
    object_map = {}
    object_relationships: Dict[str, set] = defaultdict(set)

    for charge in charges:
        obj = getattr(charge, obj_name)
        if obj:
            obj_id = getattr(obj, obj_name + '_id')
            if not obj_id:
                obj_id = common_utils.create_generated_id(obj)
            object_map[obj_id] = obj
            object_relationships[obj_id].add(charge.charge_id)
    return object_map, object_relationships


def _match_from_charges(*, db_booking: entities.Booking,
                        ingested_booking: entities.Booking, name: str):
    """Helper function that, within a booking, matches objects that are children
    of the booking's charges. |name| should be 'bond' or 'sentence'.
    """
    id_name = name + '_id'
    db_obj_map, db_relationship_map = _build_maps_from_charges(
        db_booking.charges, name)
    ing_obj_map, ing_relationship_map = _build_maps_from_charges(
        ingested_booking.charges, name)

    def _is_match_with_relationships(*, db_entity, ingested_entity):
        ing_entity_id = common_utils.create_generated_id(ingested_entity)
        db_entity_id = getattr(db_entity, id_name)
        matcher = getattr(utils, 'is_{}_match'.format(name))
        obj_match = matcher(db_entity=db_entity,
                            ingested_entity=ingested_entity)
        relationship_match = ing_relationship_map[ing_entity_id] == \
                             db_relationship_map[db_entity_id]
        return obj_match and relationship_match

    dropped_objs = []
    for db_obj in db_obj_map.values():
        ingested_obj = _get_next_available_match(
            db_obj, ing_obj_map.values(),
            _is_match_with_relationships)
        if ingested_obj:
            db_id = getattr(db_obj, name + '_id')
            logging.info('successfully matched %s with id %s',
                         name, db_id)
            setattr(ingested_obj, name + '_id', db_id)
        else:
            logging.info('Did not match %s to any ingested %s, dropping',
                         db_obj, name)
            # TODO: figure out how to drop sentences/bonds
            # _drop_bond(db_bond) / _drop_sentence(db_sentence)
            dropped_objs.append(db_obj)

    # TODO: keep bonds/sentences around on booking after being dropped
    # ingested_booking.bonds.extend(ingested_bonds)


def match_arrest(
        *, db_booking: entities.Booking, ingested_booking: entities.Booking):
    if db_booking.arrest and ingested_booking.arrest:
        ingested_booking.arrest.arrest_id = db_booking.arrest.arrest_id


def match_holds(
        *, db_booking: entities.Booking, ingested_booking: entities.Booking):
    dropped_holds = []

    for db_hold in db_booking.holds:
        ingested_hold = _get_only_match(
            db_hold, ingested_booking.holds, utils.is_hold_match)

        if ingested_hold:
            logging.info(
                'Successfully matched hold with ID %s', db_hold.hold_id)
            # If the match was previously matched to a different database
            # charge, raise an error.
            if ingested_hold.hold_id:
                raise EntityMatchingError('matched ingested hold to more '
                                          'than one database entity')
            ingested_hold.hold_id = db_hold.hold_id
        else:
            _drop_hold(db_hold)
            dropped_holds.append(db_hold)

    ingested_booking.holds.extend(dropped_holds)


def _drop_hold(_):
    # TODO(585): Drop necessary fields for hold
    pass


# TODO(573): what do we do with orphaned bonds/sentences?
def match_charges(
        *, db_booking: entities.Booking, ingested_booking: entities.Booking):
    """
    Attempts to match all charges on the |ingested_booking| with charges on
    the |db_booking|. For any ingested charge, if a matching charge exists on
    |db_booking|, the primary key is updated on the ingested charge. All
    db charges that are not matched to an ingested charge are marked dropped and
    added to the |ingested_booking|.
    Args:
        ingested_booking: (entities.Booking)
        db_booking: (entities.Booking)
    """
    dropped_charges = []
    for db_charge in db_booking.charges:
        ingested_charge = _get_next_available_match(db_charge,
                                                    ingested_booking.charges,
                                                    utils.is_charge_match)
        if ingested_charge:
            logging.info('Successfully matched charge with ID %s',
                         db_charge.charge_id)
            ingested_charge.charge_id = db_charge.charge_id
        else:
            _drop_charge(db_charge)
            dropped_charges.append(db_charge)

    ingested_booking.charges.extend(dropped_charges)


# TODO(585): set boolean inferred_drop to true
def _drop_charge(charge: entities.Charge):
    if charge.status != ChargeStatus.DROPPED:
        logging.info('Dropping charge with id %s', charge.charge_id)
        charge.status = ChargeStatus.DROPPED


def _get_next_available_match(db_entity, ingested_entities, matcher):
    id_name = db_entity.__class__.__name__.lower() + '_id'

    for ingested_entity in ingested_entities:
        if not getattr(ingested_entity, id_name) and matcher(
                db_entity=db_entity, ingested_entity=ingested_entity):
            return ingested_entity
    return None


def _get_only_match(db_entity, ingested_entities, matcher):
    """
    Finds the entity in |ingested_entites| that matches the |db_entity|.
    Args:
        db_entity: an entity
        ingested_entities: List of entities. Entities in the list should be the
            same type as |db_entity|.
        matcher:
            (db_entity, ingested_entity) -> (bool)
    Returns:
        The entity from |ingested_entities| that matches the |db_entity|, or
        None if no match is found.
    Raises:
        EntityMatchingError: if more than one match is found.
    """
    matches = _get_all_matches(db_entity, ingested_entities, matcher)
    if len(matches) > 1:
        raise EntityMatchingError(
            'matched database entity {} to more than one ingested '
            'entity'.format(db_entity))
    return matches[0] if matches else None


def _get_all_matches(db_entity, ingested_entities, matcher):
    return [ingested_entity for ingested_entity in ingested_entities
            if matcher(db_entity=db_entity, ingested_entity=ingested_entity)]
