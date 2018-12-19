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

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.persistence.database import database
from recidiviz.persistence import entity_matching_utils as utils


class EntityMatchingError(Exception):
    """Raised when an error with entity matching is encountered."""
    pass


def match_entities(session, region, ingested_people):
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
    db_people = database.read_people_with_open_bookings(session, region)

    for db_person in db_people:
        ingested_person = _get_only_match(db_person, ingested_people,
                                          utils.is_person_match)
        if ingested_person:
            # If the match was previously matched to a different database
            # person, raise an error.
            if ingested_person.person_id:
                raise EntityMatchingError('matched ingested person to more '
                                          'than one database entity')
            ingested_person.person_id = db_person.person_id
            match_bookings(db_person=db_person,
                           ingested_person=ingested_person)


def match_bookings(db_person, ingested_person):
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
            # If the match was previously matched to a different database
            # booking, raise an error.
            if ingested_booking.booking_id:
                raise EntityMatchingError('matched ingested booking to more '
                                          'than one database entity')
            ingested_booking.booking_id = db_booking.booking_id
            # TODO(348): logic will need to change if inferring happens
            # before matching.
            if db_booking.admission_date_inferred:
                ingested_booking.admission_date = db_booking.admission_date
                ingested_booking.admission_date_inferred = True
            match_charges(db_booking=db_booking,
                          ingested_booking=ingested_booking)
        else:
            ingested_person.bookings.append(db_booking)


def match_charges(db_booking, ingested_booking):
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
        # TODO(349): if there is more than one match, _get_only_match will
        # raise an EntityMatchingError - but we need to handle identical
        # charges.
        ingested_charge = _get_only_match(db_charge, ingested_booking.charges,
                                          utils.is_charge_match)
        if ingested_charge:
            # If the match was previously matched to a different database
            # charge, raise an error.
            if ingested_charge.charge_id:
                #TODO(349): handle identical charges
                raise EntityMatchingError('matched ingested charge to more '
                                          'than one database entity')
            ingested_charge.charge_id = db_charge.charge_id
        else:
            db_charge.status = ChargeStatus.DROPPED
            # TODO: set boolean inferred_drop to true
            # TODO: what do we do with orphaned bonds?
            dropped_charges.append(db_charge)

    ingested_booking.charges.extend(dropped_charges)


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
        raise EntityMatchingError('matched database entity {} to more than '
                                  'one ingested entity'.format(db_entity))
    return matches[0] if matches else None


def _get_all_matches(db_entity, ingested_entities, matcher):
    return [ingested_entity for ingested_entity in ingested_entities
            if matcher(db_entity, ingested_entity)]
