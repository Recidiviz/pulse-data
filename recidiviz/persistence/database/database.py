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
"""Contains logic for communicating with a SQL Database."""

from typing import List
from more_itertools import one

from sqlalchemy.orm import Session

import recidiviz.persistence.database.history.historical_snapshot_update as \
    update_snapshots
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence.database.schema.schema_person_type import \
    SchemaPersonType
from recidiviz.persistence.entity import entities
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.persistence.entity.base_entity import Entity

_DUMMY_BOOKING_ID = -1


def write_people(session: Session,
                 people: List[entities.EntityPersonType],
                 metadata: IngestMetadata,
                 orphaned_entities: List[Entity] = None):
    """
    Converts the given |people| into (SchemaPersonType) objects and persists
    their corresponding record trees. Returns the list of persisted
    (SchemaPersonType) objects.
    """
    if not orphaned_entities:
        orphaned_entities = []
    return _save_record_trees(
        session,
        converter.convert_entities_to_schema(people),
        converter.convert_entities_to_schema(orphaned_entities),
        metadata)


def write_person(session: Session,
                 person: entities.EntityPersonType,
                 metadata: IngestMetadata,
                 orphaned_entities: List[Entity] = None):
    """
    Converts the given |person| into a (SchemaPersonType) object and persists
    the record tree rooted at that |person|. Returns the persisted
    (SchemaPersonType) object.
    """
    if not orphaned_entities:
        orphaned_entities = []
    persisted_people = _save_record_trees(
        session,
        [converter.convert_entity_to_schema_object(person)],
        converter.convert_entities_to_schema(orphaned_entities),
        metadata)
    # persisted_people will only contain the single person passed in
    return one(persisted_people)


def _save_record_trees(session: Session,
                       root_people: List[SchemaPersonType],
                       orphaned_entities: List[DatabaseEntity],
                       metadata: IngestMetadata):
    """Persists all record trees rooted at |root_people|. Also performs any
    historical snapshot updates required for any entities in any of these
    record trees. Returns the list of persisted (SchemaPersonType) objects.
    """

    if all(isinstance(person, county_schema.Person) for person in root_people):
        _set_dummy_booking_ids(root_people)

    # Merge is recursive for all related entities, so this persists all master
    # entities in all record trees
    #
    # Merge and flush is required to ensure all master entities, including
    # newly created ones, have primary keys set before performing historical
    # snapshot operations
    root_people = [session.merge(root_person) for root_person in root_people]
    orphaned_entities = [session.merge(entity) for entity in orphaned_entities]
    session.flush()

    if all(isinstance(person, county_schema.Person) for person in root_people):
        _overwrite_dummy_booking_ids(root_people)

    update_snapshots.update_historical_snapshots(
        session, root_people, orphaned_entities, metadata)

    return root_people


def _set_dummy_booking_ids(root_people: List[county_schema.Person]) -> None:
    """Horrible hack to allow flushing new bookings. If the booking is new, it
    won't have a primary key until it is flushed. However, that flush will fail
    if the booking has child bonds or sentences, which require the booking_id
    column to be set. To get around this, temporarily set a dummy value on all
    bonds and sentences without booking IDs, to be overwritten after the flush
    ensures all bookings have IDs
    """
    for person in root_people:
        for booking in person.bookings:
            for charge in booking.charges:
                if charge.bond is not None and charge.bond.booking_id is None:
                    charge.bond.booking_id = _DUMMY_BOOKING_ID
                if charge.sentence is not None and \
                        charge.sentence.booking_id is None:
                    charge.sentence.booking_id = _DUMMY_BOOKING_ID


def _overwrite_dummy_booking_ids(
        root_people: List[county_schema.Person]
) -> None:
    """Overwrites the dummy booking ID for any bonds and sentences that have
    it set with the real ID of their parent booking
    """
    for person in root_people:
        for booking in person.bookings:
            for charge in booking.charges:
                if charge.bond is not None \
                        and charge.bond.booking_id == _DUMMY_BOOKING_ID:
                    charge.bond.booking_id = booking.booking_id
                if charge.sentence is not None \
                        and charge.sentence.booking_id == _DUMMY_BOOKING_ID:
                    charge.sentence.booking_id = booking.booking_id
