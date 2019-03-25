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

from collections import defaultdict
import logging
from typing import Dict, List, Set
from more_itertools import one

import pandas as pd
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import Session

import recidiviz
import recidiviz.persistence.database.update_historical_snapshots as \
    update_snapshots
from recidiviz.common.constants.booking import CustodyStatus
from recidiviz.common.constants.entity_enum import EntityEnum
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence import entities
from recidiviz.persistence.database import database_utils
from recidiviz.persistence.database.schema import DatabaseEntity, Person, \
    Booking, Charge


_DUMMY_BOOKING_ID = -1


def read_people(session, full_name=None, birthdate=None):
    """
    Read all people matching the optional surname and birthdate. If neither
    the surname or birthdate are provided, then read all people.

    Args:
        full_name: The full name to match against
        birthdate: The birthdate to match against
        session: The transaction to read from
    Returns:
        List of people matching the surname and birthdate, if provided
    """
    query = session.query(Person)
    if full_name is not None:
        query = query.filter(Person.full_name == full_name)
    if birthdate is not None:
        query = query.filter(Person.birthdate == birthdate)
    return _convert_and_normalize_record_trees(query.all())


def read_bookings(session):
    """
    Reads all bookings in the db.

    Args:
        session: The transaction to read from
    Return:
        List of all bookings
    """
    return database_utils.convert_all(session.query(Booking).all())


def read_people_by_external_ids(
        session: Session, region: str,
        ingested_people: List[entities.Person]) -> List[entities.Person]:
    """
    Reads all people for the given |region| that have external_ids that match
    the external_ids from the |ingested_people|

    Args:
        session: The transaction to read from
        region: The region to match against
        ingested_people: The ingested people to match against
    Returns: List of people that match the provided |ingested_people|
    """
    external_ids = {p.external_id for p in ingested_people}
    query = session.query(Person) \
        .filter(Person.region == region) \
        .filter(Person.external_id.in_(external_ids))
    return _convert_and_normalize_record_trees(query.all())


def read_people_with_open_bookings(session, region, ingested_people):
    """
    Reads all people for a given |region| that have open bookings and can be
    matched with the provided |ingested_people|.

    Args:
        session: The transaction to read from
        region: The region to match against
        ingested_people: The ingested people to match against
    Returns:
        List of people with open bookings matching the provided args
    """
    query = _query_people_and_open_bookings(session, region)

    full_names = {p.full_name for p in ingested_people}
    query = query.filter(Person.full_name.in_(full_names))
    return _convert_and_normalize_record_trees(
        [person for person, _ in query.all()])


def read_people_with_open_bookings_scraped_before_time(session, region, time):
    """
    Reads all people with open bookings in the given region that have a
    last_scraped_time set to a time earlier than the provided datetime.

    Args:
        session: The transaction to read from
        region: The region to match against
        time: The datetime exclusive upper bound on last_scrape_time to match
            against
    Returns:
        List of people matching the provided args
    """
    query = _query_people_and_open_bookings(session, region) \
        .filter(Booking.last_seen_time < time)
    return _convert_and_normalize_record_trees(
        [person for person, _ in query.all()])


def _query_people_and_open_bookings(session, region):
    """
    Returns a list of tuples of (person, booking) for all open bookings.

    Args:
        session: Transaction to read from
        region: The region to match against.
    """
    return session.query(Person, Booking) \
        .filter(Person.person_id == Booking.person_id) \
        .filter(Person.region == region) \
        .filter(Booking.custody_status.notin_(
            CustodyStatus.get_raw_released_statuses()))


def _convert_and_normalize_record_trees(
        people: List[Person]) -> List[entities.Person]:
    """Converts schema record trees to persistence layer models and removes
    any duplicate people created by how SQLAlchemy handles joins
    """
    converted_people: List[entities.Person] = []
    count_by_id: Dict[int, int] = defaultdict(lambda: 0)
    for person in people:
        if count_by_id[person.person_id] == 0:
            converted_people.append(database_utils.convert(person))
        count_by_id[person.person_id] += 1

    duplicates = [(person_id, count) for person_id, count
                  in count_by_id.items() if count > 1]
    if duplicates:
        id_counts = '\n'.join(
            ['ID {} with count {}'.format(duplicate[0], duplicate[1])
             for duplicate in duplicates])
        logging.error(
            'Duplicate records returned for person IDs:\n%s', id_counts)

    return converted_people


def write_people(session: Session, people: List[entities.Person],
                 metadata: IngestMetadata,
                 orphaned_entities: List[entities.Entity] = None) -> \
                 List[Person]:
    """Converts the given |people| into schema.Person objects and persists their
    corresponding record trees. Returns the list of persisted (Person) objects
    """
    if not orphaned_entities:
        orphaned_entities = []
    return _save_record_trees(
        session,
        [database_utils.convert(person) for person in people],
        [database_utils.convert(entity) for entity in orphaned_entities],
        metadata)


def write_person(session: Session, person: entities.Person,
                 metadata: IngestMetadata,
                 orphaned_entities: List[entities.Entity] = None) -> Person:
    """Converts the given |person| into a schema.Person object and persists the
    record tree rooted at that |person|. Returns the persisted (Person)
    """
    if not orphaned_entities:
        orphaned_entities = []
    persisted_people = _save_record_trees(
        session,
        [database_utils.convert(person)],
        [database_utils.convert(entity) for entity in orphaned_entities],
        metadata)
    # persisted_people will only contain the single person passed in
    return one(persisted_people)


def _save_record_trees(session: Session,
                       root_people: List[Person],
                       orphaned_entities: List[DatabaseEntity],
                       metadata: IngestMetadata) -> List[Person]:
    """Persists all record trees rooted at |root_people|. Also performs any
    historical snapshot updates required for any entities in any of these
    record trees. Returns the list of persisted (Person) objects
    """

    _set_dummy_booking_ids(root_people)

    _log_untracked_entities(session, root_people, orphaned_entities)
    _log_charges_with_missing_booking_ids(
        session, root_people, orphaned_entities)

    # Merge is recursive for all related entities, so this persists all master
    # entities in all record trees
    #
    # Merge and flush is required to ensure all master entities, including
    # newly created ones, have primary keys set before performing historical
    # snapshot operations
    root_people = [session.merge(root_person) for root_person in root_people]
    orphaned_entities = [session.merge(entity) for entity in orphaned_entities]
    session.flush()

    _overwrite_dummy_booking_ids(root_people)

    update_snapshots.update_historical_snapshots(
        session, root_people, orphaned_entities, metadata.ingest_time)

    return root_people


# TODO(1464): remove after bug is found and fixed
def _log_charges_with_missing_booking_ids(
        session: Session,
        root_people: List[Person],
        orphaned_entities: List[DatabaseEntity]) -> None:
    """Checks charges with missing booking IDs and attempts to handle them
    according to the nature of the issue
    """

    for entity in orphaned_entities:
        if isinstance(entity, Charge):
            # Only bonds and sentences should ever be orphaned, so if this is
            # happening we have a bigger problem than expected.
            raise RuntimeError('[missing booking IDs] Orphaned charge with '
                               'ID {}'.format(entity.get_primary_key()))

    # pylint: disable=too-many-nested-blocks
    for person in root_people:
        for booking in person.bookings:
            for charge in booking.charges:
                if not charge.booking_id:
                    # Two possibilities, either the booking ID doesn't exist at
                    # all, or it exists but isn't set yet. Note that these are
                    # both valid states to be in, so it's unclear why this has
                    # been causing problems. So here we will both log the
                    # issue and also attempt a manual workaround.
                    if booking.booking_id:
                        logging.error('[missing booking IDs] Booking ID not '
                                      'set for charge with booking ID %s and '
                                      'charge ID %s, will set manually',
                                      str(booking.booking_id),
                                      str(charge.charge_id))
                        charge.booking_id = booking.booking_id
                    else:
                        # Attempt to add from the highest new parent
                        if not person.person_id:
                            logging.error('[missing booking IDs] Booking ID '
                                          'not set on charge for new record '
                                          'tree. Will attempt to fix by '
                                          'adding record tree')
                            session.add(person)
                        else:
                            logging.error('[missing booking IDs] Booking ID '
                                          'not set on charge for new booking '
                                          'on existing record tree with person '
                                          'ID %s. Will attempt to fix by '
                                          'adding booking',
                                          str(person.person_id))
                            session.add(booking)


# TODO(1464): remove after bug is found and fixed
#
# NOTE: this method expects the set of entities reachable from
# root_people/orphaned_entities to cover the complete session. Therefore this
# will give a false positive for multiple consecutive calls to write from
# within a single session. This should be okay to run temporarily, as we
# don't have that pattern anywhere in real code, just in tests.
def _log_untracked_entities(
        session: Session,
        root_people: List[Person],
        orphaned_entities: List[DatabaseEntity]) -> None:
    """Checks if there are any entities on the |session| not reachable from
    |root_people| or |orphaned_entities|
    """

    # For existing entities, track their IDs
    ids_by_type: Dict[str, Set[int]] = defaultdict(set)

    # For new entities, since they have no IDs yet, track by total count
    new_count_by_type: Dict[str, int] = defaultdict(lambda: 0)

    def track_entity(entity):
        if entity.get_primary_key():
            ids_by_type[type(entity).__name__].add(entity.get_primary_key())
        else:
            new_count_by_type[type(entity).__name__] += 1

    for person in root_people:
        track_entity(person)
        for booking in person.bookings:
            track_entity(booking)
            if booking.arrest:
                track_entity(booking.arrest)
            for hold in booking.holds:
                track_entity(hold)
            for charge in booking.charges:
                track_entity(charge)
                if charge.bond:
                    track_entity(charge.bond)
                if charge.sentence:
                    track_entity(charge.sentence)
    for entity in orphaned_entities:
        track_entity(entity)

    error_messages: List[str] = []

    session_new_count_by_type: Dict[str, int] = defaultdict(lambda: 0)
    for entity in session.new:
        session_new_count_by_type[type(entity).__name__] += 1
    for type_name, count in session_new_count_by_type.items():
        tracked_count = new_count_by_type[type_name]
        if tracked_count != count:
            error_messages.append(
                'Mismatch in number of new entities of type {}. {} entities '
                'present on session, but {} entities tracked in record trees '
                'or orphaned entities list'.format(
                    type_name, count, tracked_count))

    for entity in session.dirty:
        if entity.get_primary_key() not in ids_by_type[type(entity).__name__]:
            error_messages.append(
                'Entity of type {} with ID {} present on session but not '
                'reachable from record trees or orphaned entities list'.format(
                    type(entity).__name__, entity.get_primary_key()))

    if error_messages:
        logging.error('[missing booking IDs] Errors with untracked '
                      'entities:\n%s',
                      '\n'.join(error_messages))


def _set_dummy_booking_ids(root_people: List[Person]) -> None:
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


def _overwrite_dummy_booking_ids(root_people: List[Person]) -> None:
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


def write_df(table: DeclarativeMeta, df: pd.DataFrame) -> None:
    """
    Writes the |df| to the |table|.

    The column headers on |df| must match the column names in |table|. All rows
    in |df| will be appended to |table|. If a row in |df| already exists in
    |table|, then that row will be skipped.
    """
    try:
        df.to_sql(table.__tablename__, recidiviz.db_engine, if_exists='append',
                  index=False)
    except IntegrityError:
        _write_df_only_successful_rows(table, df)


def _write_df_only_successful_rows(
        table: DeclarativeMeta, df: pd.DataFrame) -> None:
    """If the dataframe can't be written all at once (eg. some rows already
    exist in the database) then we write only the rows that we can."""
    for i in range(len(df)):
        row = df.iloc[i:i + 1]
        try:
            row.to_sql(table.__tablename__, recidiviz.db_engine,
                       if_exists='append', index=False)
        except IntegrityError:
            # Skip rows that can't be written
            logging.info("Skipping write_df to %s table: %s.", table, row)


def _convert_enums_to_strings(dictionary):
    result = {}
    for k, v in dictionary.items():
        if issubclass(type(v), EntityEnum):
            result[k] = v.value
        else:
            result[k] = v

    return result
