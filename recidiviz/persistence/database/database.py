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
import logging
from collections import defaultdict
from typing import List, Dict

import sqlalchemy
from more_itertools import one
from sqlalchemy import select

from sqlalchemy.orm import Session

import recidiviz.persistence.database.history.historical_snapshot_update as \
    update_snapshots
from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.schema_person_type import \
    SchemaPersonType
from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.common.common_utils import check_all_objs_have_type
from recidiviz.persistence.errors import PersistenceError

_DUMMY_BOOKING_ID = -1
ASSOCIATION_TABLE_NAME_SUFFIX = '_association'


def write_people(session: Session,
                 people: List[SchemaPersonType],
                 metadata: IngestMetadata,
                 orphaned_entities: List[DatabaseEntity] = None):
    """
    Converts the given |people| into (SchemaPersonType) objects and persists
    their corresponding record trees. Returns the list of persisted
    (SchemaPersonType) objects.
    """
    if not orphaned_entities:
        orphaned_entities = []

    return _save_record_trees(session, people, orphaned_entities, metadata)


def write_person(session: Session,
                 person: SchemaPersonType,
                 metadata: IngestMetadata,
                 orphaned_entities: List[DatabaseEntity] = None):
    """
    Converts the given |person| into a (SchemaPersonType) object and persists
    the record tree rooted at that |person|. Returns the persisted
    (SchemaPersonType) object.
    """
    if not orphaned_entities:
        orphaned_entities = []
    persisted_people = _save_record_trees(session,
                                          [person],
                                          orphaned_entities,
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

    # TODO(2382): Once County entity matching is updated to use
    #  DatabaseEntity objects directly, we shouldn't need to add dummy ids / do
    #  a session merge/flush for the county code.
    if metadata.system_level == SystemLevel.COUNTY:
        check_all_objs_have_type(root_people, county_schema.Person)
        _set_dummy_booking_ids(root_people)

        # Merge is recursive for all related entities, so this persists all
        # master entities in all record trees
        #
        # Merge and flush is required to ensure all master entities, including
        # newly created ones, have primary keys set before performing historical
        # snapshot operations

        logging.info("Starting Session merge of [%s] persons.",
                     str(len(root_people)))

        merged_root_people = []
        for root_person in root_people:
            merged_root_people.append(session.merge(root_person))
            if len(merged_root_people) % 200 == 0:
                logging.info("Merged [%s] of [%s] people.",
                             str(len(merged_root_people)),
                             str(len(root_people)))

        logging.info("Starting Session merge of [%s] orphaned entities.",
                     str(len(orphaned_entities)))
        merged_orphaned_entities = []
        for entity in orphaned_entities:
            merged_orphaned_entities.append(session.merge(entity))
            if len(merged_orphaned_entities) % 200 == 0:
                logging.info("Merged [%s] of [%s] entities.",
                             str(len(merged_orphaned_entities)),
                             str(len(orphaned_entities)))

        logging.info("Session flush start.")
        session.flush()
        logging.info("Session flush complete.")

        check_all_objs_have_type(merged_root_people, county_schema.Person)
        _overwrite_dummy_booking_ids(merged_root_people)

    elif metadata.system_level == SystemLevel.STATE:
        merged_root_people = root_people
        if orphaned_entities:
            raise PersistenceError("State doesn't use orphaned entities")
        merged_orphaned_entities = []

        _hydrate_state_codes_in_association_tables(session)

    else:
        raise PersistenceError(
            f"Unexpected system level [{metadata.system_level}]")

    update_snapshots.update_historical_snapshots(
        session, merged_root_people, merged_orphaned_entities, metadata)

    return merged_root_people


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


def _hydrate_state_codes_in_association_tables(session: Session):
    """
    Hydrates state codes in association tables because there is no way to manually update extra (non-relationship)
    columns in association tables through the SQLAlchemy ORM.
    """

    logging.info("Starting association table state_code hydration...")
    association_tables = [table for table in reversed(StateBase.metadata.sorted_tables) if
                          table.name.endswith(ASSOCIATION_TABLE_NAME_SUFFIX)]
    for association_table in association_tables:
        foreign_key_constraints = [
            constraint for constraint in association_table.constraints
            if isinstance(constraint, sqlalchemy.ForeignKeyConstraint)]

        constraint: sqlalchemy.ForeignKeyConstraint = foreign_key_constraints[0]

        foreign_key_table = constraint.referred_table
        foreign_key_column = constraint.column_keys[0]

        results = _get_state_codes_table_for_rows_with_no_state_code(foreign_key_table,
                                                                     foreign_key_column,
                                                                     association_table,
                                                                     session)
        foreign_key_values_by_state_code: Dict[str, List[int]] = defaultdict(list)
        for row in results:
            foreign_key_values_by_state_code[row.state_code].append(row[foreign_key_column])
        for state_code, foreign_keys in foreign_key_values_by_state_code.items():
            session.execute(association_table.update().values(state_code=state_code).
                            where(association_table.c[foreign_key_column].in_(foreign_keys)))
            session.commit()

    logging.info("Finished association table state_code hydration")


def _get_state_codes_table_for_rows_with_no_state_code(
        foreign_key_table: sqlalchemy.ForeignKeyConstraint.referred_table,
        foreign_key_column: str,
        association_table: sqlalchemy.table, session: Session):
    """Returns state codes table for rows without state code"""

    stmt = select([foreign_key_table.c.state_code, foreign_key_table.c[foreign_key_column]]). \
        where(association_table.c[foreign_key_column] == foreign_key_table.c[foreign_key_column]). \
        where(association_table.c.state_code.is_(None))

    results = session.execute(stmt)

    return results
