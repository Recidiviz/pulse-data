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
from typing import List

from more_itertools import one
from sqlalchemy.orm import Session

import recidiviz.persistence.database.history.historical_snapshot_update as update_snapshots
from recidiviz.common.common_utils import check_all_objs_have_type
from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.persistence.database.schema.schema_person_type import SchemaPersonType
from recidiviz.persistence.errors import PersistenceError
from recidiviz.utils import trace

_DUMMY_BOOKING_ID = -1


@trace.span
def write_people(
    session: Session,
    people: List[SchemaPersonType],
    metadata: IngestMetadata,
    orphaned_entities: List[DatabaseEntity] = None,
) -> List[SchemaPersonType]:
    """
    Converts the given |people| into (SchemaPersonType) objects and persists
    their corresponding record trees. Returns the list of persisted
    (SchemaPersonType) objects.
    """
    if not orphaned_entities:
        orphaned_entities = []

    return _save_record_trees(session, people, orphaned_entities, metadata)


def write_person(
    session: Session,
    person: SchemaPersonType,
    metadata: IngestMetadata,
    orphaned_entities: List[DatabaseEntity] = None,
) -> SchemaPersonType:
    """
    Converts the given |person| into a (SchemaPersonType) object and persists
    the record tree rooted at that |person|. Returns the persisted
    (SchemaPersonType) object.
    """
    if not orphaned_entities:
        orphaned_entities = []
    persisted_people = _save_record_trees(
        session, [person], orphaned_entities, metadata
    )
    # persisted_people will only contain the single person passed in
    return one(persisted_people)


def _save_record_trees(
    session: Session,
    root_people: List[SchemaPersonType],
    orphaned_entities: List[DatabaseEntity],
    metadata: IngestMetadata,
) -> List[SchemaPersonType]:
    """Persists all record trees rooted at |root_people|. Also performs any
    historical snapshot updates required for any entities in any of these
    record trees. Returns the list of persisted (SchemaPersonType) objects.
    """

    # TODO(#2382): Once County entity matching is updated to use
    #  DatabaseEntity objects directly, we shouldn't need to add dummy ids / do
    #  a session merge/flush for the county code.
    if metadata.system_level == SystemLevel.COUNTY:
        check_all_objs_have_type(root_people, county_schema.Person)
        _set_dummy_booking_ids(root_people)

        # Merge is recursive for all related entities, so this persists all
        # primary entities in all record trees
        #
        # Merge and flush is required to ensure all primary entities, including
        # newly created ones, have primary keys set before performing historical
        # snapshot operations

        logging.info("Starting Session merge of [%s] persons.", str(len(root_people)))

        merged_root_people = []
        for root_person in root_people:
            merged_root_people.append(session.merge(root_person))
            if len(merged_root_people) % 200 == 0:
                logging.info(
                    "Merged [%s] of [%s] people.",
                    str(len(merged_root_people)),
                    str(len(root_people)),
                )

        logging.info(
            "Starting Session merge of [%s] orphaned entities.",
            str(len(orphaned_entities)),
        )
        merged_orphaned_entities = []
        for entity in orphaned_entities:
            merged_orphaned_entities.append(session.merge(entity))
            if len(merged_orphaned_entities) % 200 == 0:
                logging.info(
                    "Merged [%s] of [%s] entities.",
                    str(len(merged_orphaned_entities)),
                    str(len(orphaned_entities)),
                )

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
    else:
        raise PersistenceError(f"Unexpected system level [{metadata.system_level}]")

    update_snapshots.update_historical_snapshots(
        session, merged_root_people, merged_orphaned_entities, metadata
    )

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
                if charge.sentence is not None and charge.sentence.booking_id is None:
                    charge.sentence.booking_id = _DUMMY_BOOKING_ID


def _overwrite_dummy_booking_ids(root_people: List[county_schema.Person]) -> None:
    """Overwrites the dummy booking ID for any bonds and sentences that have
    it set with the real ID of their parent booking
    """
    for person in root_people:
        for booking in person.bookings:
            for charge in booking.charges:
                if (
                    charge.bond is not None
                    and charge.bond.booking_id == _DUMMY_BOOKING_ID
                ):
                    charge.bond.booking_id = booking.booking_id
                if (
                    charge.sentence is not None
                    and charge.sentence.booking_id == _DUMMY_BOOKING_ID
                ):
                    charge.sentence.booking_id = booking.booking_id
