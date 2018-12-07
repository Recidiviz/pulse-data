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

# pylint: disable=protected-access

"""Tools for migrating data after changing Datastore entities.

These are for migrating data for Datastore entities after backwards incompatible
changes have been made.
"""


import httplib
import logging

from flask import Blueprint, request
from google.appengine.ext import deferred, ndb
from google.appengine.ext.ndb import polymodel

from recidiviz.ingest.us_ny.us_ny_person import UsNyPerson
from recidiviz.ingest.us_ny.us_ny_record import UsNyRecord
from recidiviz.models.record import Offense, SentenceDuration
from recidiviz.models.snapshot import Snapshot
from recidiviz.utils.auth import authenticate_request
from recidiviz.utils.params import get_value

# We default the first pass batch size to 12 because the first pass creates two
# parallel sets of entity groups, one of old inmates and their records to delete
# and one of new people and their new rcords to create. There is a limit of 25
# entity groups per transaction in NDB so we do 12 people at a time.
FIRST_PASS_BATCH_SIZE = 12
SECOND_PASS_BATCH_SIZE = 25
REGION = "us_ny"

data_migration = Blueprint('data_migration', __name__)

@data_migration.route('/')
@authenticate_request
def migrate():
    """Request handler to kick off migration work

    Request handler for data migration tasks. Migrates last_release_date
    and last_release_type from UsNyRecord model to Record model, and old
    InmateFacilitySnapshot entities to new Snapshot entities.

    Example queries:

        # First pass, 15 inmates and their child records/snapshots only,
        # 10 people at a time, non-destructive
        http://localhost:8080/data_migration?test_only=true&migration_type=first_pass&batch_size=10

        # Second pass, 15 records and their child snapshots only,
        # 20 records at a time, non-destructive
        http://localhost:8080/data_migration?test_only=false&migration_type=second_pass&batch_size=20

        # To migrate all inmates or records, and delete migrated entities,
        # change test_only parameter to 'false'

    URL Params:
        migration_type: (string) 'Record' or 'Snapshot' migration
        test_only: (string) "true" or "false", whether to non-
            destructively migrate only 10 records

    Returns:
        N/A

    Response Codes:
        HTTP 200 if successful
        HTTP 400 if not
    """
    test_only = get_value('test_only', request.args, 'true') == "true"
    migration_type = get_value('migration_type', request.args)

    if migration_type == "first_pass":
        batch_size = get_value('batch_size', request.args,
                               FIRST_PASS_BATCH_SIZE)
        deferred.defer(migration_pass1,
                       batch_size=batch_size,
                       test_only=test_only)
    elif migration_type == "second_pass":
        batch_size = get_value('batch_size', request.args,
                               SECOND_PASS_BATCH_SIZE)
        deferred.defer(migration_pass2,
                       batch_size=batch_size,
                       test_only=test_only)
    else:
        logging.error("Migration type '%s' not recognized. Exiting." %
                      migration_type)
        return ("Invalid parameters, see logs.", httplib.INTERNAL_SERVER_ERROR)

    logging.info("Kicked off migration %s.", migration_type)
    return ("Kicked off migration %s." % migration_type, httplib.OK)


def migration_pass1(cursor=None,
                    people_updated=0,
                    batch_size=FIRST_PASS_BATCH_SIZE,
                    test_only=True):
    """Migrate a batch of UsNyInmates and their child records and snapshots.

    Migrates several Inmate entities and their child entities (Records and
    InmateFacilitySnapshots) into temp datastore entities that resemble the new
    Datastore schema, then deletes the old entities.

    Specific changes made during the transition to temp entities:
        - "Inmate" to "Person": Migrates all Inmate fields and us_ny_person_id
        to a new UsNyPerson entity.
        - Record migration, first pass: Migrates UsNyRecord and Record fields
        to new TempRecord63 entities as child entities of that Person object.
        - Snapshot migration, first pass: Creates child Snapshot objects under
        the new TempRecord entity, which includes info from both the old Record
        and old InmateFacilitySnapshot entities.
        - Persists all of these new entities, and deletes the source UsNyInmate,
        UsNyRecord, and InmateFacilitySnapshot entities.

    Args:
        cursor: (ndb cursor) Query cursor for where we are in the migration
        people_updated: (int) Current number of Persons updated
        batch_size: (int) Number of Inmates to handle during this run
        test_only: (bool) If true, performs non-destructive migration only
    """
    # Get (batch_size) UsNyInmate entities. If <(batch_size) returned, set
    # migration_complete to True.
    inmate_query = UsNyInmate.query()
    inmates, next_cursor, more_to_migrate = \
        inmate_query.fetch_page(batch_size, start_cursor=cursor)

    to_put = []
    to_delete = []

    # For each UsNyInmate entity, pull inmate info and construct new UsNyPerson
    for inmate in inmates:
        new_person = convert_inmate_to_person(inmate)
        to_put.append(new_person)

        # Get all UsNyRecord entities which are children of this, and migrate
        # to TempRecords under the new UsNyPerson
        old_records = UsNyRecord.query(ancestor=inmate.key).fetch()
        for old_record in old_records:
            temp_record = convert_record_to_temp(new_person, old_record)
            to_put.append(temp_record)

            # Get all inmate_facility_snapshot entities which are children of
            # this, and migrate to temp Snapshots under the TempRecord above
            inmate_facility_snapshots = InmateFacilitySnapshot.query(
                ancestor=old_record.key).fetch()
            for old_snapshot in inmate_facility_snapshots:
                temp_snapshot = convert_snapshot_to_temp(temp_record,
                                                         old_record,
                                                         old_snapshot)
                to_put.append(temp_snapshot)
                to_delete.append(old_snapshot)

            to_delete.append(old_record)

        to_delete.append(inmate)
        people_updated += 1

    transactional_batch_replace(to_put, to_delete, test_only)

    if more_to_migrate:
        deferred.defer(migration_pass1,
                       cursor=next_cursor,
                       people_updated=people_updated,
                       test_only=test_only)
    else:
        logging.info("""Migration pass #1 complete with %d Persons updated.
                        You must now take the following steps and re-deploy the
                        app prior to initiating pass 2:
                        - DELETE recidiviz/ingest/us_ny/us_ny_snapshot.py
                        - DELETE MARKED FIELDS IN
                          recidiviz/ingest/us_ny/us_ny_record.py
                        - UPDATE CURRENT INDICES FOR UsNyRecord IN index.yaml
                          TO Record, KEEPING 1x INDEX FOR UsNyRecord ON 
                          record_id
                        - UNCOMMENT NEW FIELDS IN 
                          recidiviz/models/record.py
                     """,
                     people_updated)


def convert_inmate_to_person(inmate):
    """Creates UsNyPerson entity from UsNyInmate entity

    Args:
        inmate: The UsNyInmate entity to copy data from

    Returns:
        The new UsNyPerson entity built from the old Inmate data.
    """
    entity_id = REGION + inmate.inmate_id
    new_person = UsNyPerson(
        id=entity_id,
        person_id=inmate.inmate_id,
        person_id_is_fuzzy=inmate.inmate_id_is_fuzzy,
        age=inmate.age,
        birthdate=inmate.birthday,
        given_names=inmate.given_names,
        surname=inmate.last_name,
        race=inmate.race,
        region=inmate.region,
        sex=inmate.sex,
        us_ny_person_id=inmate.inmate_id,
        created_on=inmate.created_on,
        updated_on=inmate.updated_on,
    )

    return new_person


def convert_record_to_temp(new_person, old_record):
    """Creates TempRecord63 entity from UsNyRecord entity

    Args:
        new_person: The new UsNyPerson entity to make a child for
        old_record: The old UsNyRecord entity to clone data from

    Returns:
        The new, temporary TempRecord63 entity.
    """

    new_temp_record = TempRecord63(
        parent=new_person.key,
        id=old_record.key.id(),
        record_id=old_record.record_id,
        record_id_is_fuzzy=False,
        us_ny_record_id=old_record.record_id,
        admission_type=old_record.admission_type,
        birthdate=old_record.birthday,
        cond_release_date=old_record.cond_release_date,
        county_of_commit=old_record.county_of_commit,
        custody_date=old_record.custody_date,
        custody_status=old_record.custody_status,
        earliest_release_date=old_record.earliest_release_date,
        earliest_release_type=old_record.earliest_release_type,
        given_names=old_record.given_names,
        is_released=old_record.is_released,
        last_custody_date=old_record.last_custody_date,
        latest_facility=old_record.latest_facility,
        latest_release_date=old_record.last_release_date,
        latest_release_type=old_record.last_release_type,
        max_expir_date=old_record.max_expir_date,
        max_expir_date_parole=old_record.max_expir_date_parole,
        max_expir_date_superv=old_record.max_expir_date_superv,
        max_sentence_length=old_record.max_sentence_length,
        min_sentence_length=old_record.min_sentence_length,
        offense=old_record.offense,
        offense_date=old_record.offense_date,
        parole_discharge_date=old_record.parole_discharge_date,
        parole_elig_date=old_record.parole_elig_date,
        parole_hearing_date=old_record.parole_hearing_date,
        parole_hearing_type=old_record.parole_hearing_type,
        race=old_record.race,
        region=REGION,
        sex=old_record.sex,
        surname=old_record.last_name,
        created_on=old_record.created_on,
        updated_on=old_record.updated_on
    )

    return new_temp_record


def convert_snapshot_to_temp(temp_record, old_record, old_snapshot):
    """Creates Snapshot entity from InmateFacilitySnapshot entity

    Args:
        temp_record: The TempRecord63 entity to create a child of
        old_record: The old UsNyRecord entity to copy data from
        old_snapshot: The old InmateFacilitySnapshot entity to copy data from

    Returns:
        The new Snapshot entity
    """
    new_snapshot = Snapshot(
        parent=temp_record.key,
        id=old_snapshot.key.id(),
        admission_type=old_record.admission_type,
        birthdate=old_record.birthday,
        cond_release_date=old_record.cond_release_date,
        county_of_commit=old_record.county_of_commit,
        custody_date=old_record.custody_date,
        custody_status=old_record.custody_status,
        earliest_release_date=old_record.earliest_release_date,
        earliest_release_type=old_record.earliest_release_type,
        given_names=old_record.given_names,
        surname=old_record.last_name,
        is_released=old_record.is_released,
        last_custody_date=old_record.last_custody_date,
        latest_facility=old_snapshot.facility,
        latest_release_date=old_record.last_release_date,
        latest_release_type=old_record.last_release_type,
        max_expir_date=old_record.max_expir_date,
        max_expir_date_parole=old_record.max_expir_date_parole,
        max_expir_date_superv=old_record.max_expir_date_superv,
        max_sentence_length=old_record.max_sentence_length,
        min_sentence_length=old_record.min_sentence_length,
        offense=old_record.offense,
        offense_date=old_record.offense_date,
        parole_discharge_date=old_record.parole_discharge_date,
        parole_elig_date=old_record.parole_elig_date,
        parole_hearing_date=old_record.parole_hearing_date,
        parole_hearing_type=old_record.parole_hearing_type,
        race=old_record.race,
        region=REGION,
        sex=old_record.sex,
        created_on=old_snapshot.snapshot_date
    )

    return new_snapshot


def migration_pass2(cursor=None,
                    num_updated=0,
                    batch_size=SECOND_PASS_BATCH_SIZE,
                    test_only=True):
    """ Migrate a batch of TempRecords back into new-schema Record entities

    Migrates several TempRecord entities and their child Snapshots into Record
    and child Snapshot entities that are now in the new Datastore schema, then
    deletes the old temporary entities.

    Specific changes made during the transition to tempo entities:
        - Migrates all TempRecord entities into new UsNyRecord entities which
            are children of the same Person as the TempRecord was
        - Creates new Snapshots for each Snapshot under the migrated
            TempRecord, which are children of the new UsNyRecord instead of the
            TempRecord.
        - Deletes the temporary Snapshots (children of TempRecords), TempRecords

    Args:
        cursor: (ndb cursor) Query cursor for where we are in the migration
        num_updated: (int) Current number of records updated
        batch_size: (int) Number of TempRecord entities to handle during run
        test_only: (bool) If true, performs non-destructive migration only

    Returns:
        N/A
    """
    # Get (batch_size) UsNyInmate entities. If <(batch_size) returned, set
    # migration_complete to True.
    temp_record_query = TempRecord63.query()
    temp_records, next_cursor, more_to_migrate = \
        temp_record_query.fetch_page(batch_size, start_cursor=cursor)

    to_put = []
    to_delete = []

    # For each TempRecord entity, pull record info and construct new UsNyRecord
    for temp_record in temp_records:
        new_record = convert_temp_to_record(temp_record)
        to_put.append(new_record)

        # For each Snapshot entity which is a child of this TempRecord, migrate
        # it to a new Snapshot with the new / real UsNyRecord as parent
        temp_snapshots = Snapshot.query(ancestor=temp_record.key).fetch()
        for temp_snapshot in temp_snapshots:
            new_snapshot = convert_temp_to_snapshot(new_record, temp_snapshot)
            to_put.append(new_snapshot)
            to_delete.append(temp_snapshot)

        to_delete.append(temp_record)

    num_updated += len(to_delete)

    transactional_batch_replace(to_put, to_delete, test_only)

    if more_to_migrate:
        deferred.defer(migration_pass2,
                       cursor=next_cursor,
                       num_updated=num_updated,
                       test_only=test_only)
    else:
        logging.info('Migration pass #2 complete with %d Records and Snapshots '
                     'updated. Migration complete!', num_updated)


def convert_temp_to_record(temp_record):
    """Creates Record entity from TempRecord63 entity

    Args:
        temp_record: The TempRecord63 entity currently holding data for this
            UsNyRecord

    Returns:
        The new Record entity
    """
    entity_id = REGION + temp_record.record_id
    new_record = UsNyRecord(
        id=entity_id,
        parent=temp_record.key.parent(),
        record_id=temp_record.record_id,
        record_id_is_fuzzy=temp_record.record_id_is_fuzzy,
        us_ny_record_id=temp_record.us_ny_record_id,
        admission_type=temp_record.admission_type,
        birthdate=temp_record.birthdate,
        cond_release_date=temp_record.cond_release_date,
        county_of_commit=temp_record.county_of_commit,
        custody_status=temp_record.custody_status,
        custody_date=temp_record.custody_date,
        earliest_release_date=temp_record.earliest_release_date,
        earliest_release_type=temp_record.earliest_release_type,
        given_names=temp_record.given_names,
        surname=temp_record.surname,
        is_released=temp_record.is_released,
        last_custody_date=temp_record.last_custody_date,
        latest_facility=temp_record.latest_facility,
        latest_release_date=temp_record.latest_release_date,
        latest_release_type=temp_record.latest_release_type,
        max_expir_date=temp_record.max_expir_date,
        max_expir_date_parole=temp_record.max_expir_date_parole,
        max_expir_date_superv=temp_record.max_expir_date_superv,
        max_sentence_length=temp_record.max_sentence_length,
        min_sentence_length=temp_record.min_sentence_length,
        offense=temp_record.offense,
        offense_date=temp_record.offense_date,
        parole_elig_date=temp_record.parole_elig_date,
        parole_discharge_date=temp_record.parole_discharge_date,
        parole_hearing_date=temp_record.parole_hearing_date,
        parole_hearing_type=temp_record.parole_hearing_type,
        race=temp_record.race,
        region=temp_record.region,
        sex=temp_record.sex,
        created_on=temp_record.created_on,
        updated_on=temp_record.updated_on
    )

    return new_record


def convert_temp_to_snapshot(new_record, temp_snapshot):
    """Creates Snapshot entity from InmateFacilitySnapshot entity

    Args:
        new_record: The new UsNyRecord entity this should be a child of
        temp_snapshot: The current Snapshot entity holding this Snapshot's data

    Returns:
        The new Snapshot entity
    """
    new_snapshot = Snapshot(
        id=temp_snapshot.key.id(),
        parent=new_record.key,
        admission_type=temp_snapshot.admission_type,
        birthdate=temp_snapshot.birthdate,
        cond_release_date=temp_snapshot.cond_release_date,
        county_of_commit=temp_snapshot.county_of_commit,
        custody_date=temp_snapshot.custody_date,
        custody_status=temp_snapshot.custody_status,
        earliest_release_date=temp_snapshot.earliest_release_date,
        earliest_release_type=temp_snapshot.earliest_release_type,
        given_names=temp_snapshot.given_names,
        surname=temp_snapshot.surname,
        is_released=temp_snapshot.is_released,
        latest_facility=temp_snapshot.latest_facility,
        latest_release_date=temp_snapshot.latest_release_date,
        latest_release_type=temp_snapshot.latest_release_type,
        last_custody_date=temp_snapshot.last_custody_date,
        max_expir_date=temp_snapshot.max_expir_date,
        max_expir_date_superv=temp_snapshot.max_expir_date_superv,
        max_expir_date_parole=temp_snapshot.max_expir_date_parole,
        max_sentence_length=temp_snapshot.max_sentence_length,
        min_sentence_length=temp_snapshot.min_sentence_length,
        offense=temp_snapshot.offense,
        offense_date=temp_snapshot.offense_date,
        parole_discharge_date=temp_snapshot.parole_discharge_date,
        parole_elig_date=temp_snapshot.parole_hearing_date,
        parole_hearing_date=temp_snapshot.parole_hearing_date,
        parole_hearing_type=temp_snapshot.parole_hearing_type,
        race=temp_snapshot.race,
        region=temp_snapshot.region,
        sex=temp_snapshot.sex,
        created_on=temp_snapshot.created_on
    )

    return new_snapshot


@ndb.transactional(xg=True)  # pylint: disable=no-value-for-parameter
def transactional_batch_replace(to_put, to_delete=None, test_only=True):
    """Transactionally persist updated entities and delete those they replace.

    Saves a set of entities, and deletes a set of entities, in the same
    transaction. Note that this can only be used for up to 25 entity groups
    at a time (GAE constraint).

    Args:
        to_put: List of entities to persist to Datastore
        to_delete: List of entities to delete from Datastore, if applicable
        test_only: (Boolean) True if we should not delete entities.
            False if we should.
        False otherwise.

    Returns:
        True if successful.
    """
    if to_put:
        ndb.put_multi(to_put)

    if to_delete and not test_only:
        ndb.delete_multi([entity.key for entity in to_delete])

    return True


class Inmate(polymodel.PolyModel):
    """The decommissioned, original form of our Inmate model.

    This has been replaced by Person and has been moved into this file for
    the sake of this data migration script.

    Attributes:
        inmate_id: (string) The identifier the state site uses for this person
        inmate_id_is_fuzzy: Whether we generated this ID/it's not consistent
            across multiple scrapes of the same inmate.
        given_names: (string) First and middle names (space separated),
            if available
        last_name: (string) Last name, if provided
        birthday: (date) Birth date, if available
        age: (int) Age, if birth date is not available.
        region: (string) The region code for the scraper that captured this
        sex: (string) Sex of inmate in listing, as provided by prison system
        race: (sring) Race of inmate in the listing, for now string provided
            by region
        created_on: (datetime) Python datetime object of first time we added
            this record
        updated_on: (date) Python datetime object of last time we updated
            this record
    """
    inmate_id = ndb.StringProperty()
    inmate_id_is_fuzzy = ndb.BooleanProperty()
    given_names = ndb.StringProperty()
    last_name = ndb.StringProperty()
    birthday = ndb.DateProperty()
    age = ndb.IntegerProperty()
    region = ndb.StringProperty()
    sex = ndb.StringProperty()
    race = ndb.StringProperty()
    created_on = ndb.DateTimeProperty(auto_now_add=True)
    updated_on = ndb.DateProperty(auto_now=True)


class UsNyInmate(Inmate):
    """The decommissioned form of our UsNyInmate model.

    This has been replaced by UsNyPerson and has been moved into this file for
    the sake of this data migration script.

    Attributes:
        us_ny_inmate_id: (string) Same as inmate_id, but used as key for this
            entity type to force uniqueness / prevent collisions within the
            us_ny records
        (see models.inmate for inherited attributes)
    """
    us_ny_inmate_id = ndb.StringProperty()


class InmateFacilitySnapshot(polymodel.PolyModel):
    """The decommissioned, original form of our Snapshot model.

    This has been replaced by Snapshot and has been moved into this class for
    the sake of this data migration script.

    Attributes:
        snapshot_date: (datetime) Timestamp for creation time of snapshot
        facility: (string) The name of the facility the inmate was held in
    """
    snapshot_date = ndb.DateTimeProperty(auto_now_add=True)
    facility = ndb.StringProperty()


class TempRecord63(polymodel.PolyModel):
    """The decommissioned, transient temp record required to implement our
    two-pass migration.

    PolyModel class to store Record fields during migration for issue 63.

    Attributes:
        created_on: (datetime) Creation date of this record. If data is
            migrated in the future, effort will be made to preserve this field
        updated_on: (date) Date of last change / update to this record
        offense: (record.Offense) State-provided strings describing the crimes
            of conviction and (if available) class of crimes.
        record_id: (string) The identifier the state site uses for this crime
        record_id_is_fuzzy: (bool) Whether the ID is generated by us (True) or
            provided by the corrections system (False)
        min_sentence: (record.SentenceDuration) Minimum sentence to be served
        max_sentence: (record.SentenceDuration) Maximum sentence to be served
        custody_date: (date) Date the person's sentence started
        offense_date: (date) Date the offense was committed
        latest_facility: (string) The name of the most recent facility the
            person has been held in
        latest_release_date: (date) Most recent date of release
        latest_release_type: (string) Reason given for most recent release
        is_released: (bool) Whether the person has been released from this
            sentence
        given_names: (string) Any given names provided by the source
        surname: (string) The person's surname, as provided by the source
        birthdate: (date) Date of birth for the person as provided by the source
        sex: (string) Sex of the prisoner as provided by the prison system
        race: (string) Race of the prisoner as provided by prison system
        last_custody_date: (date) Most recent date person returned for this
            sentence (may not be the initial custody date - e.g., if parole
            was violated, may be readmitted for remainder of prison term)
        admission_type: (string) 'New commitment' is beginning to serve a term,
            other reasons are usually after term has started (e.g. parole issue)
        county_of_commit: (string) County the person was convicted/committed in
        custody_status: (string) Scraped string on custody status (more granular
            than just 'released' / 'not-released')
        earliest_release_date: (date) Earliest date to be released based on
            min_sentence. In certain circumstances, may be released before this.
        earliest_release_type: (string) The reason for the earliest possible
            release date.
        parole_hearing_date: (date) Date of next hearing before Parole Board
        parole_hearing_type: (string) Type of hearing for next PB appearance.
        parole_elig_date: (date) Date person will be eligible for parole
        cond_release_date: (date) Release date based on prison discretion for
            'good time off' based on behavior. Releases prisoner on parole, but
            bypasses PB review.
        max_expir_date: (date) Date of release if no PB or conditional release,
            maximum obligation to the state.
        max_expir_date_parole: (date) Last possible date of ongoing parole
            supervision. Doesn't apply to all people.
        max_expir_date_superv: (date) Last possible date of post-release
            supervision. Doesn't apply to all people.
        parole_discharge_date: (date) Final date of parole supervision, based on
            the parole board's decision to end supervision before max
            expiration.
        region: (string) The Recidiviz region code that this Record belongs to
    """
    offense = ndb.StructuredProperty(Offense, repeated=True)
    record_id = ndb.StringProperty()
    record_id_is_fuzzy = ndb.BooleanProperty()
    min_sentence_length = ndb.StructuredProperty(SentenceDuration,
                                                 repeated=False)
    max_sentence_length = ndb.StructuredProperty(SentenceDuration,
                                                 repeated=False)
    custody_date = ndb.DateProperty()
    offense_date = ndb.DateProperty()
    latest_facility = ndb.StringProperty()
    latest_release_date = ndb.DateProperty()
    latest_release_type = ndb.StringProperty()
    is_released = ndb.BooleanProperty()
    given_names = ndb.StringProperty()
    surname = ndb.StringProperty()
    birthdate = ndb.DateProperty()
    sex = ndb.StringProperty()
    race = ndb.StringProperty()
    last_custody_date = ndb.DateProperty()
    admission_type = ndb.StringProperty()
    county_of_commit = ndb.StringProperty()
    custody_status = ndb.StringProperty()
    earliest_release_date = ndb.DateProperty()
    earliest_release_type = ndb.StringProperty()
    parole_hearing_date = ndb.DateProperty()
    parole_hearing_type = ndb.StringProperty()
    parole_elig_date = ndb.DateProperty()
    cond_release_date = ndb.DateProperty()
    max_expir_date = ndb.DateProperty()
    max_expir_date_parole = ndb.DateProperty()
    max_expir_date_superv = ndb.DateProperty()
    parole_discharge_date = ndb.DateProperty()
    region = ndb.StringProperty()
    created_on = ndb.DateTimeProperty(auto_now_add=True)
    updated_on = ndb.DateProperty(auto_now=True)
    # The field below is not being temp stored for Record, but for UsNyRecord
    us_ny_record_id = ndb.StringProperty()
