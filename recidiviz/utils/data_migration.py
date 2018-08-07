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


import logging
import webapp2
from google.appengine.ext import deferred
from google.appengine.ext import ndb
from recidiviz.utils.auth import authenticate_request
from recidiviz.ingest.us_ny.us_ny_snapshot import UsNySnapshot
from recidiviz.ingest.us_ny.us_ny_record import UsNyRecord
from recidiviz.models.snapshot import InmateFacilitySnapshot


TEST_BATCH_SIZE = 10
DEFAULT_BATCH_SIZE = 25


class DataMigration(webapp2.RequestHandler):
    """Request handler for requests related to data migration."""

    @authenticate_request
    def get(self):
        """Request handler to kick off migration work

        Request handler for data migration tasks. Migrates last_release_date
        and last_release_type from UsNyRecord model to Record model, and old
        InmateFacilitySnapshot entities to new Snapshot entities.

        Example queries:

            # Migrate 5x Snapshot entities only.
            http://localhost:8080/data_migration?test_only=true&migration_type=snapshot

            # Migrate 5x Record entities only
            http://localhost:8080/data_migration?test_only=true&migration_type=record

            # Migrate all Snapshot entities
            http://localhost:8080/data_migration?test_only=false&migration_type=snapshot

            # Migrate all Record entities
            http://localhost:8080/data_migration?test_only=false&migration_type=record

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
        test_only = self.request.get('test_only', "true").lower()
        migration_type = self.request.get('migration_type', None).lower()

        test_only = True if test_only == "true" else False

        if migration_type == "snapshot":
            deferred.defer(migrate_snapshots, test_only=test_only)
        elif migration_type == "record":
            deferred.defer(migrate_record_fields, test_only=test_only)
        else:
            logging.error("Migration type '%s' not recognized. Exiting." %
                          migration_type)
            self.response.write("Invalid parameters, see logs.")
            self.response.set_status(500)
            return

        self.response.write("Kicked off migration.")
        logging.info("Kicked off migration.")


def migrate_snapshots(cursor=None, num_updated=0, batch_size=DEFAULT_BATCH_SIZE,
                      test_only=True):
    """Migrate a batch of InmateFacilitySnapshot entities

    Migrate InmateFacilitySnapshots into Snapshot entities. Reads in N
    InmateFacilitySnapshots at a time, pulls the relevant fields from the
    corresponding UsNyRecord entities, creates corresponding Snapshots,
    and deletes corresponding InmateFacilitySnapshots.

    Args:
        cursor: (ndb cursor) Query cursor for where we are in the migration
        num_updated: (int) Current number of snapshots updated
        batch_size: (int) Number of entities to handle during this run
        test_only: (bool) If true, performs non-destructive migration only

    Returns:
        N/A
    """
    if test_only:
        batch_size = TEST_BATCH_SIZE

    # Get (batch_size) InmateFacilitySnapshots. If <(batch_size) returned, set
    # migration_complete to True.
    inmate_facility_query = InmateFacilitySnapshot.query()
    inmate_facility_snapshots, next_cursor, more = \
        inmate_facility_query.fetch_page(batch_size, start_cursor=cursor)

    to_put = []
    to_del = []

    # For each snapshot, pull date and facility info, parent record info, and
    # create new Snapshot entity. Add to new_snapshot_entities.
    for facility_snapshot in inmate_facility_snapshots:
        record_key = facility_snapshot.key.parent()
        record = record_key.get()

        snapshot = UsNySnapshot(
            parent=record_key,
            admission_type=record.admission_type,
            birthday=record.birthday,
            cond_release_date=record.cond_release_date,
            county_of_commit=record.county_of_commit,
            custody_date=record.custody_date,
            custody_status=record.custody_status,
            latest_facility=facility_snapshot.facility,
            earliest_release_date=record.earliest_release_date,
            earliest_release_type=record.earliest_release_type,
            given_names=record.given_names,
            is_released=record.is_released,
            latest_release_date=record.last_release_date,
            latest_release_type=record.last_release_type,
            last_custody_date=record.last_custody_date,
            last_name=record.last_name,
            max_expir_date=record.max_expir_date,
            max_expir_date_superv=record.max_expir_date_superv,
            max_expir_date_parole=record.max_expir_date_parole,
            max_sentence_length=record.max_sentence_length,
            min_sentence_length=record.min_sentence_length,
            offense=record.offense,
            offense_date=record.offense_date,
            parole_discharge_date=record.parole_discharge_date,
            parole_elig_date=record.parole_elig_date,
            parole_hearing_date=record.parole_hearing_date,
            parole_hearing_type=record.parole_hearing_type,
            race=record.race,
            sex=record.sex,
            created_on=facility_snapshot.snapshot_date
        )

        to_put.append(snapshot)
        to_del.append(facility_snapshot.key)
        num_updated += len(to_del)

    transactional_put_multi(to_put, to_del, test_only)

    if more and not test_only:
        deferred.defer(migrate_snapshots,
                       cursor=next_cursor,
                       num_updated=num_updated,
                       test_only=test_only)
    else:
        logging.debug('migrate_snapshots complete with %d updates!' %
                      num_updated)


def migrate_record_fields(cursor=None, num_updated=0,
                          batch_size=DEFAULT_BATCH_SIZE, test_only=True):
    """Migrate a batch of Record entities

    Transfers the UsNyRecord fields 'last_release_date' and 'last_release_type'
    to Record superclass as 'latest_release_date' and 'latest_release_type'.
    Name change is to avoid DuplicatePropertyError from Polymodel class.

    Args:
        cursor: (ndb cursor) Query cursor for where we are in the migration
        num_updated: (int) Current number of records updated
        batch_size: (int) Number of records to handle during this run
        test_only: (bool) If true, performs non-destructive migration only

    Returns
        N/A
    """
    if test_only:
        batch_size = TEST_BATCH_SIZE

    record_query = UsNyRecord.query()
    records, next_cursor, more = record_query.fetch_page(
        batch_size, start_cursor=cursor)

    to_put = []

    for record in records:
        # Move properties to new, superclass-level properties
        # 'if' checks protect against any accidental re-running of the migration
        if not hasattr(record, 'latest_release_date') \
                or not record.latest_release_date:
            record.latest_release_date = record.last_release_date

        if not hasattr(record, 'latest_release_type') \
                or not record.latest_release_type:
            record.latest_release_type = record.last_release_type

        # Clone properties to the instance (so as not to delete from the class),
        # and delete from the entity.
        if not test_only:
            record._clone_properties()
            del record._properties['last_release_type']
            del record._properties['last_release_date']

        to_put.append(record)

    if transactional_put_multi(to_put=to_put, test_only=test_only):
        num_updated += len(to_put)
        logging.info('Put %d updated record entities for a total of %s' %
                     (len(to_put), num_updated))
    else:
        logging.error("Transactional put for Record migration failed.")

    if more and not test_only:
        deferred.defer(migrate_record_fields,
                       cursor=next_cursor,
                       num_updated=num_updated,
                       test_only=test_only)
    else:
        logging.debug(
            'migrate_record_fields complete with %d updates!' % num_updated)


@ndb.transactional(xg=True)  # pylint: disable=no-value-for-parameter
def transactional_put_multi(to_put, to_del=None, test_only=True):
    """Transactionally persist updated entiies and delete those they replace

    Saves a set of entities, and deletes a set of entities, in the same
    transaction. Note that this can only be used for up to 25 entity groups
    at a time (GAE constraint).

    Args:
        to_put: List of entities to persist to datastore
        to_del: List of entities to delete from datastore, if applicable
        test_only: (bool) whether to delete the old entities

    Returns:
        True if success
    """
    if to_put:
        ndb.put_multi(to_put)

    if to_del and not test_only:
        ndb.delete_multi(to_del)

    return True


app = webapp2.WSGIApplication([
    ('/data_migration', DataMigration)
], debug=False)
