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

"""
populate_test_db

Used to set up the local server environment for testing. Will not run on
production server.

You will need a local.yaml file with appropriate environment variables to
use this tool.
"""


from datetime import datetime
import logging
import random
import string
import time
import webapp2

from google.appengine.ext import ndb
from models.env_vars import EnvironmentVariable
from models.inmate import Inmate
from models.record import Offense, SentenceDuration, Record
from models.snapshot import Snapshot
from scraper.models.scrape_session import ScrapeSession, ScrapedRecord
from utils import environment
from utils import regions
from utils.auth import authenticate_request

# TODO: Remove post-migration
from scraper.us_ny.us_ny_record import UsNyRecord  # pylint: disable=ungrouped-imports
from models.snapshot import InmateFacilitySnapshot  # pylint: disable=ungrouped-imports


class PopulateDb(webapp2.RequestHandler):
    """Request handler for populating the database."""

    @environment.local_only
    @authenticate_request
    def get(self):
        """Request handler to populate synthetic data in the local datastore

        Request handler to populate the local datastore with environment
        variables and test data.

        Example queries:

            Add (more) fake data to the datastore, but don't wipe what's already
            there:
            http://localhost:8080/test_populator/populate

            Delete any existing datastore contents, populate environment
            variables and fake data in the datastore:
            http://localhost:8080/test_populator/populate?wipe_db=true

            Delete existing datastore contents, populate env vars and fake data
            with up to 4 old snapshot entities per record, and no new snapshot
            entities:
            http://localhost:8080/test_populator/populate?wipe_db=true&region=us_ny&suppress_new_fields=true&snapshots_per_record=0&old_snapshots_per_record=4

        URL Parameters:
            wipe_db: "true" or "false", whether to remove prior datastore
                contents before loading more
            region: Region code to load data for
            suppress_new_fields: "true" or "false", whether to not populate new
                fields to test migration

        Returns:
            HTTP 200 if successful
            HTTP 400 if not
        """
        wipe_db = self.request.get('wipe_db', "false").lower()
        wipe_db = True if wipe_db == "true" else False

        region_code = self.request.get('region', None)
        if not regions.validate_region_code(region_code):
            self.invalid_params("No region code provided. Exiting.")
            return

        try:
            inmate_count = int(self.request.get('inmates', 5))
            record_count = int(self.request.get('records_per_inmate', 3))
            snapshot_count = int(self.request.get('snapshots_per_record', 3))
            old_snapshot_count = int(self.request.get(
                'old_snapshots_per_record', 0))
        except ValueError:
            self.invalid_params("Fake data quantity must be integer. Exiting.")
            return

        # TODO: Remove this after Record migration
        suppress_new_fields = self.request.get('suppress_new_fields', False)
        suppress_new_fields = True if suppress_new_fields == "true" else False

        if wipe_db:
            purge_datastore()

        load_env_vars()
        region = regions.Region(region_code)

        # Add some sample NY inmates, records, and snapshots to work with
        # Sleeps allow datastore indices to catch up, since each method
        # below relies on the prior's generated data
        generate_inmates(region, inmate_count)
        time.sleep(5)
        generate_inmate_records(region, record_count, suppress_new_fields)
        time.sleep(5)
        generate_snapshots(region, snapshot_count)
        generate_old_snapshot_type(old_snapshot_count)

        self.response.write("Database populated, feel free to test.")
        logging.info("Completed test datastore setup.")

    def invalid_params(self, msg):
        """Logs problem with request and sets HTTP response code

        Args:
            msg: (string) Message to write to service logs

        Returns:
            N/A
        """
        self.response.write("Invalid parameters, see logs.")
        logging.error(msg)
        self.response.set_status(400)
        return


class ClearDb(webapp2.RequestHandler):
    """Request handler for clearing the database."""

    @environment.local_only
    @authenticate_request
    def get(self):
        """Request handler to purge datastore, re-populate env vars only

        Request handler to empty the datastore of anything except for
        environment variables.

        Example queries:

            Wipe anything in the datastore (but keep env vars):
            http://localhost:8080/test_populator/clear

        Params:
            N/A

        Returns:
            HTTP 200 if successful
            HTTP 400 if not
        """
        purge_datastore()
        load_env_vars()

        logging.info("Completed clearing test datastore.")


def load_env_vars():
    """Load environment variables (proxy and user agent info) to datastore

    Add proxy info and user agent string to the datastore's env variables.
    Loads environment variables from local.yaml file with appropriate
    environment variables.

    Args:
        N/A

    Returns:
        N/A
    """
    logging.info("Adding environment variables...")

    env_vars = environment.load_local_vars()

    old_env_vars = EnvironmentVariable.query().fetch()
    for var in old_env_vars:
        var.key.delete()

    for _, details in env_vars.iteritems():

        new_variable = EnvironmentVariable(
            name=details['name'],
            region=details['region'],
            value=details['value'])

        new_variable.put()


def purge_datastore():
    """Clear existing entities (not env vars) from datastore

    Clears all existing entities from datastore, except for environment
    variables (use load_env_vars for environment variables).


    Args:
        N/A

    Returns:
        N/A
    """
    logging.info("Wiping datastore...")

    entities = []

    entities.extend(Inmate.query().fetch())
    entities.extend(Record.query().fetch())
    entities.extend(Snapshot.query().fetch())
    entities.extend(ScrapeSession.query().fetch())
    entities.extend(ScrapedRecord.query().fetch())

    # TODO: Remove InmateFacilitySnapshot post migration
    entities.extend(InmateFacilitySnapshot.query().fetch())

    for entity in entities:
        entity.key.delete()


def generate_inmates(region, num_inmates):
    """Generate inmate entities with semi-random field values

    Generates (num_inmates) inmate entities with pseudo-random
    property values.

    Args:
        region: (string) Region object
        num_inmates: (int) How many inmate entities to generate

    Returns:
        N/A
    """
    logging.info("Generating fake inmate data...")

    inmate_class = ndb.Model._kind_map[region.entity_kinds['inmate']]
    inmate_attrs = inmate_class._properties

    region_inmate = region.get_inmate_kind()

    for _ in range(num_inmates):

        inmate = region_inmate()

        for attr in inmate_attrs:
            if attr not in ["class"]:

                model_prop = region_inmate._properties[attr]
                enum = region_inmate._properties[attr]._choices
                required = region_inmate._properties[attr]._required

                fake_value = details_generator(model_prop, enum, required)

                setattr(inmate, attr, fake_value)

        inmate.put()


def generate_inmate_records(region, num_records, suppress_new_fields):
    """Generate record entities with semi-random field values

    Generates at least one, and at most num_records, Record entities for the
    each inmate associated with the provided region in the datastore.

    Args:
        region: (string) Region code to create entities for
        num_records: (int) Generate up to this many records per inmate
        suppress_new_fields: (bool) Suppress new fields to test migration

    Returns:
        N/A
    """
    logging.info("Generating fake record data...")

    record_class = ndb.Model._kind_map[region.entity_kinds['record']]
    record_attrs = record_class._properties

    region_inmate = region.get_inmate_kind()
    region_record = region.get_record_kind()

    exclude_attrs = ["class",
                     "record_id",
                     "min_sentence_length",
                     "max_sentence_length",
                     "offense"]

    # TODO: Remove post-Record migration
    if suppress_new_fields:
        exclude_attrs.append("latest_release_date")
        exclude_attrs.append("latest_release_type")

    inmate_query = region_inmate.query().fetch()

    for inmate in inmate_query:
        inmate_key = inmate.key

        for _ in range(random.choice(range(1, num_records))):

            record_id = ''.join(random.choice(string.lowercase)
                                for _ in range(8))
            record = region_record.get_or_insert(record_id, parent=inmate_key)
            record.record_id = record_id

            record.offense = generate_offense()
            record.min_sentence_length = generate_sentence_duration()
            record.max_sentence_length = generate_sentence_duration()

            for attr in record_attrs:
                if attr not in exclude_attrs:

                    model_prop = region_record._properties[attr]
                    enum = region_record._properties[attr]._choices
                    required = region_record._properties[attr]._required

                    fake_value = details_generator(model_prop, enum, required)

                    setattr(record, attr, fake_value)

            record.put()


def generate_sentence_duration():
    """Generate synthetic models.record.SentenceDuration entity

    Generates a single SentenceDuration entity.

    Args:
        N/A

    Returns:
        models.SentenceDuration object with pseudo-random values
    """
    sentence_duration = SentenceDuration()

    if random.choice(range(10)) != 1:
        duration_class = ndb.Model._kind_map['SentenceDuration']
        duration_attrs = duration_class._properties

        for attr in duration_attrs:
            if attr not in ["class"]:
                model_prop = SentenceDuration._properties[attr]
                enum = SentenceDuration._properties[attr]._choices
                required = SentenceDuration._properties[attr]._required

                fake_value = details_generator(model_prop, enum, required)

                setattr(sentence_duration, attr, fake_value)

    else:
        sentence_duration = None

    return sentence_duration


def generate_offense():
    """Generate models.record.Offense entities with semi-random field values

    Generates at least zero, and at most three fake crimes of conviction.
    Returns list of them.

    Args:
        N/A

    Returns:
        models.record.Offense object with pseudo-random values
    """
    offenses = []

    for _ in range(random.choice(range(0, 3))):
        offense = Offense()

        offense_class = ndb.Model._kind_map['Offense']
        offense_attrs = offense_class._properties

        for attr in offense_attrs:
            if attr not in ["class"]:
                model_prop = Offense._properties[attr]
                enum = Offense._properties[attr]._choices
                required = True

                fake_value = details_generator(model_prop, enum, required)

                setattr(offense, attr, fake_value)

        offenses.append(offense)

    return offenses


def generate_snapshots(region, num_snapshots):
    """Generate snapshot entities with pseudo-random field values

    Generates at least one, and at most (num_snapshots) snapshots per Record
    entity in the datastore.

    Args:
        region: (string) Region code to create entities for
        num_snapshots: (int) Max number of snapshots to generate per record

    Returns:
        N/A
    """
    logging.info("Generating fake snapshot data...")

    snapshot_class = ndb.Model._kind_map[region.entity_kinds['snapshot']]
    snapshot_attrs = snapshot_class._properties

    region_record = region.get_record_kind()
    region_snapshot = region.get_snapshot_kind()

    if num_snapshots:
        record_query = region_record.query().fetch()

        for record in record_query:

            for _ in range(random.choice(range(1, num_snapshots))):
                record_key = record.key
                snapshot = region_snapshot(parent=record_key)

                snapshot.offense = generate_offense()
                snapshot.min_sentence_length = generate_sentence_duration()
                snapshot.max_sentence_length = generate_sentence_duration()

                for attr in snapshot_attrs:
                    if attr not in ["class",
                                    "min_sentence_length",
                                    "max_sentence_length",
                                    "offense"]:

                        model_prop = region_snapshot._properties[attr]
                        enum = region_snapshot._properties[attr]._choices
                        required = region_snapshot._properties[attr]._required

                        fake_value = details_generator(model_prop,
                                                       enum,
                                                       required)

                        setattr(snapshot, attr, fake_value)

                snapshot.put()


# TODO: Remove this method post-snapshot migration
def generate_old_snapshot_type(num_snapshots):
    """Generates inmate entities with pseudo-random field values

    Generates old snapshot type (InmateFacilitySnapshot entities), between one
    and (num_snapshots) per Record entity found in datastore.

    Args:
        region: (string) Region code to create entities for
        num_snapshots: (int) Max number of entities to generate per Record

    Returns:
        N/A
    """
    logging.info("Generating fake old snapshot data...")

    if num_snapshots:
        record_query = UsNyRecord.query().fetch()

        for record in record_query:

            for _ in range(random.choice(range(1, num_snapshots))):
                record_key = record.key
                snapshot = InmateFacilitySnapshot(parent=record_key)

                model_prop = InmateFacilitySnapshot._properties["facility"]

                snapshot.facility = details_generator(model_prop, None, True)

                snapshot.put()


def details_generator(model_property, enum, required):
    """Generate property data for test entities

    Args:
        model_property: Property instance
        enum: None, or list of choices
        required: (bool) Field is required/cannot be None

    Returns:
        Value for property

    Raises:
        Exception: Raised if field type is unrecognized
    """
    if not required:
        if random.choice(range(100)) % 20 == 0:
            return None

    if enum:
        # TODO: Andrew to fix
        return random.choice(options)  # pylint: disable=undefined-variable

    if isinstance(model_property, ndb.StructuredProperty):
        # Too complex to handle for the moment
        logging.warning("Asked to generate details for unknown "
                        "StructuredProperty %s; setting to None."
                        % str(model_property))
        return None

    elif isinstance(model_property, ndb.StringProperty):
        length = random.randint(4, 20)
        return ''.join(random.choice(string.lowercase) for x in range(length))

    elif isinstance(model_property, ndb.DateProperty):
        year = random.randint(1950, 2010)
        month = random.randint(1, 12)
        day = random.randint(1, 28)

        return datetime(year, month, day).date()

    elif isinstance(model_property, ndb.DateTimeProperty):
        year = random.randint(1950, 2010)
        month = random.randint(1, 12)
        day = random.randint(1, 28)

        return datetime(year, month, day)

    elif isinstance(model_property, ndb.BooleanProperty):
        return random.choice([True, False])

    elif isinstance(model_property, ndb.IntegerProperty):
        return random.randint(1, 50)

    else:
        raise Exception("populate_test_db/details_generator: "
                        "Unrecognized field type.")


app = webapp2.WSGIApplication([
    ('/test_populator/populate', PopulateDb),
    ('/test_populator/clear', ClearDb)
], debug=False)
