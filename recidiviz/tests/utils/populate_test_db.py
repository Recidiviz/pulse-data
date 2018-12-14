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

You will need a secrets.yaml file with appropriate secrets to use this tool.
"""


import httplib
import logging
import random
import string
import time
from datetime import datetime

from flask import Blueprint, request
import yaml
from google.appengine.ext import ndb

from recidiviz.ingest.sessions import ScrapedRecord, ScrapeSession
from recidiviz.models.person import Person
from recidiviz.models.record import Offense, Record, SentenceDuration
from recidiviz.models.snapshot import Snapshot
from recidiviz.utils import environment, regions
from recidiviz.utils.auth import authenticate_request
from recidiviz.utils.secrets import Secret

test_populator = Blueprint('test_populator', __name__)

@test_populator.route('/populate')
@environment.local_only
@authenticate_request
def populate():
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
    wipe_db = request.args.get('wipe_db', 'false').lower() == 'true'

    region_code = request.args.get('region')
    if not regions.validate_region_code(region_code):
        logging.error("No region code provided. Exiting.")
        return ("Invalid parameters, see logs.", httplib.BAD_REQUEST)

    # If any of the parameters can't be converted to int the default is used.
    person_count = request.args.get('persons', 5, type=int)
    record_count = request.args.get('records_per_person', 3, type=int)
    snapshot_count = request.args.get('snapshots_per_record', 3, type=int)

    if wipe_db:
        _purge_datastore()

    _load_secrets()
    region = regions.Region(region_code)

    # Add some sample NY people, records, and snapshots to work with
    # Sleeps allow datastore indices to catch up, since each method
    # below relies on the prior's generated data
    generate_people(region, person_count)
    time.sleep(5)
    generate_person_records(region, record_count)
    time.sleep(5)
    generate_snapshots(region, snapshot_count)

    logging.info("Completed test datastore setup.")
    return ("Database populated, feel free to test.", httplib.OK)


@test_populator.route('/clear')
@environment.local_only
@authenticate_request
def clear():
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
    _purge_datastore()
    _load_secrets()

    logging.info("Completed clearing test datastore.")
    return ('', httplib.OK)


def _load_secrets():
    """Load secrets (proxy and user agent info) into datastore

    Get secrets (e.g. proxy info and user agent string) from secrets.yaml and
    write them into datastore.

    Args:
        N/A

    Returns:
        N/A
    """
    logging.info("Adding secrets...")

    for secret in Secret.query().fetch():
        secret.key.delete()

    with open("secrets.yaml", 'r') as ymlfile:
        for name, value in yaml.load(ymlfile)['secrets'].iteritems():
            Secret(name=name, value=value).put()


def _purge_datastore():
    """Clear existing entities (not secrets) from datastore

    Clears all existing entities from datastore, except for secrets (use
    _load_secrets instead).


    Args:
        N/A

    Returns:
        N/A
    """
    logging.info("Wiping datastore...")

    entities = []

    entities.extend(Person.query().fetch())
    entities.extend(Record.query().fetch())
    entities.extend(Snapshot.query().fetch())
    entities.extend(ScrapeSession.query().fetch())
    entities.extend(ScrapedRecord.query().fetch())

    for entity in entities:
        entity.key.delete()


def generate_people(region, num_people):
    """Generate person entities with semi-random field values

    Generates (num_people) person entities with pseudo-random
    property values.

    Args:
        region: (string) Region object
        num_people: (int) How many person entities to generate

    Returns:
        N/A
    """
    logging.info("Generating fake person data...")

    person_class = ndb.Model._kind_map[region.entity_kinds['person']]
    person_attrs = person_class._properties

    region_person = region.get_person_kind()

    for _ in range(num_people):

        person = region_person()

        for attr in person_attrs:
            if attr not in ["class"]:

                model_prop = region_person._properties[attr]
                enum = region_person._properties[attr]._choices
                required = region_person._properties[attr]._required

                fake_value = details_generator(model_prop, enum, required)

                setattr(person, attr, fake_value)

        person.put()


def generate_person_records(region, num_records):
    """Generate record entities with semi-random field values

    Generates at least one, and at most num_records, Record entities for the
    each person associated with the provided region in the datastore.

    Args:
        region: (string) Region code to create entities for
        num_records: (int) Generate up to this many records per person

    Returns:
        N/A
    """
    logging.info("Generating fake record data...")

    record_class = ndb.Model._kind_map[region.entity_kinds['record']]
    record_attrs = record_class._properties

    region_person = region.get_person_kind()
    region_record = region.get_record_kind()

    exclude_attrs = ["class",
                     "record_id",
                     "min_sentence_length",
                     "max_sentence_length",
                     "offense"]

    person_query = region_person.query().fetch()

    for person in person_query:
        person_key = person.key

        for _ in range(random.choice(range(1, num_records))):

            record_id = ''.join(random.choice(string.lowercase)
                                for _ in range(8))
            record = region_record.get_or_insert(record_id, parent=person_key)
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
        return random.choice(enum)

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
