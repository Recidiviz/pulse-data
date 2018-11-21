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


"""Generic implementation of the scraper.  This class abstracts away the need
for scraper implementations to know or understand tasks.  Child scrapers need
only to know how to extract data from a given page and whether or not more pages
need to be scraped.

This is a class that aims to handle as many DB related operations as possible
as well as operations around how to get the content of a page.  It provides
generic implementations of getter functions which aim to scrape out important
fields we care about.

In order to sublcass this the following functions must be implemented:

    1.  get_more_tasks: This function takes the page content as well as the
        scrape task params and returns a list of params defining what to scrape
        next.  The params must include 'endpoint' and 'task_type' which tell the
        generic scraper what endpoint we are getting and what we are doing
        with the endpoint when we do get it.
    2.  set_initial_vars:  This function should set any initial vars needed
        after a first scrape (session vars for example)
    3.  populate_data:  This function is called whenever a task loads a page
        that has important data in it.
"""

import abc
from copy import deepcopy
import logging

from lxml import html
from lxml.etree import XMLSyntaxError # pylint:disable=no-name-in-module

from google.appengine.ext import ndb
from google.appengine.ext.db import InternalError
from google.appengine.ext.db import Timeout, TransactionFailedError

from recidiviz.ingest import constants
from recidiviz.ingest import scraper_utils
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scraper import Scraper
# This is just temporary here, but using it for now until new DB is put into
# place.
from recidiviz.models.record import Offense


class BaseScraper(Scraper):
    """Generic class for scrapers."""

    def __init__(self, region_name):
        super(BaseScraper, self).__init__(region_name)

    def get_initial_task(self):
        """
        Get the name of the first task to be run.  For generic scraper we always
        call into the same function.

        Returns:
            The name of the task to run first.

        """
        return '_generic_scrape'

    # Each scraper can override this, by default it is treated as a url endpoint
    # but any scraper can override this and treat it as a different type of
    # endpoint like an API endpoint for example.
    def _fetch_content(self, endpoint, data=None):
        """Returns the page content.

        Args:
            endpoint: the endpoint to make a request to.
            data: dict of parameters to pass into the html request.

        Returns:
            Returns the content of the page or -1.
        """
        page = self.fetch_page(endpoint, data=data)
        if page == -1:
            return -1
        try:
            html_tree = html.fromstring(page.content)
        except XMLSyntaxError as e:
            logging.error("Error parsing initial page. Error: %s\nPage:\n\n%s",
                          e, page.content)
            return -1
        return html_tree

    def _generic_scrape(self, params):
        """
        General handler for all scrape tasks.  This function is a generic entry
        point into all types of scrapes.  It decides what to call based on
        params.

        Args:
            params: dict of parameters passed from the last scrape session.

        Returns:
            Nothing if successful, -1 if it fails
        """
        # If this is called without any params set, we assume its the first call
        endpoint = params.get('endpoint', self.get_initial_endpoint())
        task_type = params.get('task_type', self.get_initial_task_type())
        data = params.get(
            'data', self.get_initial_data() if self.is_initial_task(task_type)
            else None)
        ingest_info = None

        # Let the child transform the data if it wants before sending the
        # requests.  This hook is in here in case the child did something like
        # compress the data before it put it on the queue.
        self.transform_data(data)
        # We always fetch some content before doing anything.  Note that we
        # use get here for the data to return a default value of None if
        # this scraper doesn't set it.
        content = self._fetch_content(endpoint, data)
        if content == -1:
            return -1
        # For an initial task we just make a call to get initial variables.
        # This may be a noop for some scrapers.
        if self.is_initial_task(task_type):
            self.set_initial_vars(content, params)
        if self.should_scrape_data(task_type):
            # If we want to scrape data, we should either create an ingest_info
            # object or get the one that already exists.
            ingest_info = params.get('ingest_info', IngestInfo())
            self.populate_data(content, params, ingest_info)
        if self.should_get_more_tasks(task_type):
            tasks = self.get_more_tasks(content, params)
            for task_params in tasks:
                # Always pass along the scrape type as well.
                task_params['scrape_type'] = params['scrape_type']
                # If we have an ingest info to work with, we need to pass that
                # along as well.
                if ingest_info:
                    task_params['ingest_info'] = ingest_info
                self.add_task('_generic_scrape', task_params)
        # If we don't have any more tasks to scrape, we are at a leaf node and
        # we are ready to populate the database.
        else:
            # Something is wrong if we get here but don't have ingest_info
            # to work with.
            if not ingest_info:
                raise ValueError(
                    'IngestInfo must be populated if there are no more tasks')
            # This converter should exist elsewhere, but its in here for now
            # Because the real DB has not been put in yet, and we currently
            # still have per region DB models.  Ideally it should
            # do validations on datatypes, convert things to the proper format
            # if needed, and throw exceptions when it gets things it doesn't
            # expect.
            self._convert_ingest_info_and_populate_db(ingest_info)
        return None

    def _convert_ingest_info_and_populate_db(self, ingest_info):
        """
        Takes an ingest info object and converts it to the DB model so it can
        commit it to the database.

        Args:
            ingest_info: A populated ingest_info model.
        """
        for ingest_person in ingest_info.people:
            person_db = self._populate_person(ingest_person)
            for ingest_booking in ingest_person.bookings:
                self._populate_record_and_set_snapshot(
                    person_db, ingest_booking)


    def _populate_person(self, ingest_person):
        """
        Converts a Person object from ingest_info to a person db object

        Args:
            ingest_person: person object from the ingest_info model.

        Returns:
            person: a person db object.
        """
        # NOTE: suffix and alias are not part of the new schema so we don't
        # set it here.
        person_id = ingest_person.person_id
        person = self.get_person_class().get_or_insert(person_id)
        person.person_id = person_id
        person.person_id_is_fuzzy = self.person_id_is_fuzzy()
        person.given_names = ingest_person.given_names
        person.surname = ingest_person.surname
        person.birthdate = scraper_utils.parse_date_string(
            ingest_person.birthdate)
        person.age = int(ingest_person.age)
        person.region = self.get_region().region_code
        person.sex = ingest_person.sex
        person.race = ingest_person.race

        try:
            person.put()
        except (Timeout, TransactionFailedError, InternalError) as e:
            logging.warning("Couldn't persist person: %s", person.person_id)
            logging.error(str(e))
            return -1
        return person

    def _populate_record_and_set_snapshot(self, person_db, ingest_booking):
        """
        Converts a Booking object from ingest_info to a person db object.
        This function also creates a snapshot if needed.

        Args:
            person_db: A Person db object
            ingest_booking: A booking object from ingest_info.
        """
        record_id = ingest_booking.booking_id
        record = record = self.get_record_class().get_or_insert(
            record_id, parent=person_db.key)
        old_record = deepcopy(record)

        # First populate the information from the person.
        record.birthdate = person_db.birthdate
        record.race = person_db.race
        record.surname = person_db.surname
        record.given_names = person_db.surname
        record.region = person_db.region

        # Now populate all of the record related information.  This is a best
        # effort guess for now, when the DB becomes the schema it'll be easier
        # to do these mappings.
        record.record_id = record_id
        record.record_id_is_fuzzy = self.record_id_is_fuzzy()
        record.custody_date = scraper_utils.parse_date_string(
            ingest_booking.admission_date)
        record.release_date = scraper_utils.parse_date_string(
            ingest_booking.release_date)
        record.latest_release_type = str(ingest_booking.release_reason)
        record.custody_status = str(ingest_booking.custody_status)
        record.committed_by = ingest_booking.hold
        # This needs to be fixed, but default to false for now.
        record.is_released = False

        # Now go over the charges.
        offenses = []
        for charge in ingest_booking.charges:
            offense = Offense()
            offense.crime_description = charge.name
            offense.crime_class = str(charge.statute)
            offense.case_number = charge.case_number
            if charge.bond:
                offense.bond_amount = scraper_utils.currency_to_float(
                    charge.bond.amount)
            offenses.append(offense)

        record.offense = offenses

        try:
            record.put()
        except (Timeout, TransactionFailedError, InternalError) as e:
            logging.warning("Couldn't persist record: %s", record.record_id)
            logging.error(str(e))
            return -1

        # Finally we do a compare of the snapshot and return it if need be.
        self._compare_and_set_snapshot(record, old_record)
        return record, old_record

    def _compare_and_set_snapshot(self, record, old_record):
        """Populates a snapshot object based on params in record.

        Args:
            record: The record to populate the snapshot from.
            old_record: The old record to compare to.

        Returns:
            A snapshot database object.
        """
        snapshot_changed = False

        # pylint:disable=protected-access
        snapshot_class = ndb.Model._kind_map[self.get_snapshot_class().__name__]
        snapshot_attrs = snapshot_class._properties

        new_snapshot = self.get_snapshot_class()()
        last_snapshot = self.get_snapshot_class().query(
            ancestor=record.key).order(
                -self.get_snapshot_class().created_on).get()

        # We loop through all of the properties of the snapshot class and
        # Create this snapshot from the scraped record entities.
        if last_snapshot:
            for attr in snapshot_attrs:
                if attr not in ['class', 'created_on', 'offense']:
                    record_value = getattr(record, attr)
                    old_record_value = getattr(old_record, attr)
                    if record_value != old_record_value:
                        snapshot_changed = True
                        logging.info("Found change in person snapshot: field "
                                     "%s was '%s', is now '%s'.",
                                     attr, old_record_value, record_value)
                        setattr(new_snapshot, attr, record_value)
                    else:
                        setattr(new_snapshot, attr, None)
                elif attr == 'offense':
                    # Offenses have to be treated differently -
                    # setting them to None means an empty list or
                    # tuple instead of None, and an unordered list of
                    # them can't be compared to another unordered list
                    # of them.
                    if len(record.offense) != len(old_record.offense):
                        snapshot_changed = True
                        logging.info("Found change in person snapshot: field "
                                     "%s was '%s', is now '%s'.", attr,
                                     old_record.offense, record.offense)
                        setattr(new_snapshot, attr, record.offense)
                    else:
                        setattr(new_snapshot, attr, [])
        # If there were no snapshots previously, we need to loop through all
        # snapshot properties and copy them over from the current record.
        else:
            snapshot_changed = True
            for attr in snapshot_attrs:
                if attr not in ['class', 'created_on']:
                    record_value = getattr(record, attr)
                    setattr(new_snapshot, attr, record_value)

        if snapshot_changed:
            try:
                new_snapshot.parent = record.key
                new_snapshot.put()
            except (Timeout, TransactionFailedError, InternalError) as e:
                logging.warning("Couldn't store new snapshot for record %s",
                                old_record.record_id)
                logging.error(str(e))

    def is_initial_task(self, task_type):
        """Tells us if the task_type is initial task_type.

        Args:
            A hexcode representing the task_type

        Returns:
            boolean representing whether or not the task_type is initial task.
        """
        return task_type & constants.INITIAL_TASK

    def should_get_more_tasks(self, task_type):
        """Tells us if we should get more tasks.

        Args:
            A hexcode representing the task_type

        Returns:
            boolean whether or not we should get more tasks
        """
        return task_type & constants.GET_MORE_TASKS

    def should_scrape_data(self, task_type):
        """Tells us if we should scrape data from a page.

        Args:
            A hexcode representing the task_type

        Returns:
            boolean whether or not we should scrape a person from the page.
        """
        return task_type & constants.SCRAPE_DATA

    def person_id_to_record_id(self, person_id):
        """Convert provided person_id to record_id of any record for that
        person. This is the implementation of an abstract method in Scraper.

        Args:
            person_id: (string) Person ID for the person

        Returns:
            None if query returns None
            Record ID if a record is found for the person in the docket item

        """
        person = self.get_person_class().query(
            self.get_person_class().person_id == person_id).get()
        if not person:
            return None

        record = self.get_record_class().query(ancestor=person.key).get()
        if not record:
            return None

        return record.record_id

    @abc.abstractmethod
    def get_more_tasks(self, content, params):
        """
        Gets more tasks based on the content and params passed in.  This
        function should determine which task params, if any, should be
        added to the queue

        Every scraper must implement this.  It should return a list of params
        Where each param corresponds to a task we want to run.
        Mandatory fields are endpoint and task_type.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A list of dicts each containing endpoint and task_type at minimum
            which tell the generic scraper how to behave.
        """
        pass

    @abc.abstractmethod
    def set_initial_vars(self, content, params):
        """
        Sets initial vars in the params that it will pass on to future scrapes

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.
        """
        pass

    @abc.abstractmethod
    def populate_data(self, content, params, ingest_info):
        """
        Populates the ingest info object from the content and params given

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.
            ingest_info: The IngestInfo object to populate
        """
        pass

    def transform_data(self, data):
        """If the child needs to transform the data in any way before it sends
        the request, it can override this function.

        Args:
            data: dict of parameters to send as data to the post request.
        """
        pass

    def get_initial_endpoint(self):
        """Returns the initial endpoint to hit on the first call
        Returns:
            A string representing the initial endpoint to hit
        """
        return self.get_region().base_url

    def get_initial_data(self):
        """Returns the initial data to send on the first call
        Returns:
            A dict of data to send in the request.
        """
        return None

    def get_initial_task_type(self):
        """Returns the initial task_type to set on the first call
        Returns:
            A hex code from constants telling us how to behave on the first call
        """
        return constants.INITIAL_TASK_AND_MORE

    # The following can be overriden by the child scrapers to help scrape
    # content retrieved from an endpoint.
    # PERSON RELATED GETTERS

    def get_person_class(self):
        """Gets the person subtype to use for this scraper.

        Returns:
            The class representing the person db object.
        """
        return self.get_region().get_person_kind()

    def get_record_class(self):
        """Gets the person subtype to use for this scraper.

        Returns:
            The class representing the record db object.
        """
        return self.get_region().get_record_kind()

    def get_snapshot_class(self):
        """Gets the snapshot subtype to use for this scraper.

        Returns:
            The class representing the snapshot db object.
        """
        return self.get_region().get_snapshot_kind()
