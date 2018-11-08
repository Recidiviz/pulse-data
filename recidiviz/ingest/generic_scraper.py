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
    3.  All of the getters which populate one of the DB fields need also be
        implemented (for now).  Each getter takes content and params, and the
        child scrapers need to know how to extract out that data given content
        and params.
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
from recidiviz.ingest.scraper import Scraper


class GenericScraper(Scraper):
    """Generic class for scrapers."""

    def __init__(self, region_name):
        super(GenericScraper, self).__init__(region_name)

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
        data = params.get('data', self.get_initial_data())
        task_type = params.get('task_type', self.get_initial_task_type())

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
        if self.should_get_more_tasks(task_type):
            tasks = self.get_more_tasks(content, params)
            for task_params in tasks:
                # Always pass along the scrape type as well.
                task_params['scrape_type'] = params['scrape_type']
                self.add_task('_generic_scrape', task_params)
        if self.should_scrape_person(task_type):
            person = self._populate_person(content, params)
        if self.should_scrape_record(task_type):
            record, old_record = self._populate_record(content, params, person)
            self._populate_snapshot(record, old_record)
        return None

    def _populate_person(self, content, params):
        """
        Calls the necessary functions to populate a person in the database

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A person database object that is passed through to future functions.
        """
        person_id = self.get_person_id(content, params)
        # TODO(#170): automatically prepend person id with stable region code.
        person = self.get_person_class().get_or_insert(person_id)
        person.person_id = person_id
        person.person_id_is_fuzzy = self.person_id_is_fuzzy()
        person.given_names = self.get_given_names(content, params)
        person.surname = self.get_surname(content, params)
        person.suffix = self.get_suffix(content, params)
        person.alias = self.get_alias(content, params)
        person.birthdate = self.get_birthdate(content, params)
        person.age = self.get_age(content, params)
        person.region = self.get_region().region_code
        person.sex = self.get_sex(content, params)
        person.race = self.get_race(content, params)
        # Lastly populate any extra params that may be scraper specific.
        self.populate_extra_person_params(content, params, person)

        try:
            person.put()
        except (Timeout, TransactionFailedError, InternalError) as e:
            # Datastore error - fail task to trigger queue retry + backoff
            logging.warning("Couldn't persist person: %s", person.person_id)
            logging.error(str(e))
            return -1
        return person

    def _populate_record(self, content, params, person):
        """Calls the necessary functions to populate a record in the data.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.
            person: the Person to create a Record for

        Returns:
            A record database object.
        """
        record_id = self.get_record_id(content, params)
        record = self.get_record_class().get_or_insert(
            record_id, parent=person.key)
        old_record = deepcopy(record)

        # First populate the information from the person.
        record.birthdate = person.birthdate
        record.race = person.race
        record.sex = person.sex
        record.surname = person.surname
        record.given_names = person.given_names
        record.region = person.region

        # Now populate all of the record related information
        record.record_id = record_id
        record.record_id_is_fuzzy = self.record_id_is_fuzzy()
        record.admission_type = self.get_admission_type(content, params)
        record.case_worker = self.get_case_worker(content, params)
        record.community_supervision_agency = (
            self.get_community_supervision_agency(content, params))
        record.cond_release_date = self.get_cond_release_date(content, params)
        record.county_of_commit = self.get_county_of_commit(content, params)
        record.custody_date = self.get_custody_date(content, params)
        record.custody_status = self.get_custody_status(content, params)
        record.earliest_release_date = self.get_earliest_release_date(
            content, params)
        record.earliest_release_type = self.get_earliest_release_type(
            content, params)
        record.is_released = self.get_is_released(content, params)
        record.last_custody_date = self.get_last_custody_date(content, params)
        record.latest_facility = self.get_latest_facility(content, params)
        record.latest_release_date = self.get_latest_release_date(
            content, params)
        record.latest_release_type = self.get_latest_release_type(
            content, params)
        record.max_expir_date = self.get_max_expir_date(content, params)
        record.max_expir_date_parole = self.get_max_expir_date_parole(
            content, params)
        record.max_expir_date_superv = self.get_max_expir_date_superv(
            content, params)
        record.max_sentence_length = self.get_max_sentence_length(
            content, params)
        record.min_sentence_length = self.get_min_sentence_length(
            content, params)
        record.offense = self.get_offenses(content, params)
        record.offense_date = self.get_offense_date(content, params)
        record.parole_discharge_date = self.get_parole_discharge_date(
            content, params)
        record.parole_elig_date = self.get_parole_elig_date(content, params)
        record.parole_hearing_date = self.get_parole_hearing_date(
            content, params)
        record.parole_hearing_type = self.get_parole_hearing_type(
            content, params)
        record.parole_officer = self.get_parole_officer(content, params)
        record.release_date = self.get_release_date(content, params)
        self.populate_extra_record_params(content, params, record)

        try:
            record.put()
        except (Timeout, TransactionFailedError, InternalError) as e:
            logging.warning("Couldn't persist record: %s", record.record_id)
            logging.error(str(e))
            return -1
        return record, old_record

    def _populate_snapshot(self, record, old_record):
        """Populates a snapshot in this scrape instance.

        Args:
            record: The record to populate the snapshot from.
            old_record: The old record to compare to.
        """
        self._compare_and_set_snapshot(record, old_record)

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

    def should_scrape_person(self, task_type):
        """Tells us if we should scrape a person from the page.

        Args:
            A hexcode representing the task_type

        Returns:
            boolean whether or not we should scrape a person from the page.
        """
        return task_type & constants.SCRAPE_PERSON

    def should_scrape_record(self, task_type):
        """Tells us if we should scrape a record from the page.

        Args:
            A hexcode representing the task_type

        Returns:
            boolean whether or not we should scrape a record from the page.
        """
        return task_type & constants.SCRAPE_RECORD

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

    def get_person_id(self, content, params):
        """Gets person id given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons id
        """
        pass

    def get_record_id(self, content, params):
        """Gets record id given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons record id
        """
        pass

    def person_id_is_fuzzy(self):
        """Returns whether or not this scraper generates person ids"""
        return False

    def record_id_is_fuzzy(self):
        """Returns whether or not this scraper generates record ids"""
        return False

    def get_given_names(self, content, params):
        """Gets the persons given names from content and params that are
        passed in.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons given name
        """
        pass

    def get_surname(self, content, params):
        """Gets the persons surname.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons surname
        """
        pass

    def get_suffix(self, content, params):
        """Gets persons suffix given content.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons suffix.
        """
        pass

    def get_alias(self, content, params):
        """Gets person alias given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons alias
        """
        pass

    def get_birthdate(self, content, params):
        """Gets person birthday given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A datetime representing the persons birthdate
        """
        pass

    def get_age(self, content, params):
        """Gets person age given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            An integer representing the persons age
        """
        pass

    def get_sex(self, content, params):
        """Gets person sex given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons sex
        """
        pass

    def get_race(self, content, params):
        """Gets person race given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons race
        """
        pass

    def populate_extra_person_params(self, content, params, person):
        pass

    # RECORD RELATED GETTERS

    def get_admission_type(self, content, params):
        """Gets record admission type given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the admission type
        """
        pass

    def get_case_worker(self, content, params):
        """Gets case worker given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons case worker
        """
        pass

    def get_community_supervision_agency(self, content, params):
        """Gets the supervision agency

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons sex
        """
        pass

    def get_cond_release_date(self, content, params):
        """Gets conditional release date given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A date representing the conditional release date
        """
        pass

    def get_county_of_commit(self, content, params):
        """Gets county of commit given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the county of commit.
        """
        pass

    def get_custody_date(self, content, params):
        """Gets custody date given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A datetime representing the persons custory date
        """
        pass

    def get_custody_status(self, content, params):
        """Gets the record custody status given a page.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons custody status.
        """
        pass

    def get_earliest_release_date(self, content, params):
        """Gets earliest release date for a record.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons earliest release date.
        """
        pass

    def get_earliest_release_type(self, content, params):
        """Gets earliest release type of a person.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons earliest releast type.
        """
        pass

    def get_is_released(self, content, params):
        """Gets whether or not the person is released.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A bool representing whether the person is released.
        """
        pass

    def get_last_custody_date(self, content, params):
        """Gets last custory date of a person.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A datetime representing the persons sex
        """
        pass

    def get_latest_facility(self, content, params):
        """Gets the latest facility the person is in.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the facility the person is in.
        """
        pass

    def get_latest_release_date(self, content, params):
        """Gets latest release date.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A datetime representing the persons latest release date
        """
        pass

    def get_latest_release_type(self, content, params):
        """Gets latest release type.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the latest release type
        """
        pass

    def get_max_expir_date(self, content, params):
        """Gets a records max expiration date

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A datetime representing the max expir date.
        """
        pass

    def get_max_expir_date_parole(self, content, params):
        """Gets max expiration date for parole.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A datetime representing the persons max parole expir date.
        """
        pass

    def get_max_expir_date_superv(self, content, params):
        """Gets max expiration date for supervision

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A date representing the supervision expiration date.
        """
        pass

    def get_max_sentence_length(self, content, params):
        """Gets record admission type given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A SentenceDuration representing the persons sex
        """
        pass

    def get_min_sentence_length(self, content, params):
        """Gets minimum sentence length for the record.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A datetime representing the minimum sentence length.
        """
        pass

    def get_offense_date(self, content, params):
        """Gets date of the offense the caused the record.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.
        Returns:
            A datetime representing the date of the offense.
        """
        pass

    def get_parole_discharge_date(self, content, params):
        """Gets date of parole discharge.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A datetime representing the parole dischard date.
        """
        pass

    def get_parole_elig_date(self, content, params):
        """Gets date the person is eligible for parole.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A datetime representing the persons parole eligility.
        """
        pass

    def get_parole_hearing_date(self, content, params):
        """Gets date the person has a parole hearing

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A datetime representing the persons hearing date.
        """
        pass

    def get_parole_hearing_type(self, content, params):
        """Gets the type of parole hearing for the person

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the person's parole hearing type
        """
        pass

    def get_parole_officer(self, content, params):
        """Gets parole officer for the person.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons parole officer
        """
        pass

    def get_release_date(self, content, params):
        """Gets date the person was released.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A datetime representing the persons sex release date.
        """
        pass

    def get_offenses(self, content, params):
        """Gets the list of offenses relevant to the record.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A list of Offense objects for the record.
        """
        pass

    def populate_extra_record_params(self, content, params, record):
        pass
