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

"""Tests for ingest/docket.py."""


import json

from datetime import datetime
from dateutil.relativedelta import relativedelta
from google.appengine.api import taskqueue
from google.appengine.ext import ndb
from google.appengine.ext import testbed

from recidiviz.ingest import docket
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.sessions import ScrapeSession
from recidiviz.ingest.us_ny.us_ny_record import UsNyRecord
from recidiviz.models.person import Person
from recidiviz.models.snapshot import Snapshot


class TestPopulation(object):
    """Tests for the methods related to population of items in the docket."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_add_to_query_docket_background(self):
        scrape_key = ScrapeKey("us_ny", "background")

        docket.add_to_query_docket(scrape_key, get_payload(as_json=False))

        tasks = [docket.get_new_docket_item(scrape_key),
                 docket.get_new_docket_item(scrape_key)]
        assert len(tasks) == 2

        for i, task in enumerate(tasks):
            assert task.payload == json.dumps(
                get_payload(as_json=False)[i])
            assert task.tag == "us_ny-background"

    def test_add_to_query_docket_snapshot(self):
        scrape_key = ScrapeKey("us_ny", "snapshot")

        docket.add_to_query_docket(scrape_key,
                                   get_snapshot_payload(as_json=False))

        tasks = [docket.get_new_docket_item(scrape_key),
                 docket.get_new_docket_item(scrape_key)]
        assert len(tasks) == 2

        for i, task in enumerate(tasks):
            assert task.payload == json.dumps(
                get_snapshot_payload(as_json=False)[i])
            assert task.tag == "us_ny-snapshot"

    def test_load_target_list_background_happy_path(self):
        scrape_key = ScrapeKey("us_ny", "background")

        docket.load_target_list(scrape_key)

        task = docket.get_new_docket_item(scrape_key)
        assert task.payload == json.dumps(('aaardvark', ''))
        assert not docket.get_new_docket_item(scrape_key, back_off=1)

    def test_load_target_list_snapshot_never_ingested(self):
        scrape_key = ScrapeKey("us_ny", "snapshot")

        docket.load_target_list(scrape_key)

        assert not docket.get_new_docket_item(scrape_key, back_off=1)

    def test_load_target_list_snapshot_happy_path(self):
        scrape_key = ScrapeKey("us_ny", "snapshot")
        halfway = datetime.now() - relativedelta(years=5)

        person = get_person("clem", "12345")
        record = get_record(person.key, "56789", halfway)
        get_record(person.key, "89012", halfway - relativedelta(years=10))
        get_snapshot(record.key, halfway)

        docket.load_target_list(scrape_key)

        task = docket.get_new_docket_item(scrape_key)
        assert task.payload == json.dumps(("12345", ["89012"]))
        assert task.tag == "us_ny-snapshot"


class TestRetrieval(object):
    """Tests for the methods related to retrieval of items from the docket."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_get_new_docket_item(self):
        taskqueue.Task(tag='us_ny-background',
                       payload=get_payload(),
                       method='PULL').add(docket.DOCKET_QUEUE_NAME)

        docket_item = docket.get_new_docket_item(
            ScrapeKey("us_ny", "background"))
        assert docket_item.tag == 'us_ny-background'
        assert docket_item.payload == get_payload()

    def test_get_new_docket_item_no_matching_items(self):
        taskqueue.Task(tag='us_ny-background',
                       payload=get_payload(),
                       method='PULL').add(docket.DOCKET_QUEUE_NAME)

        docket_item = docket.get_new_docket_item(
            ScrapeKey("us_fl", "background"), back_off=1)
        assert not docket_item

    def test_get_new_docket_item_no_items_at_all(self):
        docket_item = docket.get_new_docket_item(
            ScrapeKey("us_ny", "background"), back_off=1)
        assert not docket_item


class TestRemoval(object):
    """Tests for the methods related to removal of items from the docket."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_purge_query_docket(self):
        scrape_key = ScrapeKey("us_ut", "snapshot")

        taskqueue.Task(tag='us_va-background',
                       payload=get_payload(),
                       method='PULL').add(docket.DOCKET_QUEUE_NAME)
        taskqueue.Task(tag='us_ut-snapshot',
                       payload=get_payload(),
                       method='PULL').add(docket.DOCKET_QUEUE_NAME)

        docket.purge_query_docket(scrape_key)
        assert not docket.get_new_docket_item(scrape_key, back_off=1)
        assert docket.get_new_docket_item(
            ScrapeKey("us_va", "background"), back_off=1)

    def test_purge_query_docket_nothing_matching(self):
        scrape_key = ScrapeKey("us_ny", "background")

        taskqueue.Task(tag='us_va-background',
                       payload=get_payload(),
                       method='PULL').add(docket.DOCKET_QUEUE_NAME)

        docket.purge_query_docket(scrape_key)
        assert not docket.get_new_docket_item(scrape_key, back_off=1)

    def test_purge_query_docket_already_empty(self):
        scrape_key = ScrapeKey("us_ny", "background")
        docket.purge_query_docket(scrape_key)
        assert not docket.get_new_docket_item(scrape_key, back_off=1)


def test_get_task_name_same_within_minute():
    first_time = datetime(2018, 10, 2, 16, 29, 43)
    first = docket.get_task_name("us_ny", "12345", date_time=first_time)
    second_time = datetime(2018, 10, 2, 16, 29, 48)
    second = docket.get_task_name("us_ny", "12345", date_time=second_time)

    assert first == second


def get_payload(as_json=True):
    body = [{'name': 'Jacoby, Mackenzie'}, {'name': 'Jacoby, Clementine'}]
    if as_json:
        return json.dumps(body)
    return body


def get_snapshot_payload(as_json=True):
    body = [('123', ['456']), ('789', ['012', '234'])]
    if as_json:
        return json.dumps(body)
    return body


def create_open_session(region_code, scrape_type, start, docket_item):
    session = ScrapeSession(region=region_code,
                            scrape_type=scrape_type,
                            docket_item=docket_item,
                            start=start)
    session.put()
    return session


def get_person(key, person_id):
    new_person = Person(id=key, person_id=person_id)
    new_person.put()
    return new_person


def get_record(parent_key, record_id, latest_release_date):
    new_record = UsNyRecord(parent=parent_key,
                            record_id=record_id,
                            latest_release_date=latest_release_date,
                            is_released=False)
    new_record.put()
    return new_record


def get_snapshot(parent_key, snapshot_date):
    new_snapshot = Snapshot(parent=parent_key,
                            created_on=snapshot_date,
                            is_released=False)
    new_snapshot.put()
    return new_snapshot
