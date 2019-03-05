# Recidiviz - a platform for tracking granular recidivism metrics in real time
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
"""Tests for batch_persistence.py."""
import datetime
import json
from unittest import TestCase

import pytest

from google.api_core import exceptions  # pylint: disable=no-name-in-module
from mock import patch

from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape import constants, ingest_utils
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.persistence import batch_persistence
from recidiviz.persistence.batch_persistence import PUBSUB_TYPE, BatchMessage
from recidiviz.utils import pubsub_helper

REGIONS = ['us_pa_greene', 'us_ny']
TEST_NAME = 'test'
TEST_NAME2 = 'test2'
TEST_ID = '1'
TEST_ENDPOINT = 'www.test.com'
TEST_ERROR = 'TestError'
TEST_PARAMS = {'test': 'value'}


# pylint: disable=protected-access


@pytest.mark.usefixtures("emulator")
class TestBatchPersistence(TestCase):
    """Tests for batch persistence logic"""

    def teardown_method(self, _test_method):
        for region in REGIONS:
            try:
                scrape_key = ScrapeKey(region, constants.ScrapeType.BACKGROUND)
                pubsub_helper.get_subscriber().delete_subscription(
                    pubsub_helper.get_subscription_path(
                        scrape_key, pubsub_type=PUBSUB_TYPE))
            except exceptions.NotFound:
                pass

    def test_write_to_pubsub(self):
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)
        pubsub_helper.create_topic_and_subscription(
            scrape_key, PUBSUB_TYPE)

        ii = IngestInfo()
        ii.create_person(full_name=TEST_NAME).create_booking(
            booking_id=TEST_ID)

        t = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )

        expected_batch = BatchMessage(ingest_info=ii, task=t)

        batch_persistence.write(ii, t, scrape_key)

        messages = batch_persistence._get_batch_messages(scrape_key)
        self.assertEqual(len(messages), 1)

        result = BatchMessage.from_serializable(
            json.loads(messages[0].message.data.decode()))
        self.assertEqual(expected_batch, result)

    def test_write_to_multiple_pubsub(self):
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)
        pubsub_helper.create_topic_and_subscription(
            scrape_key, PUBSUB_TYPE)

        ii = IngestInfo()
        ii.create_person(full_name=TEST_NAME).create_booking(
            booking_id=TEST_ID)

        ii2 = IngestInfo()
        ii2.create_person(full_name=TEST_NAME2).create_booking(
            booking_id=TEST_ID)

        t = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )

        t2 = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )

        expected_batches = [
            BatchMessage(ingest_info=ii, task=t),
            BatchMessage(ingest_info=ii2, task=t2)
        ]

        batch_persistence.write(ii, t, scrape_key)
        batch_persistence.write(ii2, t, scrape_key)

        messages = batch_persistence._get_batch_messages(scrape_key)
        self.assertEqual(len(messages), 2)

        result = [
            BatchMessage.from_serializable(
                json.loads(messages[0].message.data.decode())),
            BatchMessage.from_serializable(
                json.loads(messages[1].message.data.decode()))
        ]
        self.assertCountEqual(expected_batches, result)

    def test_write_error_to_pubsub(self):
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)
        pubsub_helper.create_topic_and_subscription(
            scrape_key, PUBSUB_TYPE)

        error = TEST_ERROR

        t = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )

        expected_batch = BatchMessage(error=error, task=t)

        batch_persistence.write_error(error, t, scrape_key)

        messages = batch_persistence._get_batch_messages(scrape_key)
        self.assertEqual(len(messages), 1)

        result = BatchMessage.from_serializable(
            json.loads(messages[0].message.data.decode()))
        self.assertEqual(expected_batch, result)

    @patch('recidiviz.persistence.persistence.write')
    def test_persist_to_db(self, mock_write):
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)
        pubsub_helper.create_topic_and_subscription(
            scrape_key, PUBSUB_TYPE)

        ii = IngestInfo()
        ii.create_person(person_id=TEST_ID, full_name=TEST_NAME).create_booking(
            booking_id=TEST_ID)

        t = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )

        batch_persistence.write(ii, t, scrape_key)

        expected_proto = ingest_utils.convert_ingest_info_to_proto(ii)
        start_time = datetime.datetime.now()
        batch_persistence.persist_to_database(
            scrape_key.region_code, scrape_key.scrape_type, start_time)

        result_proto = mock_write.call_args[0][0]
        self.assertEqual(result_proto, expected_proto)

        # After we persist, the messages should no longer be on the queue since
        # they should have been acked
        messages = batch_persistence._get_batch_messages(scrape_key)
        self.assertEqual(len(messages), 0)

    @patch('recidiviz.persistence.persistence.write')
    def test_persist_to_db_failed_no_write(self, mock_write):
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)
        pubsub_helper.create_topic_and_subscription(
            scrape_key, PUBSUB_TYPE)

        ii = IngestInfo()
        ii.create_person(person_id=TEST_ID, full_name=TEST_NAME).create_booking(
            booking_id=TEST_ID)

        t = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )

        # Because the tasks are different, we should fail.
        t2 = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
            params=TEST_PARAMS,
        )

        batch_persistence.write(ii, t, scrape_key)
        batch_persistence.write_error(TEST_ERROR, t2, scrape_key)

        start_time = datetime.datetime.now()
        with self.assertRaises(batch_persistence.BatchPersistError):
            batch_persistence.persist_to_database(
                scrape_key.region_code, scrape_key.scrape_type, start_time)

        self.assertEqual(mock_write.call_count, 0)

        # We should still have acked the messages even though we chose not to
        # write.
        messages = batch_persistence._get_batch_messages(scrape_key)
        self.assertEqual(len(messages), 0)

    @patch('recidiviz.persistence.persistence.write')
    def test_persist_to_db_same_task_one_fail_one_pass(self, mock_write):
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)
        pubsub_helper.create_topic_and_subscription(
            scrape_key, PUBSUB_TYPE)

        ii = IngestInfo()
        ii.create_person(person_id=TEST_ID, full_name=TEST_NAME).create_booking(
            booking_id=TEST_ID)

        t = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )

        # Because the tasks are the same, we expect that to be counted as a
        # pass.
        t2 = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )

        batch_persistence.write(ii, t, scrape_key)
        batch_persistence.write_error(TEST_ERROR, t2, scrape_key)

        expected_proto = ingest_utils.convert_ingest_info_to_proto(ii)
        start_time = datetime.datetime.now()
        batch_persistence.persist_to_database(
            scrape_key.region_code, scrape_key.scrape_type, start_time)

        result_proto = mock_write.call_args[0][0]
        self.assertEqual(result_proto, expected_proto)

        messages = batch_persistence._get_batch_messages(scrape_key)
        self.assertEqual(len(messages), 0)

    @patch('recidiviz.persistence.persistence.write')
    def test_persist_to_db_same_task_many_fail_one_pass(self, mock_write):
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)
        pubsub_helper.create_topic_and_subscription(
            scrape_key, PUBSUB_TYPE)

        ii = IngestInfo()
        ii.create_person(person_id=TEST_ID, full_name=TEST_NAME).create_booking(
            booking_id=TEST_ID)

        t = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )

        # Because the tasks are the same, we expect that to be counted as a
        # pass.
        t2 = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )

        batch_persistence.write(ii, t, scrape_key)
        # Even though a task failed many times, we should still write because it
        # passes eventually.
        batch_persistence.write_error(TEST_ERROR, t2, scrape_key)
        batch_persistence.write_error(TEST_ERROR, t2, scrape_key)
        batch_persistence.write_error(TEST_ERROR, t2, scrape_key)

        expected_proto = ingest_utils.convert_ingest_info_to_proto(ii)
        start_time = datetime.datetime.now()
        batch_persistence.persist_to_database(
            scrape_key.region_code, scrape_key.scrape_type, start_time)

        result_proto = mock_write.call_args[0][0]
        self.assertEqual(result_proto, expected_proto)

        messages = batch_persistence._get_batch_messages(scrape_key)
        self.assertEqual(len(messages), 0)

    @patch('recidiviz.persistence.persistence.write')
    def test_persist_to_db_different_regions(self, mock_write):
        scrape_key1 = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)
        scrape_key2 = ScrapeKey(REGIONS[1], constants.ScrapeType.BACKGROUND)
        pubsub_helper.create_topic_and_subscription(
            scrape_key1, PUBSUB_TYPE)
        pubsub_helper.create_topic_and_subscription(
            scrape_key2, PUBSUB_TYPE)

        ii = IngestInfo()
        ii.create_person(person_id=TEST_ID, full_name=TEST_NAME).create_booking(
            booking_id=TEST_ID)

        ii2 = IngestInfo()
        ii2.create_person(
            person_id=TEST_ID, full_name=TEST_NAME2).create_booking(
                booking_id=TEST_ID)

        t = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )

        t2 = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )

        batch_persistence.write(ii, t, scrape_key1)
        batch_persistence.write(ii2, t2, scrape_key2)

        expected_proto = ingest_utils.convert_ingest_info_to_proto(ii)
        start_time = datetime.datetime.now()
        batch_persistence.persist_to_database(
            scrape_key1.region_code, scrape_key1.scrape_type, start_time)

        result_proto = mock_write.call_args[0][0]
        self.assertEqual(result_proto, expected_proto)

        # We expect the region that we persisted to have no more messages and
        # the one we didn't yet persist to have one message.
        messages = batch_persistence._get_batch_messages(scrape_key1)
        self.assertEqual(len(messages), 0)

        expected_proto = ingest_utils.convert_ingest_info_to_proto(ii2)
        start_time = datetime.datetime.now()
        batch_persistence.persist_to_database(
            scrape_key2.region_code, scrape_key2.scrape_type, start_time)

        result_proto = mock_write.call_args[0][0]
        self.assertEqual(result_proto, expected_proto)
        self.assertEqual(mock_write.call_count, 2)
