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
"""Tests for batch_persistence.py."""
import copy
import datetime
import json
from unittest import TestCase

import pytest
from flask import Flask
from mock import Mock, patch

from recidiviz.ingest.models import ingest_info, serialization
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape import constants, infer_release, scrape_phase
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.persistence import batch_persistence, datastore_ingest_info
from recidiviz.persistence.datastore_ingest_info import BatchIngestInfoData

REGIONS = ["us_x", "us_y"]
TEST_NAME = "test"
TEST_NAME2 = "test2"
TEST_ID = "1"
TEST_ID2 = "2"
TEST_ENDPOINT = "www.test.com"
TEST_ERROR = "TestError"
TEST_PARAMS = {"test": "value"}
TEST_TRACE = "TEST TRACE"


@pytest.fixture(scope="class")
def client(request):
    app = Flask(__name__)
    app.register_blueprint(infer_release.infer_release_blueprint)
    # Include so that flask can get the url of `infer_release`.
    app.register_blueprint(batch_persistence.batch_blueprint)
    app.config["TESTING"] = True

    request.cls.client = app.test_client()


@pytest.fixture(autouse=True)
def project_id():
    with patch("recidiviz.utils.metadata.project_id") as mock_project:
        mock_project.return_value = "fake-project"
        yield mock_project


def create_mock_session():
    mock_session = Mock()
    mock_session.region = "test"
    mock_session.scrape_type = constants.ScrapeType.BACKGROUND
    session_start = datetime.datetime.now()
    mock_session.start = session_start

    return mock_session


# pylint: disable=protected-access


@pytest.mark.usefixtures("emulator")
class TestBatchPersistence(TestCase):
    """Tests for batch persistence logic"""

    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    def test_write_to_datastore(self, mock_session_return):
        mock_session = mock_session_return.return_value = create_mock_session()

        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)

        ii = ingest_info.IngestInfo()
        ii.create_person(full_name=TEST_NAME).create_booking(booking_id=TEST_ID)

        t = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )
        task_hash = hash(json.dumps(t.to_serializable(), sort_keys=True))

        expected_batch = BatchIngestInfoData(ingest_info=ii, task_hash=task_hash)

        batch_persistence.write(ii, scrape_key, t)

        batch_ingest_info_list = batch_persistence._get_batch_ingest_info_list(
            scrape_key.region_code, mock_session.start
        )

        self.assertEqual(len(batch_ingest_info_list), 1)
        self.assertEqual(expected_batch, batch_ingest_info_list[0])

    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    def test_write_to_multiple_datastore(self, mock_session_return):
        mock_session = mock_session_return.return_value = create_mock_session()
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)

        ii = ingest_info.IngestInfo()
        ii.create_person(full_name=TEST_NAME).create_booking(booking_id=TEST_ID)

        ii2 = ingest_info.IngestInfo()
        ii2.create_person(full_name=TEST_NAME2).create_booking(booking_id=TEST_ID)

        t = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )
        task_hash = hash(json.dumps(t.to_serializable(), sort_keys=True))

        t2 = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )
        task_hash2 = hash(json.dumps(t2.to_serializable(), sort_keys=True))

        expected_batches = [
            BatchIngestInfoData(ingest_info=ii, task_hash=task_hash),
            BatchIngestInfoData(ingest_info=ii2, task_hash=task_hash2),
        ]

        batch_persistence.write(ii, scrape_key, t)
        batch_persistence.write(ii2, scrape_key, t)

        batch_ingest_info_data_list = batch_persistence._get_batch_ingest_info_list(
            scrape_key.region_code, mock_session.start
        )
        self.assertEqual(len(batch_ingest_info_data_list), 2)
        self.assertCountEqual(expected_batches, batch_ingest_info_data_list)

    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    def test_write_error_to_datastore(self, mock_session_return):
        mock_session = mock_session_return.return_value = create_mock_session()
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)

        error = TEST_ERROR

        t = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )
        task_hash = hash(json.dumps(t.to_serializable(), sort_keys=True))

        expected_batch = BatchIngestInfoData(
            error=error, trace_id=TEST_TRACE, task_hash=task_hash
        )

        batch_persistence.write_error(error, TEST_TRACE, t, scrape_key)

        batch_ingest_info_list = batch_persistence._get_batch_ingest_info_list(
            scrape_key.region_code, mock_session.start
        )

        self.assertEqual(len(batch_ingest_info_list), 1)
        self.assertEqual(expected_batch, batch_ingest_info_list[0])

    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.persistence.persistence.write")
    def test_persist_to_db(self, mock_write, _mock_region, mock_session_return):
        mock_session = mock_session_return.return_value = create_mock_session()
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)

        ii = ingest_info.IngestInfo()
        ii.create_person(person_id=TEST_ID, full_name=TEST_NAME).create_booking(
            booking_id=TEST_ID
        )

        t = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )

        batch_persistence.write(ii, scrape_key, t)

        expected_proto = serialization.convert_ingest_info_to_proto(ii)

        batch_persistence.persist_to_database(
            scrape_key.region_code, mock_session.start
        )

        result_proto = mock_write.call_args[0][0]
        self.assertEqual(result_proto, expected_proto)

        # After we persist, there should no longer be ingest infos on Datastore
        ingest_infos = datastore_ingest_info.batch_get_ingest_infos_for_region(
            REGIONS[0], mock_session.start
        )
        self.assertEqual(len(ingest_infos), 0)

    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.persistence.persistence.write")
    def test_persist_to_db_multiple_tasks_one_write(
        self, mock_write, _mock_region, mock_session_return
    ):
        mock_session = mock_session_return.return_value = create_mock_session()
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)

        ii = ingest_info.IngestInfo()
        ii.create_person(person_id=TEST_ID, full_name=TEST_NAME).create_booking(
            booking_id=TEST_ID
        )
        ii2 = ingest_info.IngestInfo()
        ii2.create_person(person_id=TEST_ID, full_name=TEST_NAME).create_booking(
            booking_id=TEST_ID
        )

        t = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=TEST_ENDPOINT,
            response_type=constants.ResponseType.TEXT,
        )
        t2 = Task(
            endpoint=TEST_ENDPOINT,
            task_type=constants.TaskType.SCRAPE_DATA,
            response_type=constants.ResponseType.TEXT,
        )

        batch_persistence.write(ii, scrape_key, t)
        batch_persistence.write(ii2, scrape_key, t2)

        expected_proto = serialization.convert_ingest_info_to_proto(ii)
        batch_persistence.persist_to_database(
            scrape_key.region_code, mock_session.start
        )

        result_proto = mock_write.call_args[0][0]
        self.assertEqual(result_proto, expected_proto)

        # After we persist, there should no longer be ingest infos on Datastore
        ingest_infos = datastore_ingest_info.batch_get_ingest_infos_for_region(
            REGIONS[0], mock_session.start
        )
        self.assertEqual(len(ingest_infos), 0)

    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.persistence.persistence.write")
    def test_persist_to_db_failed_no_write(
        self, mock_write, _mock_region, mock_session_return
    ):
        mock_session = mock_session_return.return_value = create_mock_session()
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)

        ii = ingest_info.IngestInfo()
        ii.create_person(person_id=TEST_ID, full_name=TEST_NAME).create_booking(
            booking_id=TEST_ID
        )

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

        batch_persistence.write(ii, scrape_key, t)
        batch_persistence.write_error(TEST_ERROR, TEST_TRACE, t2, scrape_key)

        self.assertFalse(
            batch_persistence.persist_to_database(
                scrape_key.region_code, mock_session.start
            )
        )

        self.assertEqual(mock_write.call_count, 0)

        # We should still have both items still on Datastore because they
        # weren't persisted.
        batch_ingest_info_data_list = batch_persistence._get_batch_ingest_info_list(
            scrape_key.region_code, mock_session.start
        )
        self.assertEqual(len(batch_ingest_info_data_list), 2)

    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.persistence.persistence.write")
    def test_persist_to_db_same_task_one_fail_one_pass(
        self, mock_write, _mock_region, mock_session_return
    ):
        mock_session = mock_session_return.return_value = create_mock_session()
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)
        mock_write.return_value = True

        ii = ingest_info.IngestInfo()
        ii.create_person(person_id=TEST_ID, full_name=TEST_NAME).create_booking(
            booking_id=TEST_ID
        )

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

        batch_persistence.write(ii, scrape_key, t)
        batch_persistence.write_error(TEST_ERROR, TEST_TRACE, t2, scrape_key)

        expected_proto = serialization.convert_ingest_info_to_proto(ii)

        self.assertTrue(
            batch_persistence.persist_to_database(
                scrape_key.region_code, mock_session.start
            )
        )

        result_proto = mock_write.call_args[0][0]
        self.assertEqual(result_proto, expected_proto)

        ingest_infos = datastore_ingest_info.batch_get_ingest_infos_for_region(
            REGIONS[0], mock_session.start
        )
        self.assertEqual(len(ingest_infos), 0)

    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.persistence.persistence.write")
    def test_persist_to_db_different_regions(
        self, mock_write, _mock_region, mock_session_return
    ):
        scrape_key1 = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)
        scrape_key2 = ScrapeKey(REGIONS[1], constants.ScrapeType.BACKGROUND)

        ii = ingest_info.IngestInfo()
        ii.create_person(person_id=TEST_ID, full_name=TEST_NAME).create_booking(
            booking_id=TEST_ID
        )

        ii2 = ingest_info.IngestInfo()
        ii2.create_person(person_id=TEST_ID, full_name=TEST_NAME2).create_booking(
            booking_id=TEST_ID
        )

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

        mock_session_1 = mock_session_return.return_value = create_mock_session()

        batch_persistence.write(ii, scrape_key1, t)
        expected_proto = serialization.convert_ingest_info_to_proto(ii)
        batch_persistence.persist_to_database(
            scrape_key1.region_code, mock_session_1.start
        )

        result_proto = mock_write.call_args[0][0]
        self.assertEqual(result_proto, expected_proto)

        # We expect the region that we persisted to have no more ingest infos.
        ingest_infos_1 = datastore_ingest_info.batch_get_ingest_infos_for_region(
            REGIONS[0], mock_session_1.start
        )
        self.assertEqual(len(ingest_infos_1), 0)

        mock_session_2 = mock_session_return.return_value = create_mock_session()

        batch_persistence.write(ii2, scrape_key2, t2)
        ingest_infos_2 = datastore_ingest_info.batch_get_ingest_infos_for_region(
            REGIONS[1], mock_session_2.start
        )
        self.assertEqual(len(ingest_infos_2), 1)

        expected_proto = serialization.convert_ingest_info_to_proto(ii2)
        batch_persistence.persist_to_database(
            scrape_key2.region_code, mock_session_2.start
        )

        result_proto = mock_write.call_args[0][0]
        self.assertEqual(result_proto, expected_proto)

        self.assertEqual(mock_write.call_count, 2)

    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.persistence.persistence.write")
    def test_persist_duplicates_to_db(
        self, mock_write, _mock_region, mock_session_return
    ):
        """Tests that duplicate ingest_info.Person objects are merged before
        write."""
        mock_session = mock_session_return.return_value = create_mock_session()
        scrape_key = ScrapeKey(REGIONS[0], constants.ScrapeType.BACKGROUND)

        # Arrange
        ii = ingest_info.IngestInfo()
        ii.create_person(person_id=TEST_ID, full_name=TEST_NAME).create_booking(
            booking_id=TEST_ID
        )

        ii_2 = ingest_info.IngestInfo()
        ii.create_person(person_id=TEST_ID2, full_name=TEST_NAME2)

        ii_1_dup = copy.deepcopy(ii)

        t1, t2, t3 = (
            Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=TEST_ENDPOINT + str(i),
                response_type=constants.ResponseType.TEXT,
            )
            for i in range(3)
        )

        batch_persistence.write(ii, scrape_key, t1)
        batch_persistence.write(ii_2, scrape_key, t2)
        batch_persistence.write(ii_1_dup, scrape_key, t3)

        batch_persistence.persist_to_database(
            scrape_key.region_code, mock_session.start
        )

        expected_ii = ingest_info.IngestInfo(people=ii.people + ii_2.people)
        expected_proto = serialization.convert_ingest_info_to_proto(expected_ii)
        result_proto = mock_write.call_args[0][0]
        self.assertEqual(result_proto, expected_proto)


@pytest.mark.usefixtures("client")
class TestReadAndPersist(TestCase):
    """Tests read and persist"""

    @patch("recidiviz.utils.metadata.project_number", Mock(return_value="123456789"))
    @patch("recidiviz.persistence.batch_persistence.ScraperCloudTaskManager")
    @patch("recidiviz.ingest.scrape.sessions.get_most_recent_completed_session")
    @patch("recidiviz.ingest.scrape.sessions.update_phase")
    @patch("recidiviz.persistence.batch_persistence.persist_to_database")
    def test_read_and_persist(
        self, mock_persist, mock_session_update, mock_session_return, mock_task_manager
    ):
        mock_session = Mock()
        mock_session.region = "test"
        mock_session.scrape_type = constants.ScrapeType.BACKGROUND
        session_start = datetime.datetime.now()
        mock_session.start = session_start
        mock_session_return.return_value = mock_session

        request_args = {"region": "test"}
        headers = {"X-Appengine-Cron": "test-cron"}
        response = self.client.get(
            "/read_and_persist", query_string=request_args, headers=headers
        )
        self.assertEqual(response.status_code, 200)

        mock_persist.assert_called_once_with("test", session_start)
        mock_task_manager.return_value.create_scraper_phase_task.assert_called_once_with(
            region_code="test", url="/release"
        )
        mock_session_update.assert_called_once_with(
            mock_session, scrape_phase.ScrapePhase.RELEASE
        )
