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

"""Tests for base_scraper.py."""
import datetime
from typing import Any, Callable, List
from unittest import TestCase

import flask
from mock import Mock, patch

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.ingest_metadata import (
    LegacyStateAndJailsIngestMetadata,
    SystemLevel,
)
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.models.serialization import convert_ingest_info_to_proto
from recidiviz.ingest.scrape import constants
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.scrape.errors import ScraperFetchError, ScraperGetMoreTasksError
from recidiviz.ingest.scrape.task_params import QueueRequest, ScrapedData, Task
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey

TEST_TASK = Task(
    task_type=constants.TaskType.GET_MORE_TASKS,
    endpoint="TEST",
    response_type=constants.ResponseType.HTML,
)
TEST_HTML = "<html><body>test</body></html>"


class FakeScraper(BaseScraper):
    def __init__(self, region_name: str):  # pylint: disable=super-init-not-called
        self.region = Mock()
        self.region.region_code = region_name
        self.tasks: List[QueueRequest] = []

    def populate_data(
        self, content: Any, task: Task, ingest_info: IngestInfo
    ) -> ScrapedData:
        return super().populate_data(content, task, ingest_info)

    def add_task(self, task_name: str, request: QueueRequest) -> None:
        self.tasks.append(request)

    def get_enum_overrides(self) -> EnumOverrides:
        return EnumOverrides.empty()


# pylint: disable=protected-access


class TestBaseScraper(TestCase):
    """Tests for base_scraper"""

    def setup_method(self, _: Callable) -> None:
        ii = IngestInfo()
        person = ii.create_person(person_id="test")
        booking = person.create_booking(booking_id="test")
        booking.booking_id = "test"
        self.ii = ii

    @patch.object(BaseScraper, "_fetch_content")
    @patch.object(BaseScraper, "get_more_tasks")
    def test_get_more_tasks_failure(
        self, mock_get_more: Mock, mock_fetch: Mock
    ) -> None:
        mock_fetch.return_value = ("TEST", {})
        mock_get_more.side_effect = ValueError("TEST ERROR")

        req = QueueRequest(
            scrape_type=constants.ScrapeType.BACKGROUND,
            next_task=TEST_TASK,
            scraper_start_time=datetime.datetime.now(),
        )
        scraper = FakeScraper("test")
        scraper.BATCH_WRITES = False
        with self.assertRaises(ScraperGetMoreTasksError):
            scraper._generic_scrape(req)

    @patch("recidiviz.persistence.batch_persistence.write_error")
    @patch.object(BaseScraper, "_fetch_content")
    @patch.object(BaseScraper, "get_more_tasks")
    @patch.object(flask, "request")
    def test_get_more_tasks_failure_batch(
        self,
        mock_flask: Mock,
        mock_get_more: Mock,
        mock_fetch: Mock,
        mock_batch_error: Mock,
    ) -> None:
        mock_fetch.return_value = ("TEST", {})
        mock_get_more.side_effect = ValueError("TEST ERROR")
        mock_flask_get = Mock()
        mock_flask_get.return_value = "TRACE ID"
        mock_flask.headers.get = mock_flask_get

        start_time = datetime.datetime.now()
        req = QueueRequest(
            scrape_type=constants.ScrapeType.BACKGROUND,
            next_task=TEST_TASK,
            scraper_start_time=start_time,
        )
        scraper = FakeScraper("test")
        with self.assertRaises(ScraperGetMoreTasksError):
            scraper._generic_scrape(req)
        self.assertEqual(mock_batch_error.call_count, 1)

        scrape_key = ScrapeKey(
            region_code="test", scrape_type=constants.ScrapeType.BACKGROUND
        )
        mock_batch_error.assert_called_once_with(
            error="TEST ERROR",
            trace_id="TRACE ID",
            task=TEST_TASK,
            scrape_key=scrape_key,
        )

    @patch.object(BaseScraper, "_fetch_content")
    def test_fetch_failure(self, mock_fetch: Mock) -> None:
        mock_fetch.return_value = ValueError("TEST ERROR")

        req = QueueRequest(
            scrape_type=constants.ScrapeType.BACKGROUND,
            next_task=TEST_TASK,
            scraper_start_time=datetime.datetime.now(),
        )
        scraper = FakeScraper("test")
        scraper.BATCH_WRITES = False
        with self.assertRaises(ScraperFetchError):
            scraper._generic_scrape(req)

    @patch.object(BaseScraper, "_fetch_content")
    @patch.object(BaseScraper, "get_more_tasks")
    def test_content_no_fetch(self, mock_get_more: Mock, mock_fetch: Mock) -> None:
        t = Task.evolve(TEST_TASK, content=TEST_HTML)
        mock_get_more.return_value = [t]
        start_time = datetime.datetime.now()
        req = QueueRequest(
            scrape_type=constants.ScrapeType.BACKGROUND,
            next_task=t,
            scraper_start_time=start_time,
        )
        scraper = FakeScraper("test")
        scraper.BATCH_WRITES = False
        scraper._generic_scrape(req)

        expected_tasks = [req]

        self.assertEqual(mock_fetch.call_count, 0)
        self.assertCountEqual(expected_tasks, scraper.tasks)

    @patch.object(BaseScraper, "_fetch_content")
    @patch.object(BaseScraper, "get_more_tasks")
    def test_get_more_and_updates_cookies(
        self, mock_get_more: Mock, mock_fetch: Mock
    ) -> None:
        mock_get_more.return_value = [TEST_TASK]
        mock_fetch.return_value = (TEST_HTML, {1: 1})
        start_time = datetime.datetime.now()
        req = QueueRequest(
            scrape_type=constants.ScrapeType.BACKGROUND,
            next_task=TEST_TASK,
            scraper_start_time=start_time,
        )

        t = Task.evolve(TEST_TASK, cookies={1: 1})

        scraper = FakeScraper("test")
        scraper.BATCH_WRITES = False
        scraper._generic_scrape(req)

        expected_tasks = [
            QueueRequest(
                scrape_type=constants.ScrapeType.BACKGROUND,
                next_task=t,
                scraper_start_time=start_time,
            )
        ]

        self.assertCountEqual(expected_tasks, scraper.tasks)

    @patch.object(BaseScraper, "_fetch_content")
    @patch.object(BaseScraper, "get_more_tasks")
    def test_get_more_multiple_tasks_returned(
        self, mock_get_more: Mock, mock_fetch: Mock
    ) -> None:
        mock_get_more.return_value = [TEST_TASK, TEST_TASK]
        mock_fetch.return_value = (TEST_HTML, None)
        start_time = datetime.datetime.now()
        req = QueueRequest(
            scrape_type=constants.ScrapeType.BACKGROUND,
            next_task=TEST_TASK,
            scraper_start_time=start_time,
        )

        scraper = FakeScraper("test")
        scraper.BATCH_WRITES = False
        scraper._generic_scrape(req)

        expected_tasks = [
            QueueRequest(
                scrape_type=constants.ScrapeType.BACKGROUND,
                next_task=TEST_TASK,
                scraper_start_time=start_time,
            )
        ] * 2

        self.assertCountEqual(expected_tasks, scraper.tasks)

    @patch.object(BaseScraper, "_fetch_content")
    @patch.object(BaseScraper, "get_more_tasks")
    def test_fetch_sends_all_args(self, mock_get_more: Mock, mock_fetch: Mock) -> None:
        mock_get_more.return_value = [TEST_TASK]
        mock_fetch.return_value = (TEST_HTML, None)
        start_time = datetime.datetime.now()
        t = Task.evolve(
            TEST_TASK,
            headers="TEST_HEADERS",
            cookies="TEST_COOKIES",
            params="TEST_PARAMS",
            post_data="TEST_POST",
            json="TEST_JSON",
        )
        req = QueueRequest(
            scrape_type=constants.ScrapeType.BACKGROUND,
            next_task=t,
            scraper_start_time=start_time,
        )

        scraper = FakeScraper("test")
        scraper.BATCH_WRITES = False
        scraper._generic_scrape(req)

        expected_tasks = [
            QueueRequest(
                scrape_type=constants.ScrapeType.BACKGROUND,
                next_task=TEST_TASK,
                scraper_start_time=start_time,
            )
        ]

        mock_fetch.assert_called_once_with(
            t.endpoint,
            t.response_type,
            headers=t.headers,
            cookies=t.cookies,
            params=t.params,
            post_data=t.post_data,
            json_data=t.json,
        )
        self.assertCountEqual(expected_tasks, scraper.tasks)

    @patch("recidiviz.persistence.persistence.write_ingest_info")
    @patch.object(BaseScraper, "populate_data")
    @patch.object(BaseScraper, "_fetch_content")
    @patch.object(BaseScraper, "get_more_tasks")
    def test_scrape_data_and_more_no_persist(
        self,
        mock_get_more: Mock,
        mock_fetch: Mock,
        mock_populate: Mock,
        mock_write: Mock,
    ) -> None:
        mock_get_more.return_value = [TEST_TASK]
        mock_fetch.return_value = (TEST_HTML, {})
        mock_populate.return_value = ScrapedData(
            ingest_info=self.ii,
            persist=False,
        )
        start_time = datetime.datetime.now()
        t = Task.evolve(TEST_TASK, task_type=constants.TaskType.SCRAPE_DATA_AND_MORE)
        req = QueueRequest(
            scrape_type=constants.ScrapeType.BACKGROUND,
            next_task=t,
            scraper_start_time=start_time,
        )

        scraper = FakeScraper("test")
        scraper.BATCH_WRITES = False
        scraper._generic_scrape(req)

        # Should send the ii since we chose not to persist.
        expected_tasks = [
            QueueRequest(
                scrape_type=constants.ScrapeType.BACKGROUND,
                next_task=TEST_TASK,
                scraper_start_time=start_time,
                ingest_info=self.ii,
            )
        ]

        self.assertEqual(mock_get_more.call_count, 1)
        self.assertEqual(mock_populate.call_count, 1)
        self.assertEqual(mock_write.call_count, 0)
        mock_get_more.assert_called_once_with(TEST_HTML, t)
        self.assertCountEqual(expected_tasks, scraper.tasks)

    @patch("recidiviz.persistence.persistence.write_ingest_info")
    @patch.object(BaseScraper, "populate_data")
    @patch.object(BaseScraper, "_fetch_content")
    @patch.object(BaseScraper, "get_more_tasks")
    def test_scrape_data_and_more_no_persist_second_time_persist(
        self,
        mock_get_more: Mock,
        mock_fetch: Mock,
        mock_populate: Mock,
        mock_write: Mock,
    ) -> None:
        populate_task = Task.evolve(TEST_TASK, task_type=constants.TaskType.SCRAPE_DATA)
        mock_get_more.return_value = [populate_task]
        mock_fetch.return_value = (TEST_HTML, {})
        mock_populate.return_value = ScrapedData(
            ingest_info=self.ii,
            persist=False,
        )
        start_time = datetime.datetime.now()
        t = Task.evolve(TEST_TASK, task_type=constants.TaskType.SCRAPE_DATA_AND_MORE)
        req = QueueRequest(
            scrape_type=constants.ScrapeType.BACKGROUND,
            next_task=t,
            scraper_start_time=start_time,
        )

        scraper = FakeScraper("test")
        scraper.BATCH_WRITES = False
        scraper._generic_scrape(req)

        # Should send the ii since we chose not to persist.
        expected_tasks = [
            QueueRequest(
                scrape_type=constants.ScrapeType.BACKGROUND,
                next_task=populate_task,
                scraper_start_time=start_time,
                ingest_info=self.ii,
            )
        ]

        self.assertEqual(mock_get_more.call_count, 1)
        self.assertEqual(mock_populate.call_count, 1)
        self.assertEqual(mock_write.call_count, 0)
        mock_get_more.assert_called_once_with(TEST_HTML, t)
        self.assertCountEqual(expected_tasks, scraper.tasks)

        mock_populate.return_value = ScrapedData(
            ingest_info=self.ii,
            persist=True,
        )
        scraper._generic_scrape(scraper.tasks[0])
        self.assertEqual(mock_get_more.call_count, 1)
        self.assertEqual(mock_populate.call_count, 2)
        self.assertEqual(mock_write.call_count, 1)

        expected_metadata = LegacyStateAndJailsIngestMetadata(
            region=scraper.region.region_code,
            jurisdiction_id=scraper.region.jurisdiction_id,
            ingest_time=start_time,
            enum_overrides=scraper.get_enum_overrides(),
            system_level=SystemLevel.COUNTY,
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS),
        )
        expected_proto = convert_ingest_info_to_proto(self.ii)
        mock_write.assert_called_once_with(expected_proto, expected_metadata)

    @patch("recidiviz.persistence.persistence.write_ingest_info")
    @patch.object(BaseScraper, "populate_data")
    @patch.object(BaseScraper, "_fetch_content")
    @patch.object(BaseScraper, "get_more_tasks")
    def test_scrape_data_and_more_yes_persist(
        self,
        mock_get_more: Mock,
        mock_fetch: Mock,
        mock_populate: Mock,
        mock_write: Mock,
    ) -> None:
        mock_get_more.return_value = [TEST_TASK]
        mock_fetch.return_value = (TEST_HTML, {})
        mock_populate.return_value = ScrapedData(
            ingest_info=self.ii,
            persist=True,
        )
        start_time = datetime.datetime.now()
        t = Task.evolve(TEST_TASK, task_type=constants.TaskType.SCRAPE_DATA_AND_MORE)
        req = QueueRequest(
            scrape_type=constants.ScrapeType.BACKGROUND,
            next_task=t,
            scraper_start_time=start_time,
        )

        scraper = FakeScraper("test")
        scraper.BATCH_WRITES = False
        scraper._generic_scrape(req)

        # Should send the ii since we chose not to persist.
        expected_tasks = [
            QueueRequest(
                scrape_type=constants.ScrapeType.BACKGROUND,
                next_task=TEST_TASK,
                scraper_start_time=start_time,
            )
        ]
        expected_metadata = LegacyStateAndJailsIngestMetadata(
            region=scraper.region.region_code,
            jurisdiction_id=scraper.region.jurisdiction_id,
            ingest_time=start_time,
            enum_overrides=scraper.get_enum_overrides(),
            system_level=SystemLevel.COUNTY,
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS),
        )
        expected_proto = convert_ingest_info_to_proto(self.ii)

        self.assertEqual(mock_get_more.call_count, 1)
        self.assertEqual(mock_populate.call_count, 1)
        self.assertEqual(mock_write.call_count, 1)
        mock_write.assert_called_once_with(expected_proto, expected_metadata)
        self.assertCountEqual(expected_tasks, scraper.tasks)

    @patch("recidiviz.persistence.persistence.write_ingest_info")
    @patch.object(BaseScraper, "populate_data")
    @patch.object(BaseScraper, "_fetch_content")
    @patch.object(BaseScraper, "get_more_tasks")
    def test_scrape_data_no_more_tasks(
        self,
        mock_get_more: Mock,
        mock_fetch: Mock,
        mock_populate: Mock,
        mock_write: Mock,
    ) -> None:
        mock_fetch.return_value = (TEST_HTML, {})
        mock_populate.return_value = ScrapedData(
            ingest_info=self.ii,
            persist=True,
        )
        start_time = datetime.datetime.now()
        t = Task.evolve(TEST_TASK, task_type=constants.TaskType.SCRAPE_DATA)
        req = QueueRequest(
            scrape_type=constants.ScrapeType.BACKGROUND,
            next_task=t,
            scraper_start_time=start_time,
        )

        scraper = FakeScraper("test")
        scraper.BATCH_WRITES = False
        scraper._generic_scrape(req)

        expected_metadata = LegacyStateAndJailsIngestMetadata(
            region=scraper.region.region_code,
            jurisdiction_id=scraper.region.jurisdiction_id,
            ingest_time=start_time,
            enum_overrides=scraper.get_enum_overrides(),
            system_level=SystemLevel.COUNTY,
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.JAILS),
        )
        expected_proto = convert_ingest_info_to_proto(self.ii)

        self.assertEqual(mock_get_more.call_count, 0)
        self.assertEqual(mock_populate.call_count, 1)
        self.assertEqual(mock_write.call_count, 1)
        mock_write.assert_called_once_with(expected_proto, expected_metadata)
        self.assertEqual(len(scraper.tasks), 0)

    @patch("recidiviz.persistence.batch_persistence.write")
    @patch("recidiviz.persistence.persistence.write_ingest_info")
    @patch.object(BaseScraper, "populate_data")
    @patch.object(BaseScraper, "_fetch_content")
    @patch.object(BaseScraper, "get_more_tasks")
    def test_scrape_data_no_more_tasks_batch(
        self,
        mock_get_more: Mock,
        mock_fetch: Mock,
        mock_populate: Mock,
        mock_write: Mock,
        mock_batch_write: Mock,
    ) -> None:
        mock_fetch.return_value = (TEST_HTML, {})
        mock_populate.return_value = ScrapedData(
            ingest_info=self.ii,
            persist=True,
        )
        start_time = datetime.datetime.now()
        t = Task.evolve(TEST_TASK, task_type=constants.TaskType.SCRAPE_DATA)
        req = QueueRequest(
            scrape_type=constants.ScrapeType.BACKGROUND,
            next_task=t,
            scraper_start_time=start_time,
        )

        scraper = FakeScraper("test")
        scraper._generic_scrape(req)

        scrape_key = ScrapeKey("test", constants.ScrapeType.BACKGROUND)
        self.assertEqual(mock_get_more.call_count, 0)
        self.assertEqual(mock_populate.call_count, 1)
        self.assertEqual(mock_write.call_count, 0)
        mock_batch_write.assert_called_once_with(
            ingest_info=self.ii,
            task=t,
            scrape_key=scrape_key,
        )
        self.assertEqual(len(scraper.tasks), 0)
