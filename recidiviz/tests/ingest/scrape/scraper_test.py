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

"""Tests for ingest/scraper.py."""

import datetime
import unittest

import requests
from mock import Mock, patch
from requests.structures import CaseInsensitiveDict

from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape import constants, scrape_phase
from recidiviz.ingest.scrape.constants import BATCH_PUBSUB_TYPE
from recidiviz.ingest.scrape.scraper import FetchPageError, Scraper
from recidiviz.ingest.scrape.sessions import ScrapeSession
from recidiviz.ingest.scrape.task_params import QueueRequest, Task
from recidiviz.utils.regions import Region

_DATETIME = datetime.datetime(2018, 12, 6)


class TestAbstractScraper(unittest.TestCase):
    """Tests the abstract-ness of the Scraper base class."""

    def setUp(self) -> None:
        self.task_manager_patcher = patch(
            "recidiviz.ingest.scrape.scraper.ScraperCloudTaskManager"
        )
        self.task_manager_patcher.start()

    def tearDown(self) -> None:
        self.task_manager_patcher.stop()

    @patch("recidiviz.utils.regions.get_region")
    def test_init(self, mock_get_region: Mock) -> None:
        region = "us_nd"
        queue_name = "us-nd-scraper-v2"
        initial_task = "buy_it"

        mock_get_region.return_value = mock_region(region)
        scraper = FakeScraper(region, initial_task)

        mock_get_region.assert_called_with(region)
        assert scraper.region.region_code == region
        assert scraper.region.get_queue_name() == queue_name
        assert scraper.scraper_work_url == "/scraper/work/us_nd"


class TestStartScrape(unittest.TestCase):
    """Tests for the Scraper.start_scrape method."""

    @patch("recidiviz.ingest.scrape.scraper.datetime")
    @patch("recidiviz.ingest.scrape.scraper.ScraperCloudTaskManager")
    @patch("recidiviz.ingest.scrape.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.utils.pubsub_helper.create_topic_and_subscription")
    def test_start_scrape_background(
        self,
        mock_pubsub: Mock,
        mock_get_region: Mock,
        mock_tracker: Mock,
        mock_task_manager: Mock,
        mock_datetime: Mock,
    ) -> None:
        docket_item = ("Dog", "Cat")
        region = "us_nd"
        scrape_type = constants.ScrapeType.BACKGROUND
        queue_name = "us-nd-scraper-v2"
        initial_task = "use_it"

        mock_get_region.return_value = mock_region(region)
        mock_tracker.return_value = docket_item
        mock_task_manager.return_value.create_scrape_task.return_value = None
        mock_datetime.now.return_value = _DATETIME

        scraper = FakeScraper(region, initial_task)
        scraper.start_scrape(scrape_type)

        mock_get_region.assert_called_with(region)
        mock_tracker.assert_called_with(ScrapeKey(region, scrape_type))
        mock_pubsub.assert_called_with(
            ScrapeKey(region, scrape_type), BATCH_PUBSUB_TYPE
        )

        queue_params = QueueRequest(
            scrape_type=scrape_type,
            scraper_start_time=_DATETIME,
            next_task=FAKE_TASK,
            # content=docket_item,
        )
        request_body = {
            "region": region,
            "task": initial_task,
            "params": queue_params.to_serializable(),
        }

        mock_task_manager.return_value.create_scrape_task.assert_called_with(
            region_code=region,
            queue_name=queue_name,
            url=scraper.scraper_work_url,
            body=request_body,
        )

    @patch("recidiviz.ingest.scrape.scraper.ScraperCloudTaskManager")
    @patch("recidiviz.ingest.scrape.sessions.close_session")
    @patch("recidiviz.ingest.scrape.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.utils.pubsub_helper.create_topic_and_subscription")
    def test_start_scrape_no_docket_item(
        self,
        mock_pubsub: Mock,
        mock_get_region: Mock,
        mock_tracker: Mock,
        mock_sessions: Mock,
        mock_task_manager: Mock,
    ) -> None:
        region = "us_nd"
        scrape_type = constants.ScrapeType.BACKGROUND
        initial_task = "trash_it"

        mock_get_region.return_value = mock_region(region)
        mock_tracker.return_value = None
        mock_sessions.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.start_scrape(scrape_type)

        mock_get_region.assert_called_with(region)
        mock_tracker.assert_called_with(ScrapeKey(region, scrape_type))
        mock_sessions.assert_called_with(ScrapeKey(region, scrape_type))
        mock_pubsub.assert_called_with(
            ScrapeKey(region, scrape_type), BATCH_PUBSUB_TYPE
        )
        mock_task_manager.return_value.create_scrape_task.assert_not_called()


class TestStopScraper(unittest.TestCase):
    """Tests for the Scraper.stop_scrape method."""

    @patch("recidiviz.ingest.scrape.scraper.ScraperCloudTaskManager")
    @patch("recidiviz.ingest.scrape.sessions.get_sessions")
    @patch("recidiviz.utils.regions.get_region")
    def test_stop_scrape(
        self, mock_get_region: Mock, mock_sessions: Mock, mock_task_manager: Mock
    ) -> None:
        region = "us_sd"
        scrape_type = constants.ScrapeType.BACKGROUND
        queue_name = "us-sd-scraper-v2"
        initial_task = "change_it"

        mock_get_region.return_value = mock_region(region, is_stoppable=True)
        open_session = ScrapeSession.new(
            key=None,
            scrape_type=scrape_type,
            region=region,
            phase=scrape_phase.ScrapePhase.SCRAPE,
        )
        mock_sessions.return_value = [open_session]
        mock_task_manager.return_value.purge_scrape_tasks.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.stop_scrape(scrape_type)

        mock_get_region.assert_called_with(region)
        mock_sessions.assert_called_with(region, include_closed=False)
        mock_task_manager.return_value.purge_scrape_tasks.assert_called_with(
            region_code=region, queue_name=queue_name
        )

    @patch("recidiviz.ingest.scrape.scraper.ScraperCloudTaskManager")
    @patch("recidiviz.ingest.scrape.sessions.get_sessions")
    @patch("recidiviz.utils.regions.get_region")
    def test_stop_scrape_not_executed(
        self, mock_get_region: Mock, mock_sessions: Mock, mock_task_manager: Mock
    ) -> None:
        region = "us_sd"
        scrape_type = constants.ScrapeType.BACKGROUND
        initial_task = "change_it"

        mock_get_region.return_value = mock_region(region, is_stoppable=False)
        open_session = ScrapeSession.new(
            key=None,
            scrape_type=scrape_type,
            region=region,
            phase=scrape_phase.ScrapePhase.SCRAPE,
        )
        mock_sessions.return_value = [open_session]
        mock_task_manager.return_value.purge_scrape_tasks.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.stop_scrape([scrape_type], respect_is_stoppable=True)

        mock_get_region.assert_called_with(region)
        mock_sessions.assert_not_called()
        mock_task_manager.return_value.purge_scrape_tasks.assert_not_called()

    @patch("recidiviz.ingest.scrape.scraper.ScraperCloudTaskManager")
    @patch("recidiviz.ingest.scrape.sessions.get_sessions")
    @patch("recidiviz.utils.regions.get_region")
    @patch.object(Scraper, "resume_scrape")
    def test_stop_scrape_resume_other_scrapes(
        self,
        mock_resume: Mock,
        mock_get_region: Mock,
        mock_sessions: Mock,
        mock_task_manager: Mock,
    ) -> None:
        """Tests that the stop_scrape method will launch other scrape types we
        didn't mean to stop."""
        region = "us_sd"
        scrape_type = constants.ScrapeType.BACKGROUND
        queue_name = "us-sd-scraper-v2"
        initial_task = "mail_upgrade_it"

        mock_get_region.return_value = mock_region(region, is_stoppable=True)
        open_session_other = ScrapeSession.new(
            key=None,
            scrape_type=constants.ScrapeType.SNAPSHOT,
            phase=scrape_phase.ScrapePhase.SCRAPE,
        )
        open_session_matching = ScrapeSession.new(
            key=None,
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.SCRAPE,
        )
        mock_sessions.return_value = [open_session_other, open_session_matching]
        mock_task_manager.return_value.purge_scrape_tasks.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.stop_scrape(scrape_type)

        mock_get_region.assert_called_with(region)
        mock_sessions.assert_called_with(region, include_closed=False)
        mock_resume.assert_called_with(constants.ScrapeType.SNAPSHOT)
        mock_task_manager.return_value.purge_scrape_tasks.assert_called_with(
            region_code=region, queue_name=queue_name
        )


class TestResumeScrape(unittest.TestCase):
    """Tests for the Scraper.resume_scrape method."""

    @patch("recidiviz.ingest.scrape.scraper.datetime")
    @patch("recidiviz.ingest.scrape.scraper.ScraperCloudTaskManager")
    @patch("recidiviz.ingest.scrape.sessions.get_recent_sessions")
    @patch("recidiviz.utils.regions.get_region")
    def test_resume_scrape_background(
        self,
        mock_get_region: Mock,
        mock_sessions: Mock,
        mock_task_manager: Mock,
        mock_datetime: Mock,
    ) -> None:
        """Tests the resume_scrape flow for background scraping."""
        region = "us_nd"
        scrape_type = constants.ScrapeType.BACKGROUND
        queue_name = "us-nd-scraper-v2"
        initial_task = "charge_it"

        mock_get_region.return_value = mock_region(region)
        recent_session_none_scraped = ScrapeSession.new(
            key=None,
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.SCRAPE,
        )
        recent_session = ScrapeSession.new(
            key=None,
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.SCRAPE,
            last_scraped="Bangalter, Thomas",
        )
        mock_sessions.return_value = [recent_session_none_scraped, recent_session]
        mock_task_manager.return_value.create_scrape_task.return_value = None
        mock_datetime.now.return_value = _DATETIME

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_get_region.assert_called_with(region)
        mock_sessions.assert_called_with(ScrapeKey(region, scrape_type))

        queue_params = QueueRequest(
            scrape_type=scrape_type,
            scraper_start_time=_DATETIME,
            next_task=FAKE_TASK,
            # content=['Bangalter', 'Thomas'],
        )
        request_body = {
            "region": region,
            "task": initial_task,
            "params": queue_params.to_serializable(),
        }

        mock_task_manager.return_value.create_scrape_task.assert_called_with(
            region_code=region,
            queue_name=queue_name,
            url=scraper.scraper_work_url,
            body=request_body,
        )

    @patch("recidiviz.ingest.scrape.scraper.ScraperCloudTaskManager")
    @patch("recidiviz.ingest.scrape.sessions.get_recent_sessions")
    @patch("recidiviz.utils.regions.get_region")
    def test_resume_scrape_background_none_scraped(
        self, mock_get_region: Mock, mock_sessions: Mock, mock_task_manager: Mock
    ) -> None:
        region = "us_nd"
        scrape_type = constants.ScrapeType.BACKGROUND
        initial_task = "point_it"

        mock_get_region.return_value = mock_region(region)
        recent_session_none_scraped = ScrapeSession.new(
            key=None,
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.SCRAPE,
        )

        mock_sessions.return_value = [recent_session_none_scraped]

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_get_region.assert_called_with(region)
        mock_sessions.assert_called_with(ScrapeKey(region, scrape_type))
        mock_task_manager.return_value.create_scrape_task.assert_not_called()

    @patch("recidiviz.ingest.scrape.scraper.ScraperCloudTaskManager")
    @patch("recidiviz.ingest.scrape.sessions.get_recent_sessions")
    @patch("recidiviz.utils.regions.get_region")
    def test_resume_scrape_background_no_recent_sessions(
        self, mock_get_region: Mock, mock_sessions: Mock, mock_task_manager: Mock
    ) -> None:
        region = "us_nd"
        scrape_type = constants.ScrapeType.BACKGROUND
        initial_task = "zoom_it"

        mock_get_region.return_value = mock_region(region)
        mock_sessions.return_value = []

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_get_region.assert_called_with(region)
        mock_sessions.assert_called_with(ScrapeKey(region, scrape_type))
        mock_task_manager.return_value.create_scrape_task.assert_not_called()

    @patch("recidiviz.ingest.scrape.scraper.datetime")
    @patch("recidiviz.ingest.scrape.scraper.ScraperCloudTaskManager")
    @patch("recidiviz.ingest.scrape.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.get_region")
    def test_resume_scrape_snapshot(
        self,
        mock_get_region: Mock,
        mock_tracker: Mock,
        mock_task_manager: Mock,
        mock_datetime: Mock,
    ) -> None:
        docket_item = (41620, ["daft", "punk"])
        region = "us_nd"
        scrape_type = constants.ScrapeType.SNAPSHOT
        queue_name = "us-nd-scraper-v2"
        initial_task = "press_it"

        mock_get_region.return_value = mock_region(region)
        mock_tracker.return_value = docket_item
        mock_task_manager.return_value.create_scrape_task.return_value = None
        mock_datetime.now.return_value = _DATETIME

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_get_region.assert_called_with(region)

        queue_params = QueueRequest(
            scrape_type=scrape_type,
            scraper_start_time=_DATETIME,
            next_task=FAKE_TASK,
            # content=(83240, ['dagt', 'punk']),
        )
        request_body = {
            "region": region,
            "task": initial_task,
            "params": queue_params.to_serializable(),
        }

        mock_task_manager.return_value.create_scrape_task.assert_called_with(
            region_code=region,
            queue_name=queue_name,
            url=scraper.scraper_work_url,
            body=request_body,
        )

    @patch("recidiviz.ingest.scrape.scraper.ScraperCloudTaskManager")
    @patch("recidiviz.ingest.scrape.sessions.close_session")
    @patch("recidiviz.ingest.scrape.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.get_region")
    def test_resume_scrape_snapshot_no_docket_item(
        self,
        mock_get_region: Mock,
        mock_tracker: Mock,
        mock_close_session: Mock,
        mock_task_manager: Mock,
    ) -> None:
        region = "us_nd"
        scrape_type = constants.ScrapeType.SNAPSHOT
        initial_task = "snap_it"

        mock_get_region.return_value = mock_region(region)
        mock_tracker.return_value = None
        mock_close_session.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_get_region.assert_called_with(region)
        mock_tracker.assert_called_with(ScrapeKey(region, scrape_type))
        mock_close_session.assert_called_with(ScrapeKey(region, scrape_type))
        mock_task_manager.return_value.create_scrape_task.assert_not_called()


class TestFetchPage(unittest.TestCase):
    """Tests for the Scraper.fetch_page method."""

    def setUp(self) -> None:
        self.task_manager_patcher = patch(
            "recidiviz.ingest.scrape.scraper.ScraperCloudTaskManager"
        )
        self.task_manager_patcher.start()

    def tearDown(self) -> None:
        self.task_manager_patcher.stop()

    @patch("recidiviz.ingest.scrape.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scrape.scraper_utils.get_proxies")
    @patch("recidiviz.utils.regions.get_region")
    def test_fetch_page(
        self, mock_get_region: Mock, mock_proxies: Mock, mock_headers: Mock
    ) -> None:
        """Tests that fetch_page returns the fetched data payload."""
        url = "/around/the/world"
        region = "us_sd"
        initial_task = "work_it"

        mock_get_region.return_value = mock_region(region)
        proxies = {"http": "http://user:password@proxy.biz/"}
        mock_proxies.return_value = proxies
        headers = {"User-Agent": "test_user_agent"}
        mock_headers.return_value = headers

        scraper = FakeScraper(region, initial_task)
        page = "<blink>Get in on the ground floor</blink>"
        response = requests.Response()
        response._content = str.encode(page)  # pylint: disable=protected-access
        response.status_code = 200
        with patch("requests.get", return_value=response) as p:
            assert scraper.fetch_page(url).content == str.encode(page)
            p.assert_called_with(
                url,
                proxies=proxies,
                headers=headers,
                cookies=None,
                params=None,
                verify=False,
            )

        mock_get_region.assert_called_with(region)
        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()

    @patch("requests.post")
    @patch("recidiviz.ingest.scrape.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scrape.scraper_utils.get_proxies")
    @patch("recidiviz.utils.regions.get_region")
    def test_fetch_page_post(
        self,
        mock_get_region: Mock,
        mock_proxies: Mock,
        mock_headers: Mock,
        mock_requests: Mock,
    ) -> None:
        """Tests that fetch_page returns the fetched data payload returned
        from post requests."""
        url = "/around/the/world"
        body = {"foo": "bar"}
        json_data = {"far": "boo"}
        region = "us_sd"
        initial_task = "work_it"

        mock_get_region.return_value = mock_region(region)
        proxies = {"http": "http://user:password@proxy.biz/"}
        mock_proxies.return_value = proxies
        headers = {"User-Agent": "test_user_agent"}
        mock_headers.return_value = headers
        page = "<blink>Get in on the ground floor</blink>"
        response = requests.Response()
        response._content = str.encode(page)  # pylint: disable=protected-access
        response.status_code = 200
        mock_requests.return_value = response

        scraper = FakeScraper(region, initial_task)
        assert scraper.fetch_page(
            url, post_data=body, json_data=json_data
        ).content == str.encode(page)

        mock_get_region.assert_called_with(region)
        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()
        mock_requests.assert_called_with(
            url,
            proxies=proxies,
            headers=headers,
            cookies=None,
            data=body,
            json=json_data,
            verify=False,
        )

    @patch("requests.get")
    @patch("recidiviz.ingest.scrape.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scrape.scraper_utils.get_proxies")
    @patch("recidiviz.utils.regions.get_region")
    def test_fetch_page_error(
        self,
        mock_get_region: Mock,
        mock_proxies: Mock,
        mock_headers: Mock,
        mock_requests: Mock,
    ) -> None:
        """Tests that fetch_page successfully handles error responses."""
        url = "/around/the/world"
        region = "us_sd"
        initial_task = "work_it"

        mock_get_region.return_value = mock_region(region)
        proxies = {"http": "http://user:password@proxy.biz/"}
        mock_proxies.return_value = proxies
        headers = CaseInsensitiveDict({"User-Agent": "test_user_agent"})
        mock_headers.return_value = headers

        original_request = requests.PreparedRequest()
        original_request.headers = headers
        original_request.method = "GET"
        original_request.body = None

        # test a few types of errors
        errors = {
            500: "SERVER ERROR",
            502: "PROXY ERROR",
            503: "SERVICE UNAVAILABLE",
        }

        error_response = requests.Response()
        error_response.headers = CaseInsensitiveDict()
        error_response.request = original_request
        for code, name in errors.items():
            error_response.status_code = code
            error_response.reason = name
            mock_requests.return_value = error_response

            scraper = FakeScraper(region, initial_task)
            with self.assertRaises(FetchPageError):
                scraper.fetch_page(url)

            mock_get_region.assert_called_with(region)
            mock_proxies.assert_called_with()
            mock_headers.assert_called_with()
            mock_requests.assert_called_with(
                url,
                proxies=proxies,
                headers=headers,
                cookies=None,
                params=None,
                verify=False,
            )


def mock_region(region_code: str, is_stoppable: bool = False) -> Region:
    return Region(
        region_code=region_code,
        agency_name="the agency",
        agency_type="benevolent",
        base_url="localhost:3000",
        names_file="names.txt",
        timezone="America/New_York",
        environment="production",
        jurisdiction_id="00000000",  # must be 8 character numeric string
        is_stoppable=is_stoppable or False,
    )


FAKE_TASK = Task(task_type=constants.TaskType.INITIAL, endpoint="fake")


class FakeScraper(Scraper):
    def __init__(self, region_name: str, initial_task_method: str):
        super().__init__(region_name)
        self.initial_task_method = initial_task_method

    def get_initial_task_method(self) -> str:
        return self.initial_task_method

    def get_initial_task(self) -> Task:
        return FAKE_TASK
