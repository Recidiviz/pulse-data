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

"""Tests for ingest/scraper.py."""

import datetime

import requests
from mock import patch

from recidiviz.ingest.scrape import constants
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape.scraper import Scraper
from recidiviz.ingest.scrape.sessions import ScrapeSession
from recidiviz.ingest.scrape.task_params import QueueRequest, Task

_DATETIME = datetime.datetime(2018, 12, 6)


class TestAbstractScraper:
    """Tests the abstract-ness of the Scraper base class."""

    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_init(self, mock_region):
        region = "us_nd"
        queue_name = "us_nd_scraper"
        initial_task = "buy_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        scraper = FakeScraper(region, initial_task)

        mock_region.assert_called_with(region)
        assert scraper.region.region_code == region
        assert scraper.region.queue == queue_name
        assert scraper.fail_counter == "us_nd_next_page_fail_counter"
        assert scraper.scraper_work_url == "/scraper/work"


class TestStartScrape:
    """Tests for the Scraper.start_scrape method."""

    @patch('recidiviz.ingest.scrape.scraper.datetime')
    @patch("recidiviz.ingest.scrape.queues.create_task")
    @patch("recidiviz.ingest.scrape.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_start_scrape_background(
            self, mock_region, mock_tracker, mock_create_task, mock_datetime):
        docket_item = ("Dog", "Cat")
        region = "us_nd"
        scrape_type = constants.ScrapeType.BACKGROUND
        queue_name = "us_nd_scraper"
        initial_task = "use_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        mock_tracker.return_value = docket_item
        mock_create_task.return_value = None
        mock_datetime.now.return_value = _DATETIME

        scraper = FakeScraper(region, initial_task)
        scraper.start_scrape(scrape_type)

        mock_region.assert_called_with(region)
        mock_tracker.assert_called_with(ScrapeKey(region, scrape_type))

        queue_params = QueueRequest(
            scrape_type=scrape_type.value,
            scraper_start_time=_DATETIME,
            next_task=FAKE_TASK,
            # content=docket_item,
        )
        request_body = {
            'region': region,
            'task': initial_task,
            'params': queue_params.to_serializable()
        }

        mock_create_task.assert_called_with(
            url=scraper.scraper_work_url,
            queue_name=queue_name,
            body=request_body)

    @patch('recidiviz.ingest.scrape.scraper.datetime')
    @patch("recidiviz.ingest.scrape.queues.create_task")
    @patch("recidiviz.ingest.scrape.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_start_scrape_snapshot(self, mock_region, mock_tracker,
                                   mock_create_task, mock_datetime):
        docket_item = (41620, ["daft", "punk"])
        region = "us_nd"
        scrape_type = constants.ScrapeType.SNAPSHOT
        queue_name = "us_nd_scraper"
        initial_task = "break_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        mock_tracker.return_value = docket_item
        mock_create_task.return_value = None
        mock_datetime.now.return_value = _DATETIME

        scraper = FakeScraper(region, initial_task)
        scraper.start_scrape(scrape_type)

        mock_region.assert_called_with(region)
        mock_tracker.assert_called_with(ScrapeKey(region, scrape_type))

        queue_params = QueueRequest(
            scrape_type=scrape_type.value,
            scraper_start_time=_DATETIME,
            next_task=FAKE_TASK,
            # content=(83240, ["daft", "punk"]),
        )
        request_body = {
            'region': region,
            'task': initial_task,
            'params': queue_params.to_serializable()
        }

        mock_create_task.assert_called_with(
            url=scraper.scraper_work_url,
            queue_name=queue_name,
            body=request_body)

    @patch("recidiviz.ingest.scrape.sessions.end_session")
    @patch("recidiviz.ingest.scrape.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_start_scrape_no_record_id(self, mock_region,
                                       mock_tracker, mock_sessions):
        docket_item = (-1, ["human", "after", "all"])
        region = "us_nd"
        scrape_type = constants.ScrapeType.SNAPSHOT
        queue_name = "us_nd_scraper"
        initial_task = "fix_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        mock_tracker.return_value = docket_item
        mock_sessions.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.start_scrape(scrape_type)

        mock_region.assert_called_with(region)
        mock_tracker.assert_called_with(ScrapeKey(region, scrape_type))
        mock_sessions.assert_called_with(ScrapeKey(region, scrape_type))

    @patch("recidiviz.ingest.scrape.sessions.end_session")
    @patch("recidiviz.ingest.scrape.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_start_scrape_no_docket_item(self, mock_region,
                                         mock_tracker, mock_sessions):
        region = "us_nd"
        scrape_type = constants.ScrapeType.BACKGROUND
        queue_name = "us_nd_scraper"
        initial_task = "trash_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        mock_tracker.return_value = None
        mock_sessions.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.start_scrape(scrape_type)

        mock_region.assert_called_with(region)
        mock_tracker.assert_called_with(ScrapeKey(region, scrape_type))
        mock_sessions.assert_called_with(ScrapeKey(region, scrape_type))


class TestStopScraper:
    """Tests for the Scraper.stop_scrape method."""

    @patch("recidiviz.ingest.scrape.queues.purge_queue")
    @patch("recidiviz.ingest.scrape.sessions.get_sessions")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_stop_scrape(self, mock_region, mock_sessions, mock_purge_queue):
        region = "us_sd"
        scrape_type = constants.ScrapeType.BACKGROUND
        queue_name = "us_sd_scraper"
        initial_task = "change_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        open_session = ScrapeSession.new(
            key=None, scrape_type=scrape_type, region=region,
        )
        mock_sessions.return_value = [open_session]
        mock_purge_queue.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.stop_scrape([scrape_type])

        mock_region.assert_called_with(region)
        mock_sessions.assert_called_with(region, include_closed=False)
        mock_purge_queue.assert_called_with(queue_name)

    @patch("recidiviz.ingest.scrape.queues.purge_queue")
    @patch("recidiviz.ingest.scrape.sessions.get_sessions")
    @patch("recidiviz.utils.regions.get_region_manifest")
    @patch.object(Scraper, "resume_scrape")
    def test_stop_scrape_resume_other_scrapes(self, mock_resume, mock_region,
                                              mock_sessions, mock_purge_queue):
        """Tests that the stop_scrape method will launch other scrape types we
        didn't mean to stop."""
        region = "us_sd"
        scrape_type = constants.ScrapeType.BACKGROUND
        queue_name = "us_sd_scraper"
        initial_task = "mail_upgrade_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        open_session_other = ScrapeSession.new(
            key=None, scrape_type=constants.ScrapeType.SNAPSHOT,
        )
        open_session_matching = ScrapeSession.new(
            key=None, scrape_type=constants.ScrapeType.BACKGROUND,
        )
        mock_sessions.return_value = [open_session_other, open_session_matching]
        mock_purge_queue.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.stop_scrape([scrape_type])

        mock_region.assert_called_with(region)
        mock_sessions.assert_called_with(region, include_closed=False)
        mock_resume.assert_called_with(constants.ScrapeType.SNAPSHOT)
        mock_purge_queue.assert_called_with(queue_name)


class TestResumeScrape:
    """Tests for the Scraper.resume_scrape method."""

    @patch('recidiviz.ingest.scrape.scraper.datetime')
    @patch("recidiviz.ingest.scrape.queues.create_task")
    @patch("recidiviz.ingest.scrape.sessions.get_recent_sessions")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_resume_scrape_background(self, mock_region, mock_sessions,
                                      mock_create_task, mock_datetime):
        """Tests the resume_scrape flow for background scraping."""
        region = "us_nd"
        scrape_type = constants.ScrapeType.BACKGROUND
        queue_name = "us_nd_scraper"
        initial_task = "charge_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        recent_session_none_scraped = ScrapeSession.new(key=None)
        recent_session = ScrapeSession.new(
            key=None, last_scraped="Bangalter, Thomas")
        mock_sessions.return_value = [recent_session_none_scraped,
                                      recent_session]
        mock_create_task.return_value = None
        mock_datetime.now.return_value = _DATETIME

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_region.assert_called_with(region)
        mock_sessions.assert_called_with(ScrapeKey(region, scrape_type))

        queue_params = QueueRequest(
            scrape_type=scrape_type.value,
            scraper_start_time=_DATETIME,
            next_task=FAKE_TASK,
            # content=["Bangalter", "Thomas"],
        )
        request_body = {
            'region': region,
            'task': initial_task,
            'params': queue_params.to_serializable()
        }

        mock_create_task.assert_called_with(
            url=scraper.scraper_work_url,
            queue_name=queue_name,
            body=request_body)

    @patch("recidiviz.ingest.scrape.sessions.get_recent_sessions")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_resume_scrape_background_none_scraped(self, mock_region,
                                                   mock_sessions):
        region = "us_nd"
        scrape_type = constants.ScrapeType.BACKGROUND
        queue_name = "us_nd_scraper"
        initial_task = "point_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        recent_session_none_scraped = ScrapeSession.new(key=None)
        mock_sessions.return_value = [recent_session_none_scraped]

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_region.assert_called_with(region)
        mock_sessions.assert_called_with(ScrapeKey(region, scrape_type))

    @patch("recidiviz.ingest.scrape.sessions.get_recent_sessions")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_resume_scrape_background_no_recent_sessions(self, mock_region,
                                                         mock_sessions):
        region = "us_nd"
        scrape_type = constants.ScrapeType.BACKGROUND
        queue_name = "us_nd_scraper"
        initial_task = "zoom_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        mock_sessions.return_value = []

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_region.assert_called_with(region)
        mock_sessions.assert_called_with(ScrapeKey(region, scrape_type))

    @patch('recidiviz.ingest.scrape.scraper.datetime')
    @patch("recidiviz.ingest.scrape.queues.create_task")
    @patch("recidiviz.ingest.scrape.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_resume_scrape_snapshot(self, mock_region, mock_tracker,
                                    mock_create_task, mock_datetime):
        docket_item = (41620, ["daft", "punk"])
        region = "us_nd"
        scrape_type = constants.ScrapeType.SNAPSHOT
        queue_name = "us_nd_scraper"
        initial_task = "press_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        mock_tracker.return_value = docket_item
        mock_create_task.return_value = None
        mock_datetime.now.return_value = _DATETIME

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_region.assert_called_with(region)

        queue_params = QueueRequest(
            scrape_type=scrape_type.value,
            scraper_start_time=_DATETIME,
            next_task=FAKE_TASK,
            # content=(83240, ["dagt", "punk"]),
        )
        request_body = {
            'region': region,
            'task': initial_task,
            'params': queue_params.to_serializable()
        }

        mock_create_task.assert_called_with(
            url=scraper.scraper_work_url,
            queue_name=queue_name,
            body=request_body)

    @patch("recidiviz.ingest.scrape.sessions.end_session")
    @patch("recidiviz.ingest.scrape.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_resume_scrape_snapshot_no_docket_item(self, mock_region,
                                                   mock_tracker,
                                                   mock_end_session):
        region = "us_nd"
        scrape_type = constants.ScrapeType.SNAPSHOT
        queue_name = "us_nd_scraper"
        initial_task = "snap_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        mock_tracker.return_value = None
        mock_end_session.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_region.assert_called_with(region)
        mock_tracker.assert_called_with(ScrapeKey(region, scrape_type))
        mock_end_session.assert_called_with(ScrapeKey(region, scrape_type))


class TestFetchPage:
    """Tests for the Scraper.fetch_page method."""

    @patch("recidiviz.ingest.scrape.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scrape.scraper_utils.get_proxies")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_fetch_page(self, mock_region, mock_proxies, mock_headers):
        """Tests that fetch_page returns the fetched data payload."""
        url = "/around/the/world"
        region = "us_sd"
        queue_name = "us_sd_scraper"
        initial_task = "work_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers

        scraper = FakeScraper(region, initial_task)
        page = "<blink>Get in on the ground floor</blink>"
        response = requests.Response()
        response._content = page  # pylint: disable=protected-access
        response.status_code = 200
        with patch('requests.get', return_value=response):
            assert scraper.fetch_page(url).content == page
            requests.get.assert_called_with(
                url, proxies=proxies, headers=headers, cookies=None)

        mock_region.assert_called_with(region)
        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()

    @patch("requests.post")
    @patch("recidiviz.ingest.scrape.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scrape.scraper_utils.get_proxies")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_fetch_page_post(self, mock_region, mock_proxies, mock_headers,
                             mock_requests):
        """Tests that fetch_page returns the fetched data payload returned
        from post requests."""
        url = "/around/the/world"
        body = {'foo': 'bar'}
        json_data = {'far': 'boo'}
        region = "us_sd"
        queue_name = "us_sd_scraper"
        initial_task = "work_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers
        page = "<blink>Get in on the ground floor</blink>"
        response = requests.Response()
        response._content = page  # pylint: disable=protected-access
        response.status_code = 200
        mock_requests.return_value = response

        scraper = FakeScraper(region, initial_task)
        assert scraper.fetch_page(
            url, post_data=body, json_data=json_data).content == page

        mock_region.assert_called_with(region)
        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()
        mock_requests.assert_called_with(
            url, proxies=proxies, headers=headers, cookies=None,
            data=body, json=json_data)

    @patch("requests.get")
    @patch("recidiviz.ingest.scrape.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scrape.scraper_utils.get_proxies")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_fetch_page_error(self, mock_region, mock_proxies, mock_headers,
                              mock_requests):
        """Tests that fetch_page successfully handles error responses."""
        url = "/around/the/world"
        region = "us_sd"
        queue_name = "us_sd_scraper"
        initial_task = "work_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers

        original_request = requests.PreparedRequest()
        original_request.headers = headers
        original_request.method = 'GET'
        original_request.body = None

        # test a few types of errors
        errors = {
            500: 'SERVER ERROR',
            502: 'PROXY ERROR',
            503: 'SERVICE UNAVAILABLE',
        }

        error_response = requests.Response()
        error_response.headers = {}
        error_response.request = original_request
        for code, name in errors.items():
            error_response.status_code = code
            error_response.reason = name
            mock_requests.return_value = error_response

            scraper = FakeScraper(region, initial_task)
            assert scraper.fetch_page(url) == -1

            mock_region.assert_called_with(region)
            mock_proxies.assert_called_with()
            mock_headers.assert_called_with()
            mock_requests.assert_called_with(
                url, proxies=proxies, headers=headers, cookies=None)

def mock_region_manifest(region_code, queue_name):
    return {
        'region_code': region_code,
        'queue': queue_name,
        'agency_name': 'the agency',
        'agency_type': 'benevolent',
        'base_url': 'localhost:3000',
        'entity_kinds': [],
        'names_file': 'names.txt',
        'params': {},
        'scraper_class': 'fake_scraper',
        'scraper_package': 'recidiviz.tests.ingest',
        'timezone': 'America/Cleveland'
    }

FAKE_TASK = Task(task_type=constants.TaskType.INITIAL, endpoint='fake')

class FakeScraper(Scraper):

    def __init__(self, region_name, initial_task_method):
        super(FakeScraper, self).__init__(region_name)
        self.initial_task_method = initial_task_method

    def person_id_to_record_id(self, person_id):
        if person_id < 0:
            return None
        return person_id * 2

    def get_initial_task_method(self):
        return self.initial_task_method

    def get_initial_task(self):
        return FAKE_TASK
