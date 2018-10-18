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


import json

from mock import patch
import requests

from recidiviz.ingest.scraper import Scraper
from recidiviz.ingest.sessions import ScrapeSession
from recidiviz.ingest.models.scrape_key import ScrapeKey


class TestAbstractScraper(object):
    """Tests the abstract-ness of the Scraper base class."""

    @patch("recidiviz.utils.regions.load_region_manifest")
    def test_init(self, mock_regions):
        region = "us_nd"
        queue_name = "us_nd_scraper"
        initial_task = "buy_it"

        mock_regions.return_value = mock_region_manifest(region, queue_name)
        scraper = FakeScraper(region, initial_task)

        mock_regions.assert_called_with(region_code=region)
        assert scraper.region.region_code == region
        assert scraper.region.queue == queue_name
        assert scraper.fail_counter == "us_nd_next_page_fail_counter"
        assert scraper.scraper_work_url == "/scraper/work"


class TestStartScrape(object):
    """Tests for the Scraper.start_scrape method."""

    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.load_region_manifest")
    def test_start_scrape_background(self, mock_regions,
                                     mock_tracker, mock_taskqueue):
        docket_item = ("Dog", "Cat")
        region = "us_nd"
        scrape_type = "background"
        queue_name = "us_nd_scraper"
        initial_task = "use_it"

        mock_regions.return_value = mock_region_manifest(region, queue_name)
        mock_tracker.return_value = docket_item
        mock_taskqueue.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.start_scrape(scrape_type)

        mock_regions.assert_called_with(region_code=region)
        mock_tracker.assert_called_with(ScrapeKey(region, scrape_type))
        task_params = json.dumps({'scrape_type': scrape_type,
                                  'content': docket_item})
        mock_taskqueue.assert_called_with(url=scraper.scraper_work_url,
                                          queue_name=queue_name,
                                          params={
                                              'region': region,
                                              'task': initial_task,
                                              'params': task_params
                                          })

    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.load_region_manifest")
    def test_start_scrape_snapshot(self, mock_regions,
                                   mock_tracker, mock_taskqueue):
        docket_item = (41620, ["daft", "punk"])
        region = "us_nd"
        scrape_type = "snapshot"
        queue_name = "us_nd_scraper"
        initial_task = "break_it"

        mock_regions.return_value = mock_region_manifest(region, queue_name)
        mock_tracker.return_value = docket_item
        mock_taskqueue.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.start_scrape(scrape_type)

        mock_regions.assert_called_with(region_code=region)
        mock_tracker.assert_called_with(ScrapeKey(region, scrape_type))
        task_params = json.dumps({'scrape_type': scrape_type,
                                  'content': (83240, ["daft", "punk"])})
        mock_taskqueue.assert_called_with(url=scraper.scraper_work_url,
                                          queue_name=queue_name,
                                          params={
                                              'region': region,
                                              'task': initial_task,
                                              'params': task_params
                                          })

    @patch("recidiviz.ingest.sessions.end_session")
    @patch("recidiviz.ingest.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.load_region_manifest")
    def test_start_scrape_no_record_id(self, mock_regions,
                                       mock_tracker, mock_sessions):
        docket_item = (-1, ["human", "after", "all"])
        region = "us_nd"
        scrape_type = "snapshot"
        queue_name = "us_nd_scraper"
        initial_task = "fix_it"

        mock_regions.return_value = mock_region_manifest(region, queue_name)
        mock_tracker.return_value = docket_item
        mock_sessions.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.start_scrape(scrape_type)

        mock_regions.assert_called_with(region_code=region)
        mock_tracker.assert_called_with(ScrapeKey(region, scrape_type))
        mock_sessions.assert_called_with(ScrapeKey(region, scrape_type))

    @patch("recidiviz.ingest.sessions.end_session")
    @patch("recidiviz.ingest.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.load_region_manifest")
    def test_start_scrape_no_docket_item(self, mock_regions,
                                         mock_tracker, mock_sessions):
        region = "us_nd"
        scrape_type = "background"
        queue_name = "us_nd_scraper"
        initial_task = "trash_it"

        mock_regions.return_value = mock_region_manifest(region, queue_name)
        mock_tracker.return_value = None
        mock_sessions.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.start_scrape(scrape_type)

        mock_regions.assert_called_with(region_code=region)
        mock_tracker.assert_called_with(ScrapeKey(region, scrape_type))
        mock_sessions.assert_called_with(ScrapeKey(region, scrape_type))


class TestStopScraper(object):
    """Tests for the Scraper.stop_scrape method."""

    @patch("google.appengine.api.taskqueue.Queue")
    @patch("recidiviz.ingest.sessions.get_open_sessions")
    @patch("recidiviz.utils.regions.load_region_manifest")
    def test_stop_scrape(self, mock_regions, mock_sessions, mock_queue):
        region = "us_sd"
        scrape_type = "background"
        queue_name = "us_sd_scraper"
        initial_task = "change_it"

        mock_regions.return_value = mock_region_manifest(region, queue_name)
        open_session = ScrapeSession()
        open_session.scrape_type = scrape_type
        mock_sessions.return_value = [open_session]
        mock_queue.purge.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.stop_scrape([scrape_type])

        mock_regions.assert_called_with(region_code=region)
        mock_sessions.assert_called_with(region)

    @patch("google.appengine.api.taskqueue.Queue")
    @patch("google.appengine.ext.deferred.defer")
    @patch("recidiviz.ingest.sessions.get_open_sessions")
    @patch("recidiviz.utils.regions.load_region_manifest")
    def test_stop_scrape_other_scrapes_to_defer(self, mock_regions,
                                                mock_sessions, mock_deferred,
                                                mock_queue):
        """Tests that the stop_scrape method will defer launching of other
        scrape types we didn't mean to stop."""
        region = "us_sd"
        scrape_type = "background"
        queue_name = "us_sd_scraper"
        initial_task = "mail_upgrade_it"

        mock_regions.return_value = mock_region_manifest(region, queue_name)
        open_session_other = ScrapeSession()
        open_session_other.scrape_type = "snapshot"
        open_session_matching = ScrapeSession()
        open_session_matching.scrape_type = "background"
        mock_sessions.return_value = [open_session_other, open_session_matching]
        mock_deferred.return_value = None
        mock_queue.purge.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.stop_scrape([scrape_type])

        mock_regions.assert_called_with(region_code=region)
        mock_sessions.assert_called_with(region)
        mock_deferred.assert_called_with(scraper.resume_scrape,
                                         "snapshot",
                                         _countdown=60)


class TestResumeScrape(object):
    """Tests for the Scraper.resume_scrape method."""

    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.sessions.get_recent_sessions")
    @patch("recidiviz.utils.regions.load_region_manifest")
    def test_resume_scrape_background(self, mock_regions, mock_sessions,
                                      mock_taskqueue):
        """Tests the resume_scrape flow for background scraping."""
        region = "us_nd"
        scrape_type = "background"
        queue_name = "us_nd_scraper"
        initial_task = "charge_it"

        mock_regions.return_value = mock_region_manifest(region, queue_name)
        recent_session_none_scraped = ScrapeSession()
        recent_session = ScrapeSession()
        recent_session.last_scraped = "Bangalter, Thomas"
        mock_sessions.return_value = [recent_session_none_scraped,
                                      recent_session]
        mock_taskqueue.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_regions.assert_called_with(region_code=region)
        mock_sessions.assert_called_with(ScrapeKey(region, scrape_type))
        task_params = json.dumps({'scrape_type': scrape_type,
                                  'content': ("Bangalter", "Thomas")})
        mock_taskqueue.assert_called_with(url=scraper.scraper_work_url,
                                          queue_name=queue_name,
                                          params={
                                              'region': region,
                                              'task': initial_task,
                                              'params': task_params
                                          })

    @patch("recidiviz.ingest.sessions.get_recent_sessions")
    @patch("recidiviz.utils.regions.load_region_manifest")
    def test_resume_scrape_background_none_scraped(self, mock_regions,
                                                   mock_sessions):
        region = "us_nd"
        scrape_type = "background"
        queue_name = "us_nd_scraper"
        initial_task = "point_it"

        mock_regions.return_value = mock_region_manifest(region, queue_name)
        recent_session_none_scraped = ScrapeSession()
        mock_sessions.return_value = [recent_session_none_scraped]

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_regions.assert_called_with(region_code=region)
        mock_sessions.assert_called_with(ScrapeKey(region, scrape_type))

    @patch("recidiviz.ingest.sessions.get_recent_sessions")
    @patch("recidiviz.utils.regions.load_region_manifest")
    def test_resume_scrape_background_no_recent_sessions(self, mock_regions,
                                                         mock_sessions):
        region = "us_nd"
        scrape_type = "background"
        queue_name = "us_nd_scraper"
        initial_task = "zoom_it"

        mock_regions.return_value = mock_region_manifest(region, queue_name)
        mock_sessions.return_value = []

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_regions.assert_called_with(region_code=region)
        mock_sessions.assert_called_with(ScrapeKey(region, scrape_type))

    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.load_region_manifest")
    def test_resume_scrape_snapshot(self, mock_regions, mock_tracker,
                                    mock_taskqueue):
        docket_item = (41620, ["daft", "punk"])
        region = "us_nd"
        scrape_type = "snapshot"
        queue_name = "us_nd_scraper"
        initial_task = "press_it"

        mock_regions.return_value = mock_region_manifest(region, queue_name)
        mock_tracker.return_value = docket_item
        mock_taskqueue.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_regions.assert_called_with(region_code=region)
        task_params = json.dumps({'scrape_type': scrape_type,
                                  'content': (83240, ["daft", "punk"])})
        mock_taskqueue.assert_called_with(url=scraper.scraper_work_url,
                                          queue_name=queue_name,
                                          params={
                                              'region': region,
                                              'task': initial_task,
                                              'params': task_params
                                          })

    @patch("recidiviz.ingest.sessions.end_session")
    @patch("recidiviz.ingest.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.load_region_manifest")
    def test_resume_scrape_snapshot_no_docket_item(self, mock_regions,
                                                   mock_tracker,
                                                   mock_end_session):
        region = "us_nd"
        scrape_type = "snapshot"
        queue_name = "us_nd_scraper"
        initial_task = "snap_it"

        mock_regions.return_value = mock_region_manifest(region, queue_name)
        mock_tracker.return_value = None
        mock_end_session.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_regions.assert_called_with(region_code=region)
        mock_tracker.assert_called_with(ScrapeKey(region, scrape_type))
        mock_end_session.assert_called_with(ScrapeKey(region, scrape_type))


class TestFetchPage(object):
    """Tests for the Scraper.fetch_page method."""

    @patch("requests.get")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    @patch("recidiviz.utils.regions.load_region_manifest")
    def test_fetch_page(self, mock_regions, mock_proxies, mock_headers,
                        mock_requests):
        """Tests that fetch_page returns the fetched data payload."""
        url = "/around/the/world"
        region = "us_sd"
        queue_name = "us_sd_scraper"
        initial_task = "work_it"

        mock_regions.return_value = mock_region_manifest(region, queue_name)
        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers
        page = "<blink>Get in on the ground floor</blink>"
        mock_requests.return_value = page

        scraper = FakeScraper(region, initial_task)
        assert scraper.fetch_page(url) == page

        mock_regions.assert_called_with(region_code=region)
        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()
        mock_requests.assert_called_with(url, proxies=proxies, headers=headers)

    @patch("requests.post")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    @patch("recidiviz.utils.regions.load_region_manifest")
    def test_fetch_page_post(self, mock_regions, mock_proxies, mock_headers,
                             mock_requests):
        """Tests that fetch_page returns the fetched data payload returned
        from post requests."""
        url = "/around/the/world"
        body = {'foo': 'bar'}
        region = "us_sd"
        queue_name = "us_sd_scraper"
        initial_task = "work_it"

        mock_regions.return_value = mock_region_manifest(region, queue_name)
        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers
        page = "<blink>Get in on the ground floor</blink>"
        mock_requests.return_value = page

        scraper = FakeScraper(region, initial_task)
        assert scraper.fetch_page(url, data=body) == page

        mock_regions.assert_called_with(region_code=region)
        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()
        mock_requests.assert_called_with(url, proxies=proxies, headers=headers,
                                         data=body)

    @patch("requests.get")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    @patch("recidiviz.utils.regions.load_region_manifest")
    def test_fetch_page_error(self, mock_regions, mock_proxies, mock_headers,
                              mock_requests):
        """Tests that fetch_page successfully handles error responses."""
        url = "/around/the/world"
        region = "us_sd"
        queue_name = "us_sd_scraper"
        initial_task = "work_it"

        mock_regions.return_value = mock_region_manifest(region, queue_name)
        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers

        original_request = requests.PreparedRequest()
        original_request.headers = headers
        original_request.method = 'GET'
        original_request.body = None
        error_response = requests.Response()
        error_response.status_code = 500
        error_response.reason = 'SERVER_ERROR'
        error_response.headers = {}
        exception_response = requests.exceptions.RequestException(
            request=original_request, response=error_response)
        mock_requests.side_effect = exception_response

        scraper = FakeScraper(region, initial_task)
        assert scraper.fetch_page(url) == -1

        mock_regions.assert_called_with(region_code=region)
        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()
        mock_requests.assert_called_with(url, proxies=proxies, headers=headers)


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
        'region_name': 'an area person',
        'scraper_class': 'fake_scraper',
        'scraper_package': 'recidiviz.tests.ingest',
        'timezone': 'America/Cleveland'
    }


class FakeScraper(Scraper):

    def __init__(self, region_name, initial_task):
        super(FakeScraper, self).__init__(region_name)
        self.initial_task = initial_task

    def person_id_to_record_id(self, person_id):
        if person_id < 0:
            return None
        return person_id * 2

    def get_initial_task(self):
        return self.initial_task
