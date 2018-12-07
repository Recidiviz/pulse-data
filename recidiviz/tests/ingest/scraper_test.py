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
from datetime import datetime
from mock import patch, Mock

from google.appengine.ext import ndb
from google.appengine.ext.db import InternalError
from google.appengine.ext import testbed

import requests

from recidiviz.ingest import constants
from recidiviz.ingest.scraper import Scraper
from recidiviz.ingest.sessions import ScrapeSession
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.models.record import Offense, Record, SentenceDuration
from recidiviz.models.snapshot import Snapshot


_DATETIME_STR = "2018-12-06 00::00::00"


class TestAbstractScraper(object):
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


@patch('recidiviz.ingest.scraper.Scraper.get_now_as_str',
       Mock(return_value=_DATETIME_STR))
class TestStartScrape(object):
    """Tests for the Scraper.start_scrape method."""

    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_start_scrape_background(
            self, mock_region, mock_tracker, mock_taskqueue):
        docket_item = ("Dog", "Cat")
        region = "us_nd"
        scrape_type = constants.BACKGROUND_SCRAPE
        queue_name = "us_nd_scraper"
        initial_task = "use_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        mock_tracker.return_value = docket_item
        mock_taskqueue.return_value = None
        # mock_datetime.now = Mock(return_value=_DATETIME)

        scraper = FakeScraper(region, initial_task)
        scraper.start_scrape(scrape_type)

        mock_region.assert_called_with(region)
        mock_tracker.assert_called_with(ScrapeKey(region, scrape_type))
        task_params = json.dumps({'scrape_type': scrape_type,
                                  'content': docket_item,
                                  'scraper_start_time': _DATETIME_STR})
        mock_taskqueue.assert_called_with(url=scraper.scraper_work_url,
                                          queue_name=queue_name,
                                          params={
                                              'region': region,
                                              'task': initial_task,
                                              'params': task_params
                                          })

    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_start_scrape_snapshot(self, mock_region,
                                   mock_tracker, mock_taskqueue):
        docket_item = (41620, ["daft", "punk"])
        region = "us_nd"
        scrape_type = constants.SNAPSHOT_SCRAPE
        queue_name = "us_nd_scraper"
        initial_task = "break_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        mock_tracker.return_value = docket_item
        mock_taskqueue.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.start_scrape(scrape_type)

        mock_region.assert_called_with(region)
        mock_tracker.assert_called_with(ScrapeKey(region, scrape_type))
        task_params = json.dumps({'scrape_type': scrape_type,
                                  'content': (83240, ["daft", "punk"]),
                                  'scraper_start_time': _DATETIME_STR})
        mock_taskqueue.assert_called_with(url=scraper.scraper_work_url,
                                          queue_name=queue_name,
                                          params={
                                              'region': region,
                                              'task': initial_task,
                                              'params': task_params
                                          })

    @patch("recidiviz.ingest.sessions.end_session")
    @patch("recidiviz.ingest.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_start_scrape_no_record_id(self, mock_region,
                                       mock_tracker, mock_sessions):
        docket_item = (-1, ["human", "after", "all"])
        region = "us_nd"
        scrape_type = constants.SNAPSHOT_SCRAPE
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

    @patch("recidiviz.ingest.sessions.end_session")
    @patch("recidiviz.ingest.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_start_scrape_no_docket_item(self, mock_region,
                                         mock_tracker, mock_sessions):
        region = "us_nd"
        scrape_type = constants.BACKGROUND_SCRAPE
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


class TestStopScraper(object):
    """Tests for the Scraper.stop_scrape method."""

    @patch("google.appengine.api.taskqueue.Queue")
    @patch("recidiviz.ingest.sessions.get_open_sessions")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_stop_scrape(self, mock_region, mock_sessions, mock_queue):
        region = "us_sd"
        scrape_type = constants.BACKGROUND_SCRAPE
        queue_name = "us_sd_scraper"
        initial_task = "change_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        open_session = ScrapeSession()
        open_session.scrape_type = scrape_type
        mock_sessions.return_value = [open_session]
        mock_queue.purge.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.stop_scrape([scrape_type])

        mock_region.assert_called_with(region)
        mock_sessions.assert_called_with(region)

    @patch("google.appengine.api.taskqueue.Queue")
    @patch("google.appengine.ext.deferred.defer")
    @patch("recidiviz.ingest.sessions.get_open_sessions")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_stop_scrape_other_scrapes_to_defer(self, mock_region,
                                                mock_sessions, mock_deferred,
                                                mock_queue):
        """Tests that the stop_scrape method will defer launching of other
        scrape types we didn't mean to stop."""
        region = "us_sd"
        scrape_type = constants.BACKGROUND_SCRAPE
        queue_name = "us_sd_scraper"
        initial_task = "mail_upgrade_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        open_session_other = ScrapeSession()
        open_session_other.scrape_type = constants.SNAPSHOT_SCRAPE
        open_session_matching = ScrapeSession()
        open_session_matching.scrape_type = constants.BACKGROUND_SCRAPE
        mock_sessions.return_value = [open_session_other, open_session_matching]
        mock_deferred.return_value = None
        mock_queue.purge.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.stop_scrape([scrape_type])

        mock_region.assert_called_with(region)
        mock_sessions.assert_called_with(region)
        mock_deferred.assert_called_with(scraper.resume_scrape,
                                         constants.SNAPSHOT_SCRAPE,
                                         _countdown=60)


@patch('recidiviz.ingest.scraper.Scraper.get_now_as_str',
       Mock(return_value=_DATETIME_STR))
class TestResumeScrape(object):
    """Tests for the Scraper.resume_scrape method."""

    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.sessions.get_recent_sessions")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_resume_scrape_background(self, mock_region, mock_sessions,
                                      mock_taskqueue):
        """Tests the resume_scrape flow for background scraping."""
        region = "us_nd"
        scrape_type = constants.BACKGROUND_SCRAPE
        queue_name = "us_nd_scraper"
        initial_task = "charge_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        recent_session_none_scraped = ScrapeSession()
        recent_session = ScrapeSession()
        recent_session.last_scraped = "Bangalter, Thomas"
        mock_sessions.return_value = [recent_session_none_scraped,
                                      recent_session]
        mock_taskqueue.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_region.assert_called_with(region)
        mock_sessions.assert_called_with(ScrapeKey(region, scrape_type))
        task_params = json.dumps({'scrape_type': scrape_type,
                                  'content': ("Bangalter", "Thomas"),
                                  'scraper_start_time': _DATETIME_STR})
        mock_taskqueue.assert_called_with(url=scraper.scraper_work_url,
                                          queue_name=queue_name,
                                          params={
                                              'region': region,
                                              'task': initial_task,
                                              'params': task_params
                                          })

    @patch("recidiviz.ingest.sessions.get_recent_sessions")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_resume_scrape_background_none_scraped(self, mock_region,
                                                   mock_sessions):
        region = "us_nd"
        scrape_type = constants.BACKGROUND_SCRAPE
        queue_name = "us_nd_scraper"
        initial_task = "point_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        recent_session_none_scraped = ScrapeSession()
        mock_sessions.return_value = [recent_session_none_scraped]

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_region.assert_called_with(region)
        mock_sessions.assert_called_with(ScrapeKey(region, scrape_type))

    @patch("recidiviz.ingest.sessions.get_recent_sessions")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_resume_scrape_background_no_recent_sessions(self, mock_region,
                                                         mock_sessions):
        region = "us_nd"
        scrape_type = constants.BACKGROUND_SCRAPE
        queue_name = "us_nd_scraper"
        initial_task = "zoom_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        mock_sessions.return_value = []

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_region.assert_called_with(region)
        mock_sessions.assert_called_with(ScrapeKey(region, scrape_type))

    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_resume_scrape_snapshot(self, mock_region, mock_tracker,
                                    mock_taskqueue):
        docket_item = (41620, ["daft", "punk"])
        region = "us_nd"
        scrape_type = constants.SNAPSHOT_SCRAPE
        queue_name = "us_nd_scraper"
        initial_task = "press_it"

        mock_region.return_value = mock_region_manifest(region, queue_name)
        mock_tracker.return_value = docket_item
        mock_taskqueue.return_value = None

        scraper = FakeScraper(region, initial_task)
        scraper.resume_scrape(scrape_type)

        mock_region.assert_called_with(region)
        task_params = json.dumps({'scrape_type': scrape_type,
                                  'content': (83240, ["daft", "punk"]),
                                  'scraper_start_time': _DATETIME_STR})
        mock_taskqueue.assert_called_with(url=scraper.scraper_work_url,
                                          queue_name=queue_name,
                                          params={
                                              'region': region,
                                              'task': initial_task,
                                              'params': task_params
                                          })

    @patch("recidiviz.ingest.sessions.end_session")
    @patch("recidiviz.ingest.tracker.iterate_docket_item")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_resume_scrape_snapshot_no_docket_item(self, mock_region,
                                                   mock_tracker,
                                                   mock_end_session):
        region = "us_nd"
        scrape_type = constants.SNAPSHOT_SCRAPE
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


class TestFetchPage(object):
    """Tests for the Scraper.fetch_page method."""

    @patch("requests.get")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    @patch("recidiviz.utils.regions.get_region_manifest")
    def test_fetch_page(self, mock_region, mock_proxies, mock_headers,
                        mock_requests):
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
        page = "<blink>Get in on the ground floor</blink>"
        mock_requests.return_value = page

        scraper = FakeScraper(region, initial_task)
        assert scraper.fetch_page(url) == page

        mock_region.assert_called_with(region)
        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()
        mock_requests.assert_called_with(url, proxies=proxies, headers=headers)

    @patch("requests.post")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
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
        mock_requests.return_value = page

        scraper = FakeScraper(region, initial_task)
        assert scraper.fetch_page(url,
                                  post_data=body, json_data=json_data) == page

        mock_region.assert_called_with(region)
        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()
        mock_requests.assert_called_with(url, proxies=proxies, headers=headers,
                                         data=body, json=json_data)

    @patch("requests.get")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
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
        error_response = requests.Response()
        error_response.status_code = 500
        error_response.reason = 'SERVER_ERROR'
        error_response.headers = {}
        exception_response = requests.exceptions.RequestException(
            request=original_request, response=error_response)
        mock_requests.side_effect = exception_response

        scraper = FakeScraper(region, initial_task)
        assert scraper.fetch_page(url) == -1

        mock_region.assert_called_with(region)
        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()
        mock_requests.assert_called_with(url, proxies=proxies, headers=headers)


class TestCompareAndSetSnapshot(object):
    """Tests for the compare_and_set_snapshot method."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        context = ndb.get_context()
        context.set_memcache_policy(False)
        context.clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_first_snapshot_for_record(self):
        """Tests the happy path for compare_and_set_snapshot."""
        scraper = FakeScraper('us_ny', 'initial_task')

        record, snapshot = self.prepare_record_and_snapshot()

        scraper.compare_and_set_snapshot(record, snapshot)
        snapshots = Snapshot.query(ancestor=record.key).fetch()
        assert len(snapshots) == 1

    def test_no_changes(self):
        """Tests that a lack of changes in any field does not lead to a new
        snapshot."""
        scraper = FakeScraper('us_ny', 'initial_task')

        record, snapshot = self.prepare_record_and_snapshot()

        scraper.compare_and_set_snapshot(record, snapshot)
        snapshots = Snapshot.query(ancestor=record.key).fetch()
        assert len(snapshots) == 1

        # There should still only be one snapshot, because nothing changes
        scraper.compare_and_set_snapshot(record, snapshot)
        snapshots = Snapshot.query(ancestor=record.key).fetch()
        assert len(snapshots) == 1

    def test_changes_in_flat_field(self):
        """Tests that changes in a flat field lead to a new snapshot."""
        scraper = FakeScraper('us_ny', 'initial_task')

        record, snapshot = self.prepare_record_and_snapshot()

        scraper.compare_and_set_snapshot(record, snapshot)
        snapshots = Snapshot.query(ancestor=record.key).fetch()
        assert len(snapshots) == 1

        second_snapshot = self.record_to_snapshot(record, record.key)
        second_snapshot.latest_facility = "ANOTHER FACILITY"

        scraper.compare_and_set_snapshot(record, second_snapshot)
        snapshots = Snapshot.query(ancestor=record.key).fetch()
        assert len(snapshots) == 2

    def test_changes_in_nested_field_offense(self):
        """Tests that changes in a nested field, i.e. offense,
        lead to a new snapshot."""
        scraper = FakeScraper('us_ny', 'initial_task')

        record, snapshot = self.prepare_record_and_snapshot()

        scraper.compare_and_set_snapshot(record, snapshot)
        snapshots = Snapshot.query(ancestor=record.key).fetch()
        assert len(snapshots) == 1

        second_snapshot = self.record_to_snapshot(record, record.key)
        updated_offenses = [
            # This one remained the same
            Offense(
                crime_description="MANSLAUGHTER 1ST",
                crime_class="B"
            ),
            # This one remained the same
            Offense(
                crime_description="ARMED ROBBERY",
                crime_class="B"
            ),
            # This one is new
            Offense(
                crime_description="INTIMIDATION",
                crime_class="D"
            )
        ]
        second_snapshot.offense = updated_offenses

        scraper.compare_and_set_snapshot(record, second_snapshot)
        snapshots = Snapshot.query(ancestor=record.key).fetch()
        assert len(snapshots) == 2

        new_offenses = snapshots[1].offense
        assert new_offenses == updated_offenses

    def test_removed_array_element_offense(self):
        """Tests that removing an element from an array of offenses leads
        to a new snapshot."""
        scraper = FakeScraper('us_ny', 'initial_task')

        record, snapshot = self.prepare_record_and_snapshot()

        scraper.compare_and_set_snapshot(record, snapshot)
        snapshots = Snapshot.query(ancestor=record.key).fetch()
        assert len(snapshots) == 1

        second_snapshot = self.record_to_snapshot(record, record.key)
        second_snapshot.offense = []

        scraper.compare_and_set_snapshot(record, second_snapshot)
        snapshots = Snapshot.query(ancestor=record.key).fetch()
        assert len(snapshots) == 2

        new_offenses = snapshots[1].offense
        assert new_offenses == []

    @patch("recidiviz.models.snapshot.Snapshot.put")
    def test_error_saving_snapshot(self, mock_put):
        mock_put.side_effect = InternalError()

        scraper = FakeScraper('us_ny', 'initial_task')

        record, snapshot = self.prepare_record_and_snapshot()

        result = scraper.compare_and_set_snapshot(record, snapshot)
        assert not result

        mock_put.assert_called_with()

    @staticmethod
    def record_to_snapshot(record, record_key):
        return Snapshot(
            parent=record_key,
            admission_type=record.admission_type,
            birthdate=record.birthdate,
            cond_release_date=record.cond_release_date,
            county_of_commit=record.county_of_commit,
            custody_date=record.custody_date,
            custody_status=record.custody_status,
            earliest_release_date=record.earliest_release_date,
            earliest_release_type=record.earliest_release_type,
            is_released=record.is_released,
            last_custody_date=record.last_custody_date,
            latest_facility=record.latest_facility,
            latest_release_date=record.latest_release_date,
            latest_release_type=record.latest_release_type,
            max_expir_date=record.max_expir_date,
            max_expir_date_parole=record.max_expir_date_parole,
            max_expir_date_superv=record.max_expir_date_superv,
            max_sentence_length=record.max_sentence_length,
            min_sentence_length=record.min_sentence_length,
            offense=record.offense,
            parole_discharge_date=record.parole_discharge_date,
            parole_elig_date=record.parole_elig_date,
            parole_hearing_date=record.parole_hearing_date,
            parole_hearing_type=record.parole_hearing_type,
            race=record.race,
            region=record.region,
            sex=record.sex,
            surname=record.surname,
            given_names=record.given_names)

    def prepare_record_and_snapshot(self):
        """Prepares a Record suitable for comparing snapshots to, and a
        translated snapshot."""
        offense = Offense(
            crime_description='MANSLAUGHTER 1ST',
            crime_class='B'
        )

        record = Record(
            admission_type='REVOCATION',
            birthdate=datetime(1972, 4, 22),
            cond_release_date=datetime(2006, 8, 14),
            county_of_commit='KINGS',
            custody_date=datetime(1991, 3, 14),
            custody_status='RELEASED',
            earliest_release_date=datetime(2002, 8, 14),
            earliest_release_type='PAROLE',
            is_released=True,
            last_custody_date=datetime(1994, 7, 1),
            latest_release_date=datetime(2002, 10, 28),
            latest_release_type='PAROLE',
            latest_facility='QUEENSBORO',
            max_expir_date=datetime(2010, 1, 14),
            max_expir_date_parole=datetime(2010, 1, 14),
            max_expir_date_superv=datetime(2010, 1, 14),
            max_sentence_length=SentenceDuration(
                life_sentence=False,
                years=18,
                months=10,
                days=0),
            min_sentence_length=SentenceDuration(
                life_sentence=False,
                years=11,
                months=5,
                days=0),
            parole_elig_date=datetime(2002, 8, 14),
            parole_discharge_date=datetime(2002, 8, 14),
            parole_hearing_date=datetime(2002, 6, 17),
            parole_hearing_type='INITIAL HEARING',
            offense=[offense],
            race='WHITE',
            record_id='1234567',
            region='us_ny',
            sex='MALE',
            surname='SIMPSON',
            given_names='BART'
        )
        record_key = record.put()

        before_compare = Snapshot.query(ancestor=record_key).fetch()
        assert not before_compare

        snapshot = self.record_to_snapshot(record, record_key)

        return record, snapshot


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
