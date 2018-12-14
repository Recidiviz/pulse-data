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

"""Tests for ingest/scraper_control.py."""

from flask import Flask
from mock import call, patch
from google.appengine.ext import testbed

from recidiviz.ingest import scraper_control
from recidiviz.ingest.models.scrape_key import ScrapeKey

APP_ID = "recidiviz-worker-test"

app = Flask(__name__)
app.register_blueprint(scraper_control.scraper_control)
app.config['TESTING'] = True


class TestScraperStart(object):
    """Tests for requests to the Scraper Start API."""

    # noinspection PyAttributeOutsideInit
    def setup_method(self, _test_method):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.setup_env(app_id=APP_ID)
        self.testbed.init_app_identity_stub()
        self.testbed.init_user_stub()

        self.client = app.test_client()

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def login_user(self,
                   email='user@recidiviz.com',
                   user_id='123',
                   is_admin=True):
        self.testbed.setup_env(
            app_id=APP_ID,
            user_email=email,
            user_id=user_id,
            user_is_admin='1' if is_admin else '0',
            overwrite=True)

    @patch("recidiviz.utils.regions.get_supported_regions")
    @patch("recidiviz.utils.regions.Region")
    @patch("recidiviz.ingest.sessions.create_session")
    @patch("recidiviz.ingest.tracker.purge_docket_and_session")
    @patch("recidiviz.ingest.docket.load_target_list")
    @patch("recidiviz.utils.environment.in_prod")
    @patch("google.appengine.ext.deferred.defer")
    def test_start(self, mock_deferred, mock_environment, mock_docket,
                   mock_tracker, mock_sessions, mock_region, mock_supported):
        """Tests that the start operation chains together the correct calls."""
        mock_deferred.return_value = None
        mock_environment.return_value = False
        mock_docket.return_value = None
        mock_tracker.return_value = None
        mock_sessions.return_value = None
        fake_scraper = FakeScraper()
        mock_region.return_value = FakeRegion(fake_scraper)
        mock_supported.return_value = ['us_ut', 'us_wy']

        self.login_user()

        region = 'us_ut'
        scrape_type = 'background'
        scrape_key = ScrapeKey(region, scrape_type)
        request_args = {'region': region, 'scrape_type': scrape_type}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get('/start',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 200

        mock_deferred.assert_called_with(fake_scraper.start_scrape,
                                         scrape_type,
                                         _countdown=30)
        mock_environment.assert_called_with()
        mock_docket.assert_called_with(scrape_key, '', '')
        mock_tracker.assert_called_with(scrape_key)
        mock_sessions.assert_called_with(scrape_key)
        mock_region.assert_called_with('us_ut')
        mock_supported.assert_called_with()

    @patch("recidiviz.utils.regions.get_supported_regions")
    def test_start_unsupported_region(self, mock_supported):
        mock_supported.return_value = ['us_ny', 'us_vt']

        self.login_user()

        request_args = {'region': 'us_ca', 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get('/start',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 400
        assert response.get_data() == "Missing or invalid parameters, " \
                                      "see service logs."

        mock_supported.assert_called_with()


class TestScraperStop(object):
    """Tests for requests to the Scraper Stop API."""

    # noinspection PyAttributeOutsideInit
    def setup_method(self, _test_method):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.setup_env(app_id=APP_ID)
        self.testbed.init_app_identity_stub()
        self.testbed.init_user_stub()

        self.client = app.test_client()

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def login_user(self,
                   email='user@recidiviz.com',
                   user_id='123',
                   is_admin=True):
        self.testbed.setup_env(
            app_id=APP_ID,
            user_email=email,
            user_id=user_id,
            user_is_admin='1' if is_admin else '0',
            overwrite=True)

    @patch("recidiviz.utils.regions.get_supported_regions")
    @patch("recidiviz.utils.regions.Region")
    @patch("recidiviz.ingest.sessions.end_session")
    def test_stop(self, mock_sessions, mock_region, mock_supported):
        mock_sessions.return_value = None
        mock_region.return_value = FakeRegion(FakeScraper())
        mock_supported.return_value = ['us_ca', 'us_ut']

        self.login_user()

        request_args = {'region': 'all', 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get('/stop',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 200

        mock_sessions.assert_has_calls([call(ScrapeKey('us_ca', 'background')),
                                        call(ScrapeKey('us_ca', 'snapshot')),
                                        call(ScrapeKey('us_ut', 'background')),
                                        call(ScrapeKey('us_ut', 'snapshot'))])
        mock_region.assert_has_calls([call('us_ca'), call('us_ut')])
        mock_supported.assert_called_with()

    @patch("recidiviz.utils.regions.get_supported_regions")
    def test_stop_unsupported_region(self, mock_supported):
        mock_supported.return_value = ['us_ny', 'us_vt']

        self.login_user()

        request_args = {'region': 'us_ca', 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get('/stop',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 400
        assert response.get_data() == "Missing or invalid parameters, " \
                                      "see service logs."

        mock_supported.assert_called_with()


class TestScraperResume(object):
    """Tests for requests to the Scraper Resume API."""

    # noinspection PyAttributeOutsideInit
    def setup_method(self, _test_method):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.setup_env(app_id=APP_ID)
        self.testbed.init_app_identity_stub()
        self.testbed.init_user_stub()

        self.client = app.test_client()

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def login_user(self,
                   email='user@recidiviz.com',
                   user_id='123',
                   is_admin=True):
        self.testbed.setup_env(
            app_id=APP_ID,
            user_email=email,
            user_id=user_id,
            user_is_admin='1' if is_admin else '0',
            overwrite=True)

    @patch("recidiviz.utils.regions.get_supported_regions")
    @patch("recidiviz.utils.regions.Region")
    @patch("recidiviz.ingest.sessions.create_session")
    def test_resume(self, mock_sessions, mock_region, mock_supported):
        mock_sessions.return_value = None
        mock_region.return_value = FakeRegion(FakeScraper())
        mock_supported.return_value = ['us_ca']

        self.login_user()

        region = 'us_ca'
        request_args = {'region': region, 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get('/resume',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 200

        mock_sessions.assert_has_calls([call(ScrapeKey(region, 'background')),
                                        call(ScrapeKey(region, 'snapshot'))])
        mock_region.assert_called_with(region)
        mock_supported.assert_called_with()

    @patch("recidiviz.utils.regions.get_supported_regions")
    def test_resume_unsupported_region(self, mock_supported):
        mock_supported.return_value = ['us_ny', 'us_vt']

        self.login_user()

        request_args = {'region': 'us_ca', 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get('/resume',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 400
        assert response.get_data() == "Missing or invalid parameters, " \
                                      "see service logs."

        mock_supported.assert_called_with()


class FakeRegion(object):
    """A fake region to be returned from mocked out calls to Region"""
    def __init__(self, scraper):
        self.scraper = scraper

    def get_scraper(self):
        return self.scraper


class FakeScraper(object):
    """A fake scraper to be returned from mocked out calls to
    Region.get_scraper"""

    def start_scrape(self):
        return

    def stop_scrape(self, scrape_types):
        return scrape_types

    def resume_scrape(self, scrape_type):
        return scrape_type
