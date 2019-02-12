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

from recidiviz.ingest.scrape import constants, scraper_control
from recidiviz.ingest.models.scrape_key import ScrapeKey

APP_ID = "recidiviz-worker-test"

app = Flask(__name__)
app.register_blueprint(scraper_control.scraper_control)
app.config['TESTING'] = True


def _MockSupported(timezone=None):
    if not timezone:
        regions = ['us_ut', 'us_wy']
    elif timezone == 'America/New_York':
        regions = ['us_ut']
    else:
        regions = ['us_wy']
    return regions



class TestScraperStart:
    """Tests for requests to the Scraper Start API."""

    # noinspection PyAttributeOutsideInit
    def setup_method(self, _test_method):
        self.client = app.test_client()

    @patch("recidiviz.utils.regions.get_supported_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.create_session")
    @patch("recidiviz.ingest.scrape.tracker.purge_docket_and_session")
    @patch("recidiviz.ingest.scrape.docket.load_target_list")
    def test_start(self, mock_docket, mock_tracker, mock_sessions, mock_region,
                   mock_supported):
        """Tests that the start operation chains together the correct calls."""
        mock_docket.return_value = None
        mock_tracker.return_value = None
        mock_sessions.return_value = None
        fake_scraper = FakeScraper()
        mock_region.return_value = FakeRegion(fake_scraper)
        mock_supported.side_effect = _MockSupported

        region = 'us_ut'
        scrape_type = constants.ScrapeType.BACKGROUND
        scrape_key = ScrapeKey(region, scrape_type)
        request_args = {'region': region, 'scrape_type': scrape_type.value}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get('/start',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 200

        mock_docket.assert_called_with(scrape_key, '', '')
        mock_tracker.assert_called_with(scrape_key)
        mock_sessions.assert_called_with(scrape_key)
        mock_region.assert_called_with('us_ut')
        mock_supported.assert_called_with(timezone=None)

    @patch("recidiviz.utils.regions.get_supported_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.create_session")
    @patch("recidiviz.ingest.scrape.tracker.purge_docket_and_session")
    @patch("recidiviz.ingest.scrape.docket.load_target_list")
    def test_start_timezone(self, mock_docket, mock_tracker, mock_sessions,
                            mock_region, mock_supported):
        """Tests that the start operation chains together the correct calls."""
        mock_docket.return_value = None
        mock_tracker.return_value = None
        mock_sessions.return_value = None
        fake_scraper = FakeScraper()
        mock_region.return_value = FakeRegion(fake_scraper)
        mock_supported.side_effect = _MockSupported

        region = 'all'
        scrape_type = constants.ScrapeType.BACKGROUND
        scrape_key = ScrapeKey('us_wy', scrape_type)
        request_args = {'region': region, 'scrape_type': scrape_type.value,
                        'timezone': 'America/Los_Angeles'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get('/start',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 200

        mock_docket.assert_called_with(scrape_key, '', '')
        mock_tracker.assert_called_with(scrape_key)
        mock_sessions.assert_called_with(scrape_key)
        mock_region.assert_called_with('us_wy')
        mock_supported.assert_called_with(timezone='America/Los_Angeles')

    @patch("recidiviz.utils.regions.get_supported_region_codes")
    def test_start_unsupported_region(self, mock_supported):
        mock_supported.return_value = ['us_ny', 'us_pa']

        request_args = {'region': 'us_ca', 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get('/start',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 400
        assert response.get_data().decode() == \
            "Missing or invalid parameters, see service logs."

        mock_supported.assert_called_with(timezone=None)


class TestScraperStop:
    """Tests for requests to the Scraper Stop API."""

    # noinspection PyAttributeOutsideInit
    def setup_method(self, _test_method):
        self.client = app.test_client()

    @patch("recidiviz.utils.regions.get_supported_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.end_session")
    def test_stop(self, mock_sessions, mock_region, mock_supported):
        mock_sessions.return_value = None
        mock_region.return_value = FakeRegion(FakeScraper())
        mock_supported.return_value = ['us_ca', 'us_ut']

        request_args = {'region': 'all', 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get('/stop',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 200

        mock_sessions.assert_has_calls([
            call(ScrapeKey('us_ca', constants.ScrapeType.BACKGROUND)),
            call(ScrapeKey('us_ca', constants.ScrapeType.SNAPSHOT)),
            call(ScrapeKey('us_ut', constants.ScrapeType.BACKGROUND)),
            call(ScrapeKey('us_ut', constants.ScrapeType.SNAPSHOT))])
        mock_region.assert_has_calls([call('us_ca'), call('us_ut')])
        mock_supported.assert_called_with(timezone=None)

    @patch("recidiviz.utils.regions.get_supported_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.end_session")
    def test_stop_timezone(self, mock_sessions, mock_region, mock_supported):
        mock_sessions.return_value = None
        mock_region.return_value = FakeRegion(FakeScraper())
        mock_supported.side_effect = _MockSupported

        request_args = {
            'region': 'all',
            'scrape_type': 'all',
            'timezone': 'America/New_York'
        }
        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get('/stop',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 200

        mock_sessions.assert_has_calls([
            call(ScrapeKey('us_ut', constants.ScrapeType.BACKGROUND)),
            call(ScrapeKey('us_ut', constants.ScrapeType.SNAPSHOT))])
        mock_region.assert_has_calls([call('us_ut')])
        mock_supported.assert_called_with(timezone='America/New_York')

    @patch("recidiviz.utils.regions.get_supported_region_codes")
    def test_stop_unsupported_region(self, mock_supported):
        mock_supported.return_value = ['us_ny', 'us_pa']

        request_args = {'region': 'us_ca', 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get('/stop',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 400
        assert response.get_data().decode() == \
            "Missing or invalid parameters, see service logs."

        mock_supported.assert_called_with(timezone=None)


class TestScraperResume:
    """Tests for requests to the Scraper Resume API."""

    # noinspection PyAttributeOutsideInit
    def setup_method(self, _test_method):
        self.client = app.test_client()

    @patch("recidiviz.utils.regions.get_supported_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.create_session")
    def test_resume(self, mock_sessions, mock_region, mock_supported):
        mock_sessions.return_value = None
        mock_region.return_value = FakeRegion(FakeScraper())
        mock_supported.return_value = ['us_ca']

        region = 'us_ca'
        request_args = {'region': region, 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get('/resume',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 200

        mock_sessions.assert_has_calls(
            [call(ScrapeKey(region, constants.ScrapeType.BACKGROUND)),
             call(ScrapeKey(region, constants.ScrapeType.SNAPSHOT))])
        mock_region.assert_called_with(region)
        mock_supported.assert_called_with(timezone=None)

    @patch("recidiviz.utils.regions.get_supported_region_codes")
    def test_resume_unsupported_region(self, mock_supported):
        mock_supported.return_value = ['us_ny', 'us_pa']

        request_args = {'region': 'us_ca', 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get('/resume',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 400
        assert response.get_data().decode() == \
            "Missing or invalid parameters, see service logs."

        mock_supported.assert_called_with(timezone=None)


class FakeRegion:
    """A fake region to be returned from mocked out calls to Region"""
    def __init__(self, scraper):
        self.scraper = scraper

    def get_scraper(self):
        return self.scraper


class FakeScraper:
    """A fake scraper to be returned from mocked out calls to
    Region.get_scraper"""

    def start_scrape(self, scrape_type):
        return scrape_type

    def stop_scrape(self, scrape_types):
        return scrape_types

    def resume_scrape(self, scrape_type):
        return scrape_type
