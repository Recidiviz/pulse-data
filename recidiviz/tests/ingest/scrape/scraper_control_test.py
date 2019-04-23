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

"""Tests for ingest/scraper_control.py."""
import pytest
import pytz
from flask import Flask
from mock import call, create_autospec, patch

from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape import (constants, scrape_phase, scraper_control,
                                     sessions)
from recidiviz.persistence import batch_persistence
from recidiviz.utils.regions import Region


# pylint: disable=redefined-outer-name
@pytest.fixture
def client():
    app = Flask(__name__)
    app.register_blueprint(scraper_control.scraper_control)
    # Include so that flask can get the url of `infer_release`.
    app.register_blueprint(batch_persistence.batch_blueprint)
    app.config['TESTING'] = True

    yield app.test_client()


def _MockSupported(timezone=None):
    if not timezone:
        regions = ['us_ut', 'us_wy']
    elif timezone == pytz.timezone('America/New_York'):
        regions = ['us_ut']
    else:
        regions = ['us_wy']
    return regions


class TestScraperStart:
    """Tests for requests to the Scraper Start API."""

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_supported_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.update_phase")
    @patch("recidiviz.ingest.scrape.sessions.create_session")
    @patch("recidiviz.ingest.scrape.tracker.purge_docket_and_session")
    @patch("recidiviz.ingest.scrape.docket.load_target_list")
    @patch("recidiviz.ingest.scrape.sessions.get_sessions")
    @patch("recidiviz.utils.pubsub_helper.purge")
    def test_start(
            self, mock_purge, mock_get_sessions, mock_docket, mock_tracker,
            mock_create_session, mock_update_phase, mock_region, mock_supported,
            mock_environment, client):
        """Tests that the start operation chains together the correct calls."""
        mock_purge.return_value = None
        mock_docket.return_value = None
        mock_tracker.return_value = None
        mock_get_sessions.return_value = iter([])
        mock_region.return_value = fake_region(environment='production')
        mock_environment.return_value = 'production'
        mock_supported.side_effect = _MockSupported

        region = 'us_ut'
        scrape_type = constants.ScrapeType.BACKGROUND
        scrape_key = ScrapeKey(region, scrape_type)
        request_args = {'region': region, 'scrape_type': scrape_type.value}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/start',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 200

        mock_purge.assert_called_with(scrape_key, 'scraper_batch')
        mock_docket.assert_called_with(scrape_key, '', '')
        mock_tracker.assert_called_with(scrape_key)
        mock_create_session.assert_called_with(scrape_key)
        mock_update_phase.assert_called_with(mock_create_session.return_value,
                                             scrape_phase.ScrapePhase.SCRAPE)
        mock_region.assert_called_with('us_ut')
        mock_supported.assert_called_with(timezone=None)

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_supported_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.update_phase")
    @patch("recidiviz.ingest.scrape.sessions.create_session")
    @patch("recidiviz.ingest.scrape.tracker.purge_docket_and_session")
    @patch("recidiviz.ingest.scrape.docket.load_target_list")
    @patch("recidiviz.ingest.scrape.sessions.get_sessions")
    @patch("recidiviz.utils.pubsub_helper.purge")
    def test_start_timezone(
            self, mock_purge, mock_get_sessions, mock_docket, mock_tracker,
            mock_create_session, mock_update_phase, mock_region, mock_supported,
            mock_environment, client):
        """Tests that the start operation chains together the correct calls."""
        mock_docket.return_value = None
        mock_tracker.return_value = None
        mock_get_sessions.return_value = iter([])
        mock_purge.return_value = None
        mock_environment.return_value = 'production'
        mock_region.return_value = fake_region(environment='production')
        mock_supported.side_effect = _MockSupported

        region = 'all'
        scrape_type = constants.ScrapeType.BACKGROUND
        scrape_key = ScrapeKey('us_wy', scrape_type)
        request_args = {'region': region, 'scrape_type': scrape_type.value,
                        'timezone': 'America/Los_Angeles'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/start',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 200

        mock_purge.assert_called_with(scrape_key, 'scraper_batch')
        mock_docket.assert_called_with(scrape_key, '', '')
        mock_tracker.assert_called_with(scrape_key)
        mock_create_session.assert_called_with(scrape_key)
        mock_update_phase.assert_called_with(mock_create_session.return_value,
                                             scrape_phase.ScrapePhase.SCRAPE)
        mock_region.assert_called_with('us_wy')
        mock_supported.assert_called_with(
            timezone=pytz.timezone('America/Los_Angeles'))

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_supported_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.create_session")
    @patch("recidiviz.ingest.scrape.tracker.purge_docket_and_session")
    @patch("recidiviz.ingest.scrape.docket.load_target_list")
    @patch("recidiviz.ingest.scrape.sessions.get_sessions")
    @patch("recidiviz.utils.pubsub_helper.purge")
    def test_start_all_diff_environment(
            self, mock_purge, mock_get_sessions, mock_docket, mock_tracker,
            mock_create_session, mock_region, mock_supported, mock_environment,
            client):
        """Tests that the start operation chains together the correct calls."""
        mock_environment.return_value = 'staging'
        mock_region.return_value = fake_region(environment='production')
        mock_supported.side_effect = _MockSupported

        region = 'all'
        scrape_type = constants.ScrapeType.BACKGROUND
        request_args = {'region': region, 'scrape_type': scrape_type.value}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/start',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 400

        assert not mock_get_sessions.called
        assert not mock_docket.called
        assert not mock_tracker.called
        assert not mock_create_session.called
        assert not mock_purge.called
        mock_region.assert_called_with('us_wy')
        mock_supported.assert_called_with(timezone=None)

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_supported_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.create_session")
    @patch("recidiviz.ingest.scrape.tracker.purge_docket_and_session")
    @patch("recidiviz.ingest.scrape.docket.load_target_list")
    @patch("recidiviz.ingest.scrape.sessions.get_sessions")
    @patch("recidiviz.utils.pubsub_helper.purge")
    def test_start_existing_session(
            self, mock_purge, mock_get_sessions, mock_docket,
            mock_tracker, mock_create_session, mock_region, mock_supported,
            mock_environment, client):
        """Tests that the start operation halts if an open session exists."""
        region = 'us_ut'

        mock_get_sessions.return_value = iter([sessions.ScrapeSession.new(
            key=None, region=region,
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.SCRAPE
        )])
        mock_region.return_value = fake_region(environment='production')
        mock_environment.return_value = 'production'
        mock_supported.side_effect = _MockSupported

        scrape_type = constants.ScrapeType.BACKGROUND
        request_args = {'region': region, 'scrape_type': scrape_type.value}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/start', query_string=request_args,
                              headers=headers)
        assert response.status_code == 500
        assert not mock_purge.called
        assert not mock_create_session.called
        assert not mock_tracker.called
        assert not mock_docket.called
        assert not mock_region.called
        mock_supported.assert_called_with(timezone=None)

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_supported_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.update_phase")
    @patch("recidiviz.ingest.scrape.sessions.create_session")
    @patch("recidiviz.ingest.scrape.tracker.purge_docket_and_session")
    @patch("recidiviz.ingest.scrape.docket.load_target_list")
    @patch("recidiviz.ingest.scrape.sessions.get_sessions")
    @patch("recidiviz.utils.pubsub_helper.purge")
    def test_start_existing_session_release(
            self, mock_purge, mock_get_sessions, mock_docket, mock_tracker,
            mock_create_session, mock_update_phase, mock_region, mock_supported,
            mock_environment, client):
        """Tests that the start operation runs when there is a session running
        infer release."""
        region = 'us_ut'

        mock_get_sessions.return_value = iter([sessions.ScrapeSession.new(
            key=None, region=region,
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.RELEASE
        )])
        mock_region.return_value = fake_region(environment='production')
        mock_environment.return_value = 'production'
        mock_supported.side_effect = _MockSupported

        scrape_type = constants.ScrapeType.BACKGROUND
        scrape_key = ScrapeKey(region, scrape_type)
        request_args = {'region': region, 'scrape_type': scrape_type.value}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/start',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 200

        mock_purge.assert_called_with(scrape_key, 'scraper_batch')
        mock_docket.assert_called_with(scrape_key, '', '')
        mock_tracker.assert_called_with(scrape_key)
        mock_create_session.assert_called_with(scrape_key)
        mock_update_phase.assert_called_with(mock_create_session.return_value,
                                             scrape_phase.ScrapePhase.SCRAPE)
        mock_region.assert_called_with('us_ut')
        mock_supported.assert_called_with(timezone=None)

    @patch("recidiviz.utils.regions.get_supported_region_codes")
    def test_start_unsupported_region(self, mock_supported, client):
        mock_supported.return_value = ['us_ny', 'us_pa']

        request_args = {'region': 'us_ca', 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/start',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 400
        assert response.get_data().decode() == \
               "Missing or invalid parameters, or no regions found, see logs."

        mock_supported.assert_called_with(timezone=None)


class TestScraperStop:
    """Tests for requests to the Scraper Stop API."""

    @patch("recidiviz.utils.regions.get_supported_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.common.queues.enqueue_scraper_phase")
    @patch("recidiviz.ingest.scrape.sessions.update_phase")
    @patch("recidiviz.ingest.scrape.sessions.close_session")
    def test_stop(
            self, mock_sessions, mock_phase, mock_enqueue, mock_region,
            mock_supported, client):
        session = sessions.ScrapeSession.new(
            key=None, region='us_xx',
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.SCRAPE
        )
        mock_sessions.return_value = [session]
        mock_region.return_value = fake_region()
        mock_supported.return_value = ['us_ca', 'us_ut']

        request_args = {'region': 'all', 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/stop',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 200

        mock_sessions.assert_has_calls(
            [call(ScrapeKey('us_ca', constants.ScrapeType.BACKGROUND)),
             call(ScrapeKey('us_ca', constants.ScrapeType.SNAPSHOT)),
             call(ScrapeKey('us_ut', constants.ScrapeType.BACKGROUND)),
             call(ScrapeKey('us_ut', constants.ScrapeType.SNAPSHOT))],
            any_order=True)
        mock_phase.assert_has_calls(
            [call(session, scrape_phase.ScrapePhase.PERSIST)] * 4)
        mock_region.return_value.get_scraper().stop_scrape.assert_has_calls([
            call([constants.ScrapeType.BACKGROUND,
                  constants.ScrapeType.SNAPSHOT]),
            call([constants.ScrapeType.BACKGROUND,
                  constants.ScrapeType.SNAPSHOT])
        ])
        mock_supported.assert_called_with(timezone=None)
        mock_enqueue.assert_has_calls([
            call(region_code='us_ca', url='/read_and_persist'),
            call(region_code='us_ut', url='/read_and_persist'),
        ], any_order=True)

    @patch("recidiviz.utils.regions.get_supported_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.common.queues.enqueue_scraper_phase")
    @patch("recidiviz.ingest.scrape.sessions.close_session")
    def test_stop_no_session(
            self, mock_sessions, mock_enqueue, mock_region, mock_supported,
            client):
        mock_sessions.return_value = []
        mock_region.return_value = fake_region()
        mock_supported.return_value = ['us_ca', 'us_ut']

        request_args = {'region': 'all', 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/stop',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 200

        mock_sessions.assert_has_calls(
            [call(ScrapeKey('us_ca', constants.ScrapeType.BACKGROUND)),
             call(ScrapeKey('us_ca', constants.ScrapeType.SNAPSHOT)),
             call(ScrapeKey('us_ut', constants.ScrapeType.BACKGROUND)),
             call(ScrapeKey('us_ut', constants.ScrapeType.SNAPSHOT))],
            any_order=True)
        assert not mock_region.return_value.get_scraper().stop_scrape.called
        mock_supported.assert_called_with(timezone=None)
        assert not mock_enqueue.called

    @patch("recidiviz.utils.regions.get_supported_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.common.queues.enqueue_scraper_phase")
    @patch("recidiviz.ingest.scrape.sessions.update_phase")
    @patch("recidiviz.ingest.scrape.sessions.close_session")
    def test_stop_timezone(
            self, mock_sessions, mock_phase, mock_enqueue, mock_region,
            mock_supported, client):
        session = sessions.ScrapeSession.new(
            key=None, region='us_ut',
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.SCRAPE
        )
        mock_sessions.return_value = [session]
        mock_region.return_value = fake_region()
        mock_supported.side_effect = _MockSupported

        request_args = {
            'region': 'all',
            'scrape_type': 'all',
            'timezone': 'America/New_York'
        }
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/stop',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 200

        mock_sessions.assert_has_calls([
            call(ScrapeKey('us_ut', constants.ScrapeType.BACKGROUND)),
            call(ScrapeKey('us_ut', constants.ScrapeType.SNAPSHOT))])
        mock_phase.assert_has_calls(
            [call(session, scrape_phase.ScrapePhase.PERSIST)] * 2)
        mock_region.assert_has_calls([call('us_ut')])
        mock_region.return_value.get_scraper().stop_scrape.assert_called_with([
            constants.ScrapeType.BACKGROUND, constants.ScrapeType.SNAPSHOT
        ])
        mock_supported.assert_called_with(
            timezone=pytz.timezone('America/New_York'))
        mock_enqueue.assert_called_with(
            region_code='us_ut', url='/read_and_persist')

    @patch("recidiviz.common.queues.enqueue_scraper_phase")
    @patch("recidiviz.utils.regions.get_supported_region_codes")
    def test_stop_unsupported_region(
            self, mock_supported, mock_enqueue, client):
        mock_supported.return_value = ['us_ny', 'us_pa']

        request_args = {'region': 'us_ca', 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/stop',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 400
        assert response.get_data().decode() == \
               "Missing or invalid parameters, see service logs."

        mock_supported.assert_called_with(timezone=None)
        assert not mock_enqueue.called


class TestScraperResume:
    """Tests for requests to the Scraper Resume API."""

    @patch("recidiviz.utils.regions.get_supported_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.create_session")
    def test_resume(self, mock_sessions, mock_region, mock_supported, client):
        mock_sessions.return_value = None
        mock_region.return_value = fake_region()
        mock_supported.return_value = ['us_ca']

        region = 'us_ca'
        request_args = {'region': region, 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/resume',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 200

        mock_sessions.assert_has_calls(
            [call(ScrapeKey(region, constants.ScrapeType.BACKGROUND)),
             call(ScrapeKey(region, constants.ScrapeType.SNAPSHOT))])
        mock_region.assert_called_with(region)
        mock_supported.assert_called_with(timezone=None)

    @patch("recidiviz.utils.regions.get_supported_region_codes")
    def test_resume_unsupported_region(self, mock_supported, client):
        mock_supported.return_value = ['us_ny', 'us_pa']

        request_args = {'region': 'us_ca', 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/resume',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 400
        assert response.get_data().decode() == \
               "Missing or invalid parameters, see service logs."

        mock_supported.assert_called_with(timezone=None)


def fake_region(environment='local'):
    region = create_autospec(Region)
    region.environment = environment
    return region
