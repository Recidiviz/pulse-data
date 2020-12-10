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
import unittest

import pytz
from flask import Flask
from mock import Mock, call, patch, create_autospec

from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape import (constants, scrape_phase, scraper_control,
                                     sessions)
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.persistence import batch_persistence
from recidiviz.tests.utils.fake_region import fake_region
from recidiviz.tests.utils.thread_pool import SerialExecutor


def create_test_client():
    app = Flask(__name__)
    app.register_blueprint(scraper_control.scraper_control)
    # Include so that flask can get the url of `infer_release`.
    app.register_blueprint(batch_persistence.batch_blueprint)
    app.config['TESTING'] = True

    return app.test_client()


def _MockSupported(timezone=None, stripes=None):
    del stripes  # this is added to match the mocking arguments
    if not timezone:
        regions = ['us_ut', 'us_wy']
    elif timezone == pytz.timezone('America/New_York'):
        regions = ['us_ut']
    else:
        regions = ['us_wy']
    return regions


@patch("concurrent.futures.ThreadPoolExecutor", SerialExecutor)
@patch('recidiviz.utils.metadata.project_id', Mock(return_value='test-project'))
@patch('recidiviz.utils.metadata.project_number', Mock(return_value='123456789'))
class TestScraperStart(unittest.TestCase):
    """Tests for requests to the Scraper Start API."""

    def setUp(self) -> None:
        self.client = create_test_client()

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_supported_scrape_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.update_phase")
    @patch("recidiviz.ingest.scrape.sessions.create_session")
    @patch("recidiviz.ingest.scrape.tracker.purge_docket_and_session")
    @patch("recidiviz.ingest.scrape.docket.load_target_list")
    @patch("recidiviz.ingest.scrape.sessions.get_sessions")
    @patch("recidiviz.utils.pubsub_helper.purge")
    def test_start(
            self, mock_purge, mock_get_sessions, mock_docket, mock_tracker,
            mock_create_session, mock_update_phase, mock_region,
            mock_supported,
            mock_environment):
        """Tests that the start operation chains together the correct calls."""
        mock_purge.return_value = None
        mock_docket.return_value = None
        mock_tracker.return_value = None
        mock_get_sessions.return_value = iter([])
        mock_scraper = create_autospec(BaseScraper)
        mock_region.return_value = fake_region(environment='production',
                                               ingestor=mock_scraper)
        mock_environment.return_value = 'production'
        mock_supported.side_effect = _MockSupported

        region = 'us_ut'
        scrape_type = constants.ScrapeType.BACKGROUND
        scrape_key = ScrapeKey(region, scrape_type)
        request_args = {'region': region, 'scrape_type': scrape_type.value}
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/start',
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
        mock_scraper.start_scrape.assert_called()
        mock_supported.assert_called_with(stripes=[], timezone=None)

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_supported_scrape_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.update_phase")
    @patch("recidiviz.ingest.scrape.sessions.create_session")
    @patch("recidiviz.ingest.scrape.tracker.purge_docket_and_session")
    @patch("recidiviz.ingest.scrape.docket.load_target_list")
    @patch("recidiviz.ingest.scrape.sessions.get_sessions")
    @patch("recidiviz.utils.pubsub_helper.purge")
    def test_start_timezone(
            self, mock_purge, mock_get_sessions, mock_docket, mock_tracker,
            mock_create_session, mock_update_phase, mock_region,
            mock_supported,
            mock_environment):
        """Tests that the start operation chains together the correct calls."""
        mock_docket.return_value = None
        mock_tracker.return_value = None
        mock_get_sessions.return_value = iter([])
        mock_purge.return_value = None
        mock_environment.return_value = 'production'
        mock_scraper = create_autospec(BaseScraper)
        mock_region.return_value = fake_region(environment='production',
                                               ingestor=mock_scraper)
        mock_supported.side_effect = _MockSupported

        region = 'all'
        scrape_type = constants.ScrapeType.BACKGROUND
        scrape_key = ScrapeKey('us_wy', scrape_type)
        request_args = {'region': region, 'scrape_type': scrape_type.value,
                        'timezone': 'America/Los_Angeles'}
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/start',
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
        mock_scraper.start_scrape.assert_called()
        mock_supported.assert_called_with(stripes=[],
                                          timezone=pytz.timezone('America/Los_Angeles'))

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_supported_scrape_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.create_session")
    @patch("recidiviz.ingest.scrape.tracker.purge_docket_and_session")
    @patch("recidiviz.ingest.scrape.docket.load_target_list")
    @patch("recidiviz.ingest.scrape.sessions.get_sessions")
    @patch("recidiviz.utils.pubsub_helper.purge")
    def test_start_all_diff_environment(
            self, mock_purge, mock_get_sessions, mock_docket, mock_tracker,
            mock_create_session, mock_region, mock_supported, mock_environment):
        """Tests that the start operation chains together the correct calls."""
        mock_environment.return_value = 'staging'
        mock_scraper = create_autospec(BaseScraper)
        mock_region.return_value = fake_region(environment='production',
                                               ingestor=mock_scraper)
        mock_supported.side_effect = _MockSupported

        region = 'all'
        scrape_type = constants.ScrapeType.BACKGROUND
        request_args = {'region': region, 'scrape_type': scrape_type.value}
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/start',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 400

        assert not mock_get_sessions.called
        assert not mock_docket.called
        assert not mock_tracker.called
        assert not mock_create_session.called
        assert not mock_purge.called
        mock_region.assert_called_with('us_wy')
        mock_scraper.start_scrape.assert_not_called()
        mock_supported.assert_called_with(stripes=[], timezone=None)

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_supported_scrape_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.create_session")
    @patch("recidiviz.ingest.scrape.tracker.purge_docket_and_session")
    @patch("recidiviz.ingest.scrape.docket.load_target_list")
    @patch("recidiviz.ingest.scrape.sessions.get_sessions")
    @patch("recidiviz.utils.pubsub_helper.purge")
    def test_start_existing_session(
            self, mock_purge, mock_get_sessions, mock_docket,
            mock_tracker, mock_create_session, mock_region, mock_supported,
            mock_environment):
        """Tests that the start operation halts if an open session exists."""
        region = 'us_ut'

        mock_get_sessions.return_value = iter([sessions.ScrapeSession.new(
            key=None, region=region,
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.SCRAPE
        )])
        mock_scraper = create_autospec(BaseScraper)
        mock_region.return_value = fake_region(environment='production',
                                               ingestor=mock_scraper)
        mock_environment.return_value = 'production'
        mock_supported.side_effect = _MockSupported

        scrape_type = constants.ScrapeType.BACKGROUND
        request_args = {'region': region, 'scrape_type': scrape_type.value}
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/start', query_string=request_args,
                                   headers=headers)
        assert response.status_code == 500
        assert not mock_purge.called
        assert not mock_create_session.called
        assert not mock_tracker.called
        assert not mock_docket.called
        assert not mock_region.called
        mock_supported.assert_called_with(stripes=[], timezone=None)
        mock_scraper.start_scrape.assert_not_called()

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_supported_scrape_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.update_phase")
    @patch("recidiviz.ingest.scrape.sessions.create_session")
    @patch("recidiviz.ingest.scrape.tracker.purge_docket_and_session")
    @patch("recidiviz.ingest.scrape.docket.load_target_list")
    @patch("recidiviz.ingest.scrape.sessions.get_sessions")
    @patch("recidiviz.utils.pubsub_helper.purge")
    def test_start_existing_session_release(
            self, mock_purge, mock_get_sessions, mock_docket, mock_tracker,
            mock_create_session, mock_update_phase, mock_region,
            mock_supported,
            mock_environment):
        """Tests that the start operation runs when there is a session running
        infer release."""
        region = 'us_ut'

        mock_get_sessions.return_value = iter([sessions.ScrapeSession.new(
            key=None, region=region,
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.RELEASE
        )])
        mock_scraper = create_autospec(BaseScraper)
        mock_region.return_value = fake_region(environment='production',
                                               ingestor=mock_scraper)
        mock_environment.return_value = 'production'
        mock_supported.side_effect = _MockSupported

        scrape_type = constants.ScrapeType.BACKGROUND
        scrape_key = ScrapeKey(region, scrape_type)
        request_args = {'region': region, 'scrape_type': scrape_type.value}
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/start',
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
        mock_scraper.start_scrape.assert_called()
        mock_supported.assert_called_with(stripes=[], timezone=None)

    @patch("recidiviz.utils.regions.get_supported_scrape_region_codes")
    def test_start_unsupported_region(self, mock_supported):
        mock_supported.return_value = ['us_ny', 'us_pa']

        request_args = {'region': 'us_ca', 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/start',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 400
        assert response.get_data().decode() == \
            "Missing or invalid parameters, or no regions found, see logs."

        mock_supported.assert_called_with(stripes=[], timezone=None)


@patch("concurrent.futures.ThreadPoolExecutor", SerialExecutor)
@patch('recidiviz.utils.metadata.project_id', Mock(return_value='test-project'))
@patch('recidiviz.utils.metadata.project_number', Mock(return_value='123456789'))
class TestScraperStop(unittest.TestCase):
    """Tests for requests to the Scraper Stop API."""

    def setUp(self) -> None:
        self.client = create_test_client()

    @patch("recidiviz.utils.regions.get_supported_scrape_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.scraper_control.ScraperCloudTaskManager")
    @patch("recidiviz.ingest.scrape.sessions.update_phase")
    @patch("recidiviz.ingest.scrape.sessions.close_session")
    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    def test_stop(
            self, mock_get_session, mock_sessions, mock_phase,
            mock_task_manager, mock_region, mock_supported):
        session = sessions.ScrapeSession.new(
            key=None, region='us_xx',
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.SCRAPE
        )
        mock_get_session.return_value = session
        mock_sessions.return_value = [session]
        mock_scraper = create_autospec(BaseScraper)
        mock_region.return_value = fake_region(ingestor=mock_scraper)
        mock_supported.return_value = ['us_ca', 'us_ut']

        request_args = {'region': 'all', 'scrape_type': 'all',
                        'respect_is_stoppable': 'false'}
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/stop',
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
        self.assertEqual(mock_scraper.stop_scrape.mock_calls, [
            call(constants.ScrapeType.BACKGROUND, 'false'),
            call().__bool__(),
            call(constants.ScrapeType.SNAPSHOT, 'false'),
            call().__bool__(),
            call(constants.ScrapeType.BACKGROUND, 'false'),
            call().__bool__(),
            call(constants.ScrapeType.SNAPSHOT, 'false'),
            call().__bool__(),
        ])
        mock_supported.assert_called_with(stripes=[], timezone=None)
        mock_task_manager.return_value.create_scraper_phase_task.\
            assert_has_calls([
                call(region_code='us_ca', url='/read_and_persist'),
                call(region_code='us_ut', url='/read_and_persist'),
            ], any_order=True)

    @patch("recidiviz.utils.regions.get_supported_scrape_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.scraper_control.ScraperCloudTaskManager")
    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    def test_stop_no_session(
            self,
            mock_sessions,
            mock_task_manager,
            mock_region,
            mock_supported):
        mock_sessions.return_value = None
        mock_scraper = create_autospec(BaseScraper)
        mock_region.return_value = fake_region(ingestor=mock_scraper)
        mock_supported.return_value = ['us_ca', 'us_ut']

        request_args = {'region': 'all', 'scrape_type': 'all',
                        'respect_is_stoppable': 'false'}

        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/stop',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 200

        mock_sessions.assert_has_calls(
            [call(ScrapeKey('us_ca', constants.ScrapeType.BACKGROUND)),
             call(ScrapeKey('us_ca', constants.ScrapeType.SNAPSHOT)),
             call(ScrapeKey('us_ut', constants.ScrapeType.BACKGROUND)),
             call(ScrapeKey('us_ut', constants.ScrapeType.SNAPSHOT))],
            any_order=True)
        mock_scraper.stop_scrape.assert_not_called()
        mock_supported.assert_called_with(stripes=[], timezone=None)
        mock_task_manager.return_value.create_scraper_phase_task.\
            assert_not_called()

    @patch("recidiviz.utils.regions.get_supported_scrape_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.scraper_control.ScraperCloudTaskManager")
    @patch("recidiviz.ingest.scrape.sessions.update_phase")
    @patch("recidiviz.ingest.scrape.sessions.close_session")
    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    def test_stop_timezone(
            self, mock_sessions, mock_close, mock_phase, mock_task_manager,
            mock_region, mock_supported):
        session = sessions.ScrapeSession.new(
            key=None, region='us_ut',
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.SCRAPE
        )
        mock_sessions.return_value = session
        mock_close.return_value = [session]
        mock_scraper = create_autospec(BaseScraper)
        mock_region.return_value = fake_region(ingestor=mock_scraper)
        mock_supported.side_effect = _MockSupported

        request_args = {
            'region': 'all',
            'scrape_type': 'all',
            'timezone': 'America/New_York',
            'respect_is_stoppable': 'false',
        }
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/stop',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 200

        mock_sessions.assert_has_calls([
            call(ScrapeKey('us_ut', constants.ScrapeType.BACKGROUND)),
            call(ScrapeKey('us_ut', constants.ScrapeType.SNAPSHOT))])
        mock_phase.assert_has_calls(
            [call(session, scrape_phase.ScrapePhase.PERSIST)] * 2)
        mock_region.assert_has_calls([call('us_ut')])
        mock_scraper.stop_scrape. \
            assert_called_with(
                constants.ScrapeType.SNAPSHOT, 'false')
        mock_supported.assert_called_with(stripes=[],
                                          timezone=pytz.timezone('America/New_York'))
        mock_task_manager.return_value.create_scraper_phase_task.\
            assert_called_with(region_code='us_ut', url='/read_and_persist')

    @patch("recidiviz.ingest.scrape.scraper_control.ScraperCloudTaskManager")
    @patch("recidiviz.utils.regions.get_supported_scrape_region_codes")
    def test_stop_unsupported_region(
            self, mock_supported, mock_task_manager):
        mock_supported.return_value = ['us_ny', 'us_pa']

        request_args = {'region': 'us_ca', 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/stop',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 400
        assert response.get_data().decode() == \
            "Missing or invalid parameters, see service logs."

        mock_supported.assert_called_with(stripes=[], timezone=None)
        mock_task_manager.return_value.create_scraper_phase_task.\
            assert_not_called()

    @patch("recidiviz.utils.regions.get_supported_scrape_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.scraper_control.ScraperCloudTaskManager")
    @patch("recidiviz.ingest.scrape.sessions.update_phase")
    @patch("recidiviz.ingest.scrape.sessions.close_session")
    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    def test_stop_respects_region_is_not_stoppable(
            self, mock_sessions, mock_close, mock_phase, mock_task_manager,
            mock_region, mock_supported):
        session = sessions.ScrapeSession.new(
            key=None, region='us_xx',
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.SCRAPE
        )
        mock_sessions.return_value = session
        mock_close.return_value = [session]
        mock_scraper = create_autospec(BaseScraper)
        mock_region.return_value = fake_region(ingestor=mock_scraper)
        mock_region.return_value.is_stoppable = False
        mock_supported.return_value = ['us_ca', 'us_ut']

        request_args = {'region': 'all', 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/stop',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 200

        mock_sessions.assert_has_calls(
            [call(ScrapeKey('us_ca', constants.ScrapeType.BACKGROUND)),
             call(ScrapeKey('us_ca', constants.ScrapeType.SNAPSHOT)),
             call(ScrapeKey('us_ut', constants.ScrapeType.BACKGROUND)),
             call(ScrapeKey('us_ut', constants.ScrapeType.SNAPSHOT))])
        mock_phase.assert_has_calls(
            [call(session, scrape_phase.ScrapePhase.PERSIST)] * 4)
        assert mock_scraper.stop_scrape.mock_calls == [
            call(constants.ScrapeType.BACKGROUND, None),
            call().__bool__(),
            call(constants.ScrapeType.SNAPSHOT, None),
            call().__bool__(),
            call(constants.ScrapeType.BACKGROUND, None),
            call().__bool__(),
            call(constants.ScrapeType.SNAPSHOT, None),
            call().__bool__(),
        ]

        mock_supported.assert_called_with(stripes=[], timezone=None)
        mock_task_manager.return_value.create_scraper_phase_task.\
            assert_has_calls([
                call(region_code='us_ca', url='/read_and_persist'),
                call(region_code='us_ut', url='/read_and_persist'),
            ], any_order=True)


@patch('recidiviz.utils.metadata.project_id', Mock(return_value='test-project'))
@patch('recidiviz.utils.metadata.project_number', Mock(return_value='123456789'))
class TestScraperResume(unittest.TestCase):
    """Tests for requests to the Scraper Resume API."""

    def setUp(self) -> None:
        self.client = create_test_client()

    @patch("recidiviz.utils.regions.get_supported_scrape_region_codes")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.create_session")
    def test_resume(self, mock_sessions, mock_region, mock_supported):
        mock_sessions.return_value = None
        mock_scraper = create_autospec(BaseScraper)
        mock_region.return_value = fake_region(ingestor=mock_scraper)
        mock_supported.return_value = ['us_ca']

        region = 'us_ca'
        request_args = {'region': region, 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/resume',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 200

        mock_sessions.assert_has_calls(
            [call(ScrapeKey(region, constants.ScrapeType.BACKGROUND)),
             call(ScrapeKey(region, constants.ScrapeType.SNAPSHOT))])
        mock_region.assert_called_with(region)
        mock_scraper.resume_scrape.assert_called()
        mock_supported.assert_called_with(stripes=None, timezone=None)

    @patch("recidiviz.utils.regions.get_supported_scrape_region_codes")
    def test_resume_unsupported_region(self, mock_supported):
        mock_supported.return_value = ['us_ny', 'us_pa']

        request_args = {'region': 'us_ca', 'scrape_type': 'all'}
        headers = {'X-Appengine-Cron': 'test-cron'}
        response = self.client.get('/resume',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 400
        assert response.get_data().decode() == \
            "Missing or invalid parameters, see service logs."

        mock_supported.assert_called_with(stripes=None, timezone=None)
