# Recidiviz - a platform for tracking granular recidivism metrics in real time
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

"""Tests for scraper_status.py"""
import pytest

from flask import Flask
from mock import call, create_autospec, patch

from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scrape import constants, scraper_control, scraper_status
from recidiviz.utils.regions import Region

# pylint: disable=redefined-outer-name
@pytest.fixture
def client():
    app = Flask(__name__)
    app.register_blueprint(scraper_status.scraper_status)
    # Include so that flask can get the url of `stop`.
    app.register_blueprint(scraper_control.scraper_control)
    app.config['TESTING'] = True

    yield app.test_client()

@patch("recidiviz.ingest.scrape.queues.enqueue_scraper_phase")
@patch("recidiviz.ingest.scrape.queues.list_tasks")
@patch("recidiviz.ingest.scrape.sessions.get_current_session")
@patch("recidiviz.ingest.scrape.ingest_utils.validate_regions")
@patch("recidiviz.utils.regions.get_region")
def test_check_for_finished_scrapers(
        mock_region, mock_validate_regions, mock_session, mock_list_tasks,
        mock_enqueue, client):
    mock_validate_regions.return_value = ['region_x', 'region_y']
    mock_session.side_effect = ['region_x_session', None]

    fake_region_x = create_autospec(Region)
    fake_region_x.region_code = 'region_x'
    fake_region_x.get_queue_name.return_value = 'queue'
    mock_region.side_effect = [fake_region_x]

    mock_list_tasks.return_value = []

    request_args = {'region': 'all'}
    headers = {'X-Appengine-Cron': "test-cron"}
    response = client.get('/check_finished',
                          query_string=request_args,
                          headers=headers)
    assert response.status_code == 200

    mock_validate_regions.assert_called_with(['all'])
    mock_session.assert_has_calls([
        call(ScrapeKey('region_x', constants.ScrapeType.BACKGROUND)),
        call(ScrapeKey('region_y', constants.ScrapeType.BACKGROUND)),
    ])
    mock_region.assert_called_with('region_x')
    mock_list_tasks.assert_called_with(
        region_code='region_x', queue_name='queue')
    mock_enqueue.assert_called_with(region_code='region_x', url='/stop')

@patch("recidiviz.ingest.scrape.queues.enqueue_scraper_phase")
@patch("recidiviz.ingest.scrape.queues.list_tasks")
@patch("recidiviz.ingest.scrape.sessions.get_current_session")
@patch("recidiviz.ingest.scrape.ingest_utils.validate_regions")
@patch("recidiviz.utils.regions.get_region")
def test_check_for_finished_scrapers_not_done(
        mock_region, mock_validate_regions, mock_session, mock_list_tasks,
        mock_enqueue, client):
    region_code = 'region_x'

    mock_session.return_value = 'region_x_session'
    mock_validate_regions.return_value = [region_code]

    fake_region = create_autospec(Region)
    fake_region.region_code = region_code
    fake_region.get_queue_name.return_value = 'queue'
    mock_region.return_value = fake_region

    mock_list_tasks.return_value = ['fake_task']

    request_args = {'region': 'all'}
    headers = {'X-Appengine-Cron': "test-cron"}
    response = client.get('/check_finished',
                          query_string=request_args,
                          headers=headers)
    assert response.status_code == 200

    mock_validate_regions.assert_called_with(['all'])
    mock_session.assert_called_with(
        ScrapeKey('region_x', constants.ScrapeType.BACKGROUND))
    mock_region.assert_called_with(region_code)
    mock_list_tasks.assert_called_with(region_code=region_code,
                                       queue_name='queue')
    mock_enqueue.assert_not_called()
