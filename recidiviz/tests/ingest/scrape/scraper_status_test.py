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
import logging
import pytest

from flask import Flask
from mock import create_autospec, patch

from recidiviz.ingest.scrape import scraper_status
from recidiviz.utils.regions import Region

# pylint: disable=redefined-outer-name
@pytest.fixture
def client():
    app = Flask(__name__)
    app.register_blueprint(scraper_status.scraper_status)
    app.config['TESTING'] = True

    yield app.test_client()

@patch("recidiviz.ingest.scrape.queues.list_tasks")
@patch("recidiviz.ingest.scrape.ingest_utils.validate_regions")
@patch("recidiviz.utils.regions.get_region")
def test_check_for_finished_scrapers(
        mock_region, mock_validate_regions, mock_list_tasks, client, caplog):
    region_code = 'region_x'

    fake_region = create_autospec(Region)
    fake_region.region_code = region_code
    fake_region.get_queue_name.return_value = 'queue'
    mock_region.return_value = fake_region
    mock_validate_regions.return_value = [region_code]
    mock_list_tasks.return_value = []

    with caplog.at_level(logging.INFO):
        request_args = {'region': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/check_finished',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 200
        assert caplog.record_tuples[-1] == \
            ('root', logging.INFO, 'Region \'region_x\' has completed.')

    mock_validate_regions.assert_called_with(['all'])
    mock_region.assert_called_with(region_code)
    mock_list_tasks.assert_called_with(region_code=region_code,
                                       queue_name='queue')

@patch("recidiviz.ingest.scrape.queues.list_tasks")
@patch("recidiviz.ingest.scrape.ingest_utils.validate_regions")
@patch("recidiviz.utils.regions.get_region")
def test_check_for_finished_scrapers_not_done(
        mock_region, mock_validate_regions, mock_list_tasks, client, caplog):
    region_code = 'region_x'

    fake_region = create_autospec(Region)
    fake_region.region_code = region_code
    fake_region.get_queue_name.return_value = 'queue'
    mock_region.return_value = fake_region
    mock_validate_regions.return_value = [region_code]
    mock_list_tasks.return_value = ['fake_task']

    with caplog.at_level(logging.INFO):
        request_args = {'region': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/check_finished',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 200
        assert caplog.record_tuples[-1] != \
            ('root', logging.INFO, 'Region \'region_x\' has completed.')

    mock_validate_regions.assert_called_with(['all'])
    mock_region.assert_called_with(region_code)
    mock_list_tasks.assert_called_with(region_code=region_code,
                                       queue_name='queue')
