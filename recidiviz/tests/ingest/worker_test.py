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

"""Tests for ingest/worker.py."""


import datetime
import json

from flask import Flask
from mock import patch

from recidiviz.ingest import worker
from recidiviz.ingest.scrape import constants
from recidiviz.ingest.scrape.task_params import QueueRequest, Task

PATH = "/work"
FAKE_QUEUE_PARAMS = QueueRequest(
    scrape_type=constants.ScrapeType.BACKGROUND,
    scraper_start_time=datetime.datetime.utcnow(),
    next_task=Task(
        task_type=constants.TaskType.INITIAL,
        endpoint='some.endpoint',
    ),
)

app = Flask(__name__)
app.register_blueprint(worker.worker)
app.config['TESTING'] = True


class TestWorker:
    """Tests for requests to the Worker API."""

    # noinspection PyAttributeOutsideInit
    def setup_method(self, _test_method):
        self.client = app.test_client()

    @patch("recidiviz.utils.regions.Region")
    def test_post_work(self, mock_region):
        mock_region.return_value = FakeRegion(FakeScraper(1))

        form = {'region': 'us_ca', 'task': 'fake_task',
                'params': FAKE_QUEUE_PARAMS.to_serializable()}
        form_encoded = json.dumps(form).encode()
        headers = {'X-Appengine-QueueName': "test-queue"}
        response = self.client.post(PATH, data=form_encoded, headers=headers)
        assert response.status_code == 200

    @patch("recidiviz.utils.regions.Region")
    def test_post_work_params(self, mock_region):
        mock_region.return_value = FakeRegion(FakeScraper(1))

        form = {'region': 'us_ca', 'task': 'fake_task_params',
                'params': FAKE_QUEUE_PARAMS.to_serializable()}
        form_encoded = json.dumps(form).encode()
        headers = {'X-Appengine-QueueName': "test-queue"}
        response = self.client.post(PATH, data=form_encoded, headers=headers)
        assert response.status_code == 200

    @patch("recidiviz.utils.regions.Region")
    def test_post_work_error(self, mock_region):
        mock_region.return_value = FakeRegion(FakeScraper(-1))

        form = {
            'region': 'us_ca',
            'task': 'fake_task',
            'params': FAKE_QUEUE_PARAMS.to_serializable(),
        }
        form_encoded = json.dumps(form).encode()
        headers = {'X-Appengine-QueueName': "test-queue"}
        response = self.client.post(PATH, data=form_encoded, headers=headers)
        assert response.status_code == 500

    @patch("recidiviz.utils.regions.Region")
    def test_post_work_timeout(self, mock_region):
        mock_region.return_value = FakeRegion(FakeScraper(1))

        form = {
            'region': 'us_ca',
            'task': 'fake_task_timeout',
            'params': FAKE_QUEUE_PARAMS.to_serializable(),
        }
        form_encoded = json.dumps(form).encode()
        headers = {'X-Appengine-QueueName': "test-queue"}
        response = self.client.post(PATH, data=form_encoded, headers=headers)
        assert response.status_code == 500

    @patch("recidiviz.utils.validate_jwt.validate_iap_jwt_from_app_engine")
    def test_post_work_not_from_task(self, mock_jwt):
        mock_jwt.return_value = ('user', 'email', None)

        headers = {'x-goog-iap-jwt-assertion': '1234'}
        response = self.client.post(PATH, headers=headers)
        assert response.status_code == 500


class FakeRegion:
    """A fake region to be returned from mocked out calls to Region"""
    def __init__(self, scraper):
        self.scraper = scraper

    def get_scraper(self):
        return self.scraper


class FakeScraper:
    """A fake scraper to be returned from mocked out calls to
    Region.get_scraper."""

    def __init__(self, return_value):
        self.return_value = return_value

    def fake_task(self, _params):
        return self.return_value

    def fake_task_params(self, params):
        assert params is not None
        return self.return_value

    def fake_task_timeout(self, params):
        raise TimeoutError()
