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


import json

from mock import patch
from google.appengine.ext import testbed
from requests.packages.urllib3.contrib.appengine import TimeoutError
from recidiviz.ingest import worker


APP_ID = "recidiviz-worker-test"
PATH = "/scraper/work"


class TestWorker(object):
    """Tests for requests to the Worker API."""

    # noinspection PyAttributeOutsideInit
    def setup_method(self, _test_method):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.setup_env(app_id=APP_ID)
        self.testbed.init_app_identity_stub()
        self.testbed.init_user_stub()

        worker.app.config['TESTING'] = True
        self.client = worker.app.test_client()

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

    @patch("recidiviz.utils.regions.Region")
    def test_post_work(self, mock_region):
        mock_region.return_value = FakeRegion(FakeScraper(1))

        self.login_user()

        form = {'region': 'us_ca', 'task': 'fake_task'}
        headers = {'X-Appengine-QueueName': "test-queue"}
        response = self.client.post(PATH, data=form, headers=headers)
        assert response.status_code == 200

    @patch("recidiviz.utils.regions.Region")
    def test_post_work_params(self, mock_region):
        mock_region.return_value = FakeRegion(FakeScraper(1))

        self.login_user()

        scraper_params = {'foo': 'bar', 'baz': 'inga'}
        form = {'region': 'us_ca', 'task': 'fake_task_params',
                'params': json.dumps(scraper_params)}
        headers = {'X-Appengine-QueueName': "test-queue"}
        response = self.client.post(PATH, data=form, headers=headers)
        assert response.status_code == 200

    @patch("recidiviz.utils.regions.Region")
    def test_post_work_error(self, mock_region):
        mock_region.return_value = FakeRegion(FakeScraper(-1))

        self.login_user()

        form = {'region': 'us_ca', 'task': 'fake_task'}
        headers = {'X-Appengine-QueueName': "test-queue"}
        response = self.client.post(PATH, data=form, headers=headers)
        assert response.status_code == 500

    @patch("recidiviz.utils.regions.Region")
    def test_post_work_timeout(self, mock_region):
        mock_region.return_value = FakeRegion(FakeScraper(1))

        self.login_user()

        form = {'region': 'us_ca', 'task': 'fake_task_timeout'}
        headers = {'X-Appengine-QueueName': "test-queue"}
        response = self.client.post(PATH, data=form, headers=headers)
        assert response.status_code == 500

    def test_post_work_not_from_task(self):
        self.login_user()

        response = self.client.post(PATH)
        assert response.status_code == 500


class FakeRegion(object):
    """A fake region to be returned from mocked out calls to Region"""
    def __init__(self, scraper):
        self.scraper = scraper

    def get_scraper(self):
        return self.scraper


class FakeScraper(object):
    """A fake scraper to be returned from mocked out calls to
    Region.get_scraper."""

    def __init__(self, return_value):
        self.return_value = return_value

    def fake_task(self):
        return self.return_value

    def fake_task_params(self, params):
        assert params is not None
        return self.return_value

    def fake_task_timeout(self):
        raise TimeoutError()
