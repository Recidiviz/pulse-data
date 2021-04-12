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

"""Tests for ingest/worker.py."""


import datetime
import json

import pytest
import pytz
from flask import Flask
from mock import Mock, create_autospec, patch

from recidiviz.ingest.scrape import constants, scrape_phase, sessions, worker
from recidiviz.ingest.scrape.task_params import QueueRequest, Task
from recidiviz.utils.regions import Region

PATH = "/work/us_ca"
FAKE_QUEUE_PARAMS = QueueRequest(
    scrape_type=constants.ScrapeType.BACKGROUND,
    scraper_start_time=datetime.datetime.now(tz=pytz.UTC),
    next_task=Task(
        task_type=constants.TaskType.INITIAL,
        endpoint="some.endpoint",
    ),
)

app = Flask(__name__)
app.register_blueprint(worker.worker)
app.config["TESTING"] = True


@patch("recidiviz.utils.metadata.project_id", Mock(return_value="test-project"))
@patch("recidiviz.utils.metadata.project_number", Mock(return_value="123456789"))
class TestWorker:
    """Tests for requests to the Worker API."""

    # noinspection PyAttributeOutsideInit
    def setup_method(self, _test_method):
        self.client = app.test_client()

    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    def test_post_work(self, mock_session, mock_region):
        mock_session.return_value = sessions.ScrapeSession.new(
            key=None,
            region="us_ca",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.SCRAPE,
        )
        region = create_autospec(Region)
        mock_region.return_value = region

        form = {
            "region": "us_ca",
            "task": "fake_task",
            "params": FAKE_QUEUE_PARAMS.to_serializable(),
        }
        form_encoded = json.dumps(form).encode()
        headers = {"X-Appengine-QueueName": "test-queue"}
        response = self.client.post(PATH, data=form_encoded, headers=headers)
        assert response.status_code == 200

        region.get_scraper().fake_task.assert_called_with(FAKE_QUEUE_PARAMS)

    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    def test_post_work_no_session(self, mock_session, mock_region):
        mock_session.return_value = None

        form = {
            "region": "us_ca",
            "task": "fake_task",
            "params": FAKE_QUEUE_PARAMS.to_serializable(),
        }
        form_encoded = json.dumps(form).encode()
        headers = {"X-Appengine-QueueName": "test-queue"}
        response = self.client.post(PATH, data=form_encoded, headers=headers)
        assert response.status_code == 200

        assert not mock_region.called

    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    def test_post_work_error(self, mock_session, mock_region):
        mock_session.return_value = sessions.ScrapeSession.new(
            key=None,
            region="us_ca",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.SCRAPE,
        )
        region = create_autospec(Region)
        region.get_scraper().fake_task.side_effect = Exception()
        mock_region.return_value = region

        form = {
            "region": "us_ca",
            "task": "fake_task",
            "params": FAKE_QUEUE_PARAMS.to_serializable(),
        }
        form_encoded = json.dumps(form).encode()
        headers = {"X-Appengine-QueueName": "test-queue"}
        with pytest.raises(worker.RequestProcessingError):
            self.client.post(PATH, data=form_encoded, headers=headers)

        region.get_scraper().fake_task.assert_called_with(FAKE_QUEUE_PARAMS)

    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.ingest.scrape.sessions.get_current_session")
    def test_post_work_timeout(self, mock_session, mock_region):
        mock_session.return_value = sessions.ScrapeSession.new(
            key=None,
            region="us_ca",
            scrape_type=constants.ScrapeType.BACKGROUND,
            phase=scrape_phase.ScrapePhase.SCRAPE,
        )
        region = create_autospec(Region)
        region.get_scraper().fake_task.side_effect = TimeoutError()
        mock_region.return_value = region

        form = {
            "region": "us_ca",
            "task": "fake_task",
            "params": FAKE_QUEUE_PARAMS.to_serializable(),
        }
        form_encoded = json.dumps(form).encode()
        headers = {"X-Appengine-QueueName": "test-queue"}
        with pytest.raises(worker.RequestProcessingError):
            self.client.post(PATH, data=form_encoded, headers=headers)

        region.get_scraper().fake_task.assert_called_with(FAKE_QUEUE_PARAMS)

    @patch("recidiviz.utils.validate_jwt.validate_iap_jwt_from_app_engine")
    def test_post_work_not_from_task(self, mock_jwt):
        mock_jwt.return_value = ("user", "email", None)

        headers = {"x-goog-iap-jwt-assertion": "1234"}
        response = self.client.post(PATH, headers=headers)
        assert response.status_code == 500

    def test_post_work_region_mismatch(self):
        form = {
            # different region
            "region": "us_nd",
            "task": "fake_task",
            "params": FAKE_QUEUE_PARAMS.to_serializable(),
        }
        form_encoded = json.dumps(form).encode()
        headers = {"X-Appengine-QueueName": "test-queue"}
        with pytest.raises(ValueError) as exception:
            self.client.post(PATH, data=form_encoded, headers=headers)
            assert exception.message.startswith("Region specified")
