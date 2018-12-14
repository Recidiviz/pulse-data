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
from datetime import datetime
from unittest import TestCase

from flask import Flask
from mock import call, patch
from google.appengine.ext import testbed

from recidiviz.ingest import infer_release
from recidiviz.ingest.sessions import ScrapeSession

APP_ID = "recidiviz-infer-release-test"

app = Flask(__name__)
app.register_blueprint(infer_release.infer_release_blueprint)
app.config['TESTING'] = True


class TestInferRelease(TestCase):
    """Tests for requests to the infer release API"""

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

    def login_user(self, email='user@recidiviz.com', user_id='123',
                   is_admin=True):
        self.testbed.setup_env(
            app_id=APP_ID,
            user_email=email,
            user_id=user_id,
            user_is_admin='1' if is_admin else '0',
            overwrite=True)

    @patch("recidiviz.utils.regions.get_supported_regions")
    def test_infer_release_unsupported_region(self, mock_supported_regions):
        request_args = {'region': 'us_wy'}
        mock_supported_regions.return_value = ['us_ut']

        self.login_user()
        response = self.client.get(
            '/release',
            query_string=request_args)
        assert response.status_code == 400

    @patch("recidiviz.persistence.persistence.infer_release_on_open_bookings")
    @patch("recidiviz.utils.regions.get_supported_regions")
    @patch("recidiviz.ingest.sessions.get_most_recent_completed_session")
    def test_infer_release(self, mock_get_most_recent_session,
                           mock_supported_regions, mock_infer_release):
        request_args = {'region': 'all'}
        time = datetime(2014, 8, 31)
        mock_supported_regions.return_value = ['us_ut', 'us_wy']
        mock_get_most_recent_session.return_value = ScrapeSession(end=time)

        self.login_user()
        response = self.client.get(
            '/release',
            query_string=request_args)
        assert response.status_code == 200
        mock_infer_release.assert_has_calls([call('us_ut', time),
                                             call('us_wy', time)])
