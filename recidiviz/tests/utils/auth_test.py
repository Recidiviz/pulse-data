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

# pylint: disable=unused-import,wrong-import-order

"""Tests for utils/auth.py."""


import pytest
from flask import Flask, request
from mock import patch

from recidiviz.utils import auth

from ..context import utils

BEST_ALBUM = 'Music Has The Right To Children'
APP_ID = "recidiviz-auth-test"

dummy_app = Flask(__name__)

@dummy_app.route('/')
@auth.authenticate_request
def boards_of_canada_holder():
    return (BEST_ALBUM, 200)

class TestAuthenticateRequest:
    """Tests for the @authenticate_request decorator."""

    def setup_method(self, _test_method):
        dummy_app.config['TESTING'] = True
        self.client = dummy_app.test_client()

    @patch('recidiviz.utils.metadata.project_id')
    def test_authenticate_request_different_app_id(self, mock_project_id):
        mock_project_id.return_value = 'test-project'

        response = self.client.get(
            '/', headers={'X-Appengine-Inbound-Appid': 'blah'})
        assert response.status_code == 401
        assert response.get_data().decode() == \
                'Failed: Unauthorized external request.'

    @patch('recidiviz.utils.metadata.project_id')
    def test_authenticate_request_same_app_id(self, mock_project_id):
        mock_project_id.return_value = 'test-project'

        response = self.client.get(
            '/', headers={'X-Appengine-Inbound-Appid': 'test-project'})
        assert response.status_code == 200
        assert response.get_data().decode() == BEST_ALBUM

    def test_authenticate_request_is_cron(self):
        response = self.client.get('/', headers={'X-Appengine-Cron': "True"})
        assert response.status_code == 200
        assert response.get_data().decode() == BEST_ALBUM

    def test_authenticate_request_is_task(self):
        response = self.client.get(
            '/', headers={'X-Appengine-QueueName': "us_ny"})
        assert response.status_code == 200
        assert response.get_data().decode() == BEST_ALBUM

    @patch("recidiviz.utils.validate_jwt.validate_iap_jwt_from_app_engine")
    def test_authenticate_request_from_iap(self, mock_jwt):
        mock_jwt.return_value = ('user', 'email', None)

        response = self.client.get(
            '/', headers={'x-goog-iap-jwt-assertion': '0'})
        assert response.status_code == 200
        assert response.get_data().decode() == BEST_ALBUM

    @patch("recidiviz.utils.validate_jwt.validate_iap_jwt_from_app_engine")
    def test_authenticate_request_from_iap_invalid(self, mock_jwt):
        mock_jwt.return_value = (None, None, 'INVALID TOKEN')

        response = self.client.get(
            '/', headers={'x-goog-iap-jwt-assertion': '0'})
        assert response.status_code == 401
        assert response.get_data().decode() == 'Error: INVALID TOKEN'

    def test_authenticate_request_unauthorized(self):
        response = self.client.get('/')
        assert response.status_code == 401
        assert response.get_data().decode() == \
                'Failed: Unauthorized external request.'
