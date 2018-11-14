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

from ..context import utils
from google.appengine.ext import testbed
from flask import Flask, request
from recidiviz.utils import auth


BEST_ALBUM = 'Music Has The Right To Children'
APP_ID = "recidiviz-auth-test"

dummy_app = Flask(__name__)

@dummy_app.route('/')
@auth.authenticate_request
def boards_of_canada_holder():
    return (BEST_ALBUM, 200)

class TestAuthenticateRequest(object):
    """Tests for the @authenticate_request decorator."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.setup_env(app_id=APP_ID)
        self.testbed.init_app_identity_stub()
        self.testbed.init_user_stub()

        dummy_app.config['TESTING'] = True
        self.client = dummy_app.test_client()

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

    def test_authenticate_request_different_app_id(self):
        self.login_user(is_admin=False)

        response = self.client.get(
            '/', headers={'X-Appengine-Inbound-Appid': 'blah'})
        assert response.status_code == 401
        assert response.get_data() == 'Failed: Unauthorized external request.'

    def test_authenticate_request_same_app_id(self):
        self.login_user(is_admin=False)

        response = self.client.get(
            '/', headers={'X-Appengine-Inbound-Appid': APP_ID})
        assert response.status_code == 200
        assert response.get_data() == BEST_ALBUM

    def test_authenticate_request_not_admin(self):
        self.login_user(is_admin=False)

        response = self.client.get('/')
        assert response.status_code == 401
        assert response.get_data() == 'Failed: Not an admin.'

    def test_authenticate_request_is_admin(self):
        self.login_user()

        response = self.client.get('/')
        assert response.status_code == 200
        assert response.get_data() == BEST_ALBUM

    def test_authenticate_request_is_cron(self):
        response = self.client.get('/', headers={'X-Appengine-Cron': "True"})
        assert response.status_code == 200
        assert response.get_data() == BEST_ALBUM

    def test_authenticate_request_is_task(self):
        response = self.client.get(
            '/', headers={'X-Appengine-QueueName': "us_ny"})
        assert response.status_code == 200
        assert response.get_data() == BEST_ALBUM

    def test_authenticate_redirect_to_login(self):
        response = self.client.get('/')
        assert response.status_code == 302
