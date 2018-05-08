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

"""Tests for utils/environment.py."""


import pytest
import webapp2
import webtest

from ..context import utils
from google.appengine.ext import testbed
from utils import auth


BEST_ALBUM = 'Music Has The Right To Children'
APP_ID = "recidiviz-auth-test"


class BoardsOfCanadaHandler(webapp2.RequestHandler):
    @auth.authenticate_request
    def get(self):
        self.response.set_status(200)
        self.response.write(BEST_ALBUM)


class TestAuthenticateRequest(object):
    """Tests for the @authenticate_request decorator."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.setup_env(app_id=APP_ID)
        self.testbed.init_app_identity_stub()
        self.testbed.init_user_stub()

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

        app = webapp2.WSGIApplication([('/', BoardsOfCanadaHandler)])
        test_app = webtest.TestApp(app)

        response = test_app.get('/',
                                headers={'X-Appengine-Inbound-Appid': 'blah'},
                                expect_errors=True)
        assert response.status_int == 401
        response.mustcontain('Failed: Unauthorized external request.')

    def test_authenticate_request_same_app_id(self):
        self.login_user(is_admin=False)

        app = webapp2.WSGIApplication([('/', BoardsOfCanadaHandler)])
        test_app = webtest.TestApp(app)

        response = test_app.get('/',
                                headers={'X-Appengine-Inbound-Appid': APP_ID})
        assert response.status_int == 200
        response.mustcontain(BEST_ALBUM)

    def test_authenticate_request_not_admin(self):
        self.login_user(is_admin=False)

        app = webapp2.WSGIApplication([('/', BoardsOfCanadaHandler)])
        test_app = webtest.TestApp(app)

        response = test_app.get('/', expect_errors=True)
        assert response.status_int == 401
        response.mustcontain('Failed: Not an admin.')

    def test_authenticate_request_is_admin(self):
        self.login_user()

        app = webapp2.WSGIApplication([('/', BoardsOfCanadaHandler)])
        test_app = webtest.TestApp(app)

        response = test_app.get('/')
        assert response.status_int == 200
        response.mustcontain(BEST_ALBUM)

    def test_authenticate_request_is_cron(self):
        app = webapp2.WSGIApplication([('/', BoardsOfCanadaHandler)])
        test_app = webtest.TestApp(app)

        response = test_app.get('/',
                                headers={'X-Appengine-Cron': "True"})
        assert response.status_int == 200
        response.mustcontain(BEST_ALBUM)

    def test_authenticate_request_is_task(self):
        app = webapp2.WSGIApplication([('/', BoardsOfCanadaHandler)])
        test_app = webtest.TestApp(app)

        response = test_app.get('/',
                                headers={'X-Appengine-QueueName': "us_ny"})
        assert response.status_int == 200
        response.mustcontain(BEST_ALBUM)

    def test_authenticate_redirect_to_login(self):
        app = webapp2.WSGIApplication([('/', BoardsOfCanadaHandler)])
        test_app = webtest.TestApp(app)

        response = test_app.get('/')
        assert response.status_int == 302
