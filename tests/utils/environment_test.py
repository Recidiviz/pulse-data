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
from mock import patch, mock_open
from utils import environment


def test_load_local_vars():
    file_contents = """
    env_vars:
      artist: Jon Hopkins
      album: Singularity
    """

    with patch("__builtin__.open", mock_open(read_data=file_contents)) \
            as mock_file:
        env_vars = environment.load_local_vars()
        assert len(env_vars) == 2
        assert env_vars['artist'] == 'Jon Hopkins'
        assert env_vars['album'] == 'Singularity'
        mock_file.assert_called_with('local.yaml', 'r')


@patch("os.getenv")
def test_in_prod_false(mock_os):
    mock_os.return_value = 'NOT PRODUCTION'
    assert not environment.in_prod()


@patch("os.getenv")
def test_in_prod_true(mock_os):
    mock_os.return_value = 'Google App Engine/'
    assert environment.in_prod()


def test_local_only_is_local():
    track = 'Emerald Rush'

    class TestHandler(webapp2.RequestHandler):
        @environment.local_only
        def get(self):
            self.response.set_status(200)
            self.response.write(track)

    app = webapp2.WSGIApplication([('/', TestHandler)])
    test_app = webtest.TestApp(app)

    response = test_app.get('/')
    assert response.status_int == 200
    response.mustcontain(track)


@patch("os.getenv")
def test_local_only_is_prod(mock_os):
    track = 'Emerald Rush'
    mock_os.return_value = 'Google App Engine/'

    class TestHandler(webapp2.RequestHandler):
        @environment.local_only
        def get(self):
            return track

    app = webapp2.WSGIApplication([('/', TestHandler)])
    test_app = webtest.TestApp(app)

    response = test_app.get('/', expect_errors=True)
    assert response.status_int == 500
    response.mustcontain('Not available, see service logs.')
