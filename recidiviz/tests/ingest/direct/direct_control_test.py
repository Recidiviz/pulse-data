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

"""Tests for ingest/direct_control.py."""
import datetime
import importlib
import unittest

import attr
import pytest
import pytz
from flask import Flask
from mock import patch

from recidiviz.ingest.direct import direct_ingest_control
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import \
    GcsfsIngestArgs
from recidiviz.ingest.direct.errors import DirectIngestError
from recidiviz.persistence import batch_persistence
from recidiviz.tests.utils.fake_region import fake_region


# pylint: disable=redefined-outer-name
@pytest.fixture
def client():
    app = Flask(__name__)
    app.register_blueprint(direct_ingest_control.direct_ingest_control)
    # Include so that flask can get the url of `infer_release`.
    app.register_blueprint(batch_persistence.batch_blueprint)
    app.config['TESTING'] = True

    yield app.test_client()


def _MockSupported(timezone=None):
    if not timezone:
        regions = ['us_ut', 'us_wy']
    elif timezone == pytz.timezone('America/New_York'):
        regions = ['us_ut']
    else:
        regions = ['us_wy']
    return regions


class TestDirectStart:
    """Tests for requests to the Direct Start API."""

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_start(self, mock_region, mock_environment, client):
        """Tests that the start operation chains together the correct calls."""
        mock_region.return_value = fake_region(environment='production')
        mock_environment.return_value = 'production'

        region = 'us_nd'
        request_args = {'region': region}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/scheduler',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 200
        mock_region.assert_called_with('us_nd', is_direct_ingest=True)

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_start_diff_environment_in_production(
            self, mock_region, mock_environment, client):
        """Tests that the start operation chains together the correct calls."""
        mock_environment.return_value = 'production'
        mock_region.return_value = fake_region(environment='staging')

        region = 'us_nd'
        request_args = {'region': region}
        headers = {'X-Appengine-Cron': "test-cron"}
        with pytest.raises(DirectIngestError):
            response = client.get('/scheduler',
                                  query_string=request_args,
                                  headers=headers)
            assert response.status_code == 400

        mock_region.assert_called_with('us_nd', is_direct_ingest=True)

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_start_diff_environment_in_staging(
            self, mock_region, mock_environment, client):
        """Tests that the start operation chains together the correct calls."""
        mock_environment.return_value = 'staging'
        mock_region.return_value = fake_region(environment='production')

        region = 'us_nd'
        request_args = {'region': region}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/scheduler',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 200

        mock_region.assert_called_with('us_nd', is_direct_ingest=True)

    @patch("recidiviz.utils.regions.get_supported_direct_ingest_region_codes")
    def test_start_unsupported_region(self, mock_supported, client):
        mock_supported.return_value = ['us_ny', 'us_pa']

        request_args = {'region': 'us_ca', 'just_finished_job:': 'False'}
        headers = {'X-Appengine-Cron': "test-cron"}
        with pytest.raises(DirectIngestError):
            response = client.get('/scheduler',
                                  query_string=request_args,
                                  headers=headers)
            assert response.status_code == 400
            assert response.get_data().decode() == \
                   "Unsupported direct ingest region us_ca"


class TestQueueArgs(unittest.TestCase):
    def test_parse_args(self):
        ingest_args = GcsfsIngestArgs(
            ingest_time=datetime.datetime.now(),
            file_path='/foo/bar',
        )

        ingest_args_class_name = ingest_args.__class__.__name__
        ingest_args_class_module = ingest_args.__module__
        ingest_args_dict = attr.asdict(ingest_args)

        module = importlib.import_module(ingest_args_class_module)
        ingest_class = getattr(module, ingest_args_class_name)
        result_args = ingest_class(**ingest_args_dict)

        self.assertEqual(ingest_args, result_args)
