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
import pytest
import pytz
from flask import Flask
from mock import create_autospec, patch

from recidiviz.ingest.direct import direct_ingest_control
from recidiviz.persistence import batch_persistence
from recidiviz.utils.regions import Region


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

        region = 'us_ut'
        request_args = {'region': region}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/start',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 200
        mock_region.assert_called_with('us_ut', is_direct_ingest=True)

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_start_diff_environment(self, mock_region, mock_environment,
                                    client):
        """Tests that the start operation chains together the correct calls."""
        mock_environment.return_value = 'staging'
        mock_region.return_value = fake_region(environment='production')

        region = 'us_wy'
        request_args = {'region': region}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/start',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 400

        mock_region.assert_called_with('us_wy', is_direct_ingest=True)

    @patch("recidiviz.utils.regions.get_supported_region_codes")
    def test_start_unsupported_region(self, mock_supported, client):
        mock_supported.return_value = ['us_ny', 'us_pa']

        request_args = {'region': 'us_ca'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/start',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 400
        assert response.get_data().decode() == \
            "Unsupported direct ingest region us_ca"


def fake_region(environment='local'):
    region = create_autospec(Region)
    region.environment = environment
    return region
