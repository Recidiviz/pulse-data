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
import json

import pytest
import pytz
from flask import Flask
from mock import patch, create_autospec

from recidiviz.ingest.direct import direct_ingest_control
from recidiviz.ingest.direct.controllers.direct_ingest_types import IngestArgs
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_controller import \
    GcsfsDirectIngestController
from recidiviz.ingest.direct.controllers.gcsfs_path import GcsfsFilePath
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


class TestDirectIngestControl:
    """Tests for requests to the Direct Ingest API."""

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_schedule(self, mock_region, mock_environment, client):
        """Tests that the start operation chains together the correct calls."""

        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(environment='production',
                                               ingestor=mock_controller)
        mock_environment.return_value = 'production'

        region = 'us_nd'
        request_args = {'region': region}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/scheduler',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 200

        mock_region.assert_called_with('us_nd', is_direct_ingest=True)
        mock_controller.schedule_next_ingest_job_or_wait_if_necessary.\
            assert_called_with(just_finished_job=False)
        assert response.status_code == 200

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_schedule_diff_environment_in_production(
            self, mock_region, mock_environment, client):
        """Tests that the start operation chains together the correct calls."""
        mock_environment.return_value = 'production'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(environment='staging',
                                               ingestor=mock_controller)

        region = 'us_nd'
        request_args = {'region': region}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/scheduler',
                              query_string=request_args,
                              headers=headers)

        mock_controller.schedule_next_ingest_job_or_wait_if_necessary. \
            assert_not_called()
        assert response.status_code == 400
        assert response.get_data().decode() == \
               "Bad environment production for direct region us_nd."

        mock_region.assert_called_with('us_nd', is_direct_ingest=True)

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_start_diff_environment_in_staging(
            self, mock_region, mock_environment, client):
        """Tests that the start operation chains together the correct calls."""
        mock_environment.return_value = 'staging'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(environment='production',
                                               ingestor=mock_controller)

        region = 'us_nd'
        request_args = {'region': region, 'just_finished_job': 'True'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/scheduler',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 200

        mock_region.assert_called_with('us_nd', is_direct_ingest=True)
        mock_controller.schedule_next_ingest_job_or_wait_if_necessary.\
            assert_called_with(just_finished_job=True)

    @patch("recidiviz.utils.regions.get_supported_direct_ingest_region_codes")
    def test_schedule_unsupported_region(self, mock_supported, client):
        mock_supported.return_value = ['us_ny', 'us_pa']

        request_args = {'region': 'us_ca', 'just_finished_job': 'False'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/scheduler',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 400
        assert response.get_data().decode() == \
               "Unsupported direct ingest region us_ca"

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.utils.regions.get_supported_direct_ingest_region_codes")
    def test_schedule_unlaunched_region(
            self, mock_supported, mock_region, mock_environment, client):
        mock_supported.return_value = ['us_nd', 'us_pa']

        region_code = 'us_nd'

        mock_environment.return_value = 'production'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(
            region_code=region_code,
            environment='staging',
            ingestor=mock_controller)

        request_args = {'region': 'us_nd', 'just_finished_job': 'False'}
        headers = {'X-Appengine-Cron': "test-cron"}

        response = client.get('/scheduler',
                              query_string=request_args,
                              headers=headers)
        assert response.status_code == 400
        assert response.get_data().decode() == \
               "Bad environment production for direct region us_nd."

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.utils.regions.get_supported_direct_ingest_region_codes")
    def test_process_job(
            self, mock_supported, mock_region, mock_environment, client):
        mock_supported.return_value = ['us_nd', 'us_pa']

        region_code = 'us_nd'

        mock_environment.return_value = 'staging'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(
            region_code=region_code,
            environment='staging',
            ingestor=mock_controller)

        ingest_args = IngestArgs(datetime.datetime(year=2019, month=7, day=20))
        request_args = {
            'region': region_code,
        }
        body = {
            'ingest_args': ingest_args.to_serializable(),
            'args_type': 'IngestArgs',
        }
        body_encoded = json.dumps(body).encode()

        headers = {'X-Appengine-Cron': "test-cron"}

        response = client.post('/process_job',
                               query_string=request_args,
                               headers=headers,
                               data=body_encoded)
        assert response.status_code == 200
        mock_controller.\
            run_ingest_job_and_kick_scheduler_on_completion.\
            assert_called_with(ingest_args)

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_region")
    @patch("recidiviz.utils.regions.get_supported_direct_ingest_region_codes")
    def test_process_job_unlaunched_region(
            self, mock_supported, mock_region, mock_environment, client):
        mock_supported.return_value = ['us_nd', 'us_pa']

        region_code = 'us_nd'

        mock_environment.return_value = 'production'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(
            region_code=region_code,
            environment='staging',
            ingestor=mock_controller)

        ingest_args = IngestArgs(datetime.datetime(year=2019, month=7, day=20))
        request_args = {
            'region': region_code,
        }
        body = {
            'ingest_args': ingest_args.to_serializable(),
            'args_type': 'IngestArgs',
        }
        body_encoded = json.dumps(body).encode()

        headers = {'X-Appengine-Cron': "test-cron"}

        response = client.post('/process_job',
                               query_string=request_args,
                               headers=headers,
                               data=body_encoded)
        assert response.status_code == 400
        assert response.get_data().decode() == \
               "Bad environment production for direct region us_nd."

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_handle_file_no_start_ingest(
            self, mock_region, mock_environment, client):
        region_code = 'us_nd'

        mock_environment.return_value = 'production'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(
            region_code=region_code,
            environment='production',
            ingestor=mock_controller)

        path = GcsfsFilePath.from_absolute_path(
            'bucket-us-nd/Elite_Offenders.csv')

        request_args = {
            'region': region_code,
            'bucket': path.bucket_name,
            'relative_file_path': path.blob_name,
            'start_ingest': 'false',
        }
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/handle_direct_ingest_file',
                              query_string=request_args,
                              headers=headers)

        mock_controller.handle_file.assert_called_with(path, False)

        # Even though the region isn't supported, we don't crash
        assert response.status_code == 200

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_handle_file_start_ingest(
            self, mock_region, mock_environment, client):
        region_code = 'us_nd'

        mock_environment.return_value = 'production'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(region_code=region_code,
                                               environment='production',
                                               ingestor=mock_controller)
        path = GcsfsFilePath.from_absolute_path(
            'bucket-us-nd/elite_offenders.csv')

        request_args = {
            'region': region_code,
            'bucket': path.bucket_name,
            'relative_file_path': path.blob_name,
            'start_ingest': 'True',
        }
        headers = {'X-Appengine-Cron': "test-cron"}
        response = client.get('/handle_direct_ingest_file',
                              query_string=request_args,
                              headers=headers)

        mock_controller.handle_file.assert_called_with(path, True)

        # Even though the region isn't supported, we don't crash
        assert response.status_code == 200

    @patch("recidiviz.utils.environment.get_gae_environment")
    @patch("recidiviz.utils.regions.get_region")
    def test_handle_file_start_ingest_unsupported_region(
            self, mock_region, mock_environment, client):
        region_code = 'us_nd'

        mock_environment.return_value = 'production'
        mock_controller = create_autospec(GcsfsDirectIngestController)
        mock_region.return_value = fake_region(region_code=region_code,
                                               environment='staging',
                                               ingestor=mock_controller)

        path = GcsfsFilePath.from_absolute_path(
            'bucket-us-nd/elite_offenders.csv')

        request_args = {
            'region': region_code,
            'bucket': path.bucket_name,
            'relative_file_path': path.blob_name,
            'start_ingest': 'False',
        }
        headers = {'X-Appengine-Cron': "test-cron"}

        response = client.get('/handle_direct_ingest_file',
                              query_string=request_args,
                              headers=headers)

        mock_region.assert_called_with('us_nd', is_direct_ingest=True)
        mock_controller.handle_file.assert_called_with(path, False)

        # Even though the region isn't supported, we don't crash - the
        # controller handles not starting ingest, and if it does by accident,
        # the actual schedule/process_job endpoints handle the unlaunched
        # region check.
        assert response.status_code == 200
