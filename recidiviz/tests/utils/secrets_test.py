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

# pylint: disable=unused-import,wrong-import-order

"""Tests for utils/secrets.py."""
from unittest.mock import patch

from google.cloud import exceptions
from google.cloud.secretmanager_v1beta1.proto import service_pb2
from google.cloud.secretmanager_v1beta1.types import SecretPayload
from mock import Mock

from recidiviz.utils import secrets


class TestSecrets:
    """Tests for the secrets module."""

    def teardown_method(self, _test_method):
        secrets.clear_sm()
        secrets.CACHED_SECRETS.clear()

    def test_get_in_cache(self):
        write_to_local('top_track', 'An Eagle In Your Mind')

        actual = secrets.get_secret('top_track')
        assert actual == 'An Eagle In Your Mind'

    @patch('recidiviz.utils.metadata.project_id')
    def test_get_not_in_cache(self, mock_project_id):
        mock_project_id.return_value = 'test-project'
        payload = SecretPayload(data=bytes('Olson'.encode('UTF-8')))

        mock_client = Mock()
        mock_client.secret_version_path.return_value = "test-project.top_track.latest"
        mock_client.access_secret_version.return_value = service_pb2.AccessSecretVersionResponse(payload=payload)
        with patch('google.cloud.secretmanager_v1beta1.SecretManagerServiceClient', return_value=mock_client):
            actual = secrets.get_secret('top_track')
            assert actual == 'Olson'

    @patch('recidiviz.utils.metadata.project_id')
    def test_get_not_in_cache_not_found(self, mock_project_id):
        mock_project_id.return_value = 'test-project'

        mock_client = Mock()
        mock_client.secret_version_path.return_value = "test-project.top_track.latest"
        mock_client.access_secret_version.side_effect = exceptions.NotFound('Could not find it')
        with patch('google.cloud.secretmanager_v1beta1.SecretManagerServiceClient', return_value=mock_client):
            actual = secrets.get_secret('top_track')
            assert actual is None

    @patch('recidiviz.utils.metadata.project_id')
    def test_get_not_in_cache_error(self, mock_project_id):
        mock_project_id.return_value = 'test-project'

        mock_client = Mock()
        mock_client.secret_version_path.return_value = "test-project.top_track.latest"
        mock_client.access_secret_version.side_effect = Exception('Something bad happened')
        with patch('google.cloud.secretmanager_v1beta1.SecretManagerServiceClient', return_value=mock_client):
            actual = secrets.get_secret('top_track')
            assert actual is None

    @patch('recidiviz.utils.metadata.project_id')
    def test_get_not_in_cache_bad_payload(self, mock_project_id):
        mock_project_id.return_value = 'test-project'

        mock_client = Mock()
        mock_client.secret_version_path.return_value = "test-project.top_track.latest"
        mock_client.access_secret_version.return_value = service_pb2.AccessSecretVersionResponse(payload=None)
        with patch('google.cloud.secretmanager_v1beta1.SecretManagerServiceClient', return_value=mock_client):
            actual = secrets.get_secret('top_track')
            assert actual is None


def write_to_local(name, value):
    secrets.CACHED_SECRETS[name] = value
