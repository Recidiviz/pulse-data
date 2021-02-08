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
"""Tests for the GCSFileSystem."""
from unittest import TestCase

from google.api_core import exceptions
from google.cloud import storage
from google.cloud.storage import Bucket
from mock import create_autospec

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystemImpl
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath


class TestGcsFileSystem(TestCase):
    def setUp(self) -> None:
        self.mock_storage_client = create_autospec(storage.Client)
        self.fs = GCSFileSystemImpl(self.mock_storage_client)

    def test_retry(self) -> None:
        mock_bucket = create_autospec(Bucket)
        mock_bucket.exists.return_value = True
        # Client first raises a Gateway timeout, then returns a normal bucket.
        self.mock_storage_client.get_bucket.side_effect = [exceptions.GatewayTimeout('Exception'), mock_bucket]

        # Should not crash!
        self.assertTrue(self.fs.exists(GcsfsBucketPath.from_absolute_path('gs://my-bucket')))

    def test_retry_with_fatal_error(self) -> None:
        mock_bucket = create_autospec(Bucket)
        mock_bucket.exists.return_value = True
        # Client first raises a Gateway timeout, then on retry will raise a ValueError
        self.mock_storage_client.get_bucket.side_effect = [exceptions.GatewayTimeout('Exception'),
                                                           ValueError('This will crash')]

        with self.assertRaises(ValueError):
            self.fs.exists(GcsfsBucketPath.from_absolute_path('gs://my-bucket'))
