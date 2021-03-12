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
"""Factory for GCSFileSystem objects"""
import requests

from google.cloud import storage

from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem, GCSFileSystemImpl

# Adjust these variables if there are connection pool full errors to storage.googleapis
POOL_CONNECTIONS = 128
POOL_MAXSIZE = 128


class GcsfsFactory:
    @classmethod
    def build(cls) -> GCSFileSystem:
        storage_client = storage.Client()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=POOL_CONNECTIONS, pool_maxsize=POOL_MAXSIZE
        )
        # pylint: disable=protected-access
        storage_client._http.mount("https://", adapter)
        storage_client._http._auth_request.session.mount("https://", adapter)
        return GCSFileSystemImpl(storage_client)
