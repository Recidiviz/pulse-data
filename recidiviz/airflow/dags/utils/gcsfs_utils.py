# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Utils for interacting with GCS from Airflow."""
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from recidiviz.cloud_storage.gcs_file_system_impl import GCSFileSystemImpl
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.utils.yaml_dict import YAMLDict


def read_yaml_config(path: GcsfsFilePath) -> YAMLDict:
    """Reads a YAML file from GCS into a YAMLDict object."""
    gcs_hook = GCSHook()
    gcsfs = GCSFileSystemImpl(gcs_hook.get_conn())
    with gcsfs.open(path) as f:
        return YAMLDict.from_io(f)
