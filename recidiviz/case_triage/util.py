# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Utils for Case Triage"""
import logging
import os
from pathlib import Path
from typing import Optional

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.utils.environment import in_development
from recidiviz.utils.secrets import get_secret

local_path = os.path.join(
    os.path.realpath(os.path.dirname(os.path.realpath(__file__))),
    "local"
)


def get_local_secret(secret_name: str) -> Optional[str]:
    """
    Helper function for supporting local development flows.
    When in development environments, we fetch file contents from `recidiviz/case_triage/local/gsm`
    In Google Cloud environments, we delegate to Secrets Manager
    """
    if in_development():
        try:
            return Path(os.path.join(local_path, "gsm", secret_name)).read_text()
        except OSError:
            logging.error("Couldn't locate secret %s", secret_name)
            return None

    return get_secret(secret_name)


def get_local_file(file_path: GcsfsFilePath) -> str:
    """
    Helper function for supporting local development flows.
    When in development environments, we fetch file contents from `recidiviz/case_triage/local/gcs`
    In Google Cloud environments, we delegate to Cloud Storage
    """

    if in_development():
        return Path(os.path.join(local_path, "gcs",  file_path.abs_path())).read_text()

    gcs_fs = GcsfsFactory.build()
    return gcs_fs.download_as_string(file_path)
