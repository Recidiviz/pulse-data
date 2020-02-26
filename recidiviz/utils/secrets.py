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

"""Secrets for use at runtime."""

import logging
from typing import Dict

from google.cloud import exceptions
from google.cloud import secretmanager_v1beta1 as secretmanager
from recidiviz.utils import environment, metadata


__sm = None


def _sm():
    global __sm
    if not __sm:
        __sm = secretmanager.SecretManagerServiceClient()
    return __sm


@environment.test_only
def clear_sm():
    global __sm
    __sm = None


CACHED_SECRETS: Dict[str, str] = {}


def get_secret(secret_id):
    """Retrieve secret from local cache or the Secret Manager.

    A helper function for processes to retrieve secrets. First checks a local cache: if not found, this will pull from
    the secret from the Secret Manager API and populate the local cache.

    Returns None if the secret could not be found.
    """
    secret_value = CACHED_SECRETS.get(secret_id)
    if secret_value:
        return secret_value

    project_id = metadata.project_id()
    secret_name = _sm().secret_version_path(project_id, secret_id, 'latest')

    try:
        response = _sm().access_secret_version(secret_name)
    except exceptions.NotFound:
        logging.error("Couldn't locate secret: [%s].", secret_id)
        return None
    except Exception:
        logging.error("Couldn't successfully connect to secret manager to retrieve secret: [%s].",
                      secret_id, exc_info=True)
        return None

    if not response or not response.payload or not response.payload.data:
        logging.error("Couldn't retrieve secret: [%s].", secret_id)
        return None

    secret_value = response.payload.data.decode('UTF-8')
    CACHED_SECRETS[secret_id] = secret_value
    return secret_value
