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
import os
from pathlib import Path
from typing import Dict, Optional

from google.cloud import exceptions
from google.cloud import secretmanager_v1beta1 as secretmanager

from recidiviz.utils import environment, metadata
from recidiviz.utils.environment import in_development, in_test

__sm = None


def _sm() -> secretmanager.SecretManagerServiceClient:
    global __sm
    if not __sm:
        __sm = secretmanager.SecretManagerServiceClient()
    return __sm


@environment.test_only
def clear_sm() -> None:
    global __sm
    __sm = None


CACHED_SECRETS: Dict[str, str] = {}


def get_secret(secret_id: str) -> Optional[str]:
    """Retrieve secret from local cache or the Secret Manager.

    A helper function for processes to retrieve secrets. First checks a local cache: if not found, this will pull from
    the secret from the Secret Manager API and populate the local cache.

    Returns None if the secret could not be found.
    """
    secret_value = CACHED_SECRETS.get(secret_id)
    if secret_value:
        return secret_value

    project_id = metadata.project_id()
    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    try:
        response = _sm().access_secret_version(name=secret_name)
    except exceptions.NotFound:
        logging.warning("Couldn't locate secret: [%s].", secret_id)
        return None
    except Exception:
        logging.error(
            "Couldn't successfully connect to secret manager to retrieve secret: [%s].",
            secret_id,
            exc_info=True,
        )
        return None

    if not response or not response.payload or not response.payload.data:
        logging.error("Couldn't retrieve secret: [%s].", secret_id)
        return None

    secret_value = response.payload.data.decode("UTF-8")
    if secret_value is None:
        logging.error("Couldn't decode secret: [%s].", secret_id)
        return None
    CACHED_SECRETS[secret_id] = secret_value
    return secret_value


def get_local_secret(local_path: str, secret_name: str) -> Optional[str]:
    """
    Helper function for supporting local development flows.
    When in development environments, we fetch file contents from `recidiviz/<app name>/local/gsm`
    In Google Cloud environments, we delegate to Secrets Manager.
    """
    if in_development() or in_test():
        try:
            secret = Path(os.path.join(local_path, "gsm", secret_name)).read_text(
                "utf-8"
            )
            return secret.strip()
        except OSError:
            logging.error("Couldn't locate secret %s", secret_name)
            return None

    return get_secret(secret_name)
