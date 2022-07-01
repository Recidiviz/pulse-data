# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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

"""Helpers for making requests to Cloud Composer"""

import os
from typing import Any, Optional

import google.auth.compute_engine.credentials
import requests
from google.auth.credentials import Credentials
from google.auth.transport.requests import (  # type: ignore[attr-defined]
    AuthorizedSession,
)

# Following GCP best practices, these credentials should be
# constructed at start-up time and used throughout
# https://cloud.google.com/apis/docs/client-libraries-best-practices
_COMPOSER_AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"

_COMPOSER_CREDENTIALS = None


def get_composer_credentials() -> Credentials:
    global _COMPOSER_CREDENTIALS
    if _COMPOSER_CREDENTIALS is None:
        _COMPOSER_CREDENTIALS, _ = google.auth.default(scopes=[_COMPOSER_AUTH_SCOPE])
    return _COMPOSER_CREDENTIALS


def get_airflow_uri() -> Optional[str]:
    """Get the Airflow web server URL, or None if it is not set."""
    return os.getenv("AIRFLOW_URI")


# From https://cloud.google.com/composer/docs/composer-2/triggering-with-gcf#add_the_code_for_triggering_dags_using_airflow_rest_api
def make_composer2_web_server_request(
    url: str, method: str = "GET", **kwargs: Any
) -> requests.Response:
    """
    Make a request to Cloud Composer 2 environment's web server.
    Args:
      url: The URL to fetch.
      method: The request method to use ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT',
        'PATCH', 'DELETE')
      **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py
                  If no timeout is provided, it is set to 90 by default.
    """

    authed_session = AuthorizedSession(get_composer_credentials())

    # Set the default timeout, if missing
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90

    return authed_session.request(method, url, **kwargs)


def trigger_dag(dag_id: str) -> requests.Response:
    """
    Make a request to trigger a dag using the stable Airflow 2 REST API.
    https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html

    Args:
      web_server_url: The URL of the Airflow 2 web server.
      dag_id: The DAG ID.
      data: Additional configuration parameters for the DAG run (json).
    """

    if not get_airflow_uri():
        raise requests.RequestException("Airflow URI is not set on the server")

    endpoint = f"api/v1/dags/{dag_id}/dagRuns"
    request_url = f"{get_airflow_uri()}/{endpoint}"

    response = make_composer2_web_server_request(request_url, method="POST")

    if response.status_code == 403:
        raise requests.HTTPError(
            "You do not have a permission to perform this operation. "
            "Check Airflow RBAC roles for your account."
            f"{response.headers} / {response.text}"
        )
    return response
