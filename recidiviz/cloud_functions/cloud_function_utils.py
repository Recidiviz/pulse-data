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

"""This file contains all of the relevant helpers for cloud functions.

Mostly copied from:
https://cloud.google.com/iap/docs/authentication-howto#iap_make_request-python
"""
import json
import logging
from typing import Any

import google.auth
import google.auth.app_engine
import google.auth.compute_engine.credentials
import google.auth.iam
import google.oauth2.credentials
import google.oauth2.service_account
import requests
from google.auth.credentials import Credentials
from google.auth.transport.requests import (  # type: ignore[attr-defined]
    AuthorizedSession,
)

GCP_PROJECT_ID_KEY = "GCP_PROJECT"

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


def trigger_dag(web_server_url: str, dag_id: str, data: dict) -> requests.Response:
    """
    Make a request to trigger a dag using the stable Airflow 2 REST API.
    https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html

    Args:
      web_server_url: The URL of the Airflow 2 web server.
      dag_id: The DAG ID.
      data: Additional configuration parameters for the DAG run (json).
    """

    endpoint = f"api/v1/dags/{dag_id}/dagRuns"
    request_url = f"{web_server_url}/{endpoint}"
    json_data = {"conf": data}

    response = make_composer2_web_server_request(
        request_url, method="POST", json=json_data
    )

    if response.status_code == 403:
        logging.warning(
            "You do not have a permission to perform this operation. "
            "Check Airflow RBAC roles for your account."
            "%s / %s",
            response.headers,
            response.text,
        )

        raise requests.HTTPError(
            request=response.request,
            response=response,
        )
    return response


def cloud_functions_log(severity: str, message: str) -> None:
    # TODO(https://issuetracker.google.com/issues/124403972?pli=1): Workaround until
    #  built-in logging is fixed for Python cloud functions
    # Complete a structured log entry.
    entry = {
        "severity": severity,
        "message": message,
        # Log viewer accesses 'component' as jsonPayload.component'.
        "component": "arbitrary-property",
    }

    print(json.dumps(entry))
