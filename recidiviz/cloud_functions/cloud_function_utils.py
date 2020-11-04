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
import re
from typing import Optional, Match

import requests

import google.auth
import google.auth.app_engine
import google.auth.compute_engine.credentials
import google.auth.iam
from google.auth.transport.requests import Request
import google.oauth2.credentials
import google.oauth2.service_account
from googleapiclient.discovery import build
from oauth2client.client import GoogleCredentials


IAP_CLIENT_ID = {
    'recidiviz-staging': ('984160736970-flbivauv2l7sccjsppe34p7436l6890m.apps.'
                          'googleusercontent.com'),
    'recidiviz-123': ('688733534196-uol4tvqcb345md66joje9gfgm26ufqj6.apps.'
                      'googleusercontent.com'),
    'recidiviz-staging-airflow': ('17428640429-55a16miqmvtdq0damq39t1iht8vmo981.apps.'
                                  'googleusercontent.com'),
    'recidiviz-123-airflow': '236713977790-80cjalk1vgpk7niv38eb23r3gs0ls8mn.apps.googleusercontent.com'
}

GCP_PROJECT_ID_KEY = 'GCP_PROJECT'

_IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
_OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'
_STATE_DIRECT_INGEST_BUCKET_REGEX = re.compile(
        r'(recidiviz-staging|recidiviz-123)-direct-ingest-state-'
        r'([a-z]+-[a-z]+)$')

# Value to be passed to the GCSFileSystem cache_timeout to indicate that we
# should not cache.
GCSFS_NO_CACHING = -1

# pylint: disable=protected-access


def make_iap_request(url: str, client_id: str, method='GET', **kwargs):
    """Makes a request to an application protected by Identity-Aware Proxy.

    Args:
      url: The Identity-Aware Proxy-protected URL to fetch.
      client_id: The client ID used by Identity-Aware Proxy.
      method: The request method to use
              ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE')
      **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py

    Returns:
      The page body, or raises an exception if the page couldn't be retrieved.
    """
    # Figure out what environment we're running in and get some preliminary
    # information about the service account.
    bootstrap_credentials, _ = google.auth.default(scopes=[_IAM_SCOPE])
    if isinstance(bootstrap_credentials,
                  google.oauth2.credentials.Credentials):
        raise Exception('make_iap_request is only supported for service '
                        'accounts.')

    # For service accounts using the Compute Engine metadata service,
    # service_account_email isn't available until refresh is called.
    bootstrap_credentials.refresh(Request())

    signer_email = bootstrap_credentials.service_account_email
    if isinstance(bootstrap_credentials,
                  google.auth.compute_engine.credentials.Credentials):
        signer = google.auth.iam.Signer(
            Request(), bootstrap_credentials, signer_email)
    else:
        # A Signer object can sign a JWT using the service accounts key.
        signer = bootstrap_credentials.signer

    # Construct OAuth 2.0 service account credentials using the signer
    # and email acquired from the bootstrap credentials.
    service_account_credentials = google.oauth2.service_account.Credentials(
        signer, signer_email, token_uri=_OAUTH_TOKEN_URI, additional_claims={
            'target_audience': client_id
        })

    # service_account_credentials gives us a JWT signed by the service
    # account. Next, we use that to obtain an OpenID Connect token,
    # which is a JWT signed by Google.
    google_open_id_connect_token = get_google_open_id_connect_token(
        service_account_credentials)

    # Fetch the Identity-Aware Proxy-protected URL, including an
    # Authorization header containing "Bearer " followed by a
    # Google-issued OpenID Connect token for the service account.
    response = requests.request(
        method, url,
        headers={'Authorization': 'Bearer {}'.format(
            google_open_id_connect_token)}, **kwargs)
    if response.status_code == 403:
        raise Exception('Service account {} does not have permission to '
                        'access the IAP-protected application.'.format(
                            signer_email))
    if response.status_code != 200:
        raise Exception(
            'Bad response from application: {!r} / {!r} / {!r}'.format(
                response.status_code, response.headers, response.text))
    return response


def trigger_dataflow_job_from_template(project_id: str, bucket: str,
                                       template: str, job_name: str):
    """Trigger the Dataflow job at the given template location and execute it
    with the given `job_name`."""
    credentials = GoogleCredentials.get_application_default()
    service = build('dataflow', 'v1b3', credentials=credentials)

    body = {
        "jobName": "{job_name}".format(job_name=job_name),
        "gcsPath": "gs://{bucket}/templates/{template}".format(
            bucket=bucket, template=template),
        "environment": {
            "tempLocation": "gs://{bucket}/temp".format(bucket=bucket),
        }
    }

    request = service.projects().locations().templates().create(
        projectId=project_id, body=body, location="us-west1")
    response = request.execute()
    return response


def get_google_open_id_connect_token(
        service_account_credentials: google.oauth2.credentials.Credentials):
    """Get an OpenID Connect token issued by Google for the service account.
    """

    service_account_jwt = (
        service_account_credentials._make_authorization_grant_assertion())
    request = google.auth.transport.requests.Request()
    body = {
        'assertion': service_account_jwt,
        'grant_type': google.oauth2._client._JWT_GRANT_TYPE,
    }
    token_response = google.oauth2._client._token_endpoint_request(
        request, _OAUTH_TOKEN_URI, body)
    return token_response['id_token']


def get_state_region_code_from_direct_ingest_bucket(bucket) -> Optional[str]:
    match_obj: Optional[Match] = \
        re.match(_STATE_DIRECT_INGEST_BUCKET_REGEX, bucket)
    if match_obj is None:
        return None

    region_code_match = match_obj.groups()[1]  # Object at index 0 is project_id
    return region_code_match.replace('-', '_')


def get_dataflow_template_bucket(project_id: str) -> str:
    return f'{project_id}-dataflow-templates'
