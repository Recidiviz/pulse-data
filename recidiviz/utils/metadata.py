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

"""Utility methods for fetching app engine related metadata."""
import enum
import json
import logging
import os
import time
from typing import Any, Dict, Optional

import attr
import requests

from recidiviz.utils import environment
from recidiviz.utils.environment import CloudRunEnvironment

BASE_METADATA_URL = "http://metadata/computeMetadata/v1/"
HEADERS = {"Metadata-Flavor": "Google"}
TIMEOUT = 2

_metadata_cache: Dict[str, str] = {}

# Only used for this files tests
allow_local_metadata_call = False


def _get_metadata(url: str) -> Optional[str]:
    if url in _metadata_cache:
        return _metadata_cache[url]

    if not allow_local_metadata_call:
        if environment.in_test() or not environment.in_gcp():
            raise RuntimeError(
                "May not be called from test, should this have a local override?"
            )

    try:
        r = requests.get(BASE_METADATA_URL + url, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        _metadata_cache[url] = r.text
        return r.text
    except Exception as e:
        logging.error("Failed to fetch metadata [%s]: [%s]", url, e)
        return None


def project_number() -> Optional[str]:
    """Gets the numeric_project_id (project number) for this instance from the
    Compute Engine metadata server.
    """
    return _get_metadata("project/numeric-project-id")


_PROJECT_ID_URL = "project/project-id"

_override_set = False


def set_development_project_id_override(project_id_override: str) -> None:
    """Can be used when running the server for development (e.g. via docker-compose) to
    set the project id globally for all code running on that server.
    """
    if not environment.in_development():
        raise ValueError("Cannot call this outside of development.")

    _set_project_id_override(project_id_override)


def _set_project_id_override(project_id_override: str) -> Optional[str]:
    """Sets a project id override and returns the project id that was set before the
    override.
    """
    global _override_set
    if _override_set:
        raise ValueError(f"Project id override already set to {project_id()}")
    _override_set = True

    original_project_id = _metadata_cache.get(_PROJECT_ID_URL, None)
    _metadata_cache[_PROJECT_ID_URL] = project_id_override
    return original_project_id


class local_project_id_override:
    """Allows us to set a local project override for scripts running locally.

    Usage:
    if __name__ == '__main__':
        print(metadata.project_id())
        with local_project_id_override(GCP_PROJECT_STAGING):
            print(metadata.project_id())
         print(metadata.project_id())

    Prints:
        None
        recidiviz-staging
        None
    """

    def __init__(self, project_id_override: str):
        self.project_id_override = project_id_override
        self.original_project_id: Optional[str] = None

    @environment.local_only
    def __enter__(self) -> None:
        self.original_project_id = _set_project_id_override(self.project_id_override)

    def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        del _metadata_cache[_PROJECT_ID_URL]
        if self.original_project_id:
            _metadata_cache[_PROJECT_ID_URL] = self.original_project_id

        global _override_set
        _override_set = False


# mypy: ignore-errors
def project_id() -> str:
    """Gets the project_id for this instance from the Compute Engine metadata
    server. If the metadata server is unavailable, it assumes that the
    application is running locally and falls back to the GOOGLE_CLOUD_PROJECT
    environment variable.
    """
    return _get_metadata(_PROJECT_ID_URL) or os.getenv(environment.GOOGLE_CLOUD_PROJECT)


def instance_id() -> Optional[str]:
    """Returns the numerical ID of the current GCP instance."""
    return _get_metadata("instance/id")


def instance_name() -> Optional[str]:
    """Returns the name of the current GCP instance."""
    return _get_metadata("instance/name")


def zone() -> Optional[str]:
    """Returns the GCP zone of the current instance."""
    zone_string = _get_metadata("instance/zone")
    if zone_string:
        # Of the form 'projects/123456789012/zones/us-east1-c'
        zone_string = zone_string.split("/")[-1]

    return zone_string


def region() -> Optional[str]:
    """Returns the GCP region of the current instance."""
    region_string = None
    zone_string = zone()
    if zone_string:
        # Of the form 'us-east1-c'
        region_split = zone_string.split("-")[:2]
        region_string = "-".join(region_split)

    return region_string


def service_token() -> Optional[str]:
    token_response = _get_metadata("instance/service-accounts/default/token")

    if token_response:
        return json.loads(token_response)["access_token"]

    return token_response


@attr.s(auto_attribs=True)
class CloudRunMetadata:
    """Metadata about the Cloud Run service this process is running in."""

    project_id: str
    region: str
    url: str
    service_account_email: str

    class Service(enum.StrEnum):
        ADMIN_PANEL = "admin-panel"
        APPLICATION_DATA_IMPORT = "application-data-import"
        CASE_TRIAGE = "case-triage-web"
        IDENTITY_SERVICE = "identity-service"

    # Attempts and backoff for fetching this service's own metadata. The fetch
    # runs while a server boots, when several gunicorn workers issue it
    # simultaneously on a CPU-starved cold-starting container; without retries,
    # one transient error response crashes the worker and with it the whole
    # container boot. The delay before attempt N is N - 1 times the backoff
    # constant (2s, 4s, 6s, ...).
    _BUILD_METADATA_ATTEMPTS = 5
    _BUILD_METADATA_BACKOFF_SECONDS = 2

    @classmethod
    def build_from_metadata_server(
        cls, service_name: Service | None
    ) -> "CloudRunMetadata":
        """Builds the CloudRunMetadata from the googleapis
        https://cloud.google.com/run/docs/reference/rest/v1/namespaces.services/get

        Retries transient failures with backoff, since a raised exception here
        fails the calling server's boot. Fails immediately on a definitive 4xx
        response (other than 429), which retrying cannot fix.
        """
        _project_id = project_id()
        _region = region()
        service_name = (
            service_name.value
            if service_name is not None
            else CloudRunEnvironment.get_service_name()
        )
        last_error: Exception | None = None
        for attempt in range(cls._BUILD_METADATA_ATTEMPTS):
            if attempt > 0:
                time.sleep(cls._BUILD_METADATA_BACKOFF_SECONDS * attempt)
            try:
                response = requests.get(
                    f"https://{_region}-run.googleapis.com/apis/serving.knative.dev/v1/namespaces/{_project_id}/services/{service_name}",
                    headers={"Authorization": f"Bearer {service_token()}"},
                    timeout=TIMEOUT,
                )
                response.raise_for_status()
                service_metadata = response.json()
            except requests.RequestException as e:
                error_response = e.response
                if (
                    error_response is not None
                    and 400 <= error_response.status_code < 500
                    and error_response.status_code != 429
                ):
                    # A 4xx is a definitive rejection that retrying cannot fix,
                    # e.g. the 403 a service gets when its account lacks
                    # roles/run.viewer.
                    raise RuntimeError(
                        f"Request for Cloud Run service metadata for "
                        f"[{service_name}] failed with HTTP "
                        f"[{error_response.status_code}]: [{error_response.text}]"
                    ) from e
                logging.warning(
                    "Attempt [%s] to fetch Cloud Run service metadata for [%s] "
                    "failed: %s",
                    attempt + 1,
                    service_name,
                    e,
                )
                last_error = e
                continue
            try:
                url = service_metadata["status"]["url"]
                service_account_email = service_metadata["spec"]["template"]["spec"][
                    "serviceAccountName"
                ]
            except KeyError as e:
                logging.warning(
                    "Attempt [%s] to fetch Cloud Run service metadata for [%s] "
                    "returned an incomplete response missing key [%s]: [%s]",
                    attempt + 1,
                    service_name,
                    e,
                    service_metadata,
                )
                last_error = ValueError(
                    f"Cloud Run service metadata response for [{service_name}] "
                    f"is missing key [{e}]: [{service_metadata}]"
                )
                continue
            return cls(
                project_id=_project_id,
                region=_region,
                url=url,
                service_account_email=service_account_email,
            )
        raise RuntimeError(
            f"Unable to fetch Cloud Run service metadata for [{service_name}] "
            f"after [{cls._BUILD_METADATA_ATTEMPTS}] attempts"
        ) from last_error


def running_against(project: str, *, log_hint: Optional[bool] = True) -> bool:
    try:
        return project_id() == project
    except RuntimeError as e:
        if log_hint:
            logging.warning(
                "An error occurred when checking which environment we are running against: %s",
                e,
            )

        return False
