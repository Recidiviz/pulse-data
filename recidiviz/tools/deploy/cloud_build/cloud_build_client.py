# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Client wrapper for Google Cloud Build"""
import datetime
import logging
import time

from google.api_core.client_options import ClientOptions
from google.cloud.devtools.cloudbuild import CloudBuildClient as GoogleCloudBuildClient
from google.cloud.devtools.cloudbuild_v1 import (
    Build,
    CreateBuildRequest,
    GetBuildRequest,
)

from recidiviz.utils import metadata
from recidiviz.utils.types import assert_type

UNFINISHED_BUILD_STATES = [
    Build.Status.STATUS_UNKNOWN,
    Build.Status.PENDING,
    Build.Status.QUEUED,
    Build.Status.WORKING,
]


class CloudBuildClient:
    """Client wrapper for Google Cloud Build"""

    DEFAULT_LOCATION = "us-west1"

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        client: GoogleCloudBuildClient | None = None,
    ):
        self.project_id = project_id
        self.location = location
        client_options = (
            None
            if location == "global"
            else ClientOptions(api_endpoint=f"{location}-cloudbuild.googleapis.com")
        )
        self.client = client or GoogleCloudBuildClient(
            client_options=client_options,
        )

    def get_build(self, build_id: str) -> Build:
        return self.client.get_build(
            request=GetBuildRequest(
                project_id=self.project_id,
                id=build_id,
            )
        )

    def run_build(self, build: Build, execute_async: bool = False) -> Build:
        """Runs a build and optionally waits for it to finish
        Raises:
            (TimeoutError): Build execution passed its allowed timeout
            (RuntimeError): Build did not complete successfully"""
        create_build_operation = self.client.create_build(
            request=CreateBuildRequest(
                parent=f"projects/{self.project_id}/locations/{self.location}",
                project_id=self.project_id,
                build=build,
            )
        )
        log_output_printed = False

        if execute_async:
            build = self.get_build(build_id=create_build_operation.metadata.build.id)
            logging.info("Logs can be found at %s", build.log_url)
            return build

        start_time = datetime.datetime.now()
        timeout_timedelta = assert_type(build.timeout, datetime.timedelta)
        while datetime.datetime.now() - start_time < timeout_timedelta:
            build = self.get_build(build_id=create_build_operation.metadata.build.id)
            if not log_output_printed:
                log_output_printed = True
                logging.info("Logs can be found at %s", build.log_url)

            logging.info(
                "Build %s is in status %s",
                build.id,
                build.status.name,
            )
            if build.status not in UNFINISHED_BUILD_STATES:
                if build.status != Build.Status.SUCCESS:
                    raise RuntimeError(
                        f"Build was not successful got [{build.status.name}], check Cloud Build logs at {build.log_url}"
                    )

                return build

            time.sleep(10)

        raise TimeoutError(f"Build timed out; last status: {build.status}")

    @classmethod
    def build(cls, location: str = DEFAULT_LOCATION) -> "CloudBuildClient":
        return CloudBuildClient(
            project_id=metadata.project_id(),
            location=location,
        )
