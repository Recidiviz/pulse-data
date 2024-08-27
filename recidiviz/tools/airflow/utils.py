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
"""Utilities for airflow tools"""
import json
import logging
import subprocess
import time
from typing import List

import google.cloud.orchestration.airflow.service_v1beta1 as service
from google.cloud.orchestration.airflow.service_v1beta1 import types
from more_itertools import one

from recidiviz.utils.environment import GCP_PROJECT_STAGING

COMPOSER_LOCATION = "us-central1"


def await_environment_state(
    environment_name: str,
    target_state: int,
    retry_states: List[int],
) -> types.Environment:
    client = service.EnvironmentsClient()

    while environment := client.get_environment(request={"name": environment_name}):
        if environment.state == types.Environment.State.ERROR:
            raise ValueError(
                f"Environment entered ERROR [{environment.state}] state - aborting."
            )
        if environment.state in retry_states:
            logging.info("Environment is in state %s. Sleeping...", environment.state)
            time.sleep(30)
        elif environment.state == target_state:
            logging.info("Environment is running and ready!")
            break
        else:
            raise ValueError(f"Unexpected state encountered: {environment.state}")

    return environment


def get_gcloud_auth_user() -> str:
    auths = json.loads(
        subprocess.check_output(["gcloud", "auth", "list", "--format", "json"])  # nosec
    )
    try:
        active_auth = one(auth for auth in auths if auth["status"] == "ACTIVE")
    except ValueError as e:
        raise RuntimeError("Must have an active gcloud auth to continue") from e

    return active_auth["account"].split("@")[0]


def get_airflow_environments() -> List[types.Environment]:
    client = service.EnvironmentsClient()

    return list(
        client.list_environments(
            request={
                "parent": client.common_location_path(
                    project=GCP_PROJECT_STAGING,
                    location=COMPOSER_LOCATION,
                )
            }
        )
    )


def get_user_experiment_environment_name() -> str:
    return f"experiment-{get_gcloud_auth_user()}"


def get_environment_by_name(name: str) -> types.Environment:
    client = service.EnvironmentsClient()
    try:
        return one(
            [
                environment
                for environment in get_airflow_environments()
                if environment.name
                == client.environment_path(
                    project=GCP_PROJECT_STAGING,
                    location=COMPOSER_LOCATION,
                    environment=name,
                )
            ]
        )
    except ValueError as e:
        raise ValueError(f"Cannot find environment with the name of {name}") from e


def get_user_experiment_environment() -> types.Environment:
    return get_environment_by_name(name=get_user_experiment_environment_name())
