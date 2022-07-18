# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
""" Fixture for starting/stopping GCP emulators """
import os
import shlex
import subprocess
from time import sleep, time
from typing import Any, Dict, List, Optional, Tuple

import pytest
import requests
import yaml

from conftest import get_pytest_worker_id

EMULATOR_STARTUP_TIMEOUT_SEC = 30


@pytest.fixture(scope="session")
def emulator(request: Any) -> None:
    # Lazy imports here to avoid slow top-level import when running pytest for any tests
    # pylint: disable=import-outside-toplevel
    from recidiviz.utils import pubsub_helper

    datastore_emulator, pubsub_emulator = _start_emulators()

    # These environment variables can only be set while these tests are
    # running as they also impact ndb/testbed behavior
    prior_environs = _write_emulator_environs()
    pubsub_helper.clear_publisher()
    pubsub_helper.clear_subscriber()

    def cleanup() -> None:
        datastore_emulator.terminate()
        pubsub_emulator.terminate()

        _restore_environs(prior_environs)
        pubsub_helper.clear_publisher()
        pubsub_helper.clear_subscriber()

    request.addfinalizer(cleanup)


def _start_emulators() -> Tuple[subprocess.Popen, subprocess.Popen]:
    """Start gcloud datastore and pubsub emulators."""
    # pylint: disable=consider-using-with
    os.makedirs(_get_datastore_emulator_data_dir(), exist_ok=True)
    os.makedirs(_get_pubsub_emulator_data_dir(), exist_ok=True)

    datastore_command = shlex.split(
        "gcloud beta emulators datastore start --no-store-on-disk "
        "--consistency=1.0 --project=test-project "
        f"--host-port=localhost:{_get_datastore_emulator_port()} "
        f'--data-dir="{_get_datastore_emulator_data_dir()}"'
    )
    datastore_emulator = subprocess.Popen(datastore_command)

    pubsub_command = shlex.split(
        "gcloud beta emulators pubsub start "
        "--project=test-project "
        f"--host-port=localhost:{_get_pubsub_emulator_port()} "
        f'--data-dir="{_get_pubsub_emulator_data_dir()}"'
    )
    pubsub_emulator = subprocess.Popen(pubsub_command)

    # Sleep to ensure emulators successfully start
    emulator_start_time = time()

    while not _emulators_started():
        sleep(1)

        if time() - emulator_start_time > EMULATOR_STARTUP_TIMEOUT_SEC:
            raise Exception("Emulators did not start up before timeout.")

    if datastore_emulator.poll() or pubsub_emulator.poll():
        datastore_emulator.terminate()
        pubsub_emulator.terminate()
        raise Exception("Failed to start gcloud emulators!")

    return datastore_emulator, pubsub_emulator


def _get_datastore_emulator_port() -> int:
    """Returns the port that the datastore emulator should bind to
    The emulator's default port is 8081"""
    return 8081 + get_pytest_worker_id()


def _get_datastore_emulator_data_dir() -> str:
    return os.path.join(
        os.environ.get("HOME", ""),
        f".config/gcloud/emulators/datastore/{get_pytest_worker_id()}/",
    )


def _get_datastore_emulator_env_path() -> str:
    return os.path.join(_get_datastore_emulator_data_dir(), "env.yaml")


def _get_pubsub_emulator_port() -> int:
    """Returns the port that the pubsub emulator should bind to.
    The emulator's default port is 8085, which could conflict with the datastore emulator, so we added 100"""
    return 8185 + get_pytest_worker_id()


def _get_pubsub_emulator_data_dir() -> str:
    return os.path.join(
        os.environ.get("HOME", ""),
        f".config/gcloud/emulators/pubsub/{get_pytest_worker_id()}/",
    )


def _get_pubsub_emulator_env_path() -> str:
    return os.path.join(_get_pubsub_emulator_data_dir(), "env.yaml")


def _get_emulator_env_paths() -> List[str]:
    return [_get_datastore_emulator_env_path(), _get_pubsub_emulator_env_path()]


def _emulators_started() -> bool:
    try:
        responses = [
            requests.get(f"http://localhost:{port}")
            for port in [_get_datastore_emulator_port(), _get_pubsub_emulator_port()]
        ]

        return all(
            response.status_code == 200 and response.content.strip() == b"Ok"
            for response in responses
        )
    except requests.exceptions.ConnectionError:
        return False


def _write_emulator_environs() -> Dict[str, Optional[str]]:
    # Note: If multiple emulator environments contain the same key, the last one
    # wins
    env_dict = {}
    for emulator_env_path in _get_emulator_env_paths():
        # pylint: disable=consider-using-with,unspecified-encoding
        env_file = open(emulator_env_path, "r")
        env_file_contents = yaml.full_load(env_file)
        if env_file_contents:
            env_dict.update(env_file_contents)
        env_file.close()

    old_environs = {key: os.environ.get(key) for key in env_dict}
    os.environ.update(env_dict)
    # https://github.com/GoogleCloudPlatform/google-cloud-datastore/issues/168#issuecomment-294418422
    if "DATASTORE_PROJECT_ID" in env_dict:
        old_environs["APPLICATION_ID"] = os.environ.get("APPLICATION_ID")
        os.environ["APPLICATION_ID"] = env_dict["DATASTORE_PROJECT_ID"]

    return old_environs


def _restore_environs(prior_environs: Dict[str, Optional[str]]) -> None:
    for key, value in prior_environs.items():
        if value:
            os.environ[key] = value
        else:
            del os.environ[key]
