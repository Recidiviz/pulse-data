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

"""Custom configuration for how pytest should run."""
import os
import shlex
import subprocess
from time import sleep, time
from typing import Dict, List, Optional, Tuple

import pytest
import yaml
from mock import patch

import recidiviz

EMULATOR_STARTUP_TIMEOUT = 30


def pytest_configure(config) -> None:
    recidiviz.called_from_test = True
    config.addinivalue_line(
        "markers", "uses_db: for tests that spin up a new database."
    )


def pytest_unconfigure() -> None:
    del recidiviz.called_from_test


def pytest_addoption(parser) -> None:
    parser.addoption(
        "-E",
        "--with-emulator",
        action="store_true",
        help="run tests that require the datastore emulator.",
    )
    parser.addoption("--test-set", type=str, choices=["parallel", "not-parallel"])


def pytest_runtest_setup(item: pytest.Item) -> None:
    test_set = item.config.getoption("test_set", default=None)
    if "emulator" in item.fixturenames:
        if test_set == "parallel" or not item.config.getoption(
            "with_emulator", default=None
        ):
            pytest.skip("requires datastore emulator")
    else:
        # For tests without the emulator, prevent them from trying to create google cloud clients.
        item.google_auth_patcher = patch("google.auth.default")
        mock_google_auth = item.google_auth_patcher.start()
        mock_google_auth.side_effect = AssertionError(
            "Unit test may not instantiate a Google client. Please mock the appropriate client class inside this test "
            " (e.g. `patch('google.cloud.bigquery.Client')`)."
        )

        if item.get_closest_marker("uses_db") is not None:
            if test_set == "parallel":
                pytest.skip("[parallel tests] skipping because test requires database")
        else:
            if test_set == "not-parallel":
                pytest.skip(
                    "[not-parallel tests] skipping because test does not require database or emulator"
                )


def pytest_runtest_teardown(item: pytest.Item) -> None:
    if hasattr(item, "google_auth_patcher") and item.google_auth_patcher is not None:
        item.google_auth_patcher.stop()


# TODO(#263): return the datastore client from this fixture
@pytest.fixture(scope="session")
def emulator(request) -> None:
    # Lazy imports here to avoid slow top-level import when running pytest for any tests
    # pylint: disable=import-outside-toplevel
    from recidiviz.ingest.scrape import sessions
    from recidiviz.utils import pubsub_helper

    datastore_emulator, pubsub_emulator = _start_emulators()

    # These environment variables can only be set while these tests are
    # running as they also impact ndb/testbed behavior
    prior_environs = _write_emulator_environs()
    sessions.clear_ds()
    pubsub_helper.clear_publisher()
    pubsub_helper.clear_subscriber()

    def cleanup() -> None:
        datastore_emulator.terminate()
        pubsub_emulator.terminate()

        _restore_environs(prior_environs)
        sessions.clear_ds()
        pubsub_helper.clear_publisher()
        pubsub_helper.clear_subscriber()

    request.addfinalizer(cleanup)


def _start_emulators() -> Tuple[subprocess.Popen, subprocess.Popen]:
    """Start gcloud datastore and pubsub emulators."""
    # pylint: disable=consider-using-with
    datastore_emulator = subprocess.Popen(
        shlex.split(
            "gcloud beta emulators datastore start --no-store-on-disk "
            "--consistency=1.0 --project=test-project"
        )
    )
    pubsub_emulator = subprocess.Popen(
        shlex.split("gcloud beta emulators pubsub start " "--project=test-project")
    )

    # Sleep to ensure emulators successfully start
    emulator_start_time = time()
    sleep(5)
    while not _emulators_started():
        if time() - emulator_start_time > EMULATOR_STARTUP_TIMEOUT:
            raise Exception("Emulators did not start up before timeout.")
        sleep(1)

    if datastore_emulator.poll() or pubsub_emulator.poll():
        datastore_emulator.terminate()
        pubsub_emulator.terminate()
        raise Exception("Failed to start gcloud emulators!")

    return datastore_emulator, pubsub_emulator


def _get_emulator_env_paths() -> List[str]:
    return [
        os.path.join(
            os.environ.get("HOME", ""),
            f".config/gcloud/emulators/{emulator_name}/env.yaml",
        )
        for emulator_name in ["datastore", "pubsub"]
    ]


def _emulators_started() -> bool:
    for emulator_env_path in _get_emulator_env_paths():
        if not os.path.exists(emulator_env_path):
            return False
    return True


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
