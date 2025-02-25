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
import signal
import subprocess
from time import sleep
from typing import Tuple

import pytest
import yaml

import recidiviz
from recidiviz.ingest.scrape import sessions
from recidiviz.utils import secrets, pubsub_helper


def pytest_configure():
    recidiviz.called_from_test = True


def pytest_unconfigure():
    del recidiviz.called_from_test


def pytest_addoption(parser):
    parser.addoption("-E", "--with-emulator", action="store_true",
                     help="run tests that require the datastore emulator.")


def pytest_runtest_setup(item):
    if ('emulator' in item.fixturenames and
            not item.config.getoption('with_emulator', default=None)):
        pytest.skip("requires datastore emulator")


# TODO(263): return the datastore client from this fixture
@pytest.fixture(scope='session')
def emulator(request):
    datastore_emulator, pubsub_emulator = _start_emulators()

    # These environment variables can only be set while these tests are
    # running as they also impact ndb/testbed behavior
    prior_environs = _write_emulator_environs()
    sessions.clear_ds()
    secrets.clear_ds()
    pubsub_helper.clear_publisher()
    pubsub_helper.clear_subscriber()

    def cleanup():
        os.killpg(datastore_emulator.pid, signal.SIGTERM)
        os.killpg(pubsub_emulator.pid, signal.SIGTERM)

        _restore_environs(prior_environs)
        sessions.clear_ds()
        secrets.clear_ds()
        pubsub_helper.clear_publisher()
        pubsub_helper.clear_subscriber()

    request.addfinalizer(cleanup)


def _start_emulators() -> Tuple[subprocess.Popen, subprocess.Popen]:
    """Start gcloud datastore and pubsub emulators."""
    # Create a new process group for each subprocess to enable killing each
    # subprocess and their subprocesses without killing ourselves
    preexec_fn = os.setsid

    datastore_emulator = subprocess.Popen(
        shlex.split('gcloud beta emulators datastore start --no-store-on-disk '
                    '--consistency=1.0 --project=test-project'),
        preexec_fn=preexec_fn)
    pubsub_emulator = subprocess.Popen(
        shlex.split('gcloud beta emulators pubsub start '
                    '--project=test-project'),
        preexec_fn=preexec_fn)

    # Sleep to ensure emulators successfully start
    sleep(5)

    if datastore_emulator.poll() or pubsub_emulator.poll():
        os.killpg(datastore_emulator.pid, signal.SIGTERM)
        os.killpg(pubsub_emulator.pid, signal.SIGTERM)
        raise Exception('Failed to start gcloud emulators!')

    return datastore_emulator, pubsub_emulator


def _write_emulator_environs():
    # Note: If multiple emulator environments contain the same key, the last one
    # wins
    emulator_names = ['datastore', 'pubsub']
    env_dict = {}
    for emulator_name in emulator_names:
        filename = os.path.join(
            os.environ.get('HOME'),
            '.config/gcloud/emulators/{}/env.yaml'.format(emulator_name))
        env_file = open(filename, 'r')
        env_dict.update(yaml.full_load(env_file))
        env_file.close()

    old_environs = {key: os.environ.get(key) for key in env_dict}
    os.environ.update(env_dict)
    # https://github.com/GoogleCloudPlatform/google-cloud-datastore/issues/168#issuecomment-294418422
    if 'DATASTORE_PROJECT_ID' in env_dict:
        old_environs['APPLICATION_ID'] = os.environ.get('APPLICATION_ID')
        os.environ['APPLICATION_ID'] = env_dict['DATASTORE_PROJECT_ID']

    return old_environs


def _restore_environs(prior_environs):
    for key, value in prior_environs.items():
        if value:
            os.environ[key] = value
        else:
            del os.environ[key]
