# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

import pytest
import yaml

import recidiviz
from recidiviz.ingest import docket, sessions
from recidiviz.utils import secrets


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
@pytest.fixture(scope='class')
def emulator(request):
    # These environment variables can only be set while these tests are
    # running as they also impact ndb/testbed behavior
    prior_environs = write_emulator_environs()
    sessions.clear_ds()
    secrets.clear_ds()
    docket.clear_publisher()
    docket.clear_subscriber()

    def cleanup():
        restore_environs(prior_environs)
        sessions.clear_ds()
        secrets.clear_ds()
        docket.clear_publisher()
        docket.clear_subscriber()

    request.addfinalizer(cleanup)


def write_emulator_environs():
    old_environs = {}

    # Note: If multiple emulator environments contain the same key, the last one
    # wins
    emulator_names = ['datastore', 'pubsub']
    env_dict = {}
    for emulator_name in emulator_names:
        filename = os.path.join(
            os.environ.get('HOME'),
            '.config/gcloud/emulators/{}/env.yaml'.format(emulator_name))
        env_file = open(filename, 'r')
        env_dict.update(yaml.load(env_file))
        env_file.close()

    old_environs = {key: os.environ.get(key) for key in env_dict}
    os.environ.update(env_dict)
    # https://github.com/GoogleCloudPlatform/google-cloud-datastore/issues/168#issuecomment-294418422
    if 'DATASTORE_PROJECT_ID' in env_dict:
        old_environs['APPLICATION_ID'] = os.environ.get('APPLICATION_ID')
        os.environ['APPLICATION_ID'] = env_dict['DATASTORE_PROJECT_ID']

    return old_environs


def restore_environs(prior_environs):
    for key, value in prior_environs.items():
        if value:
            os.environ[key] = value
        else:
            del os.environ[key]
