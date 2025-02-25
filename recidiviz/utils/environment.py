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

"""Tools for working with environment variables.

Environment variables in Google App Engine are parsed via .yml files, which are
static and cannot be trivially made environment-specific. So this includes
functionality for determining which environment we are in and loading the
appropriate variables.
"""
from enum import Enum
from http import HTTPStatus
import logging
import os
from functools import wraps

import requests
from google.cloud import datastore, environment_vars

import recidiviz


class GaeEnvironment(Enum):
    STAGING = 'staging'
    PRODUCTION = 'production'


GAE_ENVIRONMENTS = {env.value for env in GaeEnvironment}

def in_gae():
    """ Check whether we're currently running on local dev machine or in prod

    Checks whether the current instance is running hosted on GAE (if not, likely
    running on local devserver).

    Args:
        N/A

    Returns:
        True if on hosted GAE instance
        False if not
    """
    return get_gae_environment() in GAE_ENVIRONMENTS

def get_gae_environment():
    """Get the environment we are running in

    Args:
        N/A

    Returns:
        The gae instance we are running in, or None if it is not set
    """
    return os.getenv('RECIDIVIZ_ENV')


def in_gae_production():
    return in_gae() and get_gae_environment() == GaeEnvironment.PRODUCTION


def in_gae_staging():
    return in_gae() and get_gae_environment() == GaeEnvironment.STAGING


def get_datastore_client() -> datastore.Client:
    # If we're running with the datastore emulator, we must specify `_https` due
    # to a bug in the datastore client.
    # See: https://github.com/googleapis/google-cloud-python/issues/5738
    if os.environ.get(environment_vars.GCD_HOST):
        return datastore.Client(_http=requests.Session)

    return datastore.Client()


def local_only(func):
    """Decorator function to verify request only runs locally

    Decorator function to check run environment. If prod / served on GAE,
    exits before any work can be done.

    Args:
        N/A

    Returns:
        If running locally, results of decorated function.
        If not, nothing.
    """

    @wraps(func)
    def check_env(*args, **kwargs):
        """Decorator child-method to fail if runtime is in prod

        This is the function the decorator uses to test whether or not our
        runtime is in prod, and if so error out.

        Args:
            args, kwargs: Any arguments passed to that request handler

        Returns:
            Output of the decorated function, if running locally
            HTTP 500 and error logs, if running in prod
        """

        deployed = in_gae()

        if deployed:
            # Production environment - fail
            logging.error("This API call is not allowed in production.")
            return ('Not available, see service logs.',
                    HTTPStatus.INTERNAL_SERVER_ERROR)

        # Local development server - continue
        logging.info("Test environment, proceeding.")

        return func(*args, **kwargs)

    return check_env


def in_test():
    """Check whether we are running in a test"""
    return hasattr(recidiviz, 'called_from_test')


def test_only(func):
    """Decorator to verify function only runs in tests

    If called while not in tests, throws an exception.
    """

    @wraps(func)
    def check_test_and_call(*args, **kwargs):
        if not in_test():
            raise RuntimeError("Function may only be called from tests")
        return func(*args, **kwargs)

    return check_test_and_call
