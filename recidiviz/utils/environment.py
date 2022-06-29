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
import logging
import os
import sys
from enum import Enum
from functools import wraps
from typing import Any, Callable, Optional

import requests
from google.cloud import datastore, environment_vars

import recidiviz


class GCPEnvironment(Enum):
    STAGING = "staging"
    PRODUCTION = "production"


GCP_PROJECT_STAGING = "recidiviz-staging"
GCP_PROJECT_PRODUCTION = "recidiviz-123"


GCP_ENVIRONMENTS = {env.value for env in GCPEnvironment}
GCP_PROJECTS = [GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION]


def in_gcp() -> bool:
    """Check whether we're currently running on local dev machine or in prod

    Checks whether the current instance is running hosted on GCP (if not, likely
    running on local devserver).

    Args:
        N/A

    Returns:
        True if on hosted GCP instance
        False if not
    """
    return get_gcp_environment() in GCP_ENVIRONMENTS


def get_gcp_environment() -> Optional[str]:
    """Get the environment we are running in

    GCP environment must always refer to the actual environment. RECIDIVIZ_ENV variable should never be
    set locally, and if you need to specify an environment to run in locally, use the project_id (i.e. GCP_PROJECTS).

    Args:
        N/A

    Returns:
        The gae instance we are running in, or None if it is not set
    """
    return os.getenv("RECIDIVIZ_ENV")


def in_gcp_production() -> bool:
    return in_gcp() and get_gcp_environment() == GCPEnvironment.PRODUCTION.value


def in_gcp_staging() -> bool:
    return in_gcp() and get_gcp_environment() == GCPEnvironment.STAGING.value


def get_datastore_client() -> datastore.Client:
    # If we're running with the datastore emulator, we must specify `_https` due
    # to a bug in the datastore client.
    # See: https://github.com/googleapis/google-cloud-python/issues/5738
    if os.environ.get(environment_vars.GCD_HOST):
        return datastore.Client(_http=requests.Session)

    return datastore.Client()


def local_only(func: Callable) -> Callable:
    """Decorator function to verify request only runs locally

    Decorator function to check run environment. If prod / served on GCP,
    exits before any work can be done.

    Args:
        N/A

    Returns:
        If running locally, results of decorated function.
        If not, nothing.
    """

    @wraps(func)
    def check_env(*args: Any, **kwargs: Any) -> Any:
        """Decorator child-method to fail if runtime is in prod

        This is the function the decorator uses to test whether or not our
        runtime is in prod, and if so error out.

        Args:
            args, kwargs: Any arguments passed to that request handler

        Returns:
            Output of the decorated function, if running locally
            HTTP 500 and error logs, if running in prod
        """

        deployed = in_gcp()

        if deployed:
            # Production environment - fail
            logging.error("This API call is not allowed in production.")
            raise RuntimeError("Not available, see service logs.")

        # Local development server - continue
        logging.info("Test environment, proceeding.")

        return func(*args, **kwargs)

    return check_env


def in_test() -> bool:
    """Check whether we are running in a test"""
    # Pytest sets recidiviz.called_from_test in conftest.py
    if not hasattr(recidiviz, "called_from_test"):
        # If it is not set, we may have been called from unittest. Check if unittest has been imported, if it has then
        # we assume we are running from a unittest
        setattr(recidiviz, "called_from_test", "unittest" in sys.modules)
    return getattr(recidiviz, "called_from_test")


def in_ci() -> bool:
    """Check whether we are running in a ci environment"""
    return os.environ.get("CI") == "true"


def test_only(func: Callable) -> Callable:
    """Decorator to verify function only runs in tests

    If called while not in tests, throws an exception.
    """

    @wraps(func)
    def check_test_and_call(*args: Any, **kwargs: Any) -> Callable:
        if not in_test():
            raise RuntimeError("Function may only be called from tests")
        return func(*args, **kwargs)

    return check_test_and_call


def in_development() -> bool:
    return os.environ.get("IS_DEV") == "true"


def get_version() -> str:
    return os.getenv("GAE_VERSION", "")


class ServiceType(Enum):
    DEFAULT = "default"


def get_service_type() -> ServiceType:
    return ServiceType(os.getenv("GAE_SERVICE", "default"))
