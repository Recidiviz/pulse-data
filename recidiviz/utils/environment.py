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
from importlib.metadata import PackageNotFoundError, metadata
from typing import Any, Callable, Optional

import recidiviz


class GCPEnvironment(Enum):
    STAGING = "staging"
    PRODUCTION = "production"


GCP_PROJECT_STAGING = "recidiviz-staging"
GCP_PROJECT_PRODUCTION = "recidiviz-123"
GCP_PROJECT_JUSTICE_COUNTS_STAGING = "justice-counts-staging"
GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION = "justice-counts-production"


GCP_ENVIRONMENTS = {env.value for env in GCPEnvironment}

# Data platform projects only
GCP_PROJECTS = [GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION]

RECIDIVIZ_ENV = "RECIDIVIZ_ENV"
GOOGLE_CLOUD_PROJECT = "GOOGLE_CLOUD_PROJECT"


# TODO(#21450) Rename to in_app_engine_env
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
    return get_gcp_environment() in GCP_ENVIRONMENTS or in_dataflow_worker()


def in_cloud_run() -> bool:
    try:
        CloudRunEnvironment.get_service_name()
        return True
    except NotInCloudRunError:
        return False


def in_dataflow_worker() -> bool:
    """If the `pulse-dataflow-pipelines` package is installed,
    we assume that the code is running in the Dataflow worker"""
    try:
        metadata("pulse-dataflow-pipelines")
        return True
    except PackageNotFoundError:
        return False


class NotInCloudRunError(KeyError):
    pass


class CloudRunEnvironment:
    """These environment variables are set on Cloud Run containers
    https://cloud.google.com/run/docs/container-contract#services-env-vars"""

    @staticmethod
    def get_cloud_run_environment_variable(environment_key: str) -> str:
        try:
            return os.environ[environment_key]
        except KeyError as e:
            raise NotInCloudRunError() from e

    @staticmethod
    def get_configuration_name() -> str:
        return CloudRunEnvironment.get_cloud_run_environment_variable("K_CONFIGURATION")

    @staticmethod
    def get_revision_name() -> str:
        return CloudRunEnvironment.get_cloud_run_environment_variable("K_REVISION")

    @staticmethod
    def get_service_name() -> str:
        return CloudRunEnvironment.get_cloud_run_environment_variable("K_SERVICE")


def get_gcp_environment() -> Optional[str]:
    """Get the environment we are running in

    GCP environment must always refer to the actual environment. RECIDIVIZ_ENV variable should never be
    set locally, and if you need to specify an environment to run in locally, use the project_id (i.e. GCP_PROJECTS).

    Args:
        N/A

    Returns:
        The gae instance we are running in, or None if it is not set
    """
    return os.getenv(RECIDIVIZ_ENV)


def get_project_for_environment(environment: GCPEnvironment) -> str:
    """Get the project for the given environment

    Args:
        environment: The environment to get the project for

    Returns:
        The project for the given environment
    """
    if environment == GCPEnvironment.STAGING:
        return GCP_PROJECT_STAGING
    if environment == GCPEnvironment.PRODUCTION:
        return GCP_PROJECT_PRODUCTION
    raise ValueError(f"Unknown environment {environment}")


def get_environment_for_project(project: str) -> GCPEnvironment:
    """Get the environment for the given project

    Args:
        project: The project to get the environment for

    Returns:
        The environment for the given project
    """
    if project == GCP_PROJECT_STAGING:
        return GCPEnvironment.STAGING
    if project == GCP_PROJECT_PRODUCTION:
        return GCPEnvironment.PRODUCTION
    raise ValueError(f"Unknown project {project}")


def in_gcp_production() -> bool:
    return in_gcp() and get_gcp_environment() == GCPEnvironment.PRODUCTION.value


def in_gcp_staging() -> bool:
    return in_gcp() and get_gcp_environment() == GCPEnvironment.STAGING.value


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


def gcp_only(func: Callable) -> Callable:
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

        if not deployed:
            # Local environment - fail
            logging.error("This API call is not allowed locally.")
            raise RuntimeError("Not available, see service logs.")

        # GCP environment - continue
        logging.info("GCP environment, proceeding.")

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


def in_offline_mode() -> bool:
    return os.environ.get("IS_OFFLINE_MODE") == "true"


def get_version() -> str:
    return os.getenv("GAE_VERSION", "")


class ServiceType(Enum):
    DEFAULT = "default"


def get_service_type() -> ServiceType:
    return ServiceType(os.getenv("GAE_SERVICE", "default"))
