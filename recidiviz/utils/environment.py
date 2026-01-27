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
GCP_PROJECT_DASHBOARDS_STAGING = "recidiviz-dashboard-staging"
GCP_PROJECT_DASHBOARDS_PRODUCTION = "recidiviz-dashboard-production"
# This is not a GCP environment. We use this flag when triggering local jobs.
PROJECT_JUSTICE_COUNTS_LOCAL = "justice-counts-local"

GCP_ENVIRONMENTS = {env.value for env in GCPEnvironment}

# Data platform projects only
DATA_PLATFORM_GCP_PROJECTS = [GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION]
# Includes projects outside of the data platform
ALL_GCP_PROJECTS = DATA_PLATFORM_GCP_PROJECTS + [
    GCP_PROJECT_JUSTICE_COUNTS_STAGING,
    GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
    GCP_PROJECT_DASHBOARDS_STAGING,
    GCP_PROJECT_DASHBOARDS_PRODUCTION,
]

RECIDIVIZ_ENV = "RECIDIVIZ_ENV"
GOOGLE_CLOUD_PROJECT = "GOOGLE_CLOUD_PROJECT"
DATA_PLATFORM_VERSION = "DATA_PLATFORM_VERSION"
COMPOSER_ENVIRONMENT = "COMPOSER_ENVIRONMENT"

DAG_ID = "DAG_ID"
RUN_ID = "RUN_ID"
TASK_ID = "TASK_ID"
MAP_INDEX = "MAP_INDEX"


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


def in_airflow_kubernetes_pod() -> bool:
    try:
        AirflowKubernetesPodEnvironment.get_dag_id()
        return True
    except NotInAirflowKubernetesPodError:
        return False


def in_airflow() -> bool:
    return os.getenv("COMPOSER_ENVIRONMENT") is not None


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


class NotInAirflowKubernetesPodError(KeyError):
    pass


class AirflowKubernetesPodEnvironment:
    """These environment variables are set when creating a RecidivizKubernetesPodOperator in Airflow"""

    @staticmethod
    def get_airflow_kubernetes_pod_environment_variable(environment_key: str) -> str:
        try:
            return os.environ[environment_key]
        except KeyError as e:
            raise NotInAirflowKubernetesPodError() from e

    @staticmethod
    def get_dag_id() -> str:
        return AirflowKubernetesPodEnvironment.get_airflow_kubernetes_pod_environment_variable(
            DAG_ID
        )

    @staticmethod
    def get_run_id() -> str:
        return AirflowKubernetesPodEnvironment.get_airflow_kubernetes_pod_environment_variable(
            RUN_ID
        )

    @staticmethod
    def get_task_id() -> str:
        return AirflowKubernetesPodEnvironment.get_airflow_kubernetes_pod_environment_variable(
            TASK_ID
        )

    @staticmethod
    def get_map_index() -> Optional[str]:
        index = AirflowKubernetesPodEnvironment.get_airflow_kubernetes_pod_environment_variable(
            MAP_INDEX
        )
        return index if index != "-1" else None


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
        logging.debug("Test environment, proceeding.")

        return func(*args, **kwargs)

    return check_env


def gcp_only(func: Callable) -> Callable:
    """Decorator function to verify request only runs in GCP

    Decorator function to check run environment. If run locally,
    exits before any work can be done.

    Args:
        N/A

    Returns:
        If running in GCP, results of decorated function.
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


def in_gunicorn() -> bool:
    return "gunicorn" in os.environ.get("SERVER_SOFTWARE", "")


def get_data_platform_version() -> str:
    return os.getenv(DATA_PLATFORM_VERSION, "")


def get_admin_panel_base_url() -> Optional[str]:
    if get_gcp_environment() == GCPEnvironment.PRODUCTION.value:
        return "https://admin-panel-prod.recidiviz.org"
    if get_gcp_environment() == GCPEnvironment.STAGING.value:
        return "https://admin-panel-staging.recidiviz.org"
    if in_development():
        return "http://localhost:5050"

    return None
