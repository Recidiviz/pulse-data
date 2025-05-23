# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 20223 Recidiviz, Inc.
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
"""Script that aids Airflow development by managing experiment environments

    # to create your experiment environment:
    python -m recidiviz.tools.airflow.environment_control create

    # to destroy your experiment environment:
    python -m recidiviz.tools.airflow.environment_control destroy

    # to open your experiment environment's web interface:
    python -m recidiviz.tools.airflow.environment_control open

    # to copy source files over to your environment's storage bucket:
    python -m recidiviz.tools.airflow.environment_control update_files [--files recidiviz/airflow/dags/calculation_dag.py ...]

    # to update the version of the appengine image that KubernetesPodOperators run:
    python -m recidiviz.tools.airflow.environment_control update_image
"""
import argparse
import logging
import webbrowser

import google.cloud.orchestration.airflow.service_v1beta1 as service
from google.cloud.orchestration.airflow.service_v1beta1 import (
    TaskLogsRetentionConfig,
    types,
)

from recidiviz.common.google_cloud.protobuf_builder import ProtoPlusBuilder
from recidiviz.tools.airflow.copy_source_files_to_experiment_composer import (
    copy_source_files_to_experiment,
)
from recidiviz.tools.airflow.utils import (
    COMPOSER_LOCATION,
    await_environment_state,
    get_environment_by_name,
    get_gcloud_auth_user,
    get_user_experiment_environment,
    get_user_experiment_environment_name,
)
from recidiviz.tools.gsutil_shell_helpers import gcloud_storage_rm
from recidiviz.tools.utils.script_helpers import prompt_for_confirmation
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.service_accounts import (
    get_default_compute_engine_service_account_email,
)

COMPOSER_EXPERIMENT_APPENGINE_IMAGE_NAME = f"us-docker.pkg.dev/recidiviz-staging/appengine/composer-gke-test-{get_gcloud_auth_user()}:latest"

COMPOSER_SMALL_WORKLOADS_CONFIG = types.WorkloadsConfig(
    scheduler=types.WorkloadsConfig.SchedulerResource(
        cpu=1,
        memory_gb=4,
        storage_gb=10,
        count=1,
    ),
    web_server=types.WorkloadsConfig.WebServerResource(
        cpu=1,
        memory_gb=4,
        storage_gb=10,
    ),
    worker=types.WorkloadsConfig.WorkerResource(
        cpu=1,
        memory_gb=4,
        storage_gb=10,
    ),
    triggerer=types.WorkloadsConfig.TriggererResource(
        cpu=0.5,
        memory_gb=0.5,
        count=1,
    ),
)


def action_create() -> None:
    """Creates a new experiment environment"""
    # Base the new experiment environment's configuration off of the orchestration environment
    orchestration = get_environment_by_name(
        project_id=GCP_PROJECT_STAGING,
        name="orchestration-v2",
    )
    client = service.EnvironmentsClient()

    # Disable sending emails from the development environment
    new_environment_variables = {
        key: value
        for key, value in orchestration.config.software_config.env_variables.items()
        if "SENDGRID" not in key
    }

    # Run using an experiment appengine image
    new_environment_variables[
        "RECIDIVIZ_APP_ENGINE_IMAGE"
    ] = COMPOSER_EXPERIMENT_APPENGINE_IMAGE_NAME

    software_config_builder = (
        ProtoPlusBuilder(proto_cls=types.SoftwareConfig)
        .compose(orchestration.config.software_config)
        # env_variables must be cleared as it is a repeated field. re-assigning would append entries
        # pypi_packages must be set after the environment is created
        .clear_field("env_variables", "pypi_packages")
        .update_args(env_variables=new_environment_variables)
    )

    environment = types.Environment(
        name=client.environment_path(
            project=GCP_PROJECT_STAGING,
            location=COMPOSER_LOCATION,
            environment=get_user_experiment_environment_name(),
        ),
        config=types.EnvironmentConfig(
            software_config=software_config_builder.build(),
            environment_size=types.EnvironmentConfig.EnvironmentSize.ENVIRONMENT_SIZE_SMALL,
            workloads_config=COMPOSER_SMALL_WORKLOADS_CONFIG,
            private_environment_config=types.PrivateEnvironmentConfig(
                enable_private_environment=True
            ),
            node_config=types.NodeConfig(
                service_account=get_default_compute_engine_service_account_email(
                    project_id=GCP_PROJECT_STAGING
                ),
            ),
            data_retention=types.DataRetentionConfig(
                task_logs_retention_config=TaskLogsRetentionConfig(
                    storage_mode=TaskLogsRetentionConfig.TaskLogsStorageMode.CLOUD_LOGGING_AND_CLOUD_STORAGE
                )
            ),
        ),
    )

    # Submit a create request unless one already exists
    try:
        get_user_experiment_environment()
    except ValueError:
        logging.info("Creating environment")
        client.create_environment(
            request=types.CreateEnvironmentRequest(
                parent=client.common_location_path(
                    project=GCP_PROJECT_STAGING, location="us-central1"
                ),
                environment=environment,
            )
        )

    await_environment_state(
        environment.name,
        target_state=types.Environment.State.RUNNING,
        retry_states=[types.Environment.State.CREATING],
    )

    # Must be done after the environment is created
    logging.info("Updating pypi environment packages...")

    environment_builder = (
        ProtoPlusBuilder(types.Environment)
        .compose(environment)
        .compose(
            types.Environment(
                config=types.EnvironmentConfig(
                    software_config=types.SoftwareConfig(
                        pypi_packages=orchestration.config.software_config.pypi_packages
                    )
                )
            )
        )
    )

    client.update_environment(
        request=types.UpdateEnvironmentRequest(
            name=environment.name,
            environment=environment_builder.build(),
            # Camel-case is expected here
            update_mask="config.softwareConfig.pypiPackages",
        )
    )

    await_environment_state(
        environment.name,
        target_state=types.Environment.State.RUNNING,
        retry_states=[types.Environment.State.UPDATING],
    )

    action_update_files()

    logging.info("Environment created!")
    action_open()


def action_destroy() -> None:
    """Destroys an existing experiment environment. Deleting the environment won't automatically
    delete the GCS bucket, so we need to delete it manually."""
    environment = get_user_experiment_environment()
    prompt_for_confirmation(
        f"About to delete the {environment.name} environment, continue?"
    )
    client = service.EnvironmentsClient()
    client.delete_environment(
        request=types.DeleteEnvironmentRequest(name=environment.name)
    )

    gcloud_storage_rm(
        path=f"gs://{environment.storage_config.bucket}", force_delete_contents=True
    )


def action_update_image() -> None:
    """Prints commands to update experiment image"""
    latest_image = "us-docker.pkg.dev/recidiviz-staging/appengine/default:latest"
    print(
        "\n".join(
            [
                "# To update your environment's appengine image to be the latest image from `main`, run the following:",
                f'docker pull "{latest_image}"',
                f'docker tag "{latest_image}" "{COMPOSER_EXPERIMENT_APPENGINE_IMAGE_NAME}"',
                f'docker push "{COMPOSER_EXPERIMENT_APPENGINE_IMAGE_NAME}"',
                "",
                "------------",
                "",
                "# To build a new appengine image from local source code, run the following:",
                f'pipenv run docker-build -t "{COMPOSER_EXPERIMENT_APPENGINE_IMAGE_NAME}"',
                f'docker push "{COMPOSER_EXPERIMENT_APPENGINE_IMAGE_NAME}"',
                "",
                "------------",
                "",
                "After docker image is pushed, Kubernetes will pull the latest image when running KubernetesPodOperators",
            ]
        )
    )


def action_update_files(dry_run: bool = False) -> None:
    """Copies source files to the experiment gcs bucket"""
    environment = get_user_experiment_environment()
    copy_source_files_to_experiment(
        gcs_uri=environment.config.dag_gcs_prefix, dry_run=dry_run
    )


def action_open() -> None:
    """Opens the user's experiment instance"""
    environment = get_environment_by_name(
        project_id=GCP_PROJECT_STAGING, name=get_user_experiment_environment_name()
    )
    webbrowser.open_new_tab(environment.config.airflow_uri)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    subparsers = parser.add_subparsers(help="sub-command help", dest="command")

    actions = {
        "create": action_create,
        "destroy": action_destroy,
        "update_image": action_update_image,
        "update_files": action_update_files,
        "open": action_open,
    }

    subparsers_by_name = {action: subparsers.add_parser(action) for action in actions}

    update_files_parser = subparsers_by_name["update_files"]
    update_files_parser.add_argument("--dry-run", action="store_true")

    parsed_args = parser.parse_args()
    kwargs = vars(parsed_args).copy()
    kwargs.pop("command")
    actions[parsed_args.command](**kwargs)  # type: ignore
