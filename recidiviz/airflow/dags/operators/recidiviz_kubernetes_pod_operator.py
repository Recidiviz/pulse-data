# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""
A factory function for building KubernetesPodOperators that run our appengine image
"""
# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement
import logging
import os
import string
from typing import Any, Callable, Dict, List, Optional, Union

from airflow.decorators import task
from airflow.models import DagRun, TaskInstance
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s

from recidiviz.airflow.dags.utils.environment import (
    COMPOSER_ENVIRONMENT,
    get_composer_environment,
)
from recidiviz.utils.environment import RECIDIVIZ_ENV, get_environment_for_project

_project_id = os.environ.get("GCP_PROJECT")

COMPOSER_USER_WORKLOADS = "composer-user-workloads"


class RecidivizKubernetesPodOperator(KubernetesPodOperator):
    """KubernetesPodOperator with some defaults"""

    def __init__(self, **kwargs: Any) -> None:
        env_vars = kwargs.pop("env_vars", [])
        super().__init__(
            namespace=COMPOSER_USER_WORKLOADS,
            # Do not delete pods after running, its handled `recidiviz.airflow.dags.monitoring.cleanup_exited_pods`
            is_delete_operator_pod=False,
            image=os.getenv("RECIDIVIZ_APP_ENGINE_IMAGE"),
            image_pull_policy="Always",
            # This config is provided by Cloud Composer
            config_file="/home/airflow/composer_kube_config",
            startup_timeout_seconds=240,
            env_vars=[
                k8s.V1EnvVar(name="NAMESPACE", value="composer-user-workloads"),
                # TODO(census-instrumentation/opencensus-python#796)
                k8s.V1EnvVar(
                    name=COMPOSER_ENVIRONMENT, value=get_composer_environment()
                ),
                k8s.V1EnvVar(
                    name=RECIDIVIZ_ENV,
                    value=get_environment_for_project(_project_id).value
                    # TODO(#22516): Remove testing clause
                    if _project_id and _project_id != "recidiviz-testing" else "",
                ),
                *env_vars,
            ],
            **kwargs,
        )


def build_kubernetes_pod_task_group(
    group_id: str,
    arguments: Union[Callable[[DagRun], List[str]], List[str]],
    container_name: str,
    trigger_rule: Optional[TriggerRule] = TriggerRule.ALL_SUCCESS,
) -> TaskGroup:
    """
    Builds an operator that launches a container using the appengine image in the user workloads Kubernetes namespace
    This is useful for launching arbitrary tools from within our pipenv environment.

    For information regarding the KuberenetesPodOperator:
    https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html

    group_id: name of the airflow task group
    argv: List of commands to run in the pipenv shell
    container_name: Name to group Kubernetes pod metrics
    """

    with TaskGroup(group_id) as task_group:

        @task(trigger_rule=trigger_rule, multiple_outputs=True)
        def set_kubernetes_arguments(
            dag_run: Optional[DagRun] = None,
            task_instance: Optional[TaskInstance] = None,
        ) -> Dict[str, List[str]]:
            if not task_instance:
                raise ValueError(
                    "task_instance not provided. This should be automatically set by Airflow."
                )
            if not dag_run:
                raise ValueError(
                    "dag_run not provided. This should be automatically set by Airflow."
                )

            argv = arguments(dag_run) if callable(arguments) else arguments
            logging.info(
                "Found arguments for task [%s]: %s",
                task_instance.task_id,
                " ".join(argv),
            )

            return {
                "resource_allocation_command": [
                    "run",
                    "python",
                    "-m",
                    "recidiviz.entrypoints.entrypoint_resources",
                    *argv,
                ],
                "entrypoint_execution_command": [
                    "run",
                    "python",
                    "-m",
                    "recidiviz.entrypoints.entrypoint_executor",
                    *argv,
                ],
            }

        kubernetes_arguments = set_kubernetes_arguments()

        set_kubernetes_resources = RecidivizKubernetesPodOperator(
            task_id="set_kubernetes_resources",
            cmds=["pipenv"],
            arguments=kubernetes_arguments["resource_allocation_command"],
            env_vars=[
                # TODO(census-instrumentation/opencensus-python#796)
                k8s.V1EnvVar(name="CONTAINER_NAME", value=container_name),
            ],
            do_xcom_push=True,
        )

        formatter = string.Formatter()

        execute_entrypoint_operator = RecidivizKubernetesPodOperator(
            task_id="execute_entrypoint_operator",
            cmds=["pipenv"],
            arguments=kubernetes_arguments["entrypoint_execution_command"],
            container_resources=k8s.V1ResourceRequirements(
                # Airflow turns these string values into Python dictionaries when rendering, disabling typing warnings
                limits=formatter.format(
                    "{{{{ task_instance.xcom_pull(task_ids='{set_kubernetes_resources}')['limits'] | default(dict()) }}}}",
                    set_kubernetes_resources=set_kubernetes_resources.task_id,
                ),  # type: ignore[arg-type]
                requests=formatter.format(
                    "{{{{ task_instance.xcom_pull(task_ids='{set_kubernetes_resources}')['requests'] | default(dict()) }}}}",
                    set_kubernetes_resources=set_kubernetes_resources.task_id,
                ),  # type: ignore[arg-type]
            ),
            env_vars=[
                # TODO(census-instrumentation/opencensus-python#796)
                k8s.V1EnvVar(name="CONTAINER_NAME", value=container_name),
            ],
        )

        (set_kubernetes_resources >> execute_entrypoint_operator)

    return task_group
