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
import logging
import os
from typing import Callable, List, Optional, Union

from airflow.decorators import task
from airflow.models import DagRun, TaskInstance
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

from recidiviz.utils.environment import RECIDIVIZ_ENV, get_environment_for_project

_project_id = os.environ.get("GCP_PROJECT")


def build_recidiviz_kubernetes_pod_operator(
    task_id: str,
    arguments: Union[Callable[[DagRun, TaskInstance], List[str]], List[str]],
    container_name: str,
) -> KubernetesPodOperator:
    """
    Builds an operator that launches a container using the appengine image in the user workloads Kubernetes namespace
    This is useful for launching arbitrary tools from within our pipenv environment.

    For information regarding the KuberenetesPodOperator:
    https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html

    task_id: name of the airflow task
    argv: List of commands to run in the pipenv shell
    container_name: Name to group Kubernetes pod metrics
    """

    @task(task_id=f"set_kubernetes_arguments_{task_id}")
    def get_kubernetes_arguments(
        dag_run: Optional[DagRun] = None,
        task_instance: Optional[TaskInstance] = None,
    ) -> List[str]:
        if not task_instance:
            raise ValueError(
                "task_instance not provided. This should be automatically set by Airflow."
            )
        if not dag_run:
            raise ValueError(
                "dag_run not provided. This should be automatically set by Airflow."
            )

        argv = arguments(dag_run, task_instance) if callable(arguments) else arguments
        logging.info(
            "Found arguments for task [%s]: %s", task_instance.task_id, " ".join(argv)
        )
        return ["run", *argv]

    namespace = "composer-user-workloads"
    return KubernetesPodOperator(
        task_id=task_id,
        namespace=namespace,
        image=os.getenv("RECIDIVIZ_APP_ENGINE_IMAGE"),
        image_pull_policy="Always",
        # This config is provided by Cloud Composer
        config_file="/home/airflow/composer_kube_config",
        cmds=[
            "pipenv",
        ],
        arguments=get_kubernetes_arguments(),
        env_vars=[
            # TODO(census-instrumentation/opencensus-python#796)
            k8s.V1EnvVar(name="CONTAINER_NAME", value=container_name),
            k8s.V1EnvVar(name="NAMESPACE", value=namespace),
            k8s.V1EnvVar(
                name=RECIDIVIZ_ENV,
                value=get_environment_for_project(_project_id).value
                if _project_id
                else "",
            ),
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "2000m", "memory": "1Gi"}
        ),
    )
