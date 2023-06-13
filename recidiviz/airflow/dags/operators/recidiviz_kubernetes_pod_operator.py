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
import os
from typing import List

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s


def build_recidiviz_kubernetes_pod_operator(
    task_id: str,
    argv: List[str],
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
    namespace = "composer-user-workloads"
    return KubernetesPodOperator(
        task_id=task_id,
        namespace=namespace,
        image=os.getenv("RECIDIVIZ_APP_ENGINE_IMAGE"),
        # This config is provided by Cloud Composer
        config_file="/home/airflow/composer_kube_config",
        cmds=[
            "pipenv",
        ],
        arguments=[
            "run",
            *argv,
        ],
        env_vars={
            # TODO(census-instrumentation/opencensus-python#796)
            "CONTAINER_NAME": container_name,
            "NAMESPACE": namespace,
        },
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "2000m", "memory": "1Gi"}
        ),
    )
