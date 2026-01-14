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
import os
from typing import Any, Dict, List, Optional

import jinja2
import yaml
from airflow.models import Connection, MappedOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s
from more_itertools import one

from recidiviz.airflow.dags.operators.cloud_sql_proxy_sidecar import (
    configure_cloud_sql_proxy_for_pod,
)
from recidiviz.airflow.dags.utils.cloud_sql import cloud_sql_conn_id_for_schema_type
from recidiviz.airflow.dags.utils.environment import (
    COMPOSER_ENVIRONMENT,
    DATA_PLATFORM_VERSION,
    get_app_engine_image_from_airflow_env,
    get_composer_environment,
    get_data_platform_version_from_airflow_env,
    get_project_id,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.utils.environment import (
    DAG_ID,
    MAP_INDEX,
    RECIDIVIZ_ENV,
    RUN_ID,
    TASK_ID,
    get_environment_for_project,
)

COMPOSER_USER_WORKLOADS = "composer-user-workloads"

RESOURCES_YAML_PATH = os.path.join(
    os.path.dirname(__file__),
    "recidiviz_kubernetes_resources.yaml",
)

ENTRYPOINT_ARGUMENTS = [
    "run",
    # Use --no-sync because dependencies are already installed in the Docker image
    "--no-sync",
    "python",
    "-m",
    "recidiviz.entrypoints.entrypoint_executor",
]

# Used for workload separation
# https://cloud.google.com/kubernetes-engine/docs/how-to/workload-separation#separate-workloads-autopilot
RECIDIVIZ_POD_ANNOTATION = "recidiviz-pod-node"


class KubernetesEntrypointResourceAllocator:
    """Class for allocating resources to our entrypoint tasks"""

    resources_config: Dict

    def __init__(self) -> None:
        with open(RESOURCES_YAML_PATH, "r", encoding="utf-8") as f:
            self.resources_config = yaml.safe_load(f)

    def get_resources(self, argv: List[str]) -> k8s.V1ResourceRequirements:
        """Returns the resources specified in the config
        Prioritization is based on list order; the last config is returned for args that match multiple configs
        """

        entrypoint_arg = one(
            [
                arg[len("--entrypoint=") :]
                for arg in argv
                if arg.startswith("--entrypoint=")
            ]
        )

        if not entrypoint_arg:
            raise ValueError("Must specify an entrypoint arg to allocate resources")

        if entrypoint_arg not in self.resources_config:
            raise ValueError(
                f"Entrypoint {entrypoint_arg} must have a recidiviz_kubernetes_resources.yaml entry"
            )

        config = self.resources_config[entrypoint_arg]
        resources = config["default_resources"]

        for overrides in config.get("overrides", []):
            if all(arg in argv for arg in overrides.get("args", [])):
                resources = overrides["resources"]

        return k8s.V1ResourceRequirements(**resources)


class RecidivizKubernetesPodOperator(KubernetesPodOperator):
    """KubernetesPodOperator with some defaults"""

    def __init__(
        self, cloud_sql_connections: Optional[List[SchemaType]] = None, **kwargs: Any
    ) -> None:
        env_vars = kwargs.pop("env_vars", [])
        project_id = get_project_id()
        recidiviz_env_value = (
            get_environment_for_project(project_id).value
            # TODO(#22516): Remove testing clause
            if project_id and project_id != "recidiviz-testing"
            else ""
        )
        super().__init__(
            namespace=COMPOSER_USER_WORKLOADS,
            # Do not delete pods after running, its handled `recidiviz.airflow.dags.monitoring.cleanup_exited_pods`
            on_finish_action="keep_pod",
            image=get_app_engine_image_from_airflow_env(),
            image_pull_policy="Always",
            # This config is provided by Cloud Composer
            config_file="/home/airflow/composer_kube_config",
            # Allow up to 12 minutes for the pod to start. Normally, pods will start within 10 seconds, however,
            # there may be times when it is not possible for Kubernetes to adequately fulfill a pod's resource
            # requirements. In this case, a new compute engine VM is started and the pod will not run until the node
            # fully starts. Anecdotally this happens in about 10 minutes.
            startup_timeout_seconds=12 * 60,
            # In order to prevent preemption from Cloud Composer's internal pods, configure our tasks to run on a
            # separate node that will only schedule pods with the `recidiviz-pod-node: true` annotation
            # https://cloud.google.com/kubernetes-engine/docs/how-to/workload-separation#separate-workloads-autopilot
            tolerations=[
                k8s.V1Toleration(
                    key=RECIDIVIZ_POD_ANNOTATION,
                    operator="Equal",
                    value="true",
                    effect="NoSchedule",
                )
            ],
            node_selector={RECIDIVIZ_POD_ANNOTATION: "true"},
            env_vars=[
                k8s.V1EnvVar(name="NAMESPACE", value="composer-user-workloads"),
                # TODO(census-instrumentation/opencensus-python#796)
                k8s.V1EnvVar(
                    name=COMPOSER_ENVIRONMENT, value=get_composer_environment()
                ),
                k8s.V1EnvVar(
                    name=DATA_PLATFORM_VERSION,
                    value=get_data_platform_version_from_airflow_env(),
                ),
                k8s.V1EnvVar(
                    name=RECIDIVIZ_ENV,
                    value=recidiviz_env_value,
                ),
                *env_vars,
            ],
            **kwargs,
        )

        self.cloud_sql_connections = cloud_sql_connections or []

    def render_template_fields(
        self,
        context: Context,
        jinja_env: Optional[jinja2.Environment] = None,
    ) -> None:
        super().render_template_fields(context=context, jinja_env=jinja_env)

        # Strip arguments whose Jinja template rendered None
        self.arguments: List[str] = [
            argument for argument in self.arguments if argument
        ]

    def _get_ti_metadata(self, context: Context) -> List[k8s.V1EnvVar]:
        ti = context["ti"]
        return [
            k8s.V1EnvVar(name=DAG_ID, value=ti.dag_id),
            k8s.V1EnvVar(name=TASK_ID, value=ti.task_id),
            k8s.V1EnvVar(name=RUN_ID, value=context["run_id"]),
            k8s.V1EnvVar(name=MAP_INDEX, value=str(ti.map_index)),
        ]

    # The execute method is called after templated arguments have been rendered
    def execute(self, context: Context) -> Any:
        # Assign resources based on the entrypoint that we are running
        self.container_resources = (
            KubernetesEntrypointResourceAllocator().get_resources(self.arguments)
        )
        # Make task instance metadata accessible from the pod
        self.env_vars.extend(self._get_ti_metadata(context))

        return super().execute(context)

    def build_pod_request_obj(self, context: Optional[Context] = None) -> k8s.V1Pod:
        pod = super().build_pod_request_obj(context)

        if self.cloud_sql_connections:
            connection_strings = []
            for schema_type in self.cloud_sql_connections:
                connection = Connection.get_connection_from_secrets(
                    cloud_sql_conn_id_for_schema_type(schema_type)
                )

                connection_strings.append(
                    ":".join(
                        [
                            connection.extra_dejson[key]
                            for key in ("project_id", "location", "instance")
                        ]
                    )
                )

            pod = configure_cloud_sql_proxy_for_pod(
                pod,
                app_container_name=self.base_container_name,
                connection_strings=connection_strings,
            )

        return pod


def build_kubernetes_pod_task(
    task_id: str,
    arguments: List[str],
    container_name: Optional[str] = None,
    trigger_rule: Optional[TriggerRule] = TriggerRule.ALL_SUCCESS,
    retries: int = 0,
    cloud_sql_connections: Optional[List[SchemaType]] = None,
) -> RecidivizKubernetesPodOperator:
    """
    Builds an operator that launches a container using the appengine image in the user workloads Kubernetes namespace.
    This is useful for running arbitrary code in the Recidiviz uv environment rather than the Airflow environment.
    It also allows for dedicated resource allocation to a task, ensuring better isolation and performance for
    resource-intensive operations.

    KubernetesPodOperator supports pushing to xcom, but it's unreliable for large outputs, so we don't provide the option for RecidivizKubernetesPodOperator pods.
    If you need to use the output of the task in downstream tasks, use kubernetes_pod_operator_task_with_output task group.

    For information regarding the KuberenetesPodOperator:
    https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html

    task_id: Name of the airflow task
    arguments: List of commands to pass to recidiviz.entrypoints.entrypoint_executor
    container_name: Container name used to group Kubernetes pod metrics
    trigger_rule: TriggerRule for the task
    retries: Number of retries for the task
    cloud_sql_connections: List of SchemaType objects specifying which Cloud SQL DBs to connect to
    """

    return RecidivizKubernetesPodOperator(
        arguments=[
            *ENTRYPOINT_ARGUMENTS,
            *arguments,
        ],
        **get_kubernetes_pod_kwargs(
            task_id,
            container_name,
            trigger_rule,
            retries,
            cloud_sql_connections,
        ),
    )


def build_mapped_kubernetes_pod_task(
    task_id: str,
    expand_arguments: List[List[str]],
    container_name: Optional[str] = None,
    trigger_rule: Optional[TriggerRule] = TriggerRule.ALL_SUCCESS,
    retries: int = 0,
    cloud_sql_connections: Optional[List[SchemaType]] = None,
    max_active_tis_per_dag: int | None = None,
) -> MappedOperator:
    """Builds a MappedOperator that launches len(expand_arguments) RecidivizKubernetesPodOperator pods.
    This is useful for running a dynamic number of tasks in parallel.

    KubernetesPodOperator supports pushing to xcom, but it's unreliable for large outputs, so we don't provide the option for RecidivizKubernetesPodOperator pods.
    If you need to use the output of the tasks in downstream tasks, use kubernetes_pod_operator_mapped_task_with_output task group.

    task_id: Name of the airflow task
    expand_arguments: List where each entry contains a list of commands to pass to recidiviz.entrypoints.entrypoint_executor
    container_name: Container name used to group Kubernetes pod metrics
    trigger_rule: TriggerRule for the task
    retries: Number of retries for the task
    cloud_sql_connections: List of SchemaType objects specifying which Cloud SQL DBs to connect to
    """
    return RecidivizKubernetesPodOperator.partial(
        **get_kubernetes_pod_kwargs(
            task_id=task_id,
            container_name=container_name,
            trigger_rule=trigger_rule,
            retries=retries,
            cloud_sql_connections=cloud_sql_connections,
            max_active_tis_per_dag=max_active_tis_per_dag,
        )
    ).expand(arguments=expand_arguments)


def get_kubernetes_pod_kwargs(
    task_id: str,
    container_name: Optional[str] = None,
    trigger_rule: Optional[TriggerRule] = TriggerRule.ALL_SUCCESS,
    retries: int = 0,
    cloud_sql_connections: Optional[List[SchemaType]] = None,
    max_active_tis_per_dag: int | None = None,
) -> Dict[str, Any]:
    container_name = container_name or task_id
    return {
        "task_id": task_id,
        "name": task_id,
        "cmds": ["uv"],
        "env_vars": [
            # TODO(census-instrumentation/opencensus-python#796)
            k8s.V1EnvVar(name="CONTAINER_NAME", value=container_name)
        ],
        "trigger_rule": trigger_rule,
        "retries": retries,
        "cloud_sql_connections": cloud_sql_connections,
        "max_active_tis_per_dag": max_active_tis_per_dag,
    }
