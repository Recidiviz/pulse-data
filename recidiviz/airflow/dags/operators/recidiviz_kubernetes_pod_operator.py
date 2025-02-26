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
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s
from more_itertools import one

from recidiviz.airflow.dags.utils.environment import (
    COMPOSER_ENVIRONMENT,
    get_composer_environment,
)
from recidiviz.utils.environment import RECIDIVIZ_ENV, get_environment_for_project

# TODO(#23873): Remove loading of environment variables from at beginning of file.
_project_id = os.environ.get("GCP_PROJECT")

COMPOSER_USER_WORKLOADS = "composer-user-workloads"

RESOURCES_YAML_PATH = os.path.join(
    os.path.dirname(__file__),
    "recidiviz_kubernetes_resources.yaml",
)


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

        if not entrypoint_arg in self.resources_config:
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
            # Allow up to 12 minutes for the pod to start. Normally, pods will start within 10 seconds, however,
            # there may be times when it is not possible for Kubernetes to adequately fulfill a pod's resource
            # requirements. In this case, a new compute engine VM is started and the pod will not run until the node
            # fully starts. Anecdotally this happens in about 10 minutes.
            startup_timeout_seconds=12 * 60,
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

    # The execute method is called after templated arguments have been rendered
    def execute(self, context: Context) -> None:
        # Assign resources based on the entrypoint that we are running
        self.container_resources = (
            KubernetesEntrypointResourceAllocator().get_resources(self.arguments)
        )

        super().execute(context)


def build_kubernetes_pod_task(
    task_id: str,
    arguments: List[str],
    container_name: str,
    trigger_rule: Optional[TriggerRule] = TriggerRule.ALL_SUCCESS,
) -> RecidivizKubernetesPodOperator:
    """
    Builds an operator that launches a container using the appengine image in the user workloads Kubernetes namespace
    This is useful for launching arbitrary tools from within our pipenv environment.

    For information regarding the KuberenetesPodOperator:
    https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/operators.html

    group_id: name of the airflow task group
    argv: List of commands to run in the pipenv shell
    container_name: Name to group Kubernetes pod metrics
    """

    return RecidivizKubernetesPodOperator(
        task_id=task_id,
        name=task_id,
        cmds=["pipenv"],
        arguments=[
            "run",
            "python",
            "-m",
            "recidiviz.entrypoints.entrypoint_executor",
            *arguments,
        ],
        env_vars=[
            # TODO(census-instrumentation/opencensus-python#796)
            k8s.V1EnvVar(name="CONTAINER_NAME", value=container_name),
        ],
        trigger_rule=trigger_rule,
    )
