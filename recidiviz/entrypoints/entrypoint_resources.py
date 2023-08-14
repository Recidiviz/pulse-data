# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Entrypoint for allocating resources to other entrypoints"""
import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, List

import yaml

import recidiviz
from recidiviz.entrypoints.entrypoint_executor import (
    ENTRYPOINTS_BY_NAME,
    parse_arguments,
)

RESOURCES_YAML_PATH = os.path.join(
    os.path.dirname(recidiviz.__file__),
    "airflow/dags/operators/recidiviz_kubernetes_resources.yaml",
)

DEFAULT_RESOURCES = {
    "requests": {"cpu": "2000m", "memory": "1Gi"},
    "limits": {"cpu": "2000m", "memory": "2Gi"},
}


class KubernetesEntrypointResourceAllocator:
    """Class for allocating resources to our entrypoint tasks"""

    resources_config: Dict

    def __init__(self) -> None:
        with open(RESOURCES_YAML_PATH, "r", encoding="utf-8") as f:
            self.resources_config = yaml.safe_load(f)

    def get_resources(
        self, executor_args: argparse.Namespace, entrypoint_args: argparse.Namespace
    ) -> Dict:
        """Returns the resources specified in the config
        Prioritization is based on list order; the last config is returned for args that match multiple configs
        """
        resources = DEFAULT_RESOURCES

        for config in self.resources_config:
            if executor_args.entrypoint != config["entrypoint"]:
                continue

            if all(
                hasattr(entrypoint_args, arg_key)
                and getattr(entrypoint_args, arg_key) == arg_value
                for arg_key, arg_value in config.get("args", {}).items()
            ):
                resources = config["resources"]

        return resources


def write_airflow_xcom_result(result: Any) -> None:
    """Writes the result to the path where Airflow's expects XCOM results"""
    with open("/airflow/xcom/return.json", "w", encoding="utf-8") as xcom_value:
        json.dump(result, xcom_value)


def get_entrypoint_resources(
    executor_args: argparse.Namespace, entrypoint_argv: List[str]
) -> None:
    entrypoint_cls = ENTRYPOINTS_BY_NAME[executor_args.entrypoint]
    entrypoint_parser = entrypoint_cls.get_parser()
    entrypoint_args = entrypoint_parser.parse_args(entrypoint_argv)
    resources = KubernetesEntrypointResourceAllocator().get_resources(
        executor_args=executor_args, entrypoint_args=entrypoint_args
    )

    write_airflow_xcom_result(resources)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args, unknown_args = parse_arguments(sys.argv[1:])
    get_entrypoint_resources(args, entrypoint_argv=unknown_args)
