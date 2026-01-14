# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Testing utils for working with k8s entrypoints in the airflow context"""
from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    ENTRYPOINT_ARGUMENTS,
)
from recidiviz.airflow.tests.utils.dag_helper_functions import set_k8s_operator_env_vars
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.entrypoints.entrypoint_utils import save_to_gcs_xcom
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.utils.types import assert_type


def fake_k8s_operator_for_entrypoint(
    entrypoint_cls: type[EntrypointInterface],
) -> type[BaseOperator]:
    """Uses an entrypoint class to simulate the behavior of a k8s pod in a testing
    environment, mocking save_to_xcom within the entrypoint_cls module. This is needed
    to propagate that value back to the dag because save_to_xcom function writes a to
    local json file that the airflow reads using a mounted xcom sidecar (when a normal
    airflow task tries to write to a local file, it raises an error about it being a
    read-only filesystem).

    n.b.: this is a brittle testing mechanism; because EntrypointInterface uses
    the pulse-data pyproject.toml and not the airflow pyproject.toml, execution of arbitrary
    code might fail.
    """

    class FakeK8sEntrypointOperator(BaseOperator):
        """Fake k8s entrypoint operator that executes an entrypoint and properly handles
        returning values to xcom
        """

        # pylint: disable=unused-argument
        def __init__(
            self,
            *args: Any,
            arguments: Optional[List[str]] = None,
            cloud_sql_connections: Optional[List[SchemaType]] = None,
            name: Optional[str] = None,
            cmds: Optional[str] = None,
            env_vars: Optional[Dict] = None,
            **kwargs: Any,
        ) -> None:
            self.arguments: List[str] = assert_type(arguments, list)
            super().__init__(
                dag=kwargs["dag"],
                task_id=kwargs["task_id"],
                trigger_rule=(
                    kwargs["trigger_rule"]
                    if "trigger_rule" in kwargs
                    else TriggerRule.ALL_SUCCESS
                ),
            )

        # pylint: disable=unused-argument
        def execute(self, context: Context) -> None:
            set_k8s_operator_env_vars(context)

            # Skip ENTRYPOINT_ARGUMENTS and the --entrypoint=... argument
            unknown_args = self.arguments[len(ENTRYPOINT_ARGUMENTS) + 1 :]
            entrypoint_parser = entrypoint_cls.get_parser()
            entrypoint_args = entrypoint_parser.parse_args(unknown_args)

            entrypoint_cls.run_entrypoint(args=entrypoint_args)

    return FakeK8sEntrypointOperator


def fake_k8s_operator_with_return_value(
    fs: GCSFileSystem, return_value: Any, is_mapped: bool
) -> type[BaseOperator]:
    """Returns a fake k8s operator that returns the specified values

    because .partial and .mapped inspect the init signature of the operator and throws
    if the parameters aren't present, we have to explicitly reflect the parameters of
    KubernetesPodOperator.
    """

    call_count: int = 0

    class FakeK8sOperator(BaseOperator):
        """Fake k8s operator that returns the specified values"""

        # pylint: disable=unused-argument
        def __init__(
            self,
            *args: Any,
            arguments: Optional[List[str]] = None,
            cloud_sql_connections: Optional[List[SchemaType]] = None,
            name: Optional[str] = None,
            cmds: Optional[str] = None,
            env_vars: Optional[Dict] = None,
            **kwargs: Any,
        ) -> None:
            super().__init__(
                dag=kwargs["dag"],
                task_id=kwargs["task_id"],
                trigger_rule=(
                    kwargs["trigger_rule"]
                    if "trigger_rule" in kwargs
                    else TriggerRule.ALL_SUCCESS
                ),
            )

        def execute(self, context: Context) -> None:  # pylint: disable=unused-argument
            nonlocal call_count
            val = return_value[call_count] if is_mapped else return_value
            call_count += 1

            set_k8s_operator_env_vars(context)

            save_to_gcs_xcom(fs, val)

    return FakeK8sOperator
