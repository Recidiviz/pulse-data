# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Contains task groups that wrap KubernetesPodOperator tasks and push their output to XCom.
We use these task groups because natively pushing output to XCom from a KubernetesPodOperator task requires the use of
a separate airflow-xcom-sidecar container that is resource-starved and non-configurable, causing a significant bottleneck
in our DAGs."""
from typing import List, Optional

from airflow.decorators import task, task_group
from airflow.utils.context import Context
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    build_kubernetes_pod_task,
    build_mapped_kubernetes_pod_task,
)
from recidiviz.airflow.dags.utils.gcsfs_utils import get_gcsfs_from_hook
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.utils.kubernetes_pod_operator_task_output_handler import (
    KubernetesPodOperatorTaskOutputHandler,
)

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a "disable expression-not-assigned" because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


@task
def push_kpo_task_output_from_gcs_to_xcom(task_id: str, **context: Context) -> str:
    """Pulls KPO task output from GCS and pushes it to XCom."""
    output_handler = KubernetesPodOperatorTaskOutputHandler.create_kubernetes_pod_operator_task_output_handler_from_context(
        context, get_gcsfs_from_hook()
    )
    output = output_handler.read_serialized_task_output(task_id)
    output_handler.delete_task_output(task_id)
    return output


@task
def push_kpo_mapped_task_output_from_gcs_to_xcom(
    task_id: str, **context: Context
) -> List[str]:
    """Pulls mapped KPO task output from GCS and pushes it to XCom."""
    output_handler = KubernetesPodOperatorTaskOutputHandler.create_kubernetes_pod_operator_task_output_handler_from_context(
        context, get_gcsfs_from_hook()
    )
    output = output_handler.read_serialized_mapped_task_output(task_id)
    output_handler.delete_mapped_task_output(task_id)
    return output


def kubernetes_pod_operator_mapped_task_with_output(
    task_id: str,
    expand_arguments: List[List[str]],
    group_id: Optional[str] = None,
    container_name: Optional[str] = None,
    trigger_rule: Optional[TriggerRule] = TriggerRule.ALL_SUCCESS,
    retries: int = 0,
    cloud_sql_connections: Optional[List[SchemaType]] = None,
    max_active_tis_per_dag: int | None = None,
) -> List[str]:
    """Task group that wraps a mapped KubernetesPodOperator task, where the KPO task output is pushed to GCS, then
    read from GCS and pushed to XCom by an Airflow native task so downstream tasks can access the output as normal.
    If you are using this task group, you must call save_to_gcs_xcom in your KPO task to save the output to GCS.
    """
    group_id = group_id or f"{task_id}_group"

    @task_group(group_id=group_id)  # type: ignore[type-var]
    def kpo_mapped_task_with_output(
        task_id: str,
        expand_arguments: List[List[str]],
        container_name: Optional[str] = None,
        trigger_rule: Optional[TriggerRule] = TriggerRule.ALL_SUCCESS,
        retries: int = 0,
        cloud_sql_connections: Optional[List[SchemaType]] = None,
        max_active_tis_per_dag: int | None = None,
    ) -> List[str]:
        kpo_task = build_mapped_kubernetes_pod_task(
            task_id=task_id,
            expand_arguments=expand_arguments,
            container_name=container_name,
            trigger_rule=trigger_rule,
            retries=retries,
            cloud_sql_connections=cloud_sql_connections,
            max_active_tis_per_dag=max_active_tis_per_dag,
        )
        task_output = push_kpo_mapped_task_output_from_gcs_to_xcom(
            task_id=kpo_task.task_id
        )
        kpo_task >> task_output

        return task_output

    return kpo_mapped_task_with_output(  # type: ignore[return-value]
        task_id=task_id,
        expand_arguments=expand_arguments,
        container_name=container_name,
        trigger_rule=trigger_rule,
        retries=retries,
        cloud_sql_connections=cloud_sql_connections,
        max_active_tis_per_dag=max_active_tis_per_dag,
    )


def kubernetes_pod_operator_task_with_output(
    task_id: str,
    arguments: List[str],
    group_id: Optional[str] = None,
    container_name: Optional[str] = None,
    trigger_rule: Optional[TriggerRule] = TriggerRule.ALL_SUCCESS,
    retries: int = 0,
    cloud_sql_connections: Optional[List[SchemaType]] = None,
) -> str:
    """Task group that wraps a KubernetesPodOperator task, where the KPO task output is pushed to GCS, then
    read from GCS and pushed to XCom by an Airflow native task so downstream tasks can access the output as normal.
    If you are using this task group, you must call save_to_gcs_xcom in your KPO task to save the output to GCS.
    """
    group_id = group_id or f"{task_id}_group"

    @task_group(group_id=group_id)  # type: ignore[type-var]
    def kpo_task_with_output(
        task_id: str,
        arguments: List[str],
        container_name: Optional[str] = None,
        trigger_rule: Optional[TriggerRule] = TriggerRule.ALL_SUCCESS,
        retries: int = 0,
        cloud_sql_connections: Optional[List[SchemaType]] = None,
    ) -> List[str]:
        kpo_task = build_kubernetes_pod_task(
            task_id=task_id,
            arguments=arguments,
            container_name=container_name,
            trigger_rule=trigger_rule,
            retries=retries,
            cloud_sql_connections=cloud_sql_connections,
        )
        task_output = push_kpo_task_output_from_gcs_to_xcom(task_id=kpo_task.task_id)
        kpo_task >> task_output

        return task_output

    return kpo_task_with_output(  # type: ignore[return-value]
        task_id=task_id,
        arguments=arguments,
        container_name=container_name,
        trigger_rule=trigger_rule,
        retries=retries,
        cloud_sql_connections=cloud_sql_connections,
    )
