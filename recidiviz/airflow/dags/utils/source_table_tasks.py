# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Shared builders for the per-DAG source-table pod tasks run by multiple DAGs."""

from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    RecidivizKubernetesPodOperator,
    build_kubernetes_pod_task,
)
from recidiviz.airflow.dags.utils.constants import (
    UPDATE_BIG_QUERY_TABLE_SCHEMATA_TASK_ID,
    VALIDATE_SOURCE_TABLE_DATASETS_TASK_ID,
)


def _build_source_table_pod_task(
    *, task_id: str, entrypoint: str, trigger_rule: TriggerRule
) -> RecidivizKubernetesPodOperator:
    """Builds a pod task that runs a source-table entrypoint against the update group
    owned by the DAG this task runs in."""
    return build_kubernetes_pod_task(
        task_id=task_id,
        container_name=task_id,
        arguments=[f"--entrypoint={entrypoint}"],
        trigger_rule=trigger_rule,
    )


def execute_update_big_query_table_schemata(
    trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
) -> RecidivizKubernetesPodOperator:
    """Builds the task that updates the schemas of the source table collections belonging
    to the update group owned by the DAG this task runs in."""
    return _build_source_table_pod_task(
        task_id=UPDATE_BIG_QUERY_TABLE_SCHEMATA_TASK_ID,
        entrypoint="UpdateBigQuerySourceTableSchemataEntrypoint",
        trigger_rule=trigger_rule,
    )


def execute_validate_source_table_datasets(
    trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
) -> RecidivizKubernetesPodOperator:
    """Builds the task that validates the source table datasets belonging to the update
    group owned by the DAG this task runs in."""
    return _build_source_table_pod_task(
        task_id=VALIDATE_SOURCE_TABLE_DATASETS_TASK_ID,
        entrypoint="ValidateSourceTableDatasetsEntrypoint",
        trigger_rule=trigger_rule,
    )
