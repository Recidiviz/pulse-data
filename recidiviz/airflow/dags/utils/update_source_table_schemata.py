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
"""Shared builder for the source table schema-update task run by multiple DAGs.

The task is identical across DAGs: the entrypoint derives which update group to
update from the id of the DAG that launched its pod, so there are no per-DAG
arguments. Only the trigger rule varies with where the task sits in each DAG's
graph.
"""

from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    RecidivizKubernetesPodOperator,
    build_kubernetes_pod_task,
)
from recidiviz.airflow.dags.utils.constants import (
    UPDATE_BIG_QUERY_TABLE_SCHEMATA_TASK_ID,
)


def execute_update_big_query_table_schemata(
    trigger_rule: TriggerRule = TriggerRule.ALL_SUCCESS,
) -> RecidivizKubernetesPodOperator:
    """Builds the task that updates the schemas of the source table collections belonging
    to the update group owned by the DAG this task runs in."""
    return build_kubernetes_pod_task(
        task_id=UPDATE_BIG_QUERY_TABLE_SCHEMATA_TASK_ID,
        container_name=UPDATE_BIG_QUERY_TABLE_SCHEMATA_TASK_ID,
        arguments=["--entrypoint=UpdateBigQuerySourceTableSchemataEntrypoint"],
        trigger_rule=trigger_rule,
    )
