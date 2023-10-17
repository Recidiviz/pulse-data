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
"""
Logic for state and ingest instance specific dataflow pipelines.
"""
import logging
from typing import Dict

from airflow.decorators import task, task_group
from airflow.models import BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.ingest.get_max_update_datetime_sql_query_generator import (
    GetMaxUpdateDateTimeSqlQueryGenerator,
)
from recidiviz.airflow.dags.ingest.get_watermark_sql_query_generator import (
    GetWatermarkSqlQueryGenerator,
)
from recidiviz.airflow.dags.ingest.ingest_branching import get_ingest_branch_key
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    build_kubernetes_pod_task,
)
from recidiviz.airflow.dags.utils.cloud_sql import cloud_sql_conn_id_for_schema_type
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType

# Need a disable pointless statement because Python views the chaining operator ('>>')
# as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a "disable expression-not-assigned" because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


@task.short_circuit(task_id="should_run_based_on_watermarks")
def _should_run_based_on_watermarks(
    watermarks: Dict[str, str], max_update_datetimes: Dict[str, str]
) -> bool:
    for raw_file_tag, watermark_for_file in watermarks.items():
        if raw_file_tag not in max_update_datetimes:
            raise ValueError(
                f"Watermark file_tag '{raw_file_tag}' not found in max update datetimes: {max_update_datetimes.keys()}"
            )

        if watermark_for_file > max_update_datetimes[raw_file_tag]:
            logging.error(
                "Raw file %s has older data than the last time the ingest pipeline was run. Current max update_datetime: %s. Last ingest pipeline run update_datetime: %s.",
                raw_file_tag,
                max_update_datetimes[raw_file_tag],
                watermark_for_file,
            )
            return False

    return True


@task_group(group_id="initialize_dataflow_pipeline")
def _initialize_dataflow_pipeline(
    state_code: StateCode, instance: DirectIngestInstance
) -> None:
    """
    Initializes the dataflow pipeline by getting the max update datetimes and watermarks and checking if the pipeline should run.
    """
    operations_cloud_sql_conn_id = cloud_sql_conn_id_for_schema_type(
        SchemaType.OPERATIONS
    )

    get_max_update_datetimes = CloudSqlQueryOperator(
        task_id="get_max_update_datetimes",
        cloud_sql_conn_id=operations_cloud_sql_conn_id,
        query_generator=GetMaxUpdateDateTimeSqlQueryGenerator(
            region_code=state_code.value,
            ingest_instance=instance.value,
        ),
    )

    get_watermarks = CloudSqlQueryOperator(
        task_id="get_watermarks",
        cloud_sql_conn_id=operations_cloud_sql_conn_id,
        query_generator=GetWatermarkSqlQueryGenerator(
            region_code=state_code.value,
            ingest_instance=instance.value,
        ),
    )

    should_run_based_on_watermarks = _should_run_based_on_watermarks(
        watermarks=get_watermarks.output,  # type: ignore[arg-type]
        max_update_datetimes=get_max_update_datetimes.output,  # type: ignore[arg-type]
    )

    [get_max_update_datetimes, get_watermarks] >> should_run_based_on_watermarks


def _acquire_lock(
    state_code: StateCode, instance: DirectIngestInstance
) -> KubernetesPodOperator:
    return build_kubernetes_pod_task(
        task_id="acquire_lock",
        container_name="acquire_lock",
        arguments=[
            "--entrypoint=IngestAcquireLockEntrypoint",
            f"--state_code={state_code.value}",
            f"--ingest_instance={instance.value}",
            "--lock_id={{dag_run.run_id}}" + f"_{state_code.value}_{instance.value}",
        ],
    )


def _release_lock(
    state_code: StateCode, instance: DirectIngestInstance
) -> KubernetesPodOperator:
    return build_kubernetes_pod_task(
        trigger_rule=TriggerRule.NONE_SKIPPED,
        task_id="release_lock",
        container_name="release_lock",
        arguments=[
            "--entrypoint=IngestReleaseLockEntrypoint",
            f"--state_code={state_code.value}",
            f"--ingest_instance={instance.value}",
            "--lock_id={{dag_run.run_id}}" + f"_{state_code.value}_{instance.value}",
        ],
    )


def _create_dataflow_pipeline(
    _state_code: StateCode, _instance: DirectIngestInstance
) -> BaseOperator:
    # TODO(#23962): Replace EmptyOperator with dataflow operator
    return EmptyOperator(task_id="dataflow_pipeline")


def create_single_ingest_pipeline_group(
    state_code: StateCode,
    instance: DirectIngestInstance,
) -> TaskGroup:
    """
    Creates a dataflow pipeline operator for the given state and ingest instance.
    """

    with TaskGroup(get_ingest_branch_key(state_code.value, instance.value)) as dataflow:

        initialize_dataflow_pipeline = _initialize_dataflow_pipeline(
            state_code, instance
        )

        acquire_lock = _acquire_lock(state_code, instance)

        dataflow_pipeline = _create_dataflow_pipeline(state_code, instance)

        release_lock = _release_lock(state_code, instance)

        # TODO(#23986): Replace EmptyOperator with write upper bounds operator
        write_upper_bounds = EmptyOperator(task_id="write_upper_bounds")

        (
            initialize_dataflow_pipeline
            >> acquire_lock
            >> dataflow_pipeline
            >> release_lock
        )

        dataflow_pipeline >> write_upper_bounds

    return dataflow
