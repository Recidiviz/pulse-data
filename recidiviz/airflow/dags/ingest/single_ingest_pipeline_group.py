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
from airflow.models import BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup

from recidiviz.airflow.dags.ingest.ingest_branching import get_ingest_branch_key
from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    build_kubernetes_pod_task,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

# Need a disable pointless statement because Python views the chaining operator ('>>')
# as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a "disable expression-not-assigned" because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


def _initialize_dataflow_pipeline(
    _state_code: StateCode, _instance: DirectIngestInstance
) -> BaseOperator:
    # TODO(#23984): Check raw data max update time is less than max watermark
    return EmptyOperator(task_id="check_raw_data_max_update_time")


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
        ],
    )


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

        # TODO(#23962): Replace EmptyOperator with dataflow operator
        dataflow_pipeline = EmptyOperator(task_id="dataflow_pipeline")

        # TODO(#23987): Replace EmptyOperator with release lock operator
        release_lock = EmptyOperator(task_id="release_lock")

        # TODO(#23986): Replace EmptyOperator with write upper bounds operator
        write_upper_bounds = EmptyOperator(task_id="write_upper_bounds")

        (
            initialize_dataflow_pipeline
            >> acquire_lock
            >> dataflow_pipeline
            >> release_lock
            >> write_upper_bounds
        )

    return dataflow
