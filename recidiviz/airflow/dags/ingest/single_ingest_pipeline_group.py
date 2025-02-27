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
import json
import logging
from typing import Dict, List, Tuple, Union

from airflow.decorators import task
from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.ingest.add_ingest_job_completion_sql_query_generator import (
    AddIngestJobCompletionSqlQueryGenerator,
)
from recidiviz.airflow.dags.ingest.get_max_update_datetime_sql_query_generator import (
    GetMaxUpdateDateTimeSqlQueryGenerator,
)
from recidiviz.airflow.dags.ingest.get_watermark_sql_query_generator import (
    GetWatermarkSqlQueryGenerator,
)
from recidiviz.airflow.dags.ingest.ingest_branching import get_ingest_branch_key
from recidiviz.airflow.dags.ingest.set_watermark_sql_query_generator import (
    SetWatermarkSqlQueryGenerator,
)
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.operators.recidiviz_dataflow_operator import (
    RecidivizDataflowFlexTemplateOperator,
)
from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    build_kubernetes_pod_task,
)
from recidiviz.airflow.dags.utils.cloud_sql import cloud_sql_conn_id_for_schema_type
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.pipelines.dataflow_config import PIPELINE_CONFIG_YAML_PATH
from recidiviz.pipelines.ingest.pipeline_parameters import (
    INGEST_PIPELINE_NAME,
    IngestPipelineParameters,
)
from recidiviz.pipelines.ingest.pipeline_utils import (
    DEFAULT_INGEST_PIPELINE_REGIONS_BY_STATE_CODE,
    ingest_pipeline_name,
)
from recidiviz.utils.yaml_dict import YAMLDict

# Need a disable pointless statement because Python views the chaining operator ('>>')
# as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a "disable expression-not-assigned" because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


def _ingest_pipeline_should_run_in_dag(
    state_code: StateCode, instance: DirectIngestInstance
) -> KubernetesPodOperator:
    return build_kubernetes_pod_task(
        task_id="ingest_pipeline_should_run_in_dag",
        container_name="ingest_pipeline_should_run_in_dag",
        arguments=[
            "--entrypoint=IngestPipelineShouldRunInDagEntrypoint",
            f"--state_code={state_code.value}",
            f"--ingest_instance={instance.value}",
        ],
        cloud_sql_connections=[SchemaType.OPERATIONS],
        do_xcom_push=True,
    )


@task.short_circuit(ignore_downstream_trigger_rules=False)
# allows skipping of downstream tasks until trigger rule prevents it (like ALL_DONE)
def handle_ingest_pipeline_should_run_in_dag_check(
    should_run_ingest_pipeline: bool,
) -> bool:
    """Returns True if the DAG should continue, otherwise short circuits."""
    if not isinstance(should_run_ingest_pipeline, bool):
        raise ValueError(
            f"Expected should_run_ingest_pipeline value to be of type bool, found type "
            f"[{type(should_run_ingest_pipeline)}]: {should_run_ingest_pipeline}"
        )

    if not should_run_ingest_pipeline:
        logging.info("should_run_ingest_pipeline did not return true, do not continue.")
        return False
    return True


@task.short_circuit(
    task_id="should_run_based_on_watermarks",
    ignore_downstream_trigger_rules=False
    # allows skipping of downstream tasks until trigger rule prevents it (like ALL_DONE)
)
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


def _verify_raw_data_flashing_not_in_progress(
    state_code: StateCode, instance: DirectIngestInstance
) -> KubernetesPodOperator:
    return build_kubernetes_pod_task(
        task_id="verify_raw_data_flashing_not_in_progress",
        container_name="verify_raw_data_flashing_not_in_progress",
        arguments=[
            "--entrypoint=IngestCheckRawDataFlashingEntrypoint",
            f"--state_code={state_code.value}",
            f"--ingest_instance={instance.value}",
        ],
        cloud_sql_connections=[SchemaType.OPERATIONS],
    )


def _initialize_dataflow_pipeline(
    state_code: StateCode,
    instance: DirectIngestInstance,
    operations_cloud_sql_conn_id: str,
) -> Tuple[TaskGroup, CloudSqlQueryOperator]:
    """
    Initializes the dataflow pipeline by getting the max update datetimes and watermarks and checking if the pipeline should run.
    Returns the task group and the get_max_update_datetimes task for use in downstream tasks.
    """

    with TaskGroup("initialize_dataflow_pipeline") as initialize_dataflow_pipeline:
        check_ingest_pipeline_should_run_in_dag = (
            handle_ingest_pipeline_should_run_in_dag_check(
                _ingest_pipeline_should_run_in_dag(state_code, instance).output
            )
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

        (
            check_ingest_pipeline_should_run_in_dag
            >> [get_max_update_datetimes, get_watermarks]
            >> should_run_based_on_watermarks
            >> _verify_raw_data_flashing_not_in_progress(state_code, instance)
        )

    return initialize_dataflow_pipeline, get_max_update_datetimes


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


def _get_ingest_pipeline_configs() -> List[YAMLDict]:
    return YAMLDict.from_path(PIPELINE_CONFIG_YAML_PATH).pop_dicts("ingest_pipelines")


def _create_dataflow_pipeline(
    state_code: StateCode,
    ingest_instance: DirectIngestInstance,
    max_update_datetimes_operator: BaseOperator,
) -> Tuple[TaskGroup, BaseOperator]:
    """Builds a task Group that handles creating the flex template operator for a given
    pipeline parameters.
    """

    with TaskGroup(group_id="dataflow_pipeline") as dataflow_pipeline_group:
        region = DEFAULT_INGEST_PIPELINE_REGIONS_BY_STATE_CODE[state_code]

        @task(task_id="create_flex_template")
        def create_flex_template(
            max_update_datetimes: Dict[str, str]
        ) -> Dict[str, Union[str, int, bool]]:

            parameters = IngestPipelineParameters(
                project=get_project_id(),
                ingest_instance=ingest_instance.value,
                raw_data_upper_bound_dates_json=json.dumps(max_update_datetimes),
                job_name=ingest_pipeline_name(state_code, ingest_instance),
                pipeline=INGEST_PIPELINE_NAME,
                state_code=state_code.value,
                region=region,
            )

            return parameters.flex_template_launch_body()

        run_pipeline = RecidivizDataflowFlexTemplateOperator(
            task_id="run_pipeline",
            location=region,
            body=create_flex_template(max_update_datetimes_operator.output),  # type: ignore[arg-type]
            project_id=get_project_id(),
        )

    return dataflow_pipeline_group, run_pipeline


def create_single_ingest_pipeline_group(
    state_code: StateCode,
    instance: DirectIngestInstance,
) -> TaskGroup:
    """
    Creates a dataflow pipeline operator for the given state and ingest instance.
    """

    operations_cloud_sql_conn_id = cloud_sql_conn_id_for_schema_type(
        SchemaType.OPERATIONS
    )

    with TaskGroup(get_ingest_branch_key(state_code.value, instance.value)) as dataflow:

        (
            initialize_dataflow_pipeline,
            get_max_update_datetimes,
        ) = _initialize_dataflow_pipeline(
            state_code, instance, operations_cloud_sql_conn_id
        )

        acquire_lock = _acquire_lock(state_code, instance)

        dataflow_pipeline_group, run_pipeline = _create_dataflow_pipeline(
            state_code=state_code,
            ingest_instance=instance,
            max_update_datetimes_operator=get_max_update_datetimes,
        )

        release_lock = _release_lock(state_code, instance)

        write_upper_bounds = CloudSqlQueryOperator(
            task_id="write_upper_bounds",
            cloud_sql_conn_id=operations_cloud_sql_conn_id,
            query_generator=SetWatermarkSqlQueryGenerator(
                region_code=state_code.value,
                ingest_instance=instance.value,
                get_max_update_datetime_task_id=get_max_update_datetimes.task_id,
                run_pipeline_task_id=run_pipeline.task_id,
            ),
        )

        write_ingest_job_completion = CloudSqlQueryOperator(
            task_id="write_ingest_job_completion",
            cloud_sql_conn_id=operations_cloud_sql_conn_id,
            query_generator=AddIngestJobCompletionSqlQueryGenerator(
                region_code=state_code.value,
                ingest_instance=instance.value,
                run_pipeline_task_id=run_pipeline.task_id,
            ),
        )

        (
            initialize_dataflow_pipeline
            >> acquire_lock
            >> dataflow_pipeline_group
            >> release_lock
        )

        dataflow_pipeline_group >> write_ingest_job_completion >> write_upper_bounds

    return dataflow
