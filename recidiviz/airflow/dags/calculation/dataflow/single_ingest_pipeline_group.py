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
from typing import Dict, Tuple

from airflow.decorators import task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup

from recidiviz.airflow.dags.calculation.dataflow.ingest_pipeline_task_group_delegate import (
    IngestDataflowPipelineTaskGroupDelegate,
)
from recidiviz.airflow.dags.calculation.ingest.add_ingest_job_completion_sql_query_generator import (
    AddIngestJobCompletionSqlQueryGenerator,
)
from recidiviz.airflow.dags.calculation.ingest.get_max_update_datetime_sql_query_generator import (
    GetMaxUpdateDateTimeSqlQueryGenerator,
)
from recidiviz.airflow.dags.calculation.ingest.get_watermark_sql_query_generator import (
    GetWatermarkSqlQueryGenerator,
)
from recidiviz.airflow.dags.calculation.ingest.set_watermark_sql_query_generator import (
    SetWatermarkSqlQueryGenerator,
)
from recidiviz.airflow.dags.calculation.initialize_calculation_dag_group import (
    INGEST_INSTANCE_JINJA_ARG,
)
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    build_kubernetes_pod_task,
)
from recidiviz.airflow.dags.utils.cloud_sql import cloud_sql_conn_id_for_schema_type
from recidiviz.airflow.dags.utils.constants import CHECK_FOR_VALID_WATERMARKS_TASK_ID
from recidiviz.airflow.dags.utils.dataflow_pipeline_group import (
    build_dataflow_pipeline_task_group,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.ingest_view_contents_context import (
    IngestViewContentsContext,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    StateSchemaIngestViewManifestCompilerDelegate,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.utils import metadata

# Need a disable pointless statement because Python views the chaining operator ('>>')
# as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a "disable expression-not-assigned" because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


def _has_launchable_ingest_views(state_code: StateCode) -> bool:
    region = direct_ingest_regions.get_direct_ingest_region(
        region_code=state_code.value.lower()
    )
    ingest_manifest_collector = IngestViewManifestCollector(
        region=region,
        delegate=StateSchemaIngestViewManifestCompilerDelegate(region=region),
    )
    return (
        len(
            ingest_manifest_collector.launchable_ingest_views(
                IngestViewContentsContext.build_for_project(
                    project_id=metadata.project_id()
                )
            )
        )
        > 0
    )


@task.short_circuit(ignore_downstream_trigger_rules=False)
# allows skipping of downstream tasks until trigger rule prevents it (like ALL_DONE)
def check_region_has_launchable_ingest_views(state_code: StateCode) -> bool:
    """Returns True if the state has launchable ingest views, otherwise short circuits."""
    if not _has_launchable_ingest_views(state_code):
        logging.info(
            "No launchable views found for [%s] - returning False",
            state_code.value,
        )
        return False

    logging.info(
        "State [%s] has launchable ingest views, therefore the ingest pipeline is eligible to run - returning True",
        state_code.value,
    )
    return True


@task(
    task_id=CHECK_FOR_VALID_WATERMARKS_TASK_ID,
)
def _check_for_valid_watermarks_task(
    watermarks: Dict[str, str], max_update_datetimes: Dict[str, str]
) -> bool:
    return check_for_valid_watermarks(
        watermarks=watermarks, max_update_datetimes=max_update_datetimes
    )


def check_for_valid_watermarks(
    watermarks: Dict[str, str], max_update_datetimes: Dict[str, str]
) -> bool:
    """Checks that the data currently in our raw data tables is just as new or newer
    than the data was the last time we ran this ingest pipeline.
    """
    missing_file_tags = {
        raw_file_tag: watermark_for_file
        for raw_file_tag, watermark_for_file in watermarks.items()
        if raw_file_tag not in max_update_datetimes
    }

    if missing_file_tags:
        raise ValueError(
            f"Found critical raw data tables that either do not exist or are empty: "
            f"{sorted(missing_file_tags.keys())}.\nWe cannot run the ingest pipeline "
            f"until data has been added to this table that has an update_datetime "
            f"greater than or equal to the high watermark for each of these files: "
            f"{missing_file_tags}.YOU SHOULD GENERALLY NOT CLEAR ROWS FROM THE "
            f"direct_ingest_dataflow_raw_table_upper_bounds TABLE TO RESOLVE THIS "
            f"ERROR UNLESS THE TABLES IN QUESTION ARE NOW DEFINITELY UNUSED."
        )

    stale_file_tag_errors = []

    for raw_file_tag, watermark_for_file in watermarks.items():
        if watermark_for_file > max_update_datetimes[raw_file_tag]:
            error_str = (
                f"  * [{raw_file_tag}] Current max update_datetime: "
                f"{max_update_datetimes[raw_file_tag]}. Last ingest pipeline run "
                f"update_datetime: {watermark_for_file}."
            )
            stale_file_tag_errors.append(error_str)

    if stale_file_tag_errors:
        errors = "\n".join(stale_file_tag_errors)
        raise ValueError(
            f"Found critical raw data tables with older data than the last time the "
            f"ingest pipeline was run:\n{errors}\nYOU SHOULD "
            f"ONLY CLEAR ROWS FROM direct_ingest_dataflow_raw_table_upper_bounds TO "
            f"RESOLVE THIS ERROR IF YOU'RE ABSOLUTELY CERTAIN THE OLDER VERSION OF THE "
            f"DATA IN THESE TABLES IS ACTUALLY MORE CURRENT / CORRECT."
        )

    return True


def _verify_raw_data_flashing_not_in_progress(
    state_code: StateCode,
) -> KubernetesPodOperator:
    return build_kubernetes_pod_task(
        task_id="verify_raw_data_flashing_not_in_progress",
        container_name="verify_raw_data_flashing_not_in_progress",
        arguments=[
            "--entrypoint=IngestCheckRawDataFlashingEntrypoint",
            f"--state_code={state_code.value}",
            INGEST_INSTANCE_JINJA_ARG,
        ],
        cloud_sql_connections=[SchemaType.OPERATIONS],
    )


def _initialize_ingest_pipeline(
    state_code: StateCode,
    operations_cloud_sql_conn_id: str,
) -> Tuple[TaskGroup, CloudSqlQueryOperator]:
    """
    Initializes the dataflow pipeline by getting the max update datetimes and watermarks
    and checking if the state has launachable views. Returns the task group and the
    get_max_update_datetimes task for use in downstream tasks.
    """

    with TaskGroup("initialize_ingest_pipeline") as initialize_ingest_pipeline:
        check_ingest_pipeline_should_run_in_dag = (
            check_region_has_launchable_ingest_views(state_code)
        )

        get_max_update_datetimes = CloudSqlQueryOperator(
            task_id="get_max_update_datetimes",
            cloud_sql_conn_id=operations_cloud_sql_conn_id,
            query_generator=GetMaxUpdateDateTimeSqlQueryGenerator(
                region_code=state_code.value
            ),
        )

        get_watermarks = CloudSqlQueryOperator(
            task_id="get_watermarks",
            cloud_sql_conn_id=operations_cloud_sql_conn_id,
            query_generator=GetWatermarkSqlQueryGenerator(
                region_code=state_code.value,
            ),
        )

        check_for_valid_watermarks_task = _check_for_valid_watermarks_task(
            watermarks=get_watermarks.output,  # type: ignore[arg-type]
            max_update_datetimes=get_max_update_datetimes.output,  # type: ignore[arg-type]
        )

        # TODO(#29058): add gated check for raw data resource locks

        (
            check_ingest_pipeline_should_run_in_dag
            >> [get_max_update_datetimes, get_watermarks]
            >> check_for_valid_watermarks_task
            >> _verify_raw_data_flashing_not_in_progress(state_code)
        )

    return initialize_ingest_pipeline, get_max_update_datetimes


def create_single_ingest_pipeline_group(state_code: StateCode) -> TaskGroup:
    """
    Creates a group that runs ingest logic for the given state.
    """

    operations_cloud_sql_conn_id = cloud_sql_conn_id_for_schema_type(
        SchemaType.OPERATIONS
    )

    with TaskGroup("ingest") as dataflow:
        (
            initialize_ingest_pipeline,
            get_max_update_datetimes,
        ) = _initialize_ingest_pipeline(state_code, operations_cloud_sql_conn_id)

        dataflow_pipeline_group, run_pipeline = build_dataflow_pipeline_task_group(
            delegate=IngestDataflowPipelineTaskGroupDelegate(
                state_code=state_code,
                max_update_datetimes_operator=get_max_update_datetimes,
            )
        )

        write_upper_bounds = CloudSqlQueryOperator(
            task_id="write_upper_bounds",
            cloud_sql_conn_id=operations_cloud_sql_conn_id,
            query_generator=SetWatermarkSqlQueryGenerator(
                region_code=state_code.value,
                get_max_update_datetime_task_id=get_max_update_datetimes.task_id,
                run_pipeline_task_id=run_pipeline.task_id,
            ),
        )

        write_ingest_job_completion = CloudSqlQueryOperator(
            task_id="write_ingest_job_completion",
            cloud_sql_conn_id=operations_cloud_sql_conn_id,
            query_generator=AddIngestJobCompletionSqlQueryGenerator(
                region_code=state_code.value,
                run_pipeline_task_id=run_pipeline.task_id,
            ),
        )

        (
            initialize_ingest_pipeline
            >> dataflow_pipeline_group
            >> write_ingest_job_completion
            >> write_upper_bounds
        )

    return dataflow
