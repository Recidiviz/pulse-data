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
The DAG configuration to run the calculation pipelines in Dataflow simultaneously.
This file is uploaded to GCS on deploy.
"""
from collections import defaultdict
from typing import Dict, Iterable, List, Optional

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from google.api_core.retry import Retry
from requests import Response

from recidiviz.airflow.dags.calculation.constants import (
    STATE_SPECIFIC_METRIC_EXPORTS_GROUP_ID,
)
from recidiviz.airflow.dags.calculation.dataflow.metrics_pipeline_task_group_delegate import (
    MetricsDataflowPipelineTaskGroupDelegate,
)
from recidiviz.airflow.dags.calculation.dataflow.single_ingest_pipeline_group import (
    create_single_ingest_pipeline_group,
)
from recidiviz.airflow.dags.calculation.dataflow.supplemental_pipeline_task_group_delegate import (
    SupplementalDataflowPipelineTaskGroupDelegate,
)
from recidiviz.airflow.dags.calculation.initialize_calculation_dag_group import (
    INGEST_INSTANCE_JINJA_ARG,
    SANDBOX_PREFIX_JINJA_ARG,
    initialize_calculation_dag_group,
)
from recidiviz.airflow.dags.monitoring.dag_registry import get_calculation_dag_id
from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    RecidivizKubernetesPodOperator,
    build_kubernetes_pod_task,
)
from recidiviz.airflow.dags.utils.branching_by_key import (
    create_branching_by_key,
    select_state_code_parameter_branch,
)
from recidiviz.airflow.dags.utils.dataflow_pipeline_group import (
    build_dataflow_pipeline_task_group,
)
from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_launched_in_env,
)
from recidiviz.metrics.export.products.product_configs import (
    PRODUCTS_CONFIG_PATH,
    ProductConfigs,
    ProductExportConfig,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.pipelines.config_paths import PIPELINE_CONFIG_YAML_PATH
from recidiviz.utils.yaml_dict import YAMLDict

GCP_PROJECT_STAGING = "recidiviz-staging"

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a disable expression-not-assigned because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned

retry: Retry = Retry(predicate=lambda _: False)


def _get_pipeline_config() -> YAMLDict:
    return YAMLDict.from_path(PIPELINE_CONFIG_YAML_PATH)


def update_managed_views_operator() -> RecidivizKubernetesPodOperator:
    task_id = "update_managed_views_all"
    return build_kubernetes_pod_task(
        task_id=task_id,
        container_name=task_id,
        arguments=[
            "--entrypoint=UpdateAllManagedViewsEntrypoint",
            SANDBOX_PREFIX_JINJA_ARG,
        ],
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )


def execute_update_big_query_table_schemata() -> RecidivizKubernetesPodOperator:
    task_id = "update_big_query_table_schemata"
    return build_kubernetes_pod_task(
        task_id=task_id,
        container_name=task_id,
        arguments=[
            "--entrypoint=UpdateBigQuerySourceTableSchemataEntrypoint",
        ],
        trigger_rule=TriggerRule.ALL_DONE,
    )


def refresh_bq_dataset_operator(
    schema_type: SchemaType,
) -> RecidivizKubernetesPodOperator:
    schema_type_str = schema_type.value.upper()
    return build_kubernetes_pod_task(
        task_id=f"refresh_bq_dataset_{schema_type_str}",
        container_name=f"refresh_bq_dataset_{schema_type_str}",
        arguments=[
            "--entrypoint=BigQueryRefreshEntrypoint",
            f"--schema_type={schema_type_str}",
            INGEST_INSTANCE_JINJA_ARG,
            SANDBOX_PREFIX_JINJA_ARG,
        ],
    )


def execute_validations_operator(state_code: str) -> RecidivizKubernetesPodOperator:
    return build_kubernetes_pod_task(
        task_id=f"execute_validations_{state_code}",
        container_name=f"execute_validations_{state_code}",
        arguments=[
            "--entrypoint=ValidationEntrypoint",
            f"--state_code={state_code}",
            INGEST_INSTANCE_JINJA_ARG,
            SANDBOX_PREFIX_JINJA_ARG,
        ],
    )


def trigger_metric_view_data_operator(
    export_job_name: str, state_code: Optional[str]
) -> RecidivizKubernetesPodOperator:
    state_code_component = f"_{state_code.lower()}" if state_code else ""

    additional_args = []

    if state_code:
        additional_args.append(f"--state_code={state_code}")

    return build_kubernetes_pod_task(
        task_id=f"export_{export_job_name.lower()}{state_code_component}_metric_view_data",
        container_name=f"export_{export_job_name.lower()}{state_code_component}_metric_view_data",
        arguments=[
            "--entrypoint=MetricViewExportEntrypoint",
            f"--export_job_name={export_job_name}",
            SANDBOX_PREFIX_JINJA_ARG,
            *additional_args,
        ],
        retries=1,
    )


def create_metric_view_data_export_nodes(
    relevant_product_exports: List[ProductExportConfig],
) -> List[TaskGroup]:
    """Creates trigger nodes and wait conditions for metric view data exports based on provided export job filter."""
    metric_view_data_triggers: List[TaskGroup] = []
    for export in relevant_product_exports:
        export_job_name = export["export_job_name"]
        state_code = export["state_code"]
        export_metric_view_data = trigger_metric_view_data_operator(
            export_job_name=export_job_name,
            state_code=state_code,
        )
        metric_view_data_triggers.append(export_metric_view_data)

    return metric_view_data_triggers


def response_can_refresh_proceed_check(response: Response) -> bool:
    """Checks whether the refresh lock can proceed is true."""
    data = response.text
    return data.lower() == "true"


def create_pipeline_configs_by_state(
    pipeline_configs: Iterable[YAMLDict],
) -> Dict[str, List[YAMLDict]]:
    pipeline_params_by_state: Dict[str, List[YAMLDict]] = defaultdict(list)
    for pipeline_config in pipeline_configs:
        if get_project_id() == GCP_PROJECT_STAGING or not pipeline_config.peek_optional(
            "staging_only", bool
        ):
            state_code = pipeline_config.peek("state_code", str)
            pipeline_params_by_state[state_code].append(pipeline_config)
    return pipeline_params_by_state


def dataflow_pipeline_branches_by_state() -> Dict[str, TaskGroup]:
    """For each state with ingest enabled in the current environment, creates a
    TaskGroup that will run all Dataflow pipelines for this state. Metric / supplemental
    pipelines run after ingest pipelines are completed.
    """

    metric_pipelines = _get_pipeline_config().pop_dicts("metric_pipelines")
    supplemental_dataset_pipelines = _get_pipeline_config().pop_dicts(
        "supplemental_dataset_pipelines"
    )
    metric_pipeline_params_by_state = create_pipeline_configs_by_state(metric_pipelines)
    supplemental_pipeline_parameters_by_state = create_pipeline_configs_by_state(
        supplemental_dataset_pipelines
    )

    branches_by_state_code = {}
    for state_code in get_direct_ingest_states_launched_in_env():
        with TaskGroup(
            f"{state_code.value}_dataflow_pipelines"
        ) as state_pipelines_group:
            ingest_group = create_single_ingest_pipeline_group(state_code)

            for pipeline_config in metric_pipeline_params_by_state.get(
                state_code.value, {}
            ):
                metric_pipeline_group = build_dataflow_pipeline_task_group(
                    delegate=MetricsDataflowPipelineTaskGroupDelegate(
                        state_code, pipeline_config
                    ),
                )
                ingest_group >> metric_pipeline_group

            for pipeline_config in supplemental_pipeline_parameters_by_state.get(
                state_code.value, {}
            ):
                supplemental_pipeline_group = build_dataflow_pipeline_task_group(
                    delegate=SupplementalDataflowPipelineTaskGroupDelegate(
                        state_code, pipeline_config
                    ),
                )
                ingest_group >> supplemental_pipeline_group

        branches_by_state_code[state_code.value] = state_pipelines_group
    return branches_by_state_code


def validation_branches_by_state_code(
    states_to_validate: Iterable[str],
) -> Dict[str, TaskGroup]:
    branches_by_state_code = {}
    for state_code in states_to_validate:
        branches_by_state_code[state_code] = execute_validations_operator(state_code)
    return branches_by_state_code


def metric_export_branches_by_state_code(
    allowed_metric_export_state_codes: List[str],
) -> Dict[str, TaskGroup]:
    branches_by_state_code: Dict[str, TaskGroup] = {}

    for state_code in allowed_metric_export_state_codes:
        relevant_product_exports = ProductConfigs.from_file(
            path=PRODUCTS_CONFIG_PATH
        ).get_export_configs_for_job_filter(state_code)
        if not relevant_product_exports:
            continue
        with TaskGroup(group_id=f"{state_code}_metric_exports") as state_metric_exports:
            create_metric_view_data_export_nodes(relevant_product_exports)

        branches_by_state_code[state_code] = state_metric_exports

    return branches_by_state_code


# We set catchup to False, it ensures that extra DAG runs aren't enqueued if the DAG
# is paused and re-enabled.
@dag(
    dag_id=get_calculation_dag_id(get_project_id()),
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def create_calculation_dag() -> None:
    """This represents the overall execution of our calculation pipelines.

    The series of steps is as follows:
    1. Update the normalized state output for each state.
    2. Update the metric output for each state.
    3. Trigger BigQuery exports for each state and other datasets."""

    # --- step 1: pre-ingest steps -----------------------------------
    # Here we initialize the DAG and update the BQ table schemas
    # If the schema update is successful, we kick off BQ refresh.
    # If the schema update is not successful, we do not want to continue
    # with the rest of the DAG.
    update_big_query_table_schemata = execute_update_big_query_table_schemata()

    with TaskGroup("bq_refresh") as bq_refresh:
        operations_bq_refresh_completion = refresh_bq_dataset_operator(
            SchemaType.OPERATIONS
        )
        case_triage_bq_refresh_completion = refresh_bq_dataset_operator(
            SchemaType.CASE_TRIAGE
        )
        bq_refresh_completed = EmptyOperator(
            task_id="bq_refresh_completed", trigger_rule=TriggerRule.ALL_DONE
        )
        (
            [
                operations_bq_refresh_completion,
                case_triage_bq_refresh_completion,
            ]
            >> bq_refresh_completed
        )

    initialize_dag = initialize_calculation_dag_group()
    initialize_dag >> update_big_query_table_schemata >> bq_refresh

    # --- step 2: dataflow_pipelines -----------------------------------
    with TaskGroup(group_id="dataflow_pipelines") as dataflow_pipelines_task_group:
        pipeline_branches_by_state = dataflow_pipeline_branches_by_state()
        create_branching_by_key(
            pipeline_branches_by_state,
            select_state_code_parameter_branch,
        )

    dataflow_metric_pruning_task_id = "dataflow_metric_pruning"
    dataflow_metric_pruning = build_kubernetes_pod_task(
        task_id=dataflow_metric_pruning_task_id,
        container_name=dataflow_metric_pruning_task_id,
        arguments=["--entrypoint=DataflowMetricPruningEntrypoint"],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    dataflow_pipelines_completed = EmptyOperator(
        task_id="dataflow_pipelines_completed", trigger_rule=TriggerRule.ALL_DONE
    )
    # If the schema updates successfully, run ingest pipelines for all states.
    (
        update_big_query_table_schemata
        >> dataflow_pipelines_task_group
        >> dataflow_metric_pruning
        >> dataflow_pipelines_completed
    )

    # --- step 3: managed views -----------------------------------
    # When ingest, supplemental and metrics pipelines are finished,
    # we want to update all managed views with the updated data.
    # We also wait until the BigQuery refresh to ops and case triage is complete
    update_all_views = update_managed_views_operator()
    [
        update_big_query_table_schemata,
        bq_refresh_completed,
        dataflow_pipelines_completed,
    ] >> update_all_views

    # --- step 4: validations and metric exports -----------------------------------
    # When all manged views are updated, we can run validations and metric exports.
    with TaskGroup(group_id="validations") as validations:
        create_branching_by_key(
            # We turn on validations (may still be in dev mode) as soon as there is
            # any ingest output that may feed into our BQ views.
            validation_branches_by_state_code(
                states_to_validate=pipeline_branches_by_state.keys()
            ),
            select_state_code_parameter_branch,
        )

    with TaskGroup(group_id="metric_exports") as metric_exports:
        with TaskGroup(group_id=STATE_SPECIFIC_METRIC_EXPORTS_GROUP_ID):
            metric_export_branches_by_state = metric_export_branches_by_state_code(
                allowed_metric_export_state_codes=sorted(
                    # For every state with any dataflow pipelines enabled, we can create
                    # metric exports, if any are configured for that state. There is a
                    # unittest in product_configs_test.py that enforces that metric
                    # pipelines are enabled before we enable any metric export.
                    pipeline_branches_by_state.keys()
                )
            )
            create_branching_by_key(
                metric_export_branches_by_state, select_state_code_parameter_branch
            )

            # If any dataflow pipeline for a given state fails, do not run the export
            # for that state.
            for (
                state_code,
                metric_export_branch,
            ) in metric_export_branches_by_state.items():
                pipeline_branches_by_state[state_code] >> metric_export_branch

        product_configs = ProductConfigs.from_file(path=PRODUCTS_CONFIG_PATH)
        for export_config in product_configs.get_product_agnostic_export_configs():
            with TaskGroup(
                group_id=f"{export_config['export_job_name']}_metric_exports"
            ):
                create_metric_view_data_export_nodes([export_config])

    dataset_cleanup_task_id = "dataset_cleanup"
    dataset_cleanup = build_kubernetes_pod_task(
        task_id=dataset_cleanup_task_id,
        container_name=dataset_cleanup_task_id,
        arguments=["--entrypoint=DatasetCleanupEntrypoint"],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    row_access_policy_task_id = "apply_row_access_policies"
    apply_row_access_policies = build_kubernetes_pod_task(
        task_id=row_access_policy_task_id,
        container_name=row_access_policy_task_id,
        arguments=["--entrypoint=ApplyRowLevelPermissionsEntrypoint"],
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        update_all_views
        >> [validations, metric_exports]
        >> dataset_cleanup
        >> apply_row_access_policies
    )


calculation_dag = create_calculation_dag()
