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
import os
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Type, Union

from airflow.decorators import dag, task
from airflow.models import DagRun, TaskInstance
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from google.api_core.retry import Retry
from more_itertools import one
from requests import Response

from recidiviz.airflow.dags.calculation.initialize_calculation_dag_group import (
    get_ingest_instance,
    get_sandbox_prefix,
    get_state_code_filter,
    initialize_calculation_dag_group,
)
from recidiviz.airflow.dags.operators.recidiviz_dataflow_operator import (
    RecidivizDataflowFlexTemplateOperator,
)
from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    build_kubernetes_pod_task_group,
)
from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS
from recidiviz.airflow.dags.utils.export_tasks_config import PIPELINE_AGNOSTIC_EXPORTS
from recidiviz.airflow.dags.utils.state_code_branch import create_state_code_branching
from recidiviz.metrics.export.products.product_configs import (
    PRODUCTS_CONFIG_PATH,
    ProductConfigs,
    ProductExportConfig,
)
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.pipelines.normalization.pipeline_parameters import (
    NormalizationPipelineParameters,
)
from recidiviz.pipelines.pipeline_parameters import (
    PipelineParameters,
    PipelineParametersT,
)
from recidiviz.pipelines.supplemental.pipeline_parameters import (
    SupplementalPipelineParameters,
)
from recidiviz.utils.yaml_dict import YAMLDict

GCP_PROJECT_STAGING = "recidiviz-staging"

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a disable expression-not-assigned because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned

if (config_file_opt := os.environ.get("CONFIG_FILE")) is None:
    raise ValueError("Configuration file not specified")
if not (project_id_opt := os.environ.get("GCP_PROJECT")):
    raise ValueError("project_id must be configured.")

config_file: str = config_file_opt
project_id: str = project_id_opt

retry: Retry = Retry(predicate=lambda _: False)


def update_all_managed_views_operator() -> TaskGroup:
    def get_kubernetes_arguments(
        _dag_run: DagRun, task_instance: TaskInstance
    ) -> List[str]:
        additional_args = []

        sandbox_prefix = get_sandbox_prefix(task_instance)
        if sandbox_prefix:
            additional_args.append(f"--sandbox_prefix={sandbox_prefix}")

        return [
            "python",
            "-m",
            "recidiviz.entrypoints.view_update.update_all_managed_views",
            *additional_args,
        ]

    return build_kubernetes_pod_task_group(
        group_id="update_all_managed_views",
        container_name="update_all_managed_views",
        arguments=get_kubernetes_arguments,
        trigger_rule=TriggerRule.ALL_DONE,
    )


def refresh_bq_dataset_operator(schema_type: str) -> TaskGroup:
    def get_kubernetes_arguments(
        dag_run: DagRun, task_instance: TaskInstance
    ) -> List[str]:
        additional_args = []

        sandbox_prefix = get_sandbox_prefix(task_instance)
        if sandbox_prefix:
            additional_args.append(f"--sandbox_prefix={sandbox_prefix}")

        return [
            "python",
            "-m",
            "recidiviz.entrypoints.bq_refresh.cloud_sql_to_bq_refresh",
            f"--schema_type={schema_type.upper()}",
            f"--ingest_instance={get_ingest_instance(dag_run)}",
            *additional_args,
        ]

    return build_kubernetes_pod_task_group(
        group_id=f"refresh_bq_dataset_{schema_type}",
        container_name=f"refresh_bq_dataset_{schema_type}",
        arguments=get_kubernetes_arguments,
    )


def execute_update_normalized_state() -> TaskGroup:
    def get_kubernetes_arguments(
        dag_run: DagRun, task_instance: TaskInstance
    ) -> List[str]:
        additional_args = []

        state_code_filter = get_state_code_filter(dag_run)

        if state_code_filter:
            additional_args.append(f"--state_code_filter={state_code_filter}")

        sandbox_prefix = get_sandbox_prefix(task_instance)
        if sandbox_prefix:
            additional_args.append(f"--sandbox_prefix={sandbox_prefix}")

        return [
            "python",
            "-m",
            "recidiviz.entrypoints.normalization.update_normalized_state_dataset",
            f"--ingest_instance={get_ingest_instance(dag_run)}",
            *additional_args,
        ]

    return build_kubernetes_pod_task_group(
        group_id="update_normalized_state",
        container_name="update_normalized_state",
        arguments=get_kubernetes_arguments,
        trigger_rule=TriggerRule.ALL_DONE,
    )


def execute_validations_operator(state_code: str) -> TaskGroup:
    def get_kubernetes_arguments(
        dag_run: DagRun, task_instance: TaskInstance
    ) -> List[str]:
        additional_args = []

        sandbox_prefix = get_sandbox_prefix(task_instance)
        if sandbox_prefix:
            additional_args.append(f"--sandbox_prefix={sandbox_prefix}")

        return [
            "python",
            "-m",
            "recidiviz.entrypoints.validation.validate",
            f"--state_code={state_code}",
            f"--ingest_instance={get_ingest_instance(dag_run)}",
            *additional_args,
        ]

    return build_kubernetes_pod_task_group(
        group_id=f"execute_validations_{state_code}",
        container_name=f"execute_validations_{state_code}",
        arguments=get_kubernetes_arguments,
    )


def trigger_metric_view_data_operator(
    export_job_name: str, state_code: Optional[str]
) -> TaskGroup:
    state_code_component = f"_{state_code.lower()}" if state_code else ""

    def get_kubernetes_arguments(
        _dag_run: DagRun, task_instance: TaskInstance
    ) -> List[str]:
        additional_args = []

        sandbox_prefix = get_sandbox_prefix(task_instance)
        if sandbox_prefix:
            additional_args.append(f"--sandbox_prefix={sandbox_prefix}")

        if state_code:
            additional_args.append(f"--state_code={state_code}")

        return [
            "python",
            "-m",
            "recidiviz.entrypoints.metric_export.metric_view_export",
            f"--export_job_name={export_job_name}",
            *additional_args,
        ]

    return build_kubernetes_pod_task_group(
        group_id=f"export_{export_job_name.lower()}{state_code_component}_metric_view_data",
        container_name=f"export_{export_job_name.lower()}{state_code_component}_metric_view_data",
        arguments=get_kubernetes_arguments,
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
        if project_id == GCP_PROJECT_STAGING or not pipeline_config.peek_optional(
            "staging_only", bool
        ):
            state_code = pipeline_config.peek("state_code", str)
            pipeline_params_by_state[state_code].append(pipeline_config)
    return pipeline_params_by_state


def build_dataflow_pipeline_task_group(
    pipeline_config: YAMLDict,
    parameter_cls: Type[PipelineParametersT],
) -> TaskGroup:
    """Builds a task Group that handles creating the flex template operator for a given
    pipeline parameters.
    """
    job_name = pipeline_config.peek("job_name", str)
    with TaskGroup(group_id=job_name) as dataflow_pipeline_group:

        @task(task_id="create_flex_template")
        def create_flex_template(
            dag_run: Optional[DagRun] = None,
            task_instance: Optional[TaskInstance] = None,
        ) -> Dict[str, Union[str, int, bool]]:
            if not task_instance:
                raise ValueError(
                    "task_instance not provided. This should be automatically set by Airflow."
                )
            if not dag_run:
                raise ValueError(
                    "dag_run not provided. This should be automatically set by Airflow."
                )
            parameters: PipelineParameters = parameter_cls(
                project=project_id,
                ingest_instance=get_ingest_instance(dag_run),
                **pipeline_config.get(),  # type: ignore
            )

            sandbox_prefix = get_sandbox_prefix(task_instance)
            if sandbox_prefix:
                parameters = parameters.update_datasets_with_sandbox_prefix(
                    sandbox_prefix
                )

            return parameters.flex_template_launch_body()

        _ = RecidivizDataflowFlexTemplateOperator(
            task_id="run_pipeline",
            location=pipeline_config.peek("region", str),
            body=create_flex_template(),
            project_id=project_id,
        )

    return dataflow_pipeline_group


def normalization_pipeline_branches_by_state_code() -> Dict[str, TaskGroup]:
    normalization_pipelines = YAMLDict.from_path(config_file).pop_dicts(
        "normalization_pipelines"
    )
    normalization_pipeline_params_by_state: Dict[
        str, List[YAMLDict]
    ] = create_pipeline_configs_by_state(normalization_pipelines)

    return {
        state_code: build_dataflow_pipeline_task_group(
            # There should only be one normalization pipeline per state
            pipeline_config=one(parameters_list),
            parameter_cls=NormalizationPipelineParameters,
        )
        for state_code, parameters_list in normalization_pipeline_params_by_state.items()
    }


def post_normalization_pipeline_branches_by_state_code() -> Dict[str, TaskGroup]:
    """Creates a TaskGroup for each state that contains all the post-normalization pipelines for that state."""
    metric_pipelines = YAMLDict.from_path(config_file).pop_dicts("metric_pipelines")
    supplemental_dataset_pipelines = YAMLDict.from_path(config_file).pop_dicts(
        "supplemental_dataset_pipelines"
    )
    metric_pipeline_params_by_state = create_pipeline_configs_by_state(metric_pipelines)
    supplemental_pipeline_parameters_by_state = create_pipeline_configs_by_state(
        supplemental_dataset_pipelines
    )

    all_branched_states = set(
        list(metric_pipeline_params_by_state.keys())
        + list(supplemental_pipeline_parameters_by_state.keys())
    )

    branches_by_state_code = {}
    for state_code in all_branched_states:
        with TaskGroup(
            group_id=f"{state_code}_dataflow_pipelines"
        ) as state_code_dataflow_pipelines:
            for pipeline_config in metric_pipeline_params_by_state[state_code]:
                build_dataflow_pipeline_task_group(
                    pipeline_config=pipeline_config,
                    parameter_cls=MetricsPipelineParameters,
                )

            for pipeline_config in supplemental_pipeline_parameters_by_state[
                state_code
            ]:
                build_dataflow_pipeline_task_group(
                    pipeline_config=pipeline_config,
                    parameter_cls=SupplementalPipelineParameters,
                )

        branches_by_state_code[state_code] = state_code_dataflow_pipelines

    return branches_by_state_code


def validation_branches_by_state_code(
    states_to_validate: Iterable[str],
) -> Dict[str, TaskGroup]:
    branches_by_state_code = {}
    for state_code in states_to_validate:
        branches_by_state_code[state_code] = execute_validations_operator(state_code)
    return branches_by_state_code


def metric_export_branches_by_state_code(
    post_normalization_pipelines_by_state: Dict[str, TaskGroup],
) -> Dict[str, TaskGroup]:
    branches_by_state_code: Dict[str, TaskGroup] = {}

    # For every state with post-normalization pipelines enabled, we can create metric
    # exports, if any are configured for that state.
    for (
        state_code,
        metric_pipelines_group,
    ) in post_normalization_pipelines_by_state.items():
        relevant_product_exports = ProductConfigs.from_file(
            path=PRODUCTS_CONFIG_PATH
        ).get_export_configs_for_job_filter(state_code)
        if not relevant_product_exports:
            continue
        with TaskGroup(group_id=f"{state_code}_metric_exports") as state_metric_exports:
            create_metric_view_data_export_nodes(relevant_product_exports)

        metric_pipelines_group >> state_metric_exports

        branches_by_state_code[state_code] = state_metric_exports

    return branches_by_state_code


# We set catchup to False, it ensures that extra DAG runs aren't enqueued if the DAG
# is paused and re-enabled.
@dag(
    dag_id=f"{project_id}_calculation_dag",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
)
def create_calculation_dag() -> None:
    """This represents the overall execution of our calculation pipelines.

    The series of steps is as follows:
    1. Update the normalized state output for each state.
    2. Update the metric output for each state.
    3. Trigger BigQuery exports for each state and other datasets."""

    with TaskGroup("bq_refresh") as bq_refresh:
        state_bq_refresh_completion = refresh_bq_dataset_operator("STATE")
        operations_bq_refresh_completion = refresh_bq_dataset_operator("OPERATIONS")
        case_triage_bq_refresh_completion = refresh_bq_dataset_operator("CASE_TRIAGE")

    initialize_calculation_dag_group() >> bq_refresh

    trigger_update_all_views = update_all_managed_views_operator()

    (
        [
            state_bq_refresh_completion,
            operations_bq_refresh_completion,
            case_triage_bq_refresh_completion,
        ]
        >> trigger_update_all_views
    )

    with TaskGroup(group_id="normalization") as normalization_task_group:
        create_state_code_branching(normalization_pipeline_branches_by_state_code())

    update_normalized_state = execute_update_normalized_state()

    # Normalization pipelines should run after the BQ refresh is complete, but
    # complete before normalized_state dataset is refreshed.
    (state_bq_refresh_completion >> normalization_task_group >> update_normalized_state)

    with TaskGroup(
        group_id="post_normalization_pipelines"
    ) as post_normalization_pipelines:
        post_normalization_pipelines_by_state = (
            post_normalization_pipeline_branches_by_state_code()
        )
        create_state_code_branching(
            post_normalization_pipelines_by_state, TriggerRule.NONE_FAILED
        )

    # This ensures that all of the normalization pipelines for a state will
    # run and the normalized_state dataset will be updated before the
    # metric pipelines for the state are triggered.
    update_normalized_state >> post_normalization_pipelines

    # Metric pipelines should complete before view update starts
    post_normalization_pipelines >> trigger_update_all_views

    with TaskGroup(group_id="validations") as validations:
        create_state_code_branching(
            validation_branches_by_state_code(
                post_normalization_pipelines_by_state.keys()
            ),
            TriggerRule.NONE_FAILED,
        )

    trigger_update_all_views >> validations

    with TaskGroup(group_id="metric_exports") as metric_exports:
        with TaskGroup(group_id="state_specific_metric_exports"):
            create_state_code_branching(
                metric_export_branches_by_state_code(
                    post_normalization_pipelines_by_state
                ),
                TriggerRule.NONE_FAILED,
            )

        for export in PIPELINE_AGNOSTIC_EXPORTS:
            relevant_product_exports = ProductConfigs.from_file(
                path=PRODUCTS_CONFIG_PATH
            ).get_export_configs_for_job_filter(export)
            if not relevant_product_exports:
                continue
            with TaskGroup(group_id=f"{export}_metric_exports"):
                create_metric_view_data_export_nodes(relevant_product_exports)

    trigger_update_all_views >> metric_exports


create_calculation_dag()
