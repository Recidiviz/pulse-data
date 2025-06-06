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
"""Helper for creating a TaskGroup that generates the arguments for, then runs a
 Dataflow pipeline.
 """
import abc
from typing import Any, Dict, Generic, List, Optional, Tuple, Union

import attr
from airflow.decorators import task
from airflow.models import BaseOperator, DagRun
from airflow.utils.task_group import TaskGroup

from recidiviz.airflow.dags.operators.recidiviz_dataflow_operator import (
    RecidivizDataflowFlexTemplateOperator,
)
from recidiviz.airflow.dags.utils.constants import (
    CREATE_FLEX_TEMPLATE_TASK_ID,
    DATAFLOW_OPERATOR_TASK_ID,
)
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.pipelines.pipeline_parameters import (
    PipelineParameters,
    PipelineParametersT,
)


class UpstreamTaskOutputs:
    """Object that stores the outputs of tasks that are marked as inputs to the dataflow
    pipeline job.
    """

    def __init__(self, task_id_to_output_map: Dict[str, Any]) -> None:
        self._task_id_to_output_map = task_id_to_output_map

    def get_output_for_operator(self, operator: BaseOperator) -> Any:
        """Returns the output for the task instance associated with the given operator."""
        return self._task_id_to_output_map[operator.task_id]


class DataflowPipelineTaskGroupDelegate(Generic[PipelineParametersT]):
    """Class that provides information about how to build pipeline arguments of a given
    type for a pipeline running in the context of an Airflow DAG.
    """

    @abc.abstractmethod
    def get_default_parameters(self) -> PipelineParametersT:
        """Subclasses should implement to return a set of PipelineParameters for the
        desired pipeline. This set of parameters may be updated to incorporate any
        sandbox arguments set dynamically for a given DAG run before actually running
        the pipeline.
        """

    @abc.abstractmethod
    def get_input_operators(self) -> List[BaseOperator]:
        """Returns the list of operators for tasks whose outputs must be used to
        generate pipeline arguments in get_pipeline_specific_dynamic_args(). The
        outputs of the DAG run's instance of this task will be made available via
        the |upstream_task_outputs| param passed to that function.
        """

    @abc.abstractmethod
    def get_pipeline_specific_dynamic_args(
        self, dag_run: DagRun, upstream_task_outputs: UpstreamTaskOutputs
    ) -> Dict[str, Any]:
        """Returns PipelineParameters keyword arguments that can only be generated
        at runtime (i.e. based on results of previous tasks or DAG input arguments).
        """


def build_dataflow_pipeline_task_group(
    delegate: DataflowPipelineTaskGroupDelegate,
) -> Tuple[TaskGroup, RecidivizDataflowFlexTemplateOperator]:
    """Builds a TaskGroup that handles running a dataflow pipeline specified by the
    provided |delegate|.

    Returns both the overall task group and the actual dataflow pipeline task.
    """
    with TaskGroup(
        group_id=f"{delegate.get_default_parameters().job_name}-pipeline",
    ) as dataflow_pipeline_group:

        @task(task_id=CREATE_FLEX_TEMPLATE_TASK_ID)
        def create_flex_template(
            params_no_overrides: PipelineParameters,
            upstream_task_outputs_map: Dict[str, Any],
            *,
            dag_run: Optional[DagRun] = None,
            **_kwargs: Any,
        ) -> Dict[str, Union[str, int, bool]]:
            if not dag_run:
                raise ValueError(
                    "dag_run not provided. This should be automatically set by Airflow."
                )

            dynamic_args = {
                **attr.asdict(params_no_overrides),
                **delegate.get_pipeline_specific_dynamic_args(
                    dag_run, UpstreamTaskOutputs(upstream_task_outputs_map)
                ),
            }

            parameters_cls = type(params_no_overrides)
            parameters: PipelineParameters = parameters_cls(**dynamic_args)

            return parameters.flex_template_launch_body()

        run_pipeline = RecidivizDataflowFlexTemplateOperator(
            task_id=DATAFLOW_OPERATOR_TASK_ID,
            location=delegate.get_default_parameters().region,
            body=create_flex_template(
                params_no_overrides=delegate.get_default_parameters(),
                upstream_task_outputs_map={
                    operator.task_id: operator.output
                    for operator in delegate.get_input_operators()
                },
            ),
            project_id=get_project_id(),
        )

    return dataflow_pipeline_group, run_pipeline
