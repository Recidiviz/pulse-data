# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
A subclass of DataflowTemplateOperator to ensure that the operator does not add a hash or
unique id to the task_id name on Dataflow
"""
from typing import Any, Dict

from airflow.providers.google.cloud.hooks.dataflow import DataflowHook
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowTemplatedJobStartOperator,
)
from airflow.utils.context import Context


class RecidivizDataflowTemplateOperator(DataflowTemplatedJobStartOperator):
    def execute(
        self,
        # Some context about the context: https://bcb.github.io/airflow/execute-context
        context: Context,  # pylint: disable=unused-argument
    ) -> Dict[Any, Any]:
        hook = DataflowHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            poll_sleep=self.poll_sleep,
        )

        # In DataflowTemplateOperator,  start_template_dataflow has the default append_job_name set to True
        # so it adds a unique-id to the end of the job name. This overwrites that default argument.
        return hook.start_template_dataflow(
            job_name=self.task_id,
            variables=self.dataflow_default_options,
            parameters=self.parameters,
            dataflow_template=self.template,
            project_id=self.project_id,
            append_job_name=False,
        )
