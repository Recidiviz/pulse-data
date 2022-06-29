# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Test utilities for DAG tests"""
import os
from datetime import datetime

from airflow import DAG
from airflow.models import BaseOperator, DagRun, TaskInstance

from recidiviz import airflow as recidiviz_airflow_module

AIRFLOW_WORKING_DIRECTORY = os.path.dirname(recidiviz_airflow_module.__file__)
DAG_FOLDER = "dags"

_FAKE_RUN_ID = "abc123"


def execute_task(dag: DAG, task: BaseOperator) -> None:
    """Executes a task in a given DAG, passing the appropriate context dictionary."""
    execution_date = datetime.now()
    context = {
        "task": task,
        "dag_run": DagRun(
            dag_id=dag.dag_id, execution_date=execution_date, run_id=_FAKE_RUN_ID
        ),
        "ti": TaskInstance(
            task=task, execution_date=execution_date, run_id=_FAKE_RUN_ID
        ),
    }
    task.execute(context)
