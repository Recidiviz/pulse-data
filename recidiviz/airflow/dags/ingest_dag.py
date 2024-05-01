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
The DAG configuration to run ingest.
"""

from airflow.models.dag import dag
from airflow.utils.task_group import TaskGroup

from recidiviz.airflow.dags.ingest.ingest_branching import (
    create_ingest_branch_map,
    get_state_code_and_ingest_instance_key,
)
from recidiviz.airflow.dags.ingest.initialize_ingest_dag_group import (
    create_initialize_ingest_dag,
)
from recidiviz.airflow.dags.ingest.single_ingest_pipeline_group import (
    create_single_ingest_pipeline_group,
)
from recidiviz.airflow.dags.monitoring.dag_registry import get_ingest_dag_id
from recidiviz.airflow.dags.utils.branching_by_key import create_branching_by_key
from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS
from recidiviz.airflow.dags.utils.environment import get_project_id

# Need a disable pointless statement because Python views the chaining operator ('>>')
# as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a "disable expression-not-assigned" because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


@dag(
    dag_id=get_ingest_dag_id(get_project_id()),
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
)
def create_ingest_dag() -> None:
    """
    The DAG configuration to run ingest.
    """

    with TaskGroup("ingest_branching") as ingest_branching:
        create_branching_by_key(
            create_ingest_branch_map(create_single_ingest_pipeline_group),
            get_state_code_and_ingest_instance_key,
        )

    create_initialize_ingest_dag() >> ingest_branching


create_ingest_dag()
