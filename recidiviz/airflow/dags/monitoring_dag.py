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
The DAG configuration to run our monitoring tasks pipelines in Dataflow simultaneously.
This file is uploaded to GCS on deploy.
"""
import os

from airflow.decorators import dag

from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    build_recidiviz_kubernetes_pod_operator,
)
from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS

project_id = os.environ.get("GCP_PROJECT")


# By setting catchup to False and max_active_runs to 1, we ensure that at
# most one instance of this DAG is running at a time. Because we set catchup
# to false, it ensures that new DAG runs aren't enqueued while the old one is
# waiting to finish.
@dag(
    dag_id=f"{project_id}_hourly_monitoring_dag",
    default_args=DEFAULT_ARGS,
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
)
def create_monitoring_dag() -> None:
    build_recidiviz_kubernetes_pod_operator(
        task_id="generate_export_timeliness_metrics",
        container_name="generate_export_timeliness_metric",
        arguments=[
            "python",
            "-m",
            "recidiviz.entrypoints.monitoring.report_metric_export_timeliness",
        ],
    )


monitoring_dag = create_monitoring_dag()
