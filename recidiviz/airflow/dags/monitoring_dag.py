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

from airflow.decorators import dag
from airflow.operators.python import PythonOperator

from recidiviz.airflow.dags.monitoring.cleanup_exited_pods import cleanup_exited_pods
from recidiviz.airflow.dags.monitoring.dag_registry import get_monitoring_dag_id
from recidiviz.airflow.dags.monitoring.task_failure_alerts import report_failed_tasks
from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    build_kubernetes_pod_task,
)
from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS
from recidiviz.airflow.dags.utils.email import can_send_mail
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.airflow.dags.utils.recidiviz_pagerduty_service import (
    RecidivizPagerDutyService,
)

DAG_ID = get_monitoring_dag_id(get_project_id())

pagerduty_service = RecidivizPagerDutyService.monitoring_airflow_service(
    project_id=get_project_id()
)
email = pagerduty_service.service_integration_email

# By setting catchup to False and max_active_runs to 1, we ensure that at
# most one instance of this DAG is running at a time. Because we set catchup
# to false, it ensures that new DAG runs aren't enqueued while the old one is
# waiting to finish.
@dag(
    dag_id=DAG_ID,
    default_args={
        **DEFAULT_ARGS,
        "email": email,
        "email_on_failure": can_send_mail(),
    },  # type: ignore
    schedule=None,
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
)
def create_monitoring_dag() -> None:
    """Creates the hourly monitoring dag"""
    build_kubernetes_pod_task(
        task_id="generate_export_timeliness_metrics",
        container_name="generate_export_timeliness_metric",
        arguments=[
            "--entrypoint=MetricExportTimelinessEntrypoint",
        ],
    )

    PythonOperator(
        task_id="airflow_failure_monitoring_and_alerting",
        python_callable=report_failed_tasks,
    )

    PythonOperator(
        task_id="cleanup_exited_pods",
        python_callable=cleanup_exited_pods,
    )

    build_kubernetes_pod_task(
        task_id="generate_airflow_environment_age_metrics",
        container_name="generate_airflow_environment_age_metrics",
        arguments=[
            "--entrypoint=ReportAirflowEnvironmentAgeEntrypoint",
        ],
    )


monitoring_dag = create_monitoring_dag()
