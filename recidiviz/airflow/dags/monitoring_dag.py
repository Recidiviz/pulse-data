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
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.monitoring.airflow_dag_run_history import (
    DAG_RUN_HISTORY_LOOKBACK,
    generate_airflow_dag_run_history,
)
from recidiviz.airflow.dags.monitoring.cleanup_exited_pods import cleanup_exited_pods
from recidiviz.airflow.dags.monitoring.dag_registry import get_monitoring_dag_id
from recidiviz.airflow.dags.monitoring.metadata import (
    FETCH_RAW_DATA_FILE_TAG_IMPORT_RUNS_TASK_ID,
)
from recidiviz.airflow.dags.monitoring.raw_data_file_tag_import_runs_sql_query_generator import (
    RawDataFileTagImportRunSqlQueryGenerator,
)
from recidiviz.airflow.dags.monitoring.sftp_ingest_ready_file_upload_times_sql_query_generator import (
    SftpIngestReadyFileUploadTimesSqlQueryGenerator,
)
from recidiviz.airflow.dags.monitoring.task_failure_alerts import (
    RAW_DATA_INCIDENT_START_DATE_LOOKBACK,
    report_failed_tasks,
)
from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.operators.recidiviz_kubernetes_pod_operator import (
    build_kubernetes_pod_task,
)
from recidiviz.airflow.dags.utils.cloud_sql import cloud_sql_conn_id_for_schema_type
from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS
from recidiviz.airflow.dags.utils.email import can_send_mail
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.airflow.dags.utils.recidiviz_pagerduty_service import (
    RecidivizPagerDutyService,
)
from recidiviz.persistence.database.schema_type import SchemaType

DAG_ID = get_monitoring_dag_id(get_project_id())

pagerduty_service = RecidivizPagerDutyService.monitoring_airflow_service(
    project_id=get_project_id()
)
email = pagerduty_service.service_integration_email


# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a "disable expression-not-assigned" because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned


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

    fetch_raw_data_file_tag_import_runs = CloudSqlQueryOperator(
        task_id=FETCH_RAW_DATA_FILE_TAG_IMPORT_RUNS_TASK_ID,
        cloud_sql_conn_id=cloud_sql_conn_id_for_schema_type(SchemaType.OPERATIONS),
        query_generator=RawDataFileTagImportRunSqlQueryGenerator(
            lookback=RAW_DATA_INCIDENT_START_DATE_LOOKBACK
        ),
    )

    airflow_failure_monitoring_and_alerting = PythonOperator(
        task_id="airflow_failure_monitoring_and_alerting",
        python_callable=report_failed_tasks,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fetch_raw_data_file_tag_import_runs >> airflow_failure_monitoring_and_alerting

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

    PythonOperator(
        task_id="generate_airflow_dag_run_history",
        python_callable=generate_airflow_dag_run_history,
        op_kwargs={"lookback": DAG_RUN_HISTORY_LOOKBACK},
    )

    fetch_sftp_upload_times = CloudSqlQueryOperator(
        task_id="fetch_sftp_upload_times",
        cloud_sql_conn_id=cloud_sql_conn_id_for_schema_type(SchemaType.OPERATIONS),
        query_generator=SftpIngestReadyFileUploadTimesSqlQueryGenerator(),
    )

    report_sftp_metrics = build_kubernetes_pod_task(
        task_id="report_sftp_ingest_ready_file_timeliness_metrics",
        container_name="report_sftp_ingest_ready_file_timeliness_metrics",
        arguments=[
            "--entrypoint=ReportSftpIngestReadyFileTimelinessEntrypoint",
            "--upload_times_json={{ task_instance.xcom_pull(task_ids='fetch_sftp_upload_times') | tojson }}",
        ],
    )

    fetch_sftp_upload_times >> report_sftp_metrics


monitoring_dag = create_monitoring_dag()
