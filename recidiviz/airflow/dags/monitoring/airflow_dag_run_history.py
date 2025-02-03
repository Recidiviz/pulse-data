# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Generates metrics about our airflow DAGs."""
import datetime

from airflow.models import DagRun
from airflow.utils.session import create_session
from google.cloud import bigquery
from sqlalchemy import func, text

from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_row_streamer import BigQueryRowStreamer

DAGS_TO_EXCLUDE = ["airflow_monitoring"]
DAG_RUN_METADATA_TABLE_ADDRESS = BigQueryAddress(
    dataset_id="airflow_operations", table_id="dag_run_metadata"
)
# redefines schema defined in
# recidiviz/source_tables/yaml_managed/airflow_operations/dag_run_metadata.yaml
# to avoid pulling in source table apparatus into airflow, for now.
DAG_RUN_METADATA_TABLE_SCHEMA = [
    bigquery.SchemaField(mode="REQUIRED", field_type="DATETIME", name="write_time"),
    bigquery.SchemaField(mode="REQUIRED", field_type="STRING", name="project_id"),
    bigquery.SchemaField(mode="REQUIRED", field_type="STRING", name="dag_id"),
    bigquery.SchemaField(mode="REQUIRED", field_type="STRING", name="dag_run_id"),
    bigquery.SchemaField(mode="REQUIRED", field_type="DATETIME", name="execution_time"),
    bigquery.SchemaField(mode="REQUIRED", field_type="DATETIME", name="start_time"),
    bigquery.SchemaField(mode="REQUIRED", field_type="DATETIME", name="end_time"),
    bigquery.SchemaField(mode="NULLABLE", field_type="STRING", name="terminal_state"),
    bigquery.SchemaField(mode="REQUIRED", field_type="JSON", name="dag_run_config"),
]
# we are streaming the past 14 days of data even though most of that data will be duplicative.
# because (1) tasks can be retried retroactively and (2) the monitoring DAG failing, the 7
# day window gives us some extra buffer.
DAG_RUN_HISTORY_LOOKBACK = datetime.timedelta(days=7)


def build_dag_run_history(*, lookback: datetime.timedelta) -> list[dict]:
    """Returns dictionaries of completed dag runs that started in the last |lookback|
    days.
    """
    project_id = get_project_id()
    write_datetime = datetime.datetime.now(tz=datetime.UTC)
    with create_session() as session:
        latest_dag_runs = (
            session.query(
                DagRun.dag_id,
                DagRun.run_id.label("dag_run_id"),
                DagRun.execution_date.label("execution_time"),
                DagRun.start_date.label("start_time"),
                DagRun.end_date.label("end_time"),
                DagRun.state.label("terminal_state"),
                DagRun.conf.label("dag_run_config"),
            )
            .filter(
                func.age(DagRun.execution_date)
                < text(f"interval '{lookback.total_seconds()} seconds'")
            )
            .filter(DagRun.dag_id.not_in(DAGS_TO_EXCLUDE))
            .filter(DagRun.end_date.is_not(None))
            .order_by(DagRun.execution_date)
        )

        return [
            {
                "project_id": project_id,
                "write_time": write_datetime,
                **dict(dag_run),
            }
            for dag_run in latest_dag_runs.all()
        ]


def generate_airflow_dag_run_history(*, lookback: datetime.timedelta) -> None:
    """Builds completed dag runs over the last |lookback| days and writes them out
    to BigQuery.
    """

    dag_runs = build_dag_run_history(lookback=lookback)

    row_streamer = BigQueryRowStreamer(
        bq_client=BigQueryClientImpl(),
        table_address=DAG_RUN_METADATA_TABLE_ADDRESS,
        expected_table_schema=DAG_RUN_METADATA_TABLE_SCHEMA,
    )

    row_streamer.stream_rows(dag_runs)
