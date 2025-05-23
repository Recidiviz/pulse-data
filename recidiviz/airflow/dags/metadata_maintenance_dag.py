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
""" Periodically cleans the Airflow database to keep size down; important during upgrades"""
from datetime import datetime, timedelta, timezone

import pendulum
import psycopg2
from airflow import settings
from airflow.decorators import dag
from airflow.operators.python import task
from airflow.utils.db_cleanup import run_cleanup

from recidiviz.airflow.dags.monitoring.dag_registry import (
    get_metadata_maintenance_dag_id,
)
from recidiviz.airflow.dags.utils.default_args import DEFAULT_ARGS
from recidiviz.airflow.dags.utils.email import can_send_mail
from recidiviz.airflow.dags.utils.environment import get_project_id
from recidiviz.airflow.dags.utils.recidiviz_pagerduty_service import (
    RecidivizPagerDutyService,
)

DAG_ID = get_metadata_maintenance_dag_id(get_project_id())
START_DATE = pendulum.today("UTC").add(days=-1)


MAX_DB_ENTRY_AGE_IN_DAYS = 60
MAX_XCOM_ENTRY_IN_DAYS = 7


pagerduty_service = RecidivizPagerDutyService.monitoring_airflow_service(
    project_id=get_project_id()
)
email = pagerduty_service.service_integration_email


@task
def cleanup_task() -> None:
    """Cleans the Airflow database and runs a vacuum"""
    today = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    run_cleanup(
        clean_before_timestamp=today - timedelta(days=MAX_DB_ENTRY_AGE_IN_DAYS),
        skip_archive=True,
        confirm=False,
    )

    run_cleanup(
        clean_before_timestamp=today - timedelta(days=MAX_XCOM_ENTRY_IN_DAYS),
        table_names=["xcom"],
        skip_archive=True,
        confirm=False,
    )

    # Convert SQLAlchemy engine URL to psycopg2 DSN
    url = settings.engine.url
    psycopg2_dsn = f"dbname='{url.database}' user='{url.username}' password='{url.password}' host='{url.host}' port='{url.port}'"
    conn = psycopg2.connect(psycopg2_dsn)

    # autocommit mode tells PostgreSQL to run commands like VACUUM, REINDEX, etc., outside a transaction.
    # This is necessary because these commands cannot be run inside a transaction block.
    conn.set_session(autocommit=True)
    try:
        with conn.cursor() as cursor:
            # Perform a full vacuum and analyze on the database to be sure that disk space is freed
            # after the tables have been dropped. This is done by Postgres periodically, but we want to
            # ensure that it happens immediately.
            cursor.execute("VACUUM FULL ANALYZE;")
    finally:
        conn.close()


@dag(
    dag_id=DAG_ID,
    default_args={
        **DEFAULT_ARGS,
        "email": email,
        "email_on_failure": can_send_mail(),
    },  # type: ignore
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
)
def create_metadata_maintenance_dag() -> None:
    cleanup_task()


metadata_maintenance_dag = create_metadata_maintenance_dag()
