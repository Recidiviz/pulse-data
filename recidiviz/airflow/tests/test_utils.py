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
import unittest
from datetime import datetime
from typing import Any, Optional
from unittest.mock import patch

from airflow import DAG, settings
from airflow.models import BaseOperator, DagRun, TaskInstance
from airflow.utils.db import initdb, resetdb
from airflow.www.fab_security.sqla.models import (
    RegisterUser,
    User,
    add_index_on_ab_register_user_username_postgres,
    add_index_on_ab_user_username_postgres,
)
from sqlalchemy import event

from recidiviz import airflow as recidiviz_airflow_module
from recidiviz.tools.postgres import local_postgres_helpers

AIRFLOW_WORKING_DIRECTORY = os.path.dirname(recidiviz_airflow_module.__file__)
DAG_FOLDER = "dags"

_FAKE_RUN_ID = "abc123"


def execute_task(dag: DAG, task: BaseOperator) -> Any:
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
    return task.execute(context)


# These events are implemented in Airflow source code such that every time we call create_all()
# an additional duplicate index is added to the schema
# See: https://github.com/apache/airflow/pull/32731
BUGGY_AIRFLOW_SQLALCHEMY_EVENTS = [
    (User.__table__, "before_create", add_index_on_ab_user_username_postgres),
    (
        RegisterUser.__table__,
        "before_create",
        add_index_on_ab_register_user_username_postgres,
    ),
]


class AirflowIntegrationTest(unittest.TestCase):
    """Sets up the airflow database and provides a SQLAlchemy session builder"""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]
    environment_patcher: Any

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()
        cls.environment_patcher = patch(
            "os.environ",
            {
                "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": local_postgres_helpers.on_disk_postgres_db_url(),
            },
        )
        cls.environment_patcher.start()

        # Configure settings.Session() to use the new on-disk postgres
        settings.initialize()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )
        cls.environment_patcher.stop()

    def setUp(self) -> None:
        self.engine = settings.engine

        for invalid_event in BUGGY_AIRFLOW_SQLALCHEMY_EVENTS:
            table, event_name, fn = invalid_event
            if event.contains(table, event_name, fn):
                event.remove(table, event_name, fn)

        initdb(load_connections=False)

    def tearDown(self) -> None:
        resetdb(skip_init=True)
        self.engine.dispose()
