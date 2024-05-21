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
"""Tests for the raw data import DAG"""
from unittest.mock import patch

from airflow.utils.state import DagRunState
from sqlalchemy.orm import Session

from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest

_PROJECT_ID = "recidiviz-testing"


class TestRawDataImportDag(AirflowIntegrationTest):
    def setUp(self) -> None:
        super().setUp()
        self.environment_patcher = patch(
            "os.environ",
            {
                "GCP_PROJECT": _PROJECT_ID,
            },
        )
        self.environment_patcher.start()
        # pylint: disable=import-outside-toplevel
        from recidiviz.airflow.dags.raw_data_import_dag import (
            create_raw_data_import_dag,
        )

        self.dag = create_raw_data_import_dag()

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        super().tearDown()

    def test_raw_data_import_dag(self) -> None:

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(self.dag, session=session)
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
