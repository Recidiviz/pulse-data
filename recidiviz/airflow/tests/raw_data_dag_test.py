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
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

_PROJECT_ID = "recidiviz-testing"


class RawDataImportDagIntegrationTest(AirflowIntegrationTest):
    """Integration tests for the raw data import dag"""

    def setUp(self) -> None:
        super().setUp()
        self.environment_patcher = patch(
            "os.environ",
            {
                "GCP_PROJECT": _PROJECT_ID,
            },
        )
        test_state_codes = [StateCode.US_XX, StateCode.US_YY]
        test_state_code_and_instance_pairs = [
            (StateCode.US_XX, DirectIngestInstance.PRIMARY),
            (StateCode.US_XX, DirectIngestInstance.SECONDARY),
            (StateCode.US_YY, DirectIngestInstance.PRIMARY),
            (StateCode.US_YY, DirectIngestInstance.SECONDARY),
        ]
        self.environment_patcher.start()
        self.raw_data_enabled_pairs = patch(
            "recidiviz.airflow.dags.raw_data.raw_data_branching.get_raw_data_dag_enabled_state_and_instance_pairs",
            return_value=test_state_code_and_instance_pairs,
        )
        self.raw_data_enabled_pairs.start()
        self.raw_data_enabled_states = patch(
            "recidiviz.airflow.dags.raw_data.raw_data_branching.get_raw_data_dag_enabled_states",
            return_value=test_state_codes,
        )
        self.raw_data_enabled_states.start()
        # pylint: disable=import-outside-toplevel
        from recidiviz.airflow.dags.raw_data_import_dag import (
            create_raw_data_import_dag,
        )

        self.dag = create_raw_data_import_dag()

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        self.raw_data_enabled_pairs.stop()
        self.raw_data_enabled_states.stop()
        super().tearDown()

    def test_raw_data_import_dag(self) -> None:
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                self.dag,
                session=session,
                run_conf={
                    "ingest_instance": "PRIMARY",
                },
                expected_skipped_ids=[r"us_[a-z]{2}_secondary_import_branch_start"],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)
