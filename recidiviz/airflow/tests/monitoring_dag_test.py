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
"""Unit test for the monitoring DAG"""
from unittest.mock import patch

from airflow.models.dagbag import DagBag
from airflow.utils.trigger_rule import TriggerRule

from recidiviz.airflow.dags.monitoring.dag_registry import get_monitoring_dag_id
from recidiviz.airflow.tests.test_utils import DAG_FOLDER, AirflowIntegrationTest

_PROJECT_ID = "recidiviz-testing"


@patch(
    "os.environ",
    {
        "GCP_PROJECT": _PROJECT_ID,
    },
)
class TestMonitoringDagSequencing(AirflowIntegrationTest):
    """Tests the monitoring DAGs sequencing"""

    def test_raw_data_before_airflow_monitoring(self) -> None:
        """Tests that we build raw data errors before we try to incorporate them into
        our airflow alerting.
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[get_monitoring_dag_id(_PROJECT_ID)]
        self.assertNotEqual(0, len(dag.task_ids))

        raw_data_errors = dag.task_dict["fetch_raw_data_file_tag_import_runs"]
        assert (
            "airflow_failure_monitoring_and_alerting"
            in raw_data_errors.downstream_task_ids
        )

        airflow_alerting = dag.task_dict["airflow_failure_monitoring_and_alerting"]
        assert airflow_alerting.trigger_rule == TriggerRule.ALL_DONE


@patch(
    "os.environ",
    {
        "GCP_PROJECT": _PROJECT_ID,
    },
)
class TestMonitoringDag(AirflowIntegrationTest):
    """Tests the airflow monitoring DAG."""

    def test_import(self) -> None:
        """Just tests that the monitoring_dag file can be imported."""
        # Need to import monitoring_dag inside test suite so environment variables are
        # set before importing, otherwise monitoring_dag will raise an Error and not
        # import.

        # pylint: disable=C0415 import-outside-toplevel
        # pylint: disable=unused-import
        from recidiviz.airflow.dags.monitoring_dag import monitoring_dag

        # If nothing fails, this test passes
