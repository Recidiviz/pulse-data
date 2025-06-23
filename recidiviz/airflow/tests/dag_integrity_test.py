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
"""
Unit test to ensure that the DAGs are valid and will be properly loaded into the Airflow UI.
"""
from datetime import datetime, timezone
from unittest.mock import patch

from airflow.models import DagBag

from recidiviz.airflow.dags.monitoring.airflow_alerting_incident import (
    AirflowAlertingIncident,
)
from recidiviz.airflow.dags.monitoring.dag_registry import (
    get_all_dag_ids,
    get_discrete_configuration_parameters,
    get_known_configuration_parameters,
)
from recidiviz.airflow.dags.monitoring.incident_alert_routing import (
    get_alerting_services_for_incident,
)
from recidiviz.airflow.dags.monitoring.job_run_history_delegate_factory import (
    JobRunHistoryDelegateFactory,
)
from recidiviz.airflow.tests.test_utils import DAG_FOLDER, AirflowIntegrationTest
from recidiviz.utils.environment import GCPEnvironment

_PROJECT_ID = "recidiviz-testing"


@patch(
    "os.environ",
    {
        "GCP_PROJECT": _PROJECT_ID,
    },
)
class TestDagIntegrity(AirflowIntegrationTest):
    """Tests the dags defined in the /dags package."""

    def setUp(self) -> None:
        self.github_client_patch = patch(
            "recidiviz.airflow.dags.monitoring.recidiviz_github_alerting_service.GithubHook.get_conn",
        )
        self.github_client_patch.start()
        self.env_for_project_patch = patch(
            "recidiviz.airflow.dags.monitoring.recidiviz_github_alerting_service.get_environment_for_project",
            return_value=GCPEnvironment.STAGING,
        )
        self.env_for_project_patch.start()
        super().setUp()

    def tearDown(self) -> None:
        self.github_client_patch.stop()
        self.env_for_project_patch.stop()
        super().tearDown()

    def test_dag_bag_import(self) -> None:
        """
        Verify that Airflow will be able to import all DAGs in the repository without
        errors
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        self.assertEqual(
            len(dag_bag.import_errors),
            0,
            f"There should be no DAG failures. Got: {dag_bag.import_errors}",
        )

    def test_render_template_as_native(self) -> None:
        """
        Verify all DAGs utilize the native template rendering used in our Kubernetes pod operators
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        self.assertNotEqual(len(dag_bag.dags), 0)
        for dag in dag_bag.dags.values():
            self.assertTrue(dag.render_template_as_native_obj)

    def test_get_all_dag_names_matches_discovered(self) -> None:
        """
        Verify that the DAGs discovered have the expected_names.
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        self.assertNotEqual(len(dag_bag.dags), 0)
        self.assertSetEqual(
            set(get_all_dag_ids(project_id=_PROJECT_ID)),
            set(dag_bag.dag_ids),
        )

    def test_discrete_parameters_registered(self) -> None:
        """
        Verify that the DAGs have their configuration parameters registered
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        for dag_id in dag_bag.dag_ids:
            discrete_parameters = set(
                get_discrete_configuration_parameters(_PROJECT_ID, dag_id)
            )
            known_params_set = get_known_configuration_parameters(_PROJECT_ID, dag_id)

            missing_params = discrete_parameters - known_params_set
            if missing_params:
                self.fail(
                    f"Found parameters defined in get_discrete_configuration_parameters() "
                    f"for dag [{dag_id}] which are not defined for that DAG in "
                    f"get_known_configuration_parameters(): {missing_params}"
                )

    def test_all_tasks_have_alerting_service(self) -> None:
        """
        Verify task ids are valid and can be parsed by our task alerting routing code
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        self.assertNotEqual(len(dag_bag.dags), 0)
        for dag in dag_bag.dags.values():
            for task_id in dag.task_ids:
                get_alerting_services_for_incident(
                    AirflowAlertingIncident(
                        dag_id=dag.dag_id,
                        dag_run_config="{}",
                        job_id=task_id,
                        failed_execution_dates=[datetime.now(tz=timezone.utc)],
                        error_message=None,
                        incident_type="Task Run",
                    )
                )

    def test_all_tasks_have_delegate(self) -> None:
        """
        Verify task ids are valid and can be parsed by our task alerting routing code
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        self.assertNotEqual(len(dag_bag.dags), 0)
        for dag in dag_bag.dags.values():
            JobRunHistoryDelegateFactory.build(dag_id=dag.dag_id)
