# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Unit tests for the identity ingest DAG."""
from unittest.mock import MagicMock, patch

from airflow import DAG

from recidiviz.airflow.dags.utils.branching_by_key import select_tenant_parameter_branch
from recidiviz.airflow.dags.utils.config_utils import TENANT_FILTER
from recidiviz.airflow.dags.utils.constants import DATAFLOW_OPERATOR_TASK_ID
from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest
from recidiviz.airflow.tests.utils.dag_helper_functions import (
    fake_operator_constructor,
    fake_operator_with_return_value,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.pipelines.ingest.activity.pipeline_utils import (
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
)

_PROJECT_ID = "recidiviz-testing"
_DAG_ID = f"{_PROJECT_ID}_identity_ingest_dag"
_TENANTS = [StateCode.US_XX.value, StateCode.US_YY.value]


@patch(
    "os.environ",
    {
        "GCP_PROJECT": _PROJECT_ID,
    },
)
class TestIdentityIngestDag(AirflowIntegrationTest):
    """Tests the identity ingest DAG structure and per-tenant branching.

    The DAG reuses `create_single_ingest_pipeline_group` (the same task group the
    activity DAG uses), passing `IdentityIngestDataflowPipelineTaskGroupDelegate`
    as the delegate. So each per-tenant branch should have the same task shape as
    a per-state branch of the activity DAG: launchable-views short-circuit, raw
    data freshness watermark check, raw data flashing check, the Dataflow
    pipeline itself, and the writes back to `direct_ingest_dataflow_*` tagged as
    IDENTITY.
    """

    def setUp(self) -> None:
        super().setUp()
        self.launched_states_patcher = patch(
            "recidiviz.airflow.dags.identity_ingest_dag."
            "get_direct_ingest_states_launched_in_env",
            MagicMock(return_value=[StateCode.US_XX, StateCode.US_YY]),
        )
        self.launched_states_patcher.start()

        self.ingest_regions_patcher = patch.dict(
            DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
            values={StateCode.US_XX: "us-east1", StateCode.US_YY: "us-east4"},
        )
        self.ingest_regions_patcher.start()

        self.cloud_sql_operator_patcher = patch(
            "recidiviz.airflow.dags.calculation.dataflow."
            "single_ingest_pipeline_group.CloudSqlQueryOperator",
            side_effect=fake_operator_with_return_value({}),
        )
        self.cloud_sql_operator_patcher.start()

        self.kubernetes_pod_operator_patcher = patch(
            "recidiviz.airflow.dags.calculation.dataflow."
            "single_ingest_pipeline_group.build_kubernetes_pod_task",
            side_effect=fake_operator_constructor,
        )
        self.kubernetes_pod_operator_patcher.start()

        self.dataflow_operator_patcher = patch(
            "recidiviz.airflow.dags.utils.dataflow_pipeline_group."
            "RecidivizDataflowFlexTemplateOperator",
            side_effect=fake_operator_constructor,
        )
        self.dataflow_operator_patcher.start()

    def tearDown(self) -> None:
        self.launched_states_patcher.stop()
        self.ingest_regions_patcher.stop()
        self.cloud_sql_operator_patcher.stop()
        self.kubernetes_pod_operator_patcher.stop()
        self.dataflow_operator_patcher.stop()
        super().tearDown()

    def _build_dag(self) -> DAG:
        # Import inside the test so module-level execution happens after setUp
        # patches are active.
        # pylint: disable=import-outside-toplevel
        from recidiviz.airflow.dags.identity_ingest_dag import (
            create_identity_ingest_dag,
        )

        return create_identity_ingest_dag()

    def test_dag_id(self) -> None:
        self.assertEqual(_DAG_ID, self._build_dag().dag_id)

    def test_per_tenant_branches_created(self) -> None:
        """Each launched tenant gets a branch under the top-level
        identity_ingest_pipelines group."""
        task_groups = self._build_dag().task_group.get_task_group_dict()
        for tenant in _TENANTS:
            self.assertIn(
                f"identity_ingest_pipelines.{tenant}",
                task_groups,
                f"No branch found for [{tenant}].",
            )

    def test_each_branch_has_max_update_datetimes_task(self) -> None:
        """Each per-tenant branch reads raw data upper bound dates from operations
        DB before launching the Dataflow pipeline."""
        dag = self._build_dag()
        for tenant in _TENANTS:
            task_id = (
                f"identity_ingest_pipelines.{tenant}.ingest."
                f"initialize_ingest_pipeline.get_max_update_datetimes"
            )
            self.assertIn(task_id, dag.task_ids)

    def test_each_branch_has_watermark_freshness_check(self) -> None:
        """Each per-tenant branch reads the previous run's watermarks and verifies
        current raw data is at least as new — guarding against data freshness
        regressions across runs."""
        dag = self._build_dag()
        for tenant in _TENANTS:
            branch_prefix = (
                f"identity_ingest_pipelines.{tenant}.ingest.initialize_ingest_pipeline"
            )
            self.assertIn(f"{branch_prefix}.get_watermarks", dag.task_ids)
            self.assertIn(f"{branch_prefix}.check_for_valid_watermarks", dag.task_ids)

    def test_each_branch_gates_pipeline_on_flashing_check(self) -> None:
        """Each per-tenant branch verifies raw data flashing is not in progress
        before launching the Dataflow pipeline, since identity ingest reads the
        same raw data tables and would produce wrong output mid-flash."""
        dag = self._build_dag()
        for tenant in _TENANTS:
            flashing_task_id = (
                f"identity_ingest_pipelines.{tenant}.ingest."
                f"initialize_ingest_pipeline.verify_raw_data_flashing_not_in_progress"
            )
            self.assertIn(flashing_task_id, dag.task_ids)

    def test_each_branch_writes_back_job_and_watermarks(self) -> None:
        """Each per-tenant branch writes back to direct_ingest_dataflow_job and
        direct_ingest_dataflow_raw_table_upper_bounds after the pipeline runs,
        so the next run's freshness check has a baseline to compare against."""
        dag = self._build_dag()
        for tenant in _TENANTS:
            branch_prefix = f"identity_ingest_pipelines.{tenant}.ingest"
            self.assertIn(f"{branch_prefix}.write_ingest_job_completion", dag.task_ids)
            self.assertIn(f"{branch_prefix}.write_upper_bounds", dag.task_ids)

    def test_tenant_filter_selects_only_that_tenant(self) -> None:
        """Passing tenant_filter in the DAG run conf restricts execution to that
        tenant's branch; absent the filter, all branches run."""
        filtered_dag_run = MagicMock()
        filtered_dag_run.conf = {TENANT_FILTER: StateCode.US_XX.value}
        self.assertEqual(
            [StateCode.US_XX.value],
            select_tenant_parameter_branch(filtered_dag_run),
        )

        unfiltered_dag_run = MagicMock()
        unfiltered_dag_run.conf = {}
        self.assertIsNone(select_tenant_parameter_branch(unfiltered_dag_run))

    def test_each_branch_launches_dataflow_pipeline(self) -> None:
        """Each per-tenant branch contains the Dataflow operator that runs the
        identity ingest pipeline."""
        dag = self._build_dag()
        for tenant in _TENANTS:
            branch_prefix = f"identity_ingest_pipelines.{tenant}.ingest"
            run_pipeline_task_ids = [
                task_id
                for task_id in dag.task_ids
                if task_id.startswith(branch_prefix)
                and task_id.endswith(f".{DATAFLOW_OPERATOR_TASK_ID}")
            ]
            self.assertEqual(
                len(run_pipeline_task_ids),
                1,
                f"Expected exactly one Dataflow run_pipeline task under "
                f"[{branch_prefix}], found {run_pipeline_task_ids}.",
            )
