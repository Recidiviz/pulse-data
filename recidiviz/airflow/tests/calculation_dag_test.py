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
Unit test to test the calculation pipeline DAG logic.
"""
import os
from typing import Any, Dict, List, Set
from unittest.mock import MagicMock, patch

import yaml
from airflow.models import DagRun, import_all_models
from airflow.models.baseoperator import BaseOperator
from airflow.models.dagbag import DagBag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.operators.recidiviz_dataflow_operator import (
    RecidivizDataflowFlexTemplateOperator,
)
from recidiviz.airflow.tests.test_utils import DAG_FOLDER, AirflowIntegrationTest
from recidiviz.airflow.tests.utils.dag_helper_functions import (
    fake_failing_operator_constructor,
    fake_operator_constructor,
    fake_operator_with_return_value,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.metrics.export.products.product_configs import ProductConfigs
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.pipelines.ingest.pipeline_utils import (
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
)
from recidiviz.tests import pipelines as recidiviz_pipelines_tests_module
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
from recidiviz.tests.metrics.export import fixtures as metric_export_fixtures
from recidiviz.utils.environment import GCPEnvironment
from recidiviz.utils.yaml_dict import YAMLDict

# Need to import calculation_dag inside test suite so environment variables are set before importing,
# otherwise calculation_dag will raise an Error and not import.
# pylint: disable=C0415 import-outside-toplevel

_PROJECT_ID = "recidiviz-testing"

PIPELINES_TESTS_WORKING_DIRECTORY = os.path.dirname(
    recidiviz_pipelines_tests_module.__file__
)
FAKE_PIPELINE_CONFIG_YAML_PATH = os.path.join(
    PIPELINES_TESTS_WORKING_DIRECTORY,
    "fake_calculation_pipeline_templates.yaml",
)

_UPDATE_ALL_MANAGED_VIEWS_TASK_ID = "update_managed_views_all"
_VALIDATIONS_BRANCH_START = "validations.branch_start"
_REFRESH_OPERATIONS_BQ_DATASET_TASK_ID = "bq_refresh.refresh_bq_dataset_OPERATIONS"
_EXPORT_METRIC_VIEW_DATA_TASK_ID = "metric_exports.INGEST_METADATA_metric_exports.export_ingest_metadata_metric_view_data"


def get_post_refresh_release_lock_task_id(schema_type: str) -> str:
    return f"bq_refresh.{schema_type.lower()}_bq_refresh.release_lock_{schema_type.upper()}"


import_all_models()


PRIMARY_DAG_RUN = DagRun(
    conf={
        "ingest_instance": "PRIMARY",
        "sandbox_prefix": None,
        "state_code_filter": None,
    }
)

SECONDARY_DAG_RUN = DagRun(
    conf={
        "sandbox_prefix": "test_prefix",
        "ingest_instance": "SECONDARY",
        "state_code_filter": None,
    }
)


class TestCalculationPipelineDag(AirflowIntegrationTest):
    """Tests the calculation pipeline DAGs."""

    CALCULATION_DAG_ID = f"{_PROJECT_ID}_calculation_dag"
    entrypoint_args_fixture: Dict[str, List[str]] = {}

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        with open(
            os.path.join(os.path.dirname(__file__), "fixtures/entrypoints_args.yaml"),
            "r",
            encoding="utf-8",
        ) as fixture_file:
            cls.entrypoint_args_fixture = yaml.safe_load(fixture_file)

    def setUp(self) -> None:
        super().setUp()
        self.environment_patcher = patch(
            "os.environ",
            {
                "GCP_PROJECT": _PROJECT_ID,
            },
        )
        self.environment_patcher.start()
        self.project_environment_patcher = patch(
            "recidiviz.utils.environment.get_environment_for_project",
            return_value=GCPEnvironment.STAGING,
        )
        self.project_environment_patcher.start()

    def tearDown(self) -> None:
        super().tearDown()
        self.environment_patcher.stop()
        self.project_environment_patcher.stop()

    def test_branch_sorting(self) -> None:
        """Tests that state-specific task groups are sorted in alphabetical order in
        topological_sort which determines ui visual sorting.
        """
        dag = DagBag(dag_folder=DAG_FOLDER, include_examples=False).dags[
            self.CALCULATION_DAG_ID
        ]
        for task_group_container in [
            "dataflow_pipelines",
            "metric_exports.state_specific_metric_exports",
        ]:
            branching_topological_sorted_groups = [
                maybe_group.group_id
                for maybe_group in dag.task_group.get_task_group_dict()[
                    task_group_container
                ].topological_sort()
                if isinstance(maybe_group, TaskGroup)
            ]

            assert branching_topological_sorted_groups == list(
                sorted(branching_topological_sorted_groups)
            )

        # validations
        validations_topological_sorted_groups = [
            task.task_id
            for task in dag.task_group.get_task_group_dict()[
                "validations"
            ].topological_sort()
            if "branch_" not in task.task_id
        ]

        assert validations_topological_sorted_groups == list(
            sorted(validations_topological_sorted_groups)
        )

    def update_reference_views_downstream_initialize_dag(self) -> None:
        """Tests that the `update_managed_views_reference_views_only` task is downstream of
        initialize_dag.
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        update_reference_views_group: TaskGroup = dag.task_group_dict[
            "update_managed_views_reference_views_only"
        ]
        initialize_dag = dag.task_group_dict["initialize_dag"]

        self.assertIn(
            initialize_dag.group_id,
            update_reference_views_group.upstream_group_ids,
        )

    def test_bq_refresh_downstream_update_bq_schemata(self) -> None:
        """Tests that the `bq_refresh` task is downstream of updating bigquery tables."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        bq_refresh_group: TaskGroup = dag.task_group_dict["bq_refresh"]

        self.assertIn(
            "update_big_query_table_schemata",
            bq_refresh_group.upstream_task_ids,
        )

    def test_update_all_views_branch_upstream_of_all_exports(
        self,
    ) -> None:
        """Tests that view update happens before any of the metric exports."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        metric_exports_group: TaskGroup = dag.task_group_dict["metric_exports"]
        view_materialization: BaseOperator = dag.get_task(
            _UPDATE_ALL_MANAGED_VIEWS_TASK_ID
        )
        self.assertIn(
            view_materialization.task_id, metric_exports_group.upstream_task_ids
        )

    def test_update_all_views_upstream_of_validation(
        self,
    ) -> None:
        """Tests that update_all_views happens before the validations run."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        validations_start: BaseOperator = dag.get_task(_VALIDATIONS_BRANCH_START)
        view_materialization: BaseOperator = dag.get_task(
            _UPDATE_ALL_MANAGED_VIEWS_TASK_ID
        )

        self.assertIn(
            validations_start.task_id, view_materialization.downstream_task_ids
        )
        self.assertNotIn(
            validations_start.task_id, view_materialization.upstream_task_ids
        )

    def test_view_update_downstream_of_all_pipelines(
        self,
    ) -> None:
        """Tests that view update happens after all pipelines have run."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        pipeline_task_ids: Set[str] = {
            task.task_id
            for task in dag.tasks
            if isinstance(task, RecidivizDataflowFlexTemplateOperator)
        }

        self.assertNotEqual(0, len(pipeline_task_ids))

        trigger_task_subdag = dag.partial_subset(
            task_ids_or_regex=_UPDATE_ALL_MANAGED_VIEWS_TASK_ID,
            include_downstream=False,
            include_upstream=True,
        )

        upstream_tasks = set()
        for task in trigger_task_subdag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        pipeline_tasks_not_upstream = pipeline_task_ids - upstream_tasks
        self.assertEqual(set(), pipeline_tasks_not_upstream)

    def test_update_managed_views_endpoint_exists(self) -> None:
        """Tests that update_all_managed_views triggers the proper endpoint."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        trigger_update_task = dag.get_task(_UPDATE_ALL_MANAGED_VIEWS_TASK_ID)

        if not isinstance(trigger_update_task, KubernetesPodOperator):
            raise ValueError(
                f"Expected type KubernetesPodOperator, found "
                f"[{type(trigger_update_task)}]."
            )

    def test_update_managed_views_endpoint(self) -> None:
        from recidiviz.airflow.dags.calculation_dag import update_managed_views_operator

        task = update_managed_views_operator()
        task.render_template_fields({"dag_run": PRIMARY_DAG_RUN})

        self.assertEqual(task.task_id, "update_managed_views_all")
        self.assertEqual(task.trigger_rule, TriggerRule.ALL_SUCCESS)

        self.assertEqual(
            task.arguments[4:],
            self.entrypoint_args_fixture["test_update_all_managed_views_endpoint"],
        )

    def test_update_managed_views_endpoint_sandbox_prefix(self) -> None:
        from recidiviz.airflow.dags.calculation_dag import update_managed_views_operator

        task = update_managed_views_operator()
        task.render_template_fields({"dag_run": SECONDARY_DAG_RUN})
        self.assertEqual(task.task_id, "update_managed_views_all")
        self.assertEqual(task.trigger_rule, TriggerRule.ALL_SUCCESS)

        self.assertEqual(
            task.arguments[4:],
            self.entrypoint_args_fixture[
                "test_update_all_managed_views_endpoint_sandbox_prefix"
            ],
        )

    def test_refresh_bq_dataset_task_exists(self) -> None:
        """Tests that refresh_bq_dataset_task triggers the proper script."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        bq_refresh_task = dag.get_task(_REFRESH_OPERATIONS_BQ_DATASET_TASK_ID)

        if not isinstance(bq_refresh_task, KubernetesPodOperator):
            raise ValueError(
                f"Expected type KubernetesPodOperator, found "
                f"[{type(bq_refresh_task)}]."
            )

    def test_refresh_bq_dataset_task(
        self,
    ) -> None:
        """Tests that refresh_bq_dataset_task triggers the proper script."""
        from recidiviz.airflow.dags.calculation_dag import refresh_bq_dataset_operator

        task = refresh_bq_dataset_operator(SchemaType.OPERATIONS)
        task.render_template_fields({"dag_run": PRIMARY_DAG_RUN})

        self.assertEqual(task.task_id, "refresh_bq_dataset_OPERATIONS")
        self.assertEqual(
            task.arguments[4:],
            self.entrypoint_args_fixture["test_refresh_bq_dataset_task"],
        )

    def test_validations_task_exists(self) -> None:
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        trigger_validation_task = dag.get_task("validations.execute_validations_US_ND")

        if not isinstance(trigger_validation_task, KubernetesPodOperator):
            raise ValueError(
                f"Expected type KubernetesPodOperator, found "
                f"[{type(trigger_validation_task)}]."
            )

    def test_validations_task(
        self,
    ) -> None:
        """Tests that validation triggers the proper endpoint."""
        from recidiviz.airflow.dags.calculation_dag import execute_validations_operator

        task = execute_validations_operator(state_code="US_ND")
        self.assertEqual(task.task_id, "execute_validations_US_ND")

        task.render_template_fields({"dag_run": PRIMARY_DAG_RUN})

        self.assertEqual(
            task.arguments[4:],
            self.entrypoint_args_fixture["test_validations_task"],
        )

    def test_trigger_metric_view_data_operator_exists(self) -> None:
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]

        trigger_metric_view_data_task = dag.get_task(_EXPORT_METRIC_VIEW_DATA_TASK_ID)
        if not isinstance(trigger_metric_view_data_task, KubernetesPodOperator):
            raise ValueError(
                f"Expected type KubernetesPodOperator, found "
                f"[{type(trigger_metric_view_data_task)}]."
            )

    def test_trigger_metric_view_data_operator(self) -> None:
        """Tests the trigger_metric_view_data_operator triggers the proper script."""
        from recidiviz.airflow.dags.calculation_dag import (
            trigger_metric_view_data_operator,
        )

        task = trigger_metric_view_data_operator(
            export_job_name="INGEST_METADATA", state_code=None
        )
        task.render_template_fields({"dag_run": PRIMARY_DAG_RUN})

        self.assertEqual(task.task_id, "export_ingest_metadata_metric_view_data")

        self.assertEqual(
            task.arguments[4:],
            self.entrypoint_args_fixture["test_trigger_metric_view_data_operator"],
        )

    def test_trigger_metric_view_data_operator_sandbox_prefix(self) -> None:
        """Tests the trigger_metric_view_data_operator triggers the proper script."""
        from recidiviz.airflow.dags.calculation_dag import (
            trigger_metric_view_data_operator,
        )

        task = trigger_metric_view_data_operator(
            export_job_name="INGEST_METADATA", state_code=None
        )
        task.render_template_fields({"dag_run": SECONDARY_DAG_RUN})

        self.assertEqual(task.task_id, "export_ingest_metadata_metric_view_data")

        self.assertEqual(
            task.arguments[4:],
            self.entrypoint_args_fixture[
                "test_trigger_metric_view_data_operator_sandbox_prefix"
            ],
        )

    def test_trigger_metric_view_data_operator_state_code(self) -> None:
        """Tests the trigger_metric_view_data_operator triggers the proper script."""
        from recidiviz.airflow.dags.calculation_dag import (
            trigger_metric_view_data_operator,
        )

        task = trigger_metric_view_data_operator(
            export_job_name="INGEST_METADATA", state_code="US_XX"
        )
        task.render_template_fields({"dag_run": PRIMARY_DAG_RUN})
        self.assertEqual(
            task.task_id,
            "export_ingest_metadata_us_xx_metric_view_data",
        )
        self.assertEqual(
            task.arguments[4:],
            self.entrypoint_args_fixture[
                "test_trigger_metric_view_data_operator_state_code"
            ],
        )

    def test_bq_refresh_arguments(self) -> None:
        """Tests that the `bq_refresh` task is downstream of initialize_dag."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        task = dag.get_task(_REFRESH_OPERATIONS_BQ_DATASET_TASK_ID)
        task.render_template_fields({"dag_run": PRIMARY_DAG_RUN})

        self.assertEqual(
            task.arguments[4:],
            [
                "--entrypoint=BigQueryRefreshEntrypoint",
                "--schema_type=OPERATIONS",
            ],
        )

    def test_execute_entrypoint_arguments_nonetypes(self) -> None:
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        task = dag.get_task("update_managed_views_all")
        task.render_template_fields(
            {"dag_run": DagRun(conf={**SECONDARY_DAG_RUN.conf, "sandbox_prefix": None})}
        )

        # Assert sandbox_prefix is not included
        self.assertEqual(
            task.arguments[4:],
            [
                "--entrypoint=UpdateAllManagedViewsEntrypoint",
            ],
        )

        task = dag.get_task("update_managed_views_all")
        task.render_template_fields(
            {"dag_run": DagRun(conf={**SECONDARY_DAG_RUN.conf, "sandbox_prefix": ""})}
        )

        # Assert sandbox_prefix is not included when set to empty string
        self.assertEqual(
            task.arguments[4:],
            [
                "--entrypoint=UpdateAllManagedViewsEntrypoint",
            ],
        )


@patch.dict(
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
    values={StateCode.US_XX: "us-east1", StateCode.US_YY: "us-east4"},
)
class TestCalculationDagIntegration(AirflowIntegrationTest):
    """
    Integration tests for the calculation DAG.
    """

    CALCULATION_DAG_ID = f"{_PROJECT_ID}_calculation_dag"

    def setUp(self) -> None:
        super().setUp()

        self.environment_patcher = patch(
            "os.environ",
            {
                "GCP_PROJECT": _PROJECT_ID,
            },
        )
        self.environment_patcher.start()

        self.ingest_states_patcher = patch(
            "recidiviz.airflow.dags.calculation_dag.get_direct_ingest_states_launched_in_env",
            MagicMock(return_value=[StateCode.US_XX, StateCode.US_YY]),
        )
        self.ingest_states_patcher.start()
        self.ingest_regions_patcher = patch.dict(
            DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
            values={StateCode.US_XX: "us-east1", StateCode.US_YY: "us-east4"},
        )
        self.ingest_regions_patcher.start()

        # Load mock export configs so that US_XX has metric exports configured
        self.product_configs_patcher = patch.object(
            ProductConfigs,
            "from_file",
            return_value=ProductConfigs.from_file(
                os.path.join(
                    os.path.dirname(metric_export_fixtures.__file__),
                    "fixture_products.yaml",
                )
            ),
        )

        self.product_configs_patcher.start()
        self.project_patcher = patch(
            "recidiviz.airflow.dags.calculation_dag.get_project_id",
            return_value=_PROJECT_ID,
        )
        self.project_patcher.start()

        self.pipeline_config_yaml_path_patcher = patch(
            "recidiviz.airflow.dags.calculation_dag._get_pipeline_config",
            return_value=YAMLDict.from_path(FAKE_PIPELINE_CONFIG_YAML_PATH),
        )
        self.pipeline_config_yaml_path_patcher.start()

        self.project_environment_patcher = patch(
            "recidiviz.utils.environment.get_environment_for_project",
            return_value=GCPEnvironment.STAGING,
        )
        self.project_environment_patcher.start()

        self.kubernetes_pod_operator_patcher = patch(
            "recidiviz.airflow.dags.calculation_dag.build_kubernetes_pod_task",
            side_effect=fake_operator_constructor,
        )
        self.mock_kubernetes_pod_operator_constructor = (
            self.kubernetes_pod_operator_patcher.start()
        )

        self.kubernetes_pod_operator_patcher_2 = patch(
            "recidiviz.airflow.dags.calculation.dataflow.single_ingest_pipeline_group.build_kubernetes_pod_task",
            side_effect=fake_operator_constructor,
        )
        self.mock_kubernetes_pod_operator_ = (
            self.kubernetes_pod_operator_patcher_2.start()
        )

        self.cloud_sql_query_operator_patcher = patch(
            "recidiviz.airflow.dags.calculation.dataflow.single_ingest_pipeline_group.CloudSqlQueryOperator",
            side_effect=fake_operator_with_return_value({}),
        )
        self.cloud_sql_query_operator_patcher.start()

        self.recidiviz_dataflow_operator_patcher = patch(
            "recidiviz.airflow.dags.utils.dataflow_pipeline_group.RecidivizDataflowFlexTemplateOperator",
            side_effect=fake_operator_constructor,
        )
        self.mock_dataflow_operator_constructor = (
            self.recidiviz_dataflow_operator_patcher.start()
        )
        self.direct_ingest_regions_patcher = patch(
            "recidiviz.airflow.dags.calculation.dataflow.single_ingest_pipeline_group.direct_ingest_regions",
            autospec=True,
        )
        self.mock_direct_ingest_regions = self.direct_ingest_regions_patcher.start()
        self.mock_direct_ingest_regions.get_direct_ingest_region.side_effect = (
            lambda region_code: get_direct_ingest_region(
                region_code, region_module_override=fake_regions_module
            )
        )
        self.metadata_patcher = patch(
            "recidiviz.utils.metadata.project_id",
            return_value=_PROJECT_ID,
        )
        self.metadata_patcher.start()
        self.manifest_collector_patcher = patch(
            "recidiviz.airflow.dags.calculation.dataflow.single_ingest_pipeline_group.IngestViewManifestCollector",
            autospec=True,
        )
        self.mock_manifest_collector = self.manifest_collector_patcher.start()
        self.mock_manifest_collector.return_value.launchable_ingest_views.return_value = [
            MagicMock()
        ]

        self.found_pipelines_to_fail: list[tuple[StateCode, str]] = []

    def tearDown(self) -> None:
        super().tearDown()
        self.environment_patcher.stop()
        self.ingest_states_patcher.stop()
        self.ingest_regions_patcher.stop()
        self.project_patcher.stop()
        self.pipeline_config_yaml_path_patcher.stop()
        self.project_environment_patcher.stop()
        self.kubernetes_pod_operator_patcher.stop()
        self.kubernetes_pod_operator_patcher_2.stop()
        self.cloud_sql_query_operator_patcher.stop()
        self.recidiviz_dataflow_operator_patcher.stop()
        self.product_configs_patcher.stop()
        self.direct_ingest_regions_patcher.stop()
        self.metadata_patcher.stop()
        self.manifest_collector_patcher.stop()

    def _mock_fail_dataflow_pipeline(
        self, state_code: StateCode, pipeline_name: str
    ) -> None:
        """Fails the node associated with the given state/pipeline."""

        def fail_us_yy_ingest_operator_constructor(
            *args: Any, **kwargs: Any
        ) -> BaseOperator:
            pipeline_params = kwargs["body"].operator.op_kwargs["params_no_overrides"]
            if (
                state_code == StateCode(pipeline_params.state_code.upper())
                and pipeline_name == pipeline_params.pipeline
            ):
                self.found_pipelines_to_fail.append((state_code, pipeline_name))
                return fake_failing_operator_constructor(*args, **kwargs)
            return fake_operator_constructor(*args, **kwargs)

        self.mock_dataflow_operator_constructor.side_effect = (
            fail_us_yy_ingest_operator_constructor
        )

    def test_calculation_dag(self) -> None:
        from recidiviz.airflow.dags.calculation_dag import create_calculation_dag

        dag = create_calculation_dag()

        with Session(bind=self.engine) as session:
            self.run_dag_test(
                dag,
                session=session,
                run_conf={
                    "ingest_instance": "PRIMARY",
                },
                expected_success_task_id_regexes=[
                    r"^initialize_dag.*",
                    r"^update_big_query_table_schemata",
                    r"^bq_refresh.*",
                    r"^dataflow_pipelines.*",
                    r"^update_managed_views_all",
                    r"^validations.*",
                    r"^metric_exports.*",
                    r"^dataflow_metric_pruning",
                    r"^dataset_cleanup",
                    r"^apply_row_access_policies",
                ],
            )

    def test_calculation_dag_with_state(self) -> None:
        from recidiviz.airflow.dags.calculation_dag import create_calculation_dag

        dag = create_calculation_dag()

        with Session(bind=self.engine) as session:
            self.run_dag_test(
                dag,
                session=session,
                run_conf={
                    "ingest_instance": "PRIMARY",
                    "state_code_filter": "US_XX",
                },
                expected_success_task_id_regexes=[
                    r"^initialize_dag.handle_params_check",
                ],
                expected_failure_task_id_regexes=[
                    # This fails because no sandbox prefix arg is set when there is a
                    # state_code_filter.
                    r"^initialize_dag.verify_parameters",
                ],
                expected_skipped_task_id_regexes=[
                    r"^initialize_dag.wait_to_continue_or_cancel",
                    r"^initialize_dag.handle_queueing_result",
                    r"^update_big_query_table_schemata",
                    r"^dataflow_pipelines.*",
                    r"^bq_refresh.*",
                    r"^update_managed_views",
                    r"^validations.*",
                    r"^metric_exports.*",
                    r"^dataflow_metric_pruning",
                    r"^dataset_cleanup",
                    r"^apply_row_access_policies",
                ],
            )
            self.assertIn(
                "dataflow_pipelines.US_XX_dataflow_pipelines.ingest.us-xx-ingest-pipeline.run_pipeline",
                dag.task_ids,
            )

    def test_calculation_dag_fail_single_ingest_pipeline(self) -> None:
        from recidiviz.airflow.dags.calculation_dag import create_calculation_dag

        self._mock_fail_dataflow_pipeline(
            state_code=StateCode.US_YY,
            pipeline_name="ingest",
        )

        dag = create_calculation_dag()

        with Session(bind=self.engine) as session:
            self.run_dag_test(
                dag,
                session=session,
                run_conf={
                    "ingest_instance": "PRIMARY",
                },
                expected_failure_task_id_regexes=[
                    # Pipeline fails
                    r"^dataflow_pipelines.US_YY_dataflow_pipelines.ingest.us-yy-ingest-pipeline.run_pipeline",
                    # We don't save any of the post-successful ingest pipeline state
                    r"^dataflow_pipelines.US_YY_dataflow_pipelines.ingest.write_upper_bounds",
                    r"^dataflow_pipelines.US_YY_dataflow_pipelines.ingest.write_ingest_job_completion",
                    r"^dataflow_pipelines.branch_end",
                    # Metric and supplemental pipelines for US_YY should not run
                    r"^dataflow_pipelines.US_YY_dataflow_pipelines.pipeline-to-run-pipeline.*",
                    r"^dataflow_pipelines.US_YY_dataflow_pipelines.us-yy-pipeline-with-limit-24-pipeline.*",
                    # Metric exports for US_YY should not run
                    r"^metric_exports\.state_specific_metric_exports\.US_YY_metric_exports",
                    r"^metric_exports.state_specific_metric_exports.branch_end",
                ],
                # No downstream processes are skipped!
                expected_skipped_task_id_regexes=[],
                expected_success_task_id_regexes=[
                    r"^initialize_dag.*",
                    r"^update_big_query_table_schemata",
                    r"^bq_refresh.*",
                    r"^dataflow_pipelines.branch_start",
                    r"^dataflow_pipelines.US_XX_dataflow_pipelines.*",
                    r"^dataflow_pipelines.US_YY_dataflow_pipelines.ingest.initialize_ingest_pipeline.*",
                    r"^dataflow_pipelines.US_YY_dataflow_pipelines.ingest.us-yy-ingest-pipeline.create_flex_template",
                    r"^dataflow_pipelines_completed",
                    r"^update_managed_views_all",
                    r"^validations.*",
                    r"^metric_exports.state_specific_metric_exports.branch_start",
                    # This is a state-agnostic export so it runs
                    r"^metric_exports.MOCK_EXPORT_NAME_metric_exports.export_mock_export_name_metric_view_data",
                    # Metric exports for US_XX (no failures) should run
                    r"^metric_exports\.state_specific_metric_exports\.US_XX_metric_exports",
                    r"^dataflow_metric_pruning",
                    r"^dataset_cleanup",
                    r"^apply_row_access_policies",
                ],
            )

            self.assertEqual(
                [(StateCode.US_YY, "ingest")], self.found_pipelines_to_fail
            )

    def test_calculation_dag_fail_single_metric_pipeline(self) -> None:
        from recidiviz.airflow.dags.calculation_dag import create_calculation_dag

        self._mock_fail_dataflow_pipeline(
            state_code=StateCode.US_XX,
            pipeline_name="pipeline_no_limit",
        )

        dag = create_calculation_dag()

        with Session(bind=self.engine) as session:
            self.run_dag_test(
                dag,
                session=session,
                run_conf={
                    "ingest_instance": "PRIMARY",
                },
                expected_failure_task_id_regexes=[
                    r"^dataflow_pipelines.US_XX_dataflow_pipelines.full-us-xx-pipeline-no-limit-pipeline.run_pipeline",
                    r"^dataflow_pipelines.branch_end",
                    r"^metric_exports\.state_specific_metric_exports\.US_XX_metric_exports\.",
                    r"^metric_exports.state_specific_metric_exports.branch_end",
                ],
                expected_success_task_id_regexes=[
                    r"^initialize_dag.*",
                    r"^update_big_query_table_schemata",
                    r"^bq_refresh.*",
                    r"^dataflow_pipelines.branch_start",
                    r"^dataflow_pipelines.US_XX_dataflow_pipelines.ingest.*",
                    # All dataflow pipelines for other states run
                    r"^dataflow_pipelines.US_YY.*",
                    # Completely different pipeline for US_XX runs
                    r"^dataflow_pipelines.US_XX_dataflow_pipelines.us-xx-pipeline-with-limit-36-pipeline.*",
                    r"^dataflow_pipelines.US_XX_dataflow_pipelines.full-us-xx-pipeline-no-limit-pipeline.create_flex_template",
                    # This step runs with trigger rule ALL_DONE so it runs even though a
                    # pipeline failed.
                    r"^dataflow_pipelines_completed",
                    r"^update_managed_views_all",
                    r"^metric_exports.state_specific_metric_exports.branch_start",
                    # This is a state-agnostic export so it runs
                    r"^metric_exports.MOCK_EXPORT_NAME_metric_exports.export_mock_export_name_metric_view_data",
                    # Metric exports for US_YY (no failures) should run
                    r"^metric_exports\.state_specific_metric_exports\.US_YY_metric_exports\.",
                    r"^validations.*",
                    r"^dataflow_metric_pruning",
                    r"^dataset_cleanup",
                    r"^apply_row_access_policies",
                ],
            )

            self.assertEqual(
                [(StateCode.US_XX, "pipeline_no_limit")],
                self.found_pipelines_to_fail,
            )

    def test_calculation_dag_fails_downstream_of_schema_update(self) -> None:
        """
        Tests that most tasks do not run if 'update_big_query_table_schemata' fails.
        """
        from recidiviz.airflow.dags.calculation_dag import create_calculation_dag

        self.mock_kubernetes_pod_operator_constructor.side_effect = lambda **kwargs: (
            fake_failing_operator_constructor(**kwargs)
            if kwargs["task_id"] == "update_big_query_table_schemata"
            else fake_operator_constructor(**kwargs)
        )

        downstream_of_schema_update_dag = create_calculation_dag().partial_subset(
            task_ids_or_regex=["update_big_query_table_schemata"],
            include_downstream=True,
            include_upstream=False,
        )

        with Session(bind=self.engine) as session:
            self.run_dag_test(
                downstream_of_schema_update_dag,
                session=session,
                run_conf={
                    "ingest_instance": "PRIMARY",
                },
                expected_failure_task_id_regexes=[
                    r"^update_big_query_table_schemata",
                    r"^dataflow_pipelines\.[a-zA-Z]*",
                    r"^bq_refresh.refresh_bq_dataset_",
                    r"^update_managed_views_all",
                    r"^validations.*",
                    r"^metric_exports.*",
                ],
                expected_skipped_task_id_regexes=[],
                # These indicate their respective groups completed,
                # but notice that dataflow_pipelines, etc.
                # did not complete successfully!
                # That is because these tasks simply trigger with ALL_DONE
                expected_success_task_id_regexes=[
                    r"^bq_refresh.bq_refresh_completed",
                    r"^dataflow_pipelines_completed",
                    r"^dataflow_metric_pruning",
                    r"^dataset_cleanup",
                    r"^apply_row_access_policies",
                ],
            )

    def test_bq_refresh_does_not_block_view_update(self) -> None:
        """Tests that OPERATIONS and CASE TRIAGE failure doesn't block the view update
        or other downstream processes.
        """
        from recidiviz.airflow.dags.calculation_dag import create_calculation_dag

        self.mock_kubernetes_pod_operator_constructor.side_effect = lambda **kwargs: (
            fake_failing_operator_constructor(**kwargs)
            if "refresh_bq" in kwargs["task_id"]
            else fake_operator_constructor(**kwargs)
        )

        dag = create_calculation_dag()
        upstream_of_view_update = dag.partial_subset(
            task_ids_or_regex=["update_managed_views_all"],
            include_downstream=False,
            include_upstream=True,
        )

        with Session(bind=self.engine) as session:
            self.run_dag_test(
                upstream_of_view_update,
                session=session,
                run_conf={
                    "ingest_instance": "PRIMARY",
                },
                expected_failure_task_id_regexes=[
                    r"bq_refresh.refresh_bq_dataset_",
                ],
                expected_success_task_id_regexes=[
                    r"^initialize_dag.*",
                    r"^update_big_query_table_schemata",
                    r"bq_refresh.bq_refresh_completed",
                    r"^dataflow_pipelines.*",
                    r"^update_managed_views_all",
                    r"^dataflow_metric_pruning",
                ],
            )

    def test_calculation_dag_with_state_and_sandbox(self) -> None:
        from recidiviz.airflow.dags.calculation_dag import create_calculation_dag

        dag = create_calculation_dag()

        with Session(bind=self.engine) as session:
            self.run_dag_test(
                dag,
                session=session,
                run_conf={
                    "ingest_instance": "PRIMARY",
                    "state_code_filter": "US_XX",
                    "sandbox_prefix": "test_prefix",
                },
                expected_success_task_id_regexes=[
                    r"^initialize_dag.handle_params_check",
                ],
                expected_failure_task_id_regexes=[
                    # This fails because the state_code_filter/sandbox_prefix args
                    # aren't supported
                    r"^initialize_dag.verify_parameters",
                ],
                expected_skipped_task_id_regexes=[
                    r"^initialize_dag.wait_to_continue_or_cancel",
                    r"^initialize_dag.handle_queueing_result",
                    r"^update_big_query_table_schemata",
                    r"^dataflow_pipelines.*",
                    r"^bq_refresh.*",
                    r"^update_managed_views",
                    r"^validations.*",
                    r"^metric_exports.*",
                    r"^dataflow_metric_pruning",
                    r"^dataset_cleanup",
                    r"^apply_row_access_policies",
                ],
            )
            self.assertIn(
                "dataflow_pipelines.US_XX_dataflow_pipelines.ingest.us-xx-ingest-pipeline.run_pipeline",
                dag.task_ids,
            )

    def test_calculation_dag_secondary(self) -> None:
        from recidiviz.airflow.dags.calculation_dag import create_calculation_dag

        dag = create_calculation_dag()

        with Session(bind=self.engine) as session:
            self.run_dag_test(
                dag,
                session=session,
                run_conf={
                    "ingest_instance": "SECONDARY",
                    "state_code_filter": "US_XX",
                    "sandbox_prefix": "test_prefix",
                },
                expected_success_task_id_regexes=[
                    r"^initialize_dag.handle_params_check",
                ],
                expected_failure_task_id_regexes=[
                    # This fails because the ingest_instance isn't supported
                    r"^initialize_dag.verify_parameters",
                ],
                expected_skipped_task_id_regexes=[
                    r"^initialize_dag.wait_to_continue_or_cancel",
                    r"^initialize_dag.handle_queueing_result",
                    r"^update_big_query_table_schemata",
                    r"^dataflow_pipelines.*",
                    r"^bq_refresh.*",
                    r"^update_managed_views",
                    r"^validations.*",
                    r"^metric_exports.*",
                    r"^dataflow_metric_pruning",
                    r"^dataset_cleanup",
                    r"^apply_row_access_policies",
                ],
            )
            self.assertIn(
                "dataflow_pipelines.US_XX_dataflow_pipelines.ingest.us-xx-ingest-pipeline.run_pipeline",
                dag.task_ids,
            )
