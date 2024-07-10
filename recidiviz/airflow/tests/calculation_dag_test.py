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
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.pipelines.ingest.pipeline_utils import (
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
)
from recidiviz.tests import pipelines as recidiviz_pipelines_tests_module
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
_UPDATE_STATE_DATASET_TASK_ID = "update_state"
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

    def test_update_normalized_state_upstream_of_view_update(self) -> None:
        """Tests that the `normalized_state` dataset update happens before views
        are updated (and therefore before metric export, where relevant)."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        normalized_state_downstream_dag = dag.partial_subset(
            task_ids_or_regex=["update_normalized_state"],
            include_downstream=True,
            include_upstream=False,
        )

        self.assertNotEqual(0, len(normalized_state_downstream_dag.task_ids))

        self.assertIn(
            _UPDATE_ALL_MANAGED_VIEWS_TASK_ID,
            normalized_state_downstream_dag.task_ids,
        )

    def test_update_normalized_state_downstream_of_normalization_pipelines(
        self,
    ) -> None:
        """Tests that the `normalized_state` dataset update happens after all
        normalization pipelines are run.
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        normalization_group: TaskGroup = dag.task_group_dict["normalization"]
        update_normalized_state_group = dag.get_task("update_normalized_state")

        self.assertIn(
            update_normalized_state_group.task_id,
            normalization_group.downstream_task_ids,
        )

    def test_update_normalized_state_upstream_of_non_normalization_pipelines(
        self,
    ) -> None:
        """Tests that the `normalized_state` dataset update happens after all
        normalization pipelines are run.
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        metric_pipeline_task_ids: Set[str] = {
            task.task_id
            for task in dag.tasks
            if isinstance(task, RecidivizDataflowFlexTemplateOperator)
            and "normalization" not in task.task_id
            and "ingest" not in task.task_id
        }

        normalized_state_upstream_dag = dag.partial_subset(
            task_ids_or_regex=["update_normalized_state"],
            include_downstream=True,
            include_upstream=False,
        )
        self.assertNotEqual(0, len(normalized_state_upstream_dag.task_ids))

        upstream_tasks = set()
        for task in normalized_state_upstream_dag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        pipeline_tasks_not_upstream = metric_pipeline_task_ids - upstream_tasks
        self.assertEqual(set(), pipeline_tasks_not_upstream)

    def test_update_normalized_state_upstream_of_non_normalization_flex_pipelines(
        self,
    ) -> None:
        """Tests that the `normalized_state` dataset update happens after all
        non ingest/normalization flex pipelines are run.
        """
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        metric_pipeline_task_ids: Set[str] = {
            task.task_id
            for task in dag.tasks
            if isinstance(task, RecidivizDataflowFlexTemplateOperator)
            and "normalization" not in task.task_id
            and "ingest" not in task.task_id
        }

        normalized_state_upstream_dag = dag.partial_subset(
            task_ids_or_regex=["update_normalized_state"],
            include_downstream=True,
            include_upstream=False,
        )
        self.assertNotEqual(0, len(normalized_state_upstream_dag.task_ids))

        upstream_tasks = set()
        for task in normalized_state_upstream_dag.tasks:
            upstream_tasks.update(task.upstream_task_ids)

        pipeline_tasks_not_upstream = metric_pipeline_task_ids - upstream_tasks
        self.assertEqual(set(), pipeline_tasks_not_upstream)

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
        self.assertEqual(task.trigger_rule, TriggerRule.ALL_DONE)

        self.assertEqual(
            task.arguments[4:],
            self.entrypoint_args_fixture["test_update_all_managed_views_endpoint"],
        )

    def test_update_managed_views_endpoint_sandbox_prefix(self) -> None:
        from recidiviz.airflow.dags.calculation_dag import update_managed_views_operator

        task = update_managed_views_operator()
        task.render_template_fields({"dag_run": SECONDARY_DAG_RUN})
        self.assertEqual(task.task_id, "update_managed_views_all")
        self.assertEqual(task.trigger_rule, TriggerRule.ALL_DONE)

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

    def test_update_state_dataset_task_exists(self) -> None:
        """Tests that refresh_bq_dataset_task triggers the proper script."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        bq_refresh_task = dag.get_task(_UPDATE_STATE_DATASET_TASK_ID)

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

        task = refresh_bq_dataset_operator(SchemaType.STATE)
        task.render_template_fields({"dag_run": PRIMARY_DAG_RUN})

        self.assertEqual(task.task_id, "refresh_bq_dataset_STATE")
        self.assertEqual(
            task.arguments[4:],
            self.entrypoint_args_fixture["test_refresh_bq_dataset_task"],
        )

    def test_refresh_bq_dataset_task_secondary(self) -> None:
        """Tests that refresh_bq_dataset_task triggers the proper script."""
        from recidiviz.airflow.dags.calculation_dag import refresh_bq_dataset_operator

        task = refresh_bq_dataset_operator(SchemaType.STATE)

        self.assertEqual(task.task_id, "refresh_bq_dataset_STATE")

        task.render_template_fields({"dag_run": SECONDARY_DAG_RUN})

        self.assertEqual(
            task.arguments[4:],
            self.entrypoint_args_fixture["test_refresh_bq_dataset_task_secondary"],
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

    def test_validations_task_secondary(self) -> None:
        """Tests that validation triggers the proper endpoint."""
        from recidiviz.airflow.dags.calculation_dag import execute_validations_operator

        task = execute_validations_operator(state_code="US_ND")

        task.render_template_fields({"dag_run": SECONDARY_DAG_RUN})

        self.assertEqual(task.task_id, "execute_validations_US_ND")
        self.assertEqual(
            task.arguments[4:],
            self.entrypoint_args_fixture["test_validations_task_secondary"],
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
                "--ingest_instance=PRIMARY",
            ],
        )

    def test_update_state_dataset(self) -> None:
        """Tests that the `update_state` task is downstream of initialize_dag."""
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        task = dag.get_task(_UPDATE_STATE_DATASET_TASK_ID)
        task.render_template_fields({"dag_run": PRIMARY_DAG_RUN})

        self.assertEqual(
            task.arguments[4:],
            [
                "--entrypoint=UpdateStateEntrypoint",
            ],
        )

    def test_execute_update_normalized_state_arguments(self) -> None:
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        task = dag.get_task("update_normalized_state")
        task.render_template_fields(
            {
                "dag_run": DagRun(
                    conf={**SECONDARY_DAG_RUN.conf, "state_code_filter": "us_ca"}
                )
            }
        )

        self.assertEqual(
            task.arguments[4:],
            [
                "--entrypoint=UpdateNormalizedStateEntrypoint",
                "--ingest_instance=SECONDARY",
                "--sandbox_prefix=test_prefix",
                # Assert uppercased
                "--state_code_filter=US_CA",
            ],
        )

    def test_execute_entrypoint_arguments_nonetypes(self) -> None:
        dag_bag = DagBag(dag_folder=DAG_FOLDER, include_examples=False)
        dag = dag_bag.dags[self.CALCULATION_DAG_ID]
        self.assertNotEqual(0, len(dag.task_ids))

        task = dag.get_task("update_normalized_state")
        task.render_template_fields(
            {"dag_run": DagRun(conf={**SECONDARY_DAG_RUN.conf, "sandbox_prefix": None})}
        )

        # Assert sandbox_prefix is not included
        self.assertEqual(
            task.arguments[4:],
            [
                "--entrypoint=UpdateNormalizedStateEntrypoint",
                "--ingest_instance=SECONDARY",
            ],
        )

        task = dag.get_task("update_normalized_state")
        task.render_template_fields(
            {"dag_run": DagRun(conf={**SECONDARY_DAG_RUN.conf, "sandbox_prefix": ""})}
        )

        # Assert sandbox_prefix is not included when set to empty string
        self.assertEqual(
            task.arguments[4:],
            [
                "--entrypoint=UpdateNormalizedStateEntrypoint",
                "--ingest_instance=SECONDARY",
            ],
        )


def _fake_pod_operator(*args: Any, **kwargs: Any) -> BaseOperator:
    if "--entrypoint=IngestPipelineShouldRunInDagEntrypoint" in kwargs["arguments"]:
        return fake_operator_with_return_value(True)(*args, **kwargs)

    return fake_operator_constructor(*args, **kwargs)


def _fake_pod_operator_ingest_pipeline_should_run_in_dag_false(
    *args: Any, **kwargs: Any
) -> BaseOperator:
    if "--entrypoint=IngestPipelineShouldRunInDagEntrypoint" in kwargs["arguments"]:
        return fake_operator_with_return_value(False)(*args, **kwargs)

    return fake_operator_constructor(*args, **kwargs)


@patch.dict(
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
    values={StateCode.US_XX: "us-east1", StateCode.US_YY: "us-east2"},
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
            "recidiviz.pipelines.dataflow_orchestration_utils.get_direct_ingest_states_launched_in_env",
            MagicMock(return_value=[StateCode.US_XX, StateCode.US_YY]),
        )
        self.ingest_states_patcher.start()
        self.ingest_regions_patcher = patch.dict(
            DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
            values={StateCode.US_XX: "us-east1", StateCode.US_YY: "us-east2"},
        )
        self.ingest_regions_patcher.start()

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
            side_effect=_fake_pod_operator,
        )
        self.kubernetes_pod_operator_patcher.start()

        self.kubernetes_pod_operator_patcher_2 = patch(
            "recidiviz.airflow.dags.calculation.dataflow.single_ingest_pipeline_group.build_kubernetes_pod_task",
            side_effect=_fake_pod_operator,
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
                expected_failure_ids=[
                    # This fails because no sandbox prefix arg is set when there is a
                    # state_code_filter.
                    r"verify_parameters",
                ],
                expected_skipped_ids=[
                    r"wait_to_continue_or_cancel",
                    r"handle_queueing_result",
                    r"update_big_query_table_schemata",
                    r"ingest",
                    r"update_state",
                    r"bq_refresh",
                    r"update_managed_views",
                    r"normalization",
                    r"update_normalized_state",
                    r"post_normalization_pipelines",
                    r"validations",
                    r"metric_exports",
                ],
            )
            self.assertIn(
                "normalization.US_XX_start",
                dag.task_ids,
            )

    def test_calculation_dag_fail_single_ingest_pipeline(self) -> None:
        from recidiviz.airflow.dags.calculation_dag import create_calculation_dag

        def fail_us_yy_ingest_operator_constructor(
            *args: Any, **kwargs: Any
        ) -> BaseOperator:
            task_id = kwargs["body"].operator.task_id
            if "us_yy" in task_id:
                return fake_failing_operator_constructor(*args, **kwargs)
            return fake_operator_constructor(*args, **kwargs)

        self.mock_dataflow_operator_constructor.side_effect = (
            fail_us_yy_ingest_operator_constructor
        )

        dag = create_calculation_dag()

        with Session(bind=self.engine) as session:
            self.run_dag_test(
                dag,
                session=session,
                run_conf={
                    "ingest_instance": "PRIMARY",
                },
                expected_failure_ids=[
                    r".*us_yy_dataflow.us-yy-ingest.run_pipeline",
                    r".*us_yy_dataflow.write_upper_bounds",
                    r".*us_yy_dataflow.write_ingest_job_completion",
                    r".*ingest.branch_end",
                ],
                # No downstream processes are skipped!
                expected_skipped_ids=[],
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
                expected_skipped_ids=[
                    r"US[_-]YY",
                ],
            )
            self.assertIn(
                "normalization.US_XX_start",
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
                expected_skipped_ids=[
                    r"US[_-]YY",
                ],
            )
            self.assertIn(
                "normalization.US_XX_start",
                dag.task_ids,
            )
