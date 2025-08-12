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
"""Tests for the state dataflow pipeline."""

import os
import unittest
from datetime import datetime
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytz
import yaml
from airflow.models import BaseOperator
from airflow.models.dag import DAG, dag
from airflow.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState
from sqlalchemy.orm import Session

from recidiviz.airflow.dags.calculation.dataflow.single_ingest_pipeline_group import (
    _check_for_valid_watermarks_task,
    check_for_valid_watermarks,
    create_single_ingest_pipeline_group,
)
from recidiviz.airflow.dags.operators.recidiviz_dataflow_operator import (
    RecidivizDataflowFlexTemplateOperator,
)
from recidiviz.airflow.tests import fixtures
from recidiviz.airflow.tests.test_utils import AirflowIntegrationTest
from recidiviz.airflow.tests.utils.dag_helper_functions import (
    FakeFailureOperator,
    fake_failure_task,
    fake_operator_constructor,
    fake_operator_with_return_value,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.pipelines.ingest.pipeline_utils import (
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
)
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
from recidiviz.utils.environment import GCPEnvironment
from recidiviz.utils.types import assert_type

# Need a disable pointless statement because Python views the chaining operator ('>>') as a "pointless" statement
# pylint: disable=W0104 pointless-statement

# Need a disable expression-not-assigned because the chaining ('>>') doesn't need expressions to be assigned
# pylint: disable=W0106 expression-not-assigned

_PROJECT_ID = "recidiviz-testing"
_TEST_DAG_ID = "test_single_ingest_pipeline_group"
_DOWNSTREAM_TASK_ID = "downstream_task"


def _create_test_single_ingest_pipeline_group_dag(state_code: StateCode) -> DAG:
    @dag(
        dag_id=_TEST_DAG_ID,
        start_date=datetime(2022, 1, 1),
        schedule=None,
        catchup=False,
    )
    def test_single_ingest_pipeline_group_dag() -> None:
        create_single_ingest_pipeline_group(state_code) >> EmptyOperator(
            task_id=_DOWNSTREAM_TASK_ID
        )

    return test_single_ingest_pipeline_group_dag()


class TestCheckForValidWatermarks(unittest.TestCase):
    """Tests for the check_for_valid_watermarks helper"""

    DATE_1_ISO = datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=pytz.UTC).isoformat()
    DATE_2_ISO = datetime(2025, 2, 2, 0, 0, 0, 0, tzinfo=pytz.UTC).isoformat()

    def test_check_for_valid_watermarks__valid(self) -> None:
        # We shouldn't hit this empty case, but technically valid
        self.assertTrue(
            check_for_valid_watermarks(watermarks={}, max_update_datetimes={})
        )

        # Files that don't have watermarks are ok
        self.assertTrue(
            check_for_valid_watermarks(
                watermarks={},
                max_update_datetimes={"tagA": self.DATE_1_ISO},
            )
        )

        # Equal to watermark is ok
        self.assertTrue(
            check_for_valid_watermarks(
                watermarks={"tagA": self.DATE_1_ISO},
                max_update_datetimes={"tagA": self.DATE_1_ISO},
            )
        )

        # Greater than watermark is ok
        self.assertTrue(
            check_for_valid_watermarks(
                watermarks={"tagA": self.DATE_1_ISO},
                max_update_datetimes={"tagA": self.DATE_2_ISO},
            )
        )

        # Multiple tags example
        self.assertTrue(
            check_for_valid_watermarks(
                watermarks={"tagA": self.DATE_1_ISO},
                max_update_datetimes={"tagA": self.DATE_2_ISO, "tagB": self.DATE_1_ISO},
            )
        )

    def test_check_for_valid_watermarks__no_current_max_date(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found critical raw data tables that either do not exist or are empty: "
            r"\['tagA'\]",
        ):
            # TagA is missing from max_update_datetimes
            check_for_valid_watermarks(
                watermarks={"tagA": self.DATE_1_ISO},
                max_update_datetimes={"tagB": self.DATE_2_ISO},
            )

    def test_check_for_valid_watermarks__stale_current_max_date(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"\[tagA\] Current max update_datetime: 2025-01-01T00:00:00\+00:00. Last "
            r"ingest pipeline run update_datetime: 2025-02-02T00:00:00\+00:00.",
        ):
            # TagA has stale data
            check_for_valid_watermarks(
                watermarks={"tagA": self.DATE_2_ISO},
                max_update_datetimes={"tagA": self.DATE_1_ISO, "tagB": self.DATE_2_ISO},
            )


@patch.dict(
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
    values={StateCode.US_XX: "us-east1"},
)
class TestSingleIngestPipelineGroup(unittest.TestCase):
    """Tests for the single ingest pipeline group ."""

    entrypoint_args_fixture: Dict[str, List[str]] = {}

    @classmethod
    def setUpClass(cls) -> None:
        with open(
            os.path.join(os.path.dirname(fixtures.__file__), "./entrypoints_args.yaml"),
            "r",
            encoding="utf-8",
        ) as fixture_file:
            cls.entrypoint_args_fixture = yaml.safe_load(fixture_file)

    def setUp(self) -> None:
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
        self.environment_patcher.stop()
        self.project_environment_patcher.stop()

    def test_dataflow_pipeline_task_exists(self) -> None:
        """Tests that dataflow_pipeline triggers the proper script."""

        test_dag = _create_test_single_ingest_pipeline_group_dag(StateCode.US_XX)

        task_group_id = "ingest.us-xx-ingest-pipeline"
        dataflow_pipeline_task = test_dag.get_task(f"{task_group_id}.run_pipeline")

        if not isinstance(
            dataflow_pipeline_task, RecidivizDataflowFlexTemplateOperator
        ):
            raise ValueError(
                f"Expected type RecidivizDataflowFlexTemplateOperator, found "
                f"[{type(dataflow_pipeline_task)}]."
            )

    def test_dataflow_pipeline_task(self) -> None:
        """Tests that dataflow_pipeline get the expected arguments."""

        test_dag = _create_test_single_ingest_pipeline_group_dag(StateCode.US_XX)
        task_group_id = "ingest.us-xx-ingest-pipeline"
        task: RecidivizDataflowFlexTemplateOperator = test_dag.get_task(  # type: ignore
            f"{task_group_id}.run_pipeline"
        )

        self.assertEqual(task.location, "us-east1")
        self.assertEqual(task.project_id, _PROJECT_ID)
        self.assertEqual(task.body.operator.task_id, f"{task_group_id}.create_flex_template")  # type: ignore


def _fake_failure_execute(*args: Any, **kwargs: Any) -> None:
    raise ValueError("Fake failure")


@patch.dict(
    DEFAULT_PIPELINE_REGIONS_BY_STATE_CODE,
    values={StateCode.US_XX: "us-east1"},
)
class TestSingleIngestPipelineGroupIntegration(AirflowIntegrationTest):
    """Tests for the single ingest pipeline group ."""

    def setUp(self) -> None:
        super().setUp()
        self.environment_patcher = patch(
            "os.environ",
            {
                "GCP_PROJECT": _PROJECT_ID,
            },
        )
        self.environment_patcher.start()

        self.kubernetes_pod_operator_patcher = patch(
            "recidiviz.airflow.dags.calculation.dataflow.single_ingest_pipeline_group.build_kubernetes_pod_task",
            side_effect=fake_operator_constructor,
        )
        self.mock_kubernetes_pod_operator = self.kubernetes_pod_operator_patcher.start()

        self.cloud_sql_query_operator_patcher = patch(
            "recidiviz.airflow.dags.calculation.dataflow.single_ingest_pipeline_group.CloudSqlQueryOperator",
            side_effect=fake_operator_with_return_value({}),
        )
        self.cloud_sql_query_operator_patcher.start()

        self.recidiviz_dataflow_operator_patcher = patch(
            "recidiviz.airflow.dags.utils.dataflow_pipeline_group.RecidivizDataflowFlexTemplateOperator",
            side_effect=fake_operator_constructor,
        )
        self.mock_dataflow_operator = self.recidiviz_dataflow_operator_patcher.start()
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

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        self.kubernetes_pod_operator_patcher.stop()
        self.cloud_sql_query_operator_patcher.stop()
        self.recidiviz_dataflow_operator_patcher.stop()
        self.direct_ingest_regions_patcher.stop()
        self.metadata_patcher.stop()
        super().tearDown()

    def test_single_ingest_pipeline_group(self) -> None:
        test_dag = _create_test_single_ingest_pipeline_group_dag(StateCode.US_XX)
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(test_dag, session)
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    @patch(
        "recidiviz.airflow.dags.calculation.dataflow.single_ingest_pipeline_group.IngestViewManifestCollector",
    )
    def test_ingest_pipeline_should_run_in_dag_false(
        self, mock_manifest_collector: MagicMock
    ) -> None:
        mock_manifest_collector.return_value.launchable_ingest_views.return_value = []
        test_dag = _create_test_single_ingest_pipeline_group_dag(StateCode.US_XX)
        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                test_dag,
                session,
                expected_skipped_task_id_regexes=[
                    r".*get_max_update_datetimes",
                    r".*get_watermarks",
                    r".*check_for_valid_watermarks",
                    r".*verify_raw_data_flashing_not_in_progress",
                    r"^ingest\.us-xx-ingest-pipeline\.",
                    r".*write_ingest_job_completion",
                    r".*write_upper_bounds",
                    _DOWNSTREAM_TASK_ID,
                ],
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    @patch(
        "recidiviz.airflow.dags.calculation.dataflow.single_ingest_pipeline_group._check_for_valid_watermarks_task"
    )
    def test_initialize_ingest_pipeline_fails_when_watermark_datetime_greater_than_max_update_datetime(
        self,
        mock_check_for_valid_watermarks: MagicMock,
    ) -> None:
        mock_check_for_valid_watermarks.side_effect = (
            lambda watermarks, max_update_datetimes: _check_for_valid_watermarks_task(
                watermarks={"test_file_tag": "2023-01-26 00:00:0.000000+00"},
                max_update_datetimes={"test_file_tag": "2023-01-25 00:00:0.000000+00"},
            )
        )

        test_dag = _create_test_single_ingest_pipeline_group_dag(StateCode.US_XX)

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                test_dag,
                session,
                expected_failure_task_id_regexes=[
                    r".*check_for_valid_watermarks",
                    r".*verify_raw_data_flashing_not_in_progress",
                    r"^ingest\.us-xx-ingest-pipeline\.",
                    r".*write_ingest_job_completion",
                    r".*write_upper_bounds",
                    _DOWNSTREAM_TASK_ID,
                ],
            )
            self.assertEqual(DagRunState.FAILED, result.dag_run_state)

    @patch(
        "recidiviz.airflow.dags.calculation.dataflow.single_ingest_pipeline_group._verify_raw_data_flashing_not_in_progress"
    )
    def test_failed_verify_raw_data_flashing_not_in_progress(
        self, mock_verify_raw_data_flashing_not_in_progress: MagicMock
    ) -> None:
        mock_verify_raw_data_flashing_not_in_progress.side_effect = (
            lambda _state_code: fake_failure_task(
                task_id="verify_raw_data_flashing_not_in_progress"
            )
        )

        test_dag = _create_test_single_ingest_pipeline_group_dag(StateCode.US_XX)

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                test_dag,
                session,
                expected_failure_task_id_regexes=[
                    r".*verify_raw_data_flashing_not_in_progress",
                    r"^ingest\.us-xx-ingest-pipeline\.",
                    r".*write_ingest_job_completion",
                    r".*write_upper_bounds",
                    _DOWNSTREAM_TASK_ID,
                ],
            )
            self.assertEqual(DagRunState.FAILED, result.dag_run_state)

    @patch(
        "recidiviz.airflow.dags.calculation.dataflow.single_ingest_pipeline_group._check_for_valid_watermarks_task"
    )
    def test_initialize_ingest_pipeline_when_watermark_datetime_less_than_max_update_datetime(
        self,
        mock_check_for_valid_watermarks: MagicMock,
    ) -> None:
        mock_check_for_valid_watermarks.side_effect = (
            lambda watermarks, max_update_datetimes: _check_for_valid_watermarks_task(
                watermarks={"test_file_tag": "2023-01-24 00:00:0.000000+00"},
                max_update_datetimes={"test_file_tag": "2023-01-25 00:00:0.000000+00"},
            )
        )

        test_dag = _create_test_single_ingest_pipeline_group_dag(StateCode.US_XX)

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                test_dag,
                session,
            )
            self.assertEqual(DagRunState.SUCCESS, result.dag_run_state)

    def test_failed_dataflow_pipeline(self) -> None:
        self.mock_dataflow_operator.side_effect = FakeFailureOperator

        test_dag = _create_test_single_ingest_pipeline_group_dag(StateCode.US_XX)

        with Session(bind=self.engine) as session:
            result = self.run_dag_test(
                test_dag,
                session,
                expected_failure_task_id_regexes=[
                    r"^ingest\.us-xx-ingest-pipeline\.run_pipeline",
                    r".*write_ingest_job_completion",
                    r".*write_upper_bounds",
                    _DOWNSTREAM_TASK_ID,
                ],
            )
            self.assertEqual(DagRunState.FAILED, result.dag_run_state)

    def test_failed_tasks_fail_group(self) -> None:
        """
        Tests that if any task in the group fails, the entire group fails.
        """
        test_dag = _create_test_single_ingest_pipeline_group_dag(StateCode.US_XX)

        task_ids_to_fail = [task.task_id for task in test_dag.task_group_dict["ingest"]]

        with Session(bind=self.engine) as session:
            for task_id in task_ids_to_fail:
                test_dag = _create_test_single_ingest_pipeline_group_dag(
                    StateCode.US_XX
                )
                task = assert_type(test_dag.get_task(task_id), BaseOperator)
                old_execute_function = task.execute
                task.execute = _fake_failure_execute  # type: ignore
                result = self.run_dag_test(
                    test_dag,
                    session,
                    skip_checking_task_statuses=True,
                )
                task.execute = old_execute_function  # type: ignore
                self.assertEqual(
                    DagRunState.FAILED,
                    result.dag_run_state,
                    f"Incorrect dag run state when failing task: {task.task_id}",
                )
                self.assertIn(
                    task.task_id,
                    result.failure_messages,
                )
                self.assertEqual(
                    result.failure_messages[task.task_id],
                    "Fake failure",
                )
