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
"""Tests for sequencing_tasks"""
from unittest import TestCase
from unittest.mock import MagicMock, patch

from airflow.exceptions import DagRunAlreadyExists

from recidiviz.airflow.dags.monitoring.dag_registry import get_identity_ingest_dag_id
from recidiviz.airflow.dags.raw_data.sequencing_tasks import (
    has_attempted_file_imports,
    has_files_to_import,
    successfully_acquired_all_locks,
    trigger_identity_ingest_dag,
)
from recidiviz.airflow.dags.utils.config_utils import TENANT_FILTER
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.ingest.direct.types.raw_data_import_types import RawDataResourceLock

_SEQUENCING_TASKS_MODULE = "recidiviz.airflow.dags.raw_data.sequencing_tasks"
_TEST_PROJECT_ID = "recidiviz-testing"
_TEST_RUN_ID = "scheduled__2024-01-01T00:00:00+00:00"


class SequencingTests(TestCase):
    """Tests for sequencing_tasks."""

    def test_has_files_to_import(self) -> None:
        assert not has_files_to_import.function(None)
        assert not has_files_to_import.function([])
        assert has_files_to_import.function(["a"])

    def test_has_attempted_file_imports(self) -> None:
        assert not has_attempted_file_imports.function(None)
        assert not has_attempted_file_imports.function([])
        assert has_attempted_file_imports.function(["a"])

    def test_successfully_acquired_all_locks(self) -> None:
        assert not successfully_acquired_all_locks.function(None, [])
        assert successfully_acquired_all_locks.function([], [])
        assert not successfully_acquired_all_locks.function(
            [], [DirectIngestRawDataResourceLockResource.BUCKET]
        )
        assert not successfully_acquired_all_locks.function(
            [
                RawDataResourceLock(
                    lock_id=1,
                    lock_resource=DirectIngestRawDataResourceLockResource.BIG_QUERY_RAW_DATA_DATASET,
                    released=False,
                ).serialize()
            ],
            [DirectIngestRawDataResourceLockResource.BUCKET],
        )
        assert not successfully_acquired_all_locks.function(
            [
                RawDataResourceLock(
                    lock_id=1,
                    lock_resource=DirectIngestRawDataResourceLockResource.BUCKET,
                    released=True,
                ).serialize()
            ],
            [DirectIngestRawDataResourceLockResource.BUCKET],
        )
        assert successfully_acquired_all_locks.function(
            [
                RawDataResourceLock(
                    lock_id=1,
                    lock_resource=DirectIngestRawDataResourceLockResource.BUCKET,
                    released=False,
                ).serialize()
            ],
            [DirectIngestRawDataResourceLockResource.BUCKET],
        )


class TriggerIdentityIngestDagTest(TestCase):
    """Tests for trigger_identity_ingest_dag."""

    def setUp(self) -> None:
        self.trigger_dag_patcher = patch(f"{_SEQUENCING_TASKS_MODULE}.trigger_dag")
        self.trigger_dag_mock = self.trigger_dag_patcher.start()

        self.project_patcher = patch(
            f"{_SEQUENCING_TASKS_MODULE}.get_project_id",
            return_value=_TEST_PROJECT_ID,
        )
        self.project_patcher.start()

        self.context_patcher = patch(
            f"{_SEQUENCING_TASKS_MODULE}.get_current_context",
            return_value={"run_id": _TEST_RUN_ID},
        )
        self.context_patcher.start()

        self.get_region_patcher = patch(
            f"{_SEQUENCING_TASKS_MODULE}.direct_ingest_regions.get_direct_ingest_region"
        )
        self.get_region_mock = self.get_region_patcher.start()

    def tearDown(self) -> None:
        self.trigger_dag_patcher.stop()
        self.project_patcher.stop()
        self.context_patcher.stop()
        self.get_region_patcher.stop()

    def _set_identity_views_launchable(self, launchable: bool) -> None:
        region = MagicMock()
        region.has_launchable_ingest_views_for_pipeline.return_value = launchable
        self.get_region_mock.return_value = region

    def test_no_launchable_identity_views_does_not_trigger(self) -> None:
        self._set_identity_views_launchable(False)
        trigger_identity_ingest_dag.function(region_code="US_XX")
        self.trigger_dag_mock.assert_not_called()

    def test_triggers_identity_dag_for_tenant(self) -> None:
        self._set_identity_views_launchable(True)
        trigger_identity_ingest_dag.function(region_code="US_XX")
        self.trigger_dag_mock.assert_called_once_with(
            dag_id=get_identity_ingest_dag_id(_TEST_PROJECT_ID),
            run_id=f"triggered_by_raw_data_import__US_XX__{_TEST_RUN_ID}",
            conf={TENANT_FILTER: "US_XX"},
            replace_microseconds=False,
        )

    def test_already_triggered_run_is_swallowed(self) -> None:
        self._set_identity_views_launchable(True)
        identity_run_id = f"triggered_by_raw_data_import__US_XX__{_TEST_RUN_ID}"
        existing_run = MagicMock()
        existing_run.run_id = identity_run_id
        self.trigger_dag_mock.side_effect = DagRunAlreadyExists(
            dag_run=existing_run,
            execution_date=MagicMock(),
            run_id=identity_run_id,
        )
        # A retry or clear re-requests our own run_id, so the existing run is ours and
        # should be treated as already triggered rather than re-raising.
        trigger_identity_ingest_dag.function(region_code="US_XX")
        self.trigger_dag_mock.assert_called_once()

    def test_collision_with_different_run_id_reraises(self) -> None:
        self._set_identity_views_launchable(True)
        other_run = MagicMock()
        other_run.run_id = "some_other_identity_run"
        self.trigger_dag_mock.side_effect = DagRunAlreadyExists(
            dag_run=other_run,
            execution_date=MagicMock(),
            run_id=f"triggered_by_raw_data_import__US_XX__{_TEST_RUN_ID}",
        )
        # A collision on execution_date alone matches a different run; fail loudly rather
        # than silently dropping this tenant's run.
        with self.assertRaises(DagRunAlreadyExists):
            trigger_identity_ingest_dag.function(region_code="US_XX")
