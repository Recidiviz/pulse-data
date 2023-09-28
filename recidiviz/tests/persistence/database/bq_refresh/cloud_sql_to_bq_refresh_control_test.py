# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for cloud_sql_to_bq_refresh_control.py."""
# pylint: disable=unused-argument
import unittest
import uuid
from typing import Any, Callable, Optional
from unittest import mock

from mock import Mock, create_autospec
from parameterized import parameterized

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.success_persister import RefreshBQDatasetSuccessPersister
from recidiviz.cloud_storage.gcs_pseudo_lock_manager import GCSPseudoLockAlreadyExists
from recidiviz.ingest.direct import direct_ingest_control
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.bq_refresh import cloud_sql_to_bq_refresh_control
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_lock_manager import (
    CloudSqlToBQLockManager,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.pipelines.state_update_lock_manager import StateUpdateLockManager
from recidiviz.utils.environment import GCPEnvironment

REFRESH_CONTROL_PACKAGE_NAME = cloud_sql_to_bq_refresh_control.__name__
INGEST_CONTROL_PACKAGE_NAME = direct_ingest_control.__name__


def mock_acquire_lock_fail(**kwargs: Any) -> None:
    raise GCSPseudoLockAlreadyExists("test lock already exists")


def mock_acquire_lock_pass(**kwargs: Any) -> None:
    return None


def mock_can_proceed_true(
    schema_type: SchemaType, ingest_instance: DirectIngestInstance
) -> bool:
    return True


def mock_can_proceed_false(
    schema_type: SchemaType, ingest_instance: DirectIngestInstance
) -> bool:
    return False


class SideEffect:
    """
    Calls the next function in the iteration of functions passed into it each time it is invoked.
    """

    def __init__(self, *functions: Callable[..., Any]) -> None:
        self.functions = iter(functions)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        function = next(self.functions)
        return function(*args, **kwargs)


@mock.patch(
    f"{REFRESH_CONTROL_PACKAGE_NAME}.BigQueryClientImpl",
    create_autospec(BigQueryClientImpl),
)
@mock.patch("time.sleep", Mock(side_effect=lambda _: None))
@mock.patch(
    "uuid.uuid4", Mock(return_value=uuid.UUID("8367379ff8674b04adfb9b595b277dc3"))
)
class ExecuteCloudSqlToBQRefreshTest(unittest.TestCase):
    """Tests the execute_cloud_sql_to_bq_refresh function"""

    def setUp(self) -> None:
        self.mock_cloud_sql_to_bq_lock_manager = create_autospec(
            CloudSqlToBQLockManager
        )
        self.mock_cloud_sql_to_bq_lock_patcher = mock.patch(
            f"{REFRESH_CONTROL_PACKAGE_NAME}.CloudSqlToBQLockManager"
        )
        self.mock_cloud_sql_to_bq_lock_patcher.start().return_value = (
            self.mock_cloud_sql_to_bq_lock_manager
        )

        self.mock_state_update_lock_manager = create_autospec(StateUpdateLockManager)
        self.mock_state_update_lock_patcher = mock.patch(
            f"{REFRESH_CONTROL_PACKAGE_NAME}.StateUpdateLockManager"
        )
        self.mock_state_update_lock_patcher.start().return_value = (
            self.mock_state_update_lock_manager
        )

        self.mock_refresh_bq_dataset_persister = create_autospec(
            RefreshBQDatasetSuccessPersister
        )
        self.mock_refresh_bq_dataset_persister_patcher = mock.patch(
            f"{REFRESH_CONTROL_PACKAGE_NAME}.RefreshBQDatasetSuccessPersister"
        )
        self.mock_refresh_bq_dataset_persister_patcher.start().return_value = (
            self.mock_refresh_bq_dataset_persister
        )
        self.environment_patcher = mock.patch(
            "recidiviz.utils.environment.get_gcp_environment",
            return_value=GCPEnvironment.PRODUCTION.value,
        )
        self.environment_patcher.start()

    def tearDown(self) -> None:
        self.environment_patcher.stop()
        self.mock_cloud_sql_to_bq_lock_patcher.stop()
        self.mock_state_update_lock_patcher.stop()
        self.mock_refresh_bq_dataset_persister_patcher.stop()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_to_bq_refresh(
        self, mock_federated_bq: mock.MagicMock
    ) -> None:
        self.mock_cloud_sql_to_bq_lock_manager.acquire_lock.side_effect = (
            mock_acquire_lock_pass
        )
        self.mock_cloud_sql_to_bq_lock_manager.can_proceed.side_effect = (
            mock_can_proceed_true
        )
        self.mock_state_update_lock_manager.acquire_lock.side_effect = (
            mock_acquire_lock_pass
        )

        def federated_bq_pass(
            schema_type: SchemaType,
            direct_ingest_instance: DirectIngestInstance,
            dataset_override_prefix: Optional[str] = None,
        ) -> None:
            return None

        mock_federated_bq.side_effect = federated_bq_pass

        def mock_record_success(
            schema_type: SchemaType,
            direct_ingest_instance: DirectIngestInstance,
            dataset_override_prefix: Optional[str],
            runtime_sec: int,
        ) -> None:
            return None

        self.mock_refresh_bq_dataset_persister.record_success_in_bq.side_effect = (
            mock_record_success
        )

        cloud_sql_to_bq_refresh_control.execute_cloud_sql_to_bq_refresh(
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
            sandbox_prefix=None,
        )

        self.mock_cloud_sql_to_bq_lock_manager.release_lock.assert_called_once_with(
            schema_type=SchemaType.STATE, ingest_instance=DirectIngestInstance.PRIMARY
        )
        self.mock_state_update_lock_manager.release_lock.assert_called_once()
        mock_federated_bq.assert_called_once_with(
            schema_type=SchemaType.STATE,
            direct_ingest_instance=DirectIngestInstance.PRIMARY,
            dataset_override_prefix=None,
        )

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_bq_refresh_invalid_schema_type(
        self, mock_federated_bq: mock.MagicMock
    ) -> None:
        with self.assertRaisesRegex(ValueError, r"Unsupported schema type*"):
            cloud_sql_to_bq_refresh_control.execute_cloud_sql_to_bq_refresh(
                schema_type=SchemaType.JUSTICE_COUNTS,
                ingest_instance=DirectIngestInstance.PRIMARY,
                sandbox_prefix=None,
            )
        self.mock_cloud_sql_to_bq_lock_manager.acquire_lock.assert_not_called()
        self.mock_state_update_lock_manager.acquire_lock.assert_not_called()
        self.mock_cloud_sql_to_bq_lock_manager.can_proceed.assert_not_called()
        mock_federated_bq.assert_not_called()
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.assert_not_called()
        self.mock_cloud_sql_to_bq_lock_manager.release_lock.assert_not_called()
        self.mock_state_update_lock_manager.release_lock.assert_not_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_to_bq_refresh_federated_throws(
        self, mock_federated_bq: mock.MagicMock
    ) -> None:
        self.mock_cloud_sql_to_bq_lock_manager.acquire_lock.side_effect = (
            mock_acquire_lock_pass
        )
        self.mock_cloud_sql_to_bq_lock_manager.can_proceed.side_effect = (
            mock_can_proceed_true
        )
        self.mock_state_update_lock_manager.acquire_lock.side_effect = (
            mock_acquire_lock_pass
        )

        def federated_bq_pass(
            schema_type: SchemaType,
            direct_ingest_instance: DirectIngestInstance,
            dataset_override_prefix: Optional[str] = None,
        ) -> None:
            raise ValueError

        mock_federated_bq.side_effect = federated_bq_pass

        with self.assertRaises(ValueError):
            cloud_sql_to_bq_refresh_control.execute_cloud_sql_to_bq_refresh(
                schema_type=SchemaType.STATE,
                ingest_instance=DirectIngestInstance.PRIMARY,
                sandbox_prefix=None,
            )
        self.mock_cloud_sql_to_bq_lock_manager.acquire_lock.assert_called()
        self.mock_cloud_sql_to_bq_lock_manager.can_proceed.assert_called()
        self.mock_cloud_sql_to_bq_lock_manager.release_lock.assert_called()
        self.mock_state_update_lock_manager.acquire_lock.assert_called()
        self.mock_state_update_lock_manager.release_lock.assert_called()
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.assert_not_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_to_bq_refresh_bq_success_throws(
        self, mock_federated_bq: mock.MagicMock
    ) -> None:
        self.mock_cloud_sql_to_bq_lock_manager.acquire_lock.side_effect = (
            mock_acquire_lock_pass
        )
        self.mock_cloud_sql_to_bq_lock_manager.can_proceed.side_effect = (
            mock_can_proceed_true
        )
        self.mock_state_update_lock_manager.acquire_lock.side_effect = (
            mock_acquire_lock_pass
        )

        def federated_bq_pass(
            schema_type: SchemaType,
            direct_ingest_instance: DirectIngestInstance,
            dataset_override_prefix: Optional[str] = None,
        ) -> None:
            return None

        mock_federated_bq.side_effect = federated_bq_pass

        def mock_record_success(
            schema_type: SchemaType,
            direct_ingest_instance: DirectIngestInstance,
            dataset_override_prefix: Optional[str],
            runtime_sec: int,
        ) -> None:
            raise ValueError

        self.mock_refresh_bq_dataset_persister.record_success_in_bq.side_effect = (
            mock_record_success
        )

        with self.assertRaises(ValueError):
            cloud_sql_to_bq_refresh_control.execute_cloud_sql_to_bq_refresh(
                schema_type=SchemaType.STATE,
                ingest_instance=DirectIngestInstance.PRIMARY,
                sandbox_prefix=None,
            )
        self.mock_cloud_sql_to_bq_lock_manager.acquire_lock.assert_called()
        self.mock_cloud_sql_to_bq_lock_manager.can_proceed.assert_called()
        self.mock_cloud_sql_to_bq_lock_manager.release_lock.assert_called()
        self.mock_state_update_lock_manager.acquire_lock.assert_called()
        self.mock_state_update_lock_manager.release_lock.assert_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_to_bq_refresh_lock_acquisition_timeout(
        self, mock_federated_bq: mock.MagicMock
    ) -> None:
        self.mock_cloud_sql_to_bq_lock_manager.acquire_lock.side_effect = (
            mock_acquire_lock_pass
        )
        self.mock_state_update_lock_manager.acquire_lock.side_effect = (
            mock_acquire_lock_pass
        )

        def mock_can_proceed(
            schema_type: SchemaType, ingest_instance: DirectIngestInstance
        ) -> bool:
            return False

        self.mock_cloud_sql_to_bq_lock_manager.can_proceed.side_effect = (
            mock_can_proceed
        )

        with self.assertRaisesRegex(
            ValueError, r"Could not acquire lock after waiting*"
        ):
            cloud_sql_to_bq_refresh_control.execute_cloud_sql_to_bq_refresh(
                schema_type=SchemaType.STATE,
                ingest_instance=DirectIngestInstance.PRIMARY,
                sandbox_prefix=None,
            )

        self.mock_cloud_sql_to_bq_lock_manager.release_lock.assert_called()
        self.mock_state_update_lock_manager.release_lock.assert_called()
        mock_federated_bq.assert_not_called()
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.assert_not_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_to_bq_refresh_acquire_lock_acquisition_timeout(
        self, mock_federated_bq: mock.MagicMock
    ) -> None:
        self.mock_cloud_sql_to_bq_lock_manager.acquire_lock.side_effect = (
            mock_acquire_lock_fail
        )
        self.mock_cloud_sql_to_bq_lock_manager.can_proceed.side_effect = (
            mock_can_proceed_true
        )
        self.mock_state_update_lock_manager.acquire_lock.side_effect = (
            mock_acquire_lock_pass
        )

        with self.assertRaisesRegex(
            ValueError, r"Could not acquire lock after waiting*"
        ):
            cloud_sql_to_bq_refresh_control.execute_cloud_sql_to_bq_refresh(
                schema_type=SchemaType.STATE,
                ingest_instance=DirectIngestInstance.PRIMARY,
                sandbox_prefix=None,
            )

        self.mock_cloud_sql_to_bq_lock_manager.release_lock.assert_called()
        self.mock_state_update_lock_manager.release_lock.assert_called()
        self.mock_cloud_sql_to_bq_lock_manager.can_proceed.assert_not_called()
        mock_federated_bq.assert_not_called()
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.assert_not_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_to_bq_refresh_acquire_lock_fails_then_succeeds(
        self, mock_federated_bq: mock.MagicMock
    ) -> None:
        self.mock_cloud_sql_to_bq_lock_manager.acquire_lock.side_effect = SideEffect(
            mock_acquire_lock_fail, mock_acquire_lock_pass
        )
        self.mock_cloud_sql_to_bq_lock_manager.can_proceed.side_effect = (
            mock_can_proceed_true
        )
        self.mock_state_update_lock_manager.acquire_lock.side_effect = (
            mock_acquire_lock_pass
        )

        cloud_sql_to_bq_refresh_control.execute_cloud_sql_to_bq_refresh(
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
            sandbox_prefix=None,
        )

        self.mock_cloud_sql_to_bq_lock_manager.release_lock.assert_called()
        self.mock_cloud_sql_to_bq_lock_manager.can_proceed.assert_called_once()
        self.assertEqual(
            2, self.mock_cloud_sql_to_bq_lock_manager.acquire_lock.call_count
        )
        mock_federated_bq.assert_called()
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.assert_called()
        self.mock_state_update_lock_manager.release_lock.assert_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_to_bq_refresh_can_proceed_fails_then_succeeds(
        self, mock_federated_bq: mock.MagicMock
    ) -> None:
        self.mock_cloud_sql_to_bq_lock_manager.acquire_lock.side_effect = (
            mock_acquire_lock_pass
        )
        self.mock_cloud_sql_to_bq_lock_manager.can_proceed.side_effect = SideEffect(
            mock_can_proceed_false, mock_can_proceed_true
        )
        self.mock_state_update_lock_manager.acquire_lock.side_effect = (
            mock_acquire_lock_pass
        )

        cloud_sql_to_bq_refresh_control.execute_cloud_sql_to_bq_refresh(
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
            sandbox_prefix=None,
        )

        self.mock_cloud_sql_to_bq_lock_manager.release_lock.assert_called()
        self.assertEqual(
            2, self.mock_cloud_sql_to_bq_lock_manager.can_proceed.call_count
        )
        self.mock_cloud_sql_to_bq_lock_manager.acquire_lock.assert_called_once()
        mock_federated_bq.assert_called()
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.assert_called()
        self.mock_state_update_lock_manager.release_lock.assert_called()

    @parameterized.expand(
        [
            (
                "state_with_primary",
                SchemaType.STATE,
                DirectIngestInstance.PRIMARY,
                None,
                DirectIngestInstance.PRIMARY,
            ),
            (
                "state_with_secondary",
                SchemaType.STATE,
                DirectIngestInstance.SECONDARY,
                "sandbox",
                DirectIngestInstance.SECONDARY,
            ),
            (
                "nonstate_with_primary",
                SchemaType.OPERATIONS,
                DirectIngestInstance.PRIMARY,
                None,
                None,
            ),
            (
                "nonstate_with_secondary",
                SchemaType.CASE_TRIAGE,
                DirectIngestInstance.SECONDARY,
                "sandbox",
                None,
            ),
        ]
    )
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_to_bq_refresh_args_check(
        self,
        _name: str,
        schema_called_with: SchemaType,
        instance_called_with: DirectIngestInstance,
        sandbox_called_with: Optional[str],
        instance_in_func_call: DirectIngestInstance,
        mock_federated_bq: mock.MagicMock,
    ) -> None:
        self.mock_cloud_sql_to_bq_lock_manager.acquire_lock.side_effect = (
            mock_acquire_lock_pass
        )
        self.mock_cloud_sql_to_bq_lock_manager.can_proceed.side_effect = (
            mock_can_proceed_true
        )
        self.mock_state_update_lock_manager.acquire_lock.side_effect = (
            mock_acquire_lock_pass
        )

        cloud_sql_to_bq_refresh_control.execute_cloud_sql_to_bq_refresh(
            schema_type=schema_called_with,
            ingest_instance=instance_called_with,
            sandbox_prefix=sandbox_called_with,
        )

        mock_federated_bq.assert_called_with(
            schema_type=schema_called_with,
            direct_ingest_instance=instance_in_func_call,
            dataset_override_prefix=sandbox_called_with,
        )
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.assert_called_with(
            schema_type=schema_called_with,
            direct_ingest_instance=instance_called_with,
            dataset_override_prefix=sandbox_called_with,
            runtime_sec=mock.ANY,
        )
        self.mock_cloud_sql_to_bq_lock_manager.release_lock.assert_called_with(
            schema_type=schema_called_with,
            ingest_instance=instance_called_with,
        )
        self.mock_state_update_lock_manager.release_lock.assert_called_once()
