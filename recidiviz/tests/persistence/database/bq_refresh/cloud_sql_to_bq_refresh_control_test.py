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

import json
import unittest
from http import HTTPStatus
from typing import Any, Dict, Optional
from unittest import mock

import flask
from mock import Mock

from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.ingest.direct import direct_ingest_control
from recidiviz.ingest.direct.controllers.direct_ingest_region_lock_manager import (
    DirectIngestRegionLockManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.bq_refresh import cloud_sql_to_bq_refresh_control
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_lock_manager import (
    CloudSqlToBQLockManager,
)
from recidiviz.persistence.database.schema_type import SchemaType

REFRESH_CONTROL_PACKAGE_NAME = cloud_sql_to_bq_refresh_control.__name__
INGEST_CONTROL_PACKAGE_NAME = direct_ingest_control.__name__


class CloudSqlToBQExportControlTest(unittest.TestCase):
    """Tests for cloud_sql_to_bq_refresh_control.py."""

    def setUp(self) -> None:
        self.fake_table_name = "first_table"

        self.mock_app = flask.Flask(__name__)
        self.mock_app.config["TESTING"] = True
        self.mock_app.register_blueprint(
            cloud_sql_to_bq_refresh_control.cloud_sql_to_bq_blueprint
        )
        self.mock_flask_client = self.mock_app.test_client()

        self.base_headers: Dict[str, Dict[Any, Any]] = {"x-goog-iap-jwt-assertion": {}}

        self.mock_project_id_patcher = mock.patch(
            "recidiviz.utils.metadata.project_id", Mock(return_value="recidiviz-456")
        )
        self.mock_project_id_patcher.start()
        self.mock_project_number_patcher = mock.patch(
            "recidiviz.utils.metadata.project_number", Mock(return_value="123456789")
        )
        self.mock_project_number_patcher.start()
        self.fs_patcher = mock.patch(
            "recidiviz.cloud_storage.gcs_pseudo_lock_manager.GcsfsFactory.build",
            Mock(return_value=FakeGCSFileSystem()),
        )
        self.fs_patcher.start()

        # The FS on this lock manager is mocked but it otherwise operates like a normal
        # lock manager.
        self.mock_lock_manager = CloudSqlToBQLockManager()

        self.refresh_bq_datase_success_persister_patcher = mock.patch(
            f"{REFRESH_CONTROL_PACKAGE_NAME}.RefreshBQDatasetSuccessPersister"
        )
        self.refresh_bq_datase_success_persister_constructor = (
            self.refresh_bq_datase_success_persister_patcher.start()
        )
        self.mock_refresh_bq_datase_success_persister = (
            self.refresh_bq_datase_success_persister_constructor.return_value
        )

    def tearDown(self) -> None:
        self.mock_project_id_patcher.stop()
        self.mock_project_number_patcher.stop()
        self.fs_patcher.stop()
        self.refresh_bq_datase_success_persister_patcher.stop()

    def assertIsOnlySchemaLocked(self, schema_type: SchemaType) -> None:
        for s in SchemaType:
            if s == schema_type:
                self.assertTrue(self.mock_lock_manager.is_locked(schema_type))
            else:
                self.assertFalse(self.mock_lock_manager.is_locked(s), f"Locked for {s}")

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.BigQueryClientImpl")
    @mock.patch(
        f"{REFRESH_CONTROL_PACKAGE_NAME}.get_current_cloud_task_id",
        return_value="test_cloud_task_id",
    )
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_dataset_state_incremental(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_get_current_cloud_task_id: mock.MagicMock,
        mock_big_query_client_impl: mock.MagicMock,
    ) -> None:
        # Grab lock, just like the /create_tasks... endpoint does
        self.mock_lock_manager.acquire_lock("any_lock_id", schema_type=SchemaType.STATE)

        def mock_federated_refresh_fn(
            # pylint: disable=unused-argument
            schema_type: SchemaType,
            dataset_override_prefix: Optional[str] = None,
        ) -> None:
            self.assertTrue(self.mock_lock_manager.can_proceed(SchemaType.STATE))
            # At the moment the federated refresh is called, the state schema should
            # be locked.
            self.assertIsOnlySchemaLocked(SchemaType.STATE)

        mock_federated_refresh.side_effect = mock_federated_refresh_fn

        module = SchemaType.STATE.value
        route = f"/refresh_bq_dataset/{module}"
        data = json.dumps({})

        response = self.mock_flask_client.post(
            route,
            data=data,
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_federated_refresh.assert_called_with(schema_type=SchemaType.STATE)
        mock_get_current_cloud_task_id.assert_called()
        mock_big_query_client_impl.assert_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.BigQueryClientImpl")
    @mock.patch(
        f"{REFRESH_CONTROL_PACKAGE_NAME}.get_current_cloud_task_id",
        return_value="test_cloud_task_id",
    )
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_dataset_state_historical(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_get_current_cloud_task_id: mock.MagicMock,
        mock_big_query_client_impl: mock.MagicMock,
    ) -> None:
        # Grab lock, just like the /create_tasks... endpoint does
        self.mock_lock_manager.acquire_lock("any_lock_id", schema_type=SchemaType.STATE)

        def mock_federated_refresh_fn(
            # pylint: disable=unused-argument
            schema_type: SchemaType,
            dataset_override_prefix: Optional[str] = None,
        ) -> None:
            self.assertTrue(self.mock_lock_manager.can_proceed(SchemaType.STATE))
            # At the moment the federated refresh is called, the state schema should
            # be locked.
            self.assertIsOnlySchemaLocked(SchemaType.STATE)

        mock_federated_refresh.side_effect = mock_federated_refresh_fn

        module = SchemaType.STATE.value
        route = f"/refresh_bq_dataset/{module}"
        data = json.dumps({})

        headers: Dict[str, Any] = {
            **self.base_headers,
            "X-AppEngine-Inbound-Appid": "recidiviz-456",
        }

        response = self.mock_flask_client.post(
            route,
            data=data,
            content_type="application/json",
            headers=headers,
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_federated_refresh.assert_called_with(schema_type=SchemaType.STATE)
        mock_get_current_cloud_task_id.assert_called()
        mock_big_query_client_impl.assert_called()

    @mock.patch("time.sleep")
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.BigQueryClientImpl")
    @mock.patch(
        f"{REFRESH_CONTROL_PACKAGE_NAME}.get_current_cloud_task_id",
        return_value="test_cloud_task_id",
    )
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_dataset_state_historical_dry_run(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_get_current_cloud_task_id: mock.MagicMock,
        mock_big_query_client_impl: mock.MagicMock,
        mock_time_sleep: mock.MagicMock,
    ) -> None:
        # Grab lock, just like the /create_tasks... endpoint does
        self.mock_lock_manager.acquire_lock("any_lock_id", schema_type=SchemaType.STATE)

        module = SchemaType.STATE.value
        route = f"/refresh_bq_dataset/{module}?dry_run=True"
        data = json.dumps({})

        headers: Dict[str, Any] = {
            **self.base_headers,
            "X-AppEngine-Inbound-Appid": "recidiviz-456",
        }

        response = self.mock_flask_client.post(
            route,
            data=data,
            content_type="application/json",
            headers=headers,
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_get_current_cloud_task_id.assert_called()
        mock_big_query_client_impl.assert_called()
        mock_federated_refresh.assert_not_called()
        mock_time_sleep.assert_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.BigQueryClientImpl")
    @mock.patch(
        f"{REFRESH_CONTROL_PACKAGE_NAME}.get_current_cloud_task_id",
        return_value="test_cloud_task_id",
    )
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_dataset_state_record_success_in_bq(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_get_current_cloud_task_id: mock.MagicMock,
        mock_big_query_client_impl: mock.MagicMock,
    ) -> None:
        # Grab lock, just like the /create_tasks... endpoint does
        self.mock_lock_manager.acquire_lock("any_lock_id", schema_type=SchemaType.STATE)

        def mock_federated_refresh_fn(
            # pylint: disable=unused-argument
            schema_type: SchemaType,
            dataset_override_prefix: Optional[str] = None,
        ) -> None:
            self.assertTrue(self.mock_lock_manager.can_proceed(SchemaType.STATE))
            # At the moment the federated refresh is called, the state schema should
            # be locked.
            self.assertIsOnlySchemaLocked(SchemaType.STATE)

        mock_federated_refresh.side_effect = mock_federated_refresh_fn

        module = SchemaType.STATE.value
        route = f"/refresh_bq_dataset/{module}"
        data = json.dumps({})

        headers: Dict[str, Any] = {
            **self.base_headers,
            "X-AppEngine-Inbound-Appid": "recidiviz-456",
        }

        response = self.mock_flask_client.post(
            route,
            data=data,
            content_type="application/json",
            headers=headers,
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_federated_refresh.assert_called_with(schema_type=SchemaType.STATE)
        mock_get_current_cloud_task_id.assert_called()
        mock_big_query_client_impl.assert_called()
        self.mock_refresh_bq_datase_success_persister.record_success_in_bq.assert_called_with(
            schema_type=SchemaType.STATE,
            runtime_sec=mock.ANY,
            cloud_task_id="test_cloud_task_id",
        )

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.BigQueryClientImpl")
    @mock.patch(
        f"{REFRESH_CONTROL_PACKAGE_NAME}.get_current_cloud_task_id",
        return_value="test_cloud_task_id",
    )
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_dataset_case_triage(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_get_current_cloud_task_id: mock.MagicMock,
        mock_big_query_client_impl: mock.MagicMock,
    ) -> None:
        # Grab lock, just like the /create_tasks... endpoint does
        self.mock_lock_manager.acquire_lock(
            "any_lock_id", schema_type=SchemaType.CASE_TRIAGE
        )

        def mock_federated_refresh_fn(
            # pylint: disable=unused-argument
            schema_type: SchemaType,
            dataset_override_prefix: Optional[str] = None,
        ) -> None:
            self.assertTrue(self.mock_lock_manager.can_proceed(SchemaType.CASE_TRIAGE))
            # At the moment the federated refresh is called, the state schema should
            # be locked.
            self.assertIsOnlySchemaLocked(SchemaType.CASE_TRIAGE)

        mock_federated_refresh.side_effect = mock_federated_refresh_fn

        module = SchemaType.CASE_TRIAGE.value
        route = f"/refresh_bq_dataset/{module}"

        response = self.mock_flask_client.post(
            route,
            data=json.dumps({}),
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_federated_refresh.assert_called_with(schema_type=SchemaType.CASE_TRIAGE)
        mock_get_current_cloud_task_id.assert_called()
        mock_big_query_client_impl.assert_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.BigQueryClientImpl")
    @mock.patch(
        f"{REFRESH_CONTROL_PACKAGE_NAME}.get_current_cloud_task_id",
        return_value="test_cloud_task_id",
    )
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_dataset_garbage_schema(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_get_current_cloud_task_id: mock.MagicMock,
        mock_big_query_client_impl: mock.MagicMock,
    ) -> None:
        route = "/refresh_bq_dataset/GARBAGE_SCHEMA"

        response = self.mock_flask_client.post(
            route,
            data=json.dumps({}),
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.data.decode(), "Unexpected value for schema_arg: [GARBAGE_SCHEMA]"
        )
        mock_federated_refresh.assert_not_called()
        mock_get_current_cloud_task_id.assert_not_called()
        mock_big_query_client_impl.assert_not_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.BigQueryClientImpl")
    @mock.patch(
        f"{REFRESH_CONTROL_PACKAGE_NAME}.get_current_cloud_task_id",
        return_value="test_cloud_task_id",
    )
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_datset_unsupported_schema(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_get_current_cloud_task_id: mock.MagicMock,
        mock_big_query_client_impl: mock.MagicMock,
    ) -> None:
        route = f"/refresh_bq_dataset/{SchemaType.JUSTICE_COUNTS.value}"

        response = self.mock_flask_client.post(
            route,
            data=json.dumps({}),
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.data.decode(),
            "Unsupported schema type: [SchemaType.JUSTICE_COUNTS]",
        )
        mock_federated_refresh.assert_not_called()
        mock_get_current_cloud_task_id.assert_not_called()
        mock_big_query_client_impl.assert_not_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.BigQueryClientImpl")
    @mock.patch(
        f"{REFRESH_CONTROL_PACKAGE_NAME}.get_current_cloud_task_id",
        return_value="test_cloud_task_id",
    )
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_dataset_not_locked(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_get_current_cloud_task_id: mock.MagicMock,
        mock_big_query_client_impl: mock.MagicMock,
    ) -> None:
        # Do not grab lock
        # self.mock_lock_manager.acquire_lock("any_lock_id", schema_type=SchemaType.OPERATIONS)

        route = f"/refresh_bq_dataset/{SchemaType.STATE.value}"

        response = self.mock_flask_client.post(
            route,
            data=json.dumps({}),
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(response.status_code, HTTPStatus.EXPECTATION_FAILED)
        self.assertEqual(
            response.data.decode(),
            "Expected lock for [STATE] BQ refresh to already exist.",
        )
        mock_federated_refresh.assert_not_called()
        mock_get_current_cloud_task_id.assert_not_called()
        mock_big_query_client_impl.assert_not_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.BigQueryClientImpl")
    @mock.patch(
        f"{REFRESH_CONTROL_PACKAGE_NAME}.get_current_cloud_task_id",
        return_value="test_cloud_task_id",
    )
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_dataset_ingest_still_locked(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_get_current_cloud_task_id: mock.MagicMock,
        mock_big_query_client_impl: mock.MagicMock,
    ) -> None:
        ingest_lock_manager = DirectIngestRegionLockManager(
            region_code=StateCode.US_XX.value,
            blocking_locks=[],
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        # Attempt to refresh while ingest is still locked for a STATE
        with ingest_lock_manager.using_region_lock(expiration_in_seconds=10):
            # Grab lock, just like the /create_tasks... endpoint does
            self.mock_lock_manager.acquire_lock(
                "any_lock_id", schema_type=SchemaType.STATE
            )

            route = f"/refresh_bq_dataset/{SchemaType.STATE.value}"

            response = self.mock_flask_client.post(
                route,
                data=json.dumps({}),
                content_type="application/json",
                headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
            )
        self.assertEqual(response.status_code, HTTPStatus.EXPECTATION_FAILED)
        self.assertEqual(
            response.data.decode(),
            "Expected to be able to proceed with refresh before this endpoint was called for [STATE].",
        )
        mock_federated_refresh.assert_not_called()
        mock_get_current_cloud_task_id.assert_not_called()
        mock_big_query_client_impl.assert_not_called()

    @mock.patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="production"),
    )
    def test_acquire_lock_state(
        self,
    ) -> None:
        # Act

        response = self.mock_flask_client.get(
            "/acquire_lock/state",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
            data=json.dumps({"lock_id": "test-lock-id"}),
        )

        # Assert
        self.assertIsOnlySchemaLocked(SchemaType.STATE)

        self.assertEqual(response.status_code, HTTPStatus.OK)

    def test_acquire_lock_state_ingest_locked(
        self,
    ) -> None:
        # Arrange
        lock_manager = DirectIngestRegionLockManager(
            region_code="US_XX",
            blocking_locks=[],
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        # Act
        with lock_manager.using_region_lock(expiration_in_seconds=10):
            response = self.mock_flask_client.get(
                "/acquire_lock/state",
                headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
                data=json.dumps({"lock_id": "test-lock-id"}),
            )

        # Assert
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertIsOnlySchemaLocked(SchemaType.STATE)

    def test_check_can_refresh_proceed_state_ingest_locked(
        self,
    ) -> None:

        # Grab lock, just like the /acquire_lock... endpoint does
        self.mock_lock_manager.acquire_lock("any_lock_id", schema_type=SchemaType.STATE)

        # Arrange
        lock_manager = DirectIngestRegionLockManager(
            region_code="US_XX",
            blocking_locks=[],
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        # Act
        with lock_manager.using_region_lock(expiration_in_seconds=10):
            response = self.mock_flask_client.get(
                "/check_can_refresh_proceed/state",
                headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
            )

        # Assert
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertEqual(response.get_data(as_text=True), "False")

    def test_check_can_refresh_proceed_state_can_proceed(
        self,
    ) -> None:

        # Grab lock, just like the /acquire_lock... endpoint does
        self.mock_lock_manager.acquire_lock("any_lock_id", schema_type=SchemaType.STATE)

        # Arrange
        lock_manager = DirectIngestRegionLockManager(
            region_code="US_XX",
            blocking_locks=[],
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        # Act
        with lock_manager.using_region_lock(expiration_in_seconds=0):
            response = self.mock_flask_client.get(
                "/check_can_refresh_proceed/state",
                headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
            )

        # Assert
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertEqual(response.get_data(as_text=True), "True")

    @mock.patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="production"),
    )
    def test_release_lock_state(
        self,
    ) -> None:

        # Grab lock, just like the /create_tasks... endpoint does
        self.mock_lock_manager.acquire_lock("any_lock_id", schema_type=SchemaType.STATE)

        # Act
        response = self.mock_flask_client.get(
            "/release_lock/state",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )

        # Assert
        self.assertFalse(self.mock_lock_manager.is_locked(SchemaType.STATE))
        self.assertEqual(response.status_code, HTTPStatus.OK)
