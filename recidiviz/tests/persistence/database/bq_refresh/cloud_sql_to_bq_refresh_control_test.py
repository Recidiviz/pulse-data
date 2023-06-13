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
import json
import unittest
import uuid
from http import HTTPStatus
from typing import Any, Dict, Optional
from unittest import mock

import flask
from mock import Mock, create_autospec
from parameterized import parameterized

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.success_persister import RefreshBQDatasetSuccessPersister
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


# TODO(#21446) Remove these tests once we move the callsite to Kubernetes in Airflow.
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

    def assert_is_only_schema_locked(
        self, schema_type: SchemaType, ingest_instance: DirectIngestInstance
    ) -> None:
        for s in SchemaType:
            if s == schema_type:
                self.assertTrue(
                    self.mock_lock_manager.is_locked(schema_type, ingest_instance)
                )
            else:
                self.assertFalse(
                    self.mock_lock_manager.is_locked(s, ingest_instance),
                    f"Locked for {s}",
                )

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
        self.mock_lock_manager.acquire_lock(
            "any_lock_id",
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        def mock_federated_refresh_fn(
            # pylint: disable=unused-argument
            schema_type: SchemaType,
            direct_ingest_instance: Optional[DirectIngestInstance] = None,
            dataset_override_prefix: Optional[str] = None,
        ) -> None:
            self.assertTrue(
                self.mock_lock_manager.can_proceed(
                    SchemaType.STATE, DirectIngestInstance.PRIMARY
                )
            )
            # At the moment the federated refresh is called, the state schema should
            # be locked.
            self.assert_is_only_schema_locked(
                SchemaType.STATE, DirectIngestInstance.PRIMARY
            )

        mock_federated_refresh.side_effect = mock_federated_refresh_fn

        route = "/refresh_bq_dataset"
        data = json.dumps(
            {
                "schema_type": SchemaType.STATE.value,
                "ingest_instance": DirectIngestInstance.PRIMARY.value,
            }
        )

        response = self.mock_flask_client.post(
            route,
            data=data,
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_federated_refresh.assert_called_with(
            schema_type=SchemaType.STATE,
            direct_ingest_instance=DirectIngestInstance.PRIMARY,
            dataset_override_prefix=None,
        )
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
        self.mock_lock_manager.acquire_lock(
            "any_lock_id",
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        def mock_federated_refresh_fn(
            # pylint: disable=unused-argument
            schema_type: SchemaType,
            direct_ingest_instance: Optional[DirectIngestInstance] = None,
            dataset_override_prefix: Optional[str] = None,
        ) -> None:
            self.assertTrue(
                self.mock_lock_manager.can_proceed(
                    SchemaType.STATE, DirectIngestInstance.PRIMARY
                )
            )
            # At the moment the federated refresh is called, the state schema should
            # be locked.
            self.assert_is_only_schema_locked(
                SchemaType.STATE, DirectIngestInstance.PRIMARY
            )

        mock_federated_refresh.side_effect = mock_federated_refresh_fn

        route = "/refresh_bq_dataset"
        data = json.dumps(
            {
                "schema_type": SchemaType.STATE.value,
                "ingest_instance": DirectIngestInstance.PRIMARY.value,
            }
        )

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
        mock_federated_refresh.assert_called_with(
            schema_type=SchemaType.STATE,
            direct_ingest_instance=DirectIngestInstance.PRIMARY,
            dataset_override_prefix=None,
        )
        mock_get_current_cloud_task_id.assert_called()
        mock_big_query_client_impl.assert_called()

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
        self.mock_lock_manager.acquire_lock(
            "any_lock_id",
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        def mock_federated_refresh_fn(
            # pylint: disable=unused-argument
            schema_type: SchemaType,
            direct_ingest_instance: Optional[DirectIngestInstance] = None,
            dataset_override_prefix: Optional[str] = None,
        ) -> None:
            self.assertTrue(
                self.mock_lock_manager.can_proceed(
                    SchemaType.STATE, DirectIngestInstance.PRIMARY
                )
            )
            # At the moment the federated refresh is called, the state schema should
            # be locked.
            self.assert_is_only_schema_locked(
                SchemaType.STATE, DirectIngestInstance.PRIMARY
            )

        mock_federated_refresh.side_effect = mock_federated_refresh_fn

        route = "/refresh_bq_dataset"
        data = json.dumps(
            {
                "schema_type": SchemaType.STATE.value,
                "ingest_instance": DirectIngestInstance.PRIMARY.value,
            }
        )

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
        mock_federated_refresh.assert_called_with(
            schema_type=SchemaType.STATE,
            direct_ingest_instance=DirectIngestInstance.PRIMARY,
            dataset_override_prefix=None,
        )
        mock_get_current_cloud_task_id.assert_called()
        mock_big_query_client_impl.assert_called()
        self.mock_refresh_bq_datase_success_persister.record_success_in_bq.assert_called_with(
            schema_type=SchemaType.STATE,
            direct_ingest_instance=DirectIngestInstance.PRIMARY,
            dataset_override_prefix=None,
            runtime_sec=mock.ANY,
            cloud_task_id="test_cloud_task_id",
        )

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.BigQueryClientImpl")
    @mock.patch(
        f"{REFRESH_CONTROL_PACKAGE_NAME}.get_current_cloud_task_id",
        return_value="test_cloud_task_id",
    )
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_dataset_state_record_success_in_bq_for_secondary(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_get_current_cloud_task_id: mock.MagicMock,
        mock_big_query_client_impl: mock.MagicMock,
    ) -> None:
        # Grab lock, just like the /create_tasks... endpoint does
        self.mock_lock_manager.acquire_lock(
            "any_lock_id",
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.SECONDARY,
        )

        def mock_federated_refresh_fn(
            # pylint: disable=unused-argument
            schema_type: SchemaType,
            direct_ingest_instance: Optional[DirectIngestInstance] = None,
            dataset_override_prefix: Optional[str] = None,
        ) -> None:
            self.assertTrue(
                self.mock_lock_manager.can_proceed(
                    SchemaType.STATE, DirectIngestInstance.SECONDARY
                )
            )
            # At the moment the federated refresh is called, the state schema should
            # be locked.
            self.assert_is_only_schema_locked(
                SchemaType.STATE, DirectIngestInstance.SECONDARY
            )

        mock_federated_refresh.side_effect = mock_federated_refresh_fn

        route = "/refresh_bq_dataset"
        data = json.dumps(
            {
                "schema_type": SchemaType.STATE.value,
                "ingest_instance": DirectIngestInstance.SECONDARY.value,
                "sandbox_prefix": "test_prefix",
            }
        )

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
        mock_federated_refresh.assert_called_with(
            schema_type=SchemaType.STATE,
            direct_ingest_instance=DirectIngestInstance.SECONDARY,
            dataset_override_prefix="test_prefix",
        )
        mock_get_current_cloud_task_id.assert_called()
        mock_big_query_client_impl.assert_called()
        self.mock_refresh_bq_datase_success_persister.record_success_in_bq.assert_called_with(
            schema_type=SchemaType.STATE,
            direct_ingest_instance=DirectIngestInstance.SECONDARY,
            dataset_override_prefix="test_prefix",
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
            "any_lock_id",
            schema_type=SchemaType.CASE_TRIAGE,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        def mock_federated_refresh_fn(
            # pylint: disable=unused-argument
            schema_type: SchemaType,
            direct_ingest_instance: Optional[DirectIngestInstance] = None,
            dataset_override_prefix: Optional[str] = None,
        ) -> None:
            self.assertTrue(
                self.mock_lock_manager.can_proceed(
                    SchemaType.CASE_TRIAGE, DirectIngestInstance.PRIMARY
                )
            )
            # At the moment the federated refresh is called, the state schema should
            # be locked.
            self.assert_is_only_schema_locked(
                SchemaType.CASE_TRIAGE, DirectIngestInstance.PRIMARY
            )

        mock_federated_refresh.side_effect = mock_federated_refresh_fn

        route = "/refresh_bq_dataset"
        data = json.dumps(
            {
                "schema_type": SchemaType.CASE_TRIAGE.value,
                "ingest_instance": DirectIngestInstance.PRIMARY.value,
            }
        )

        response = self.mock_flask_client.post(
            route,
            data=data,
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_federated_refresh.assert_called_with(
            schema_type=SchemaType.CASE_TRIAGE,
            direct_ingest_instance=None,
            dataset_override_prefix=None,
        )
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
        route = "/refresh_bq_dataset"
        data = json.dumps(
            {
                "schema_type": "GARBAGE_SCHEMA",
                "ingest_instance": DirectIngestInstance.PRIMARY.value,
            }
        )

        response = self.mock_flask_client.post(
            route,
            data=data,
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
        self.assertEqual(
            "'GARBAGE_SCHEMA' is not a valid SchemaType",
            response.data.decode(),
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
        route = "/refresh_bq_dataset"
        data = json.dumps(
            {
                "schema_type": SchemaType.JUSTICE_COUNTS.value,
                "ingest_instance": DirectIngestInstance.PRIMARY.value,
            }
        )

        response = self.mock_flask_client.post(
            route,
            data=data,
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

        route = "/refresh_bq_dataset"
        data = json.dumps(
            {
                "schema_type": SchemaType.STATE.value,
                "ingest_instance": DirectIngestInstance.PRIMARY.value,
            }
        )

        response = self.mock_flask_client.post(
            route,
            data=data,
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
                "any_lock_id",
                schema_type=SchemaType.STATE,
                ingest_instance=DirectIngestInstance.PRIMARY,
            )

            route = "/refresh_bq_dataset"
            data = json.dumps(
                {
                    "schema_type": SchemaType.STATE.value,
                    "ingest_instance": DirectIngestInstance.PRIMARY.value,
                }
            )

            response = self.mock_flask_client.post(
                route,
                data=data,
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

        response = self.mock_flask_client.post(
            "/acquire_lock",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
            data=json.dumps(
                {
                    "lock_id": "test-lock-id",
                    "schema_type": "STATE",
                    "ingest_instance": "PRIMARY",
                }
            ),
        )

        # Assert
        self.assert_is_only_schema_locked(
            SchemaType.STATE, DirectIngestInstance.PRIMARY
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)

    @mock.patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="production"),
    )
    def test_acquire_lock_state_secondary(
        self,
    ) -> None:
        # Act

        response = self.mock_flask_client.post(
            "/acquire_lock",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
            data=json.dumps(
                {
                    "lock_id": "test-lock-id",
                    "schema_type": "STATE",
                    "ingest_instance": "SECONDARY",
                }
            ),
        )

        # Assert
        self.assert_is_only_schema_locked(
            SchemaType.STATE, DirectIngestInstance.SECONDARY
        )

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
            response = self.mock_flask_client.post(
                "/acquire_lock",
                headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
                data=json.dumps(
                    {
                        "lock_id": "test-lock-id",
                        "schema_type": "STATE",
                        "ingest_instance": "PRIMARY",
                    }
                ),
            )

        # Assert
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assert_is_only_schema_locked(
            SchemaType.STATE, DirectIngestInstance.PRIMARY
        )

    def test_check_can_refresh_proceed_state_ingest_locked(
        self,
    ) -> None:
        # Grab lock, just like the /acquire_lock... endpoint does
        self.mock_lock_manager.acquire_lock(
            "any_lock_id",
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        # Arrange
        lock_manager = DirectIngestRegionLockManager(
            region_code="US_XX",
            blocking_locks=[],
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        # Act
        with lock_manager.using_region_lock(expiration_in_seconds=10):
            response = self.mock_flask_client.post(
                "/check_can_refresh_proceed",
                headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
                data=json.dumps(
                    {
                        "schema_type": "STATE",
                        "ingest_instance": "PRIMARY",
                    }
                ),
            )

        # Assert
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertEqual(response.get_data(as_text=True), "False")

    def test_check_can_refresh_proceed_state_ingest_locked_secondary(
        self,
    ) -> None:
        # Grab lock, just like the /acquire_lock... endpoint does
        self.mock_lock_manager.acquire_lock(
            "any_lock_id",
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.SECONDARY,
        )

        # Arrange
        lock_manager = DirectIngestRegionLockManager(
            region_code="US_XX",
            blocking_locks=[],
            ingest_instance=DirectIngestInstance.SECONDARY,
        )

        # Act
        with lock_manager.using_region_lock(expiration_in_seconds=10):
            response = self.mock_flask_client.post(
                "/check_can_refresh_proceed",
                headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
                data=json.dumps(
                    {
                        "schema_type": "STATE",
                        "ingest_instance": "SECONDARY",
                    }
                ),
            )

        # Assert
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertEqual(response.get_data(as_text=True), "False")

    def test_check_can_refresh_proceed_state_can_proceed(
        self,
    ) -> None:
        # Grab lock, just like the /acquire_lock... endpoint does
        self.mock_lock_manager.acquire_lock(
            "any_lock_id",
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        # Arrange
        lock_manager = DirectIngestRegionLockManager(
            region_code="US_XX",
            blocking_locks=[],
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        # Act
        with lock_manager.using_region_lock(expiration_in_seconds=0):
            response = self.mock_flask_client.post(
                "/check_can_refresh_proceed",
                headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
                data=json.dumps(
                    {
                        "schema_type": "STATE",
                        "ingest_instance": "PRIMARY",
                    }
                ),
            )

        # Assert
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertEqual(response.get_data(as_text=True), "True")

    def test_check_can_refresh_proceed_state_can_proceed_secondary(
        self,
    ) -> None:
        # Grab lock, just like the /acquire_lock... endpoint does
        self.mock_lock_manager.acquire_lock(
            "any_lock_id",
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.SECONDARY,
        )

        # Arrange
        lock_manager = DirectIngestRegionLockManager(
            region_code="US_XX",
            blocking_locks=[],
            ingest_instance=DirectIngestInstance.SECONDARY,
        )

        # Act
        with lock_manager.using_region_lock(expiration_in_seconds=0):
            response = self.mock_flask_client.post(
                "/check_can_refresh_proceed",
                headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
                data=json.dumps(
                    {
                        "schema_type": "STATE",
                        "ingest_instance": "SECONDARY",
                    }
                ),
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
        self.mock_lock_manager.acquire_lock(
            "any_lock_id",
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        # Act
        response = self.mock_flask_client.post(
            "/release_lock",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
            data=json.dumps(
                {
                    "schema_type": "STATE",
                    "ingest_instance": "PRIMARY",
                }
            ),
        )

        # Assert
        self.assertFalse(
            self.mock_lock_manager.is_locked(
                SchemaType.STATE, DirectIngestInstance.PRIMARY
            )
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)

    @mock.patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="production"),
    )
    def test_release_lock_state_secondary(
        self,
    ) -> None:
        # Grab lock, just like the /create_tasks... endpoint does
        self.mock_lock_manager.acquire_lock(
            "any_lock_id",
            schema_type=SchemaType.STATE,
            ingest_instance=DirectIngestInstance.SECONDARY,
        )

        # Act
        response = self.mock_flask_client.post(
            "/release_lock",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
            data=json.dumps(
                {
                    "schema_type": "STATE",
                    "ingest_instance": "SECONDARY",
                }
            ),
        )

        # Assert
        self.assertFalse(
            self.mock_lock_manager.is_locked(
                SchemaType.STATE, DirectIngestInstance.SECONDARY
            )
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)


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
        self.mock_lock_manager = create_autospec(CloudSqlToBQLockManager)
        self.mock_lock_patcher = mock.patch(
            f"{REFRESH_CONTROL_PACKAGE_NAME}.CloudSqlToBQLockManager"
        )
        self.mock_lock_patcher.start().return_value = self.mock_lock_manager

        self.mock_refresh_bq_dataset_persister = create_autospec(
            RefreshBQDatasetSuccessPersister
        )
        self.mock_refresh_bq_dataset_persister_patcher = mock.patch(
            f"{REFRESH_CONTROL_PACKAGE_NAME}.RefreshBQDatasetSuccessPersister"
        )
        self.mock_refresh_bq_dataset_persister_patcher.start().return_value = (
            self.mock_refresh_bq_dataset_persister
        )

    def tearDown(self) -> None:
        self.mock_lock_patcher.stop()
        self.mock_refresh_bq_dataset_persister_patcher.stop()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_to_bq_refresh(
        self, mock_federated_bq: mock.MagicMock
    ) -> None:
        def mock_acquire_lock(
            lock_id: str,
            schema_type: SchemaType,
            ingest_instance: DirectIngestInstance,
        ) -> None:
            return None

        self.mock_lock_manager.acquire_lock.side_effect = mock_acquire_lock

        def mock_can_proceed(
            schema_type: SchemaType, ingest_instance: DirectIngestInstance
        ) -> bool:
            return True

        self.mock_lock_manager.can_proceed.side_effect = mock_can_proceed

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
            cloud_task_id: str,
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

        self.mock_lock_manager.release_lock.assert_called_once_with(
            schema_type=SchemaType.STATE, ingest_instance=DirectIngestInstance.PRIMARY
        )
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
        self.mock_lock_manager.acquire_lock.assert_not_called()
        self.mock_lock_manager.can_proceed.assert_not_called()
        mock_federated_bq.assert_not_called()
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.assert_not_called()
        self.mock_lock_manager.release_lock.assert_not_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_to_bq_refresh_federated_throws(
        self, mock_federated_bq: mock.MagicMock
    ) -> None:
        def mock_acquire_lock(
            lock_id: str,
            schema_type: SchemaType,
            ingest_instance: DirectIngestInstance,
        ) -> None:
            return None

        self.mock_lock_manager.acquire_lock.side_effect = mock_acquire_lock

        def mock_can_proceed(
            schema_type: SchemaType, ingest_instance: DirectIngestInstance
        ) -> bool:
            return True

        self.mock_lock_manager.can_proceed.side_effect = mock_can_proceed

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
        self.mock_lock_manager.acquire_lock.assert_called()
        self.mock_lock_manager.can_proceed.assert_called()
        self.mock_lock_manager.release_lock.assert_called()
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.assert_not_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_to_bq_refresh_bq_success_throws(
        self, mock_federated_bq: mock.MagicMock
    ) -> None:
        def mock_acquire_lock(
            lock_id: str,
            schema_type: SchemaType,
            ingest_instance: DirectIngestInstance,
        ) -> None:
            return None

        self.mock_lock_manager.acquire_lock.side_effect = mock_acquire_lock

        def mock_can_proceed(
            schema_type: SchemaType, ingest_instance: DirectIngestInstance
        ) -> bool:
            return True

        self.mock_lock_manager.can_proceed.side_effect = mock_can_proceed

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
            cloud_task_id: str,
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
        self.mock_lock_manager.acquire_lock.assert_called()
        self.mock_lock_manager.can_proceed.assert_called()
        self.mock_lock_manager.release_lock.assert_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_execute_cloud_sql_to_bq_refresh_lock_acquisition_timeout(
        self, mock_federated_bq: mock.MagicMock
    ) -> None:
        def mock_acquire_lock(
            lock_id: str,
            schema_type: SchemaType,
            ingest_instance: DirectIngestInstance,
        ) -> None:
            return None

        self.mock_lock_manager.acquire_lock.side_effect = mock_acquire_lock

        def mock_can_proceed(
            schema_type: SchemaType, ingest_instance: DirectIngestInstance
        ) -> bool:
            return False

        self.mock_lock_manager.can_proceed.side_effect = mock_can_proceed

        with self.assertRaisesRegex(
            ValueError, r"Could not acquire lock after waiting*"
        ):
            cloud_sql_to_bq_refresh_control.execute_cloud_sql_to_bq_refresh(
                schema_type=SchemaType.STATE,
                ingest_instance=DirectIngestInstance.PRIMARY,
                sandbox_prefix=None,
            )

        self.mock_lock_manager.release_lock.assert_called()
        mock_federated_bq.assert_not_called()
        self.mock_refresh_bq_dataset_persister.record_success_in_bq.assert_not_called()

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
        def mock_acquire_lock(
            lock_id: str,
            schema_type: SchemaType,
            ingest_instance: DirectIngestInstance,
        ) -> None:
            return None

        self.mock_lock_manager.acquire_lock.side_effect = mock_acquire_lock

        def mock_can_proceed(
            schema_type: SchemaType, ingest_instance: DirectIngestInstance
        ) -> bool:
            return True

        self.mock_lock_manager.can_proceed.side_effect = mock_can_proceed

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
            cloud_task_id="AIRFLOW_FEDERATED_REFRESH",
        )
        self.mock_lock_manager.release_lock.assert_called_with(
            schema_type=schema_called_with,
            ingest_instance=instance_called_with,
        )
