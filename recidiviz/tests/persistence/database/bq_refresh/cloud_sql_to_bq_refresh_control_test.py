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
from typing import Optional
from unittest import mock

import flask
from mock import Mock, create_autospec

from recidiviz.calculator.pipeline.pipeline_type import MetricPipelineRunType
from recidiviz.cloud_functions.cloudsql_to_bq_refresh_utils import (
    PIPELINE_RUN_TYPE_HISTORICAL_VALUE,
    PIPELINE_RUN_TYPE_NONE_VALUE,
    PIPELINE_RUN_TYPE_REQUEST_ARG,
    UPDATE_MANAGED_VIEWS_REQUEST_ARG,
)
from recidiviz.cloud_storage.gcs_pseudo_lock_manager import GCSPseudoLockAlreadyExists
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import direct_ingest_control
from recidiviz.ingest.direct.controllers.direct_ingest_region_lock_manager import (
    DirectIngestRegionLockManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.bq_refresh import cloud_sql_to_bq_refresh_control
from recidiviz.persistence.database.bq_refresh.bq_refresh_cloud_task_manager import (
    BQRefreshCloudTaskManager,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_lock_manager import (
    CloudSqlToBQLockManager,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem

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

        self.task_manager_patcher = mock.patch(
            f"{REFRESH_CONTROL_PACKAGE_NAME}.BQRefreshCloudTaskManager",
        )
        self.mock_task_manager = create_autospec(BQRefreshCloudTaskManager)
        self.mock_task_manager_fn = self.task_manager_patcher.start()
        self.mock_task_manager_fn.return_value = self.mock_task_manager

        self.pubsub_helper_patcher = mock.patch(
            f"{REFRESH_CONTROL_PACKAGE_NAME}.pubsub_helper"
        )
        self.mock_pubsub_helper = self.pubsub_helper_patcher.start()

    def tearDown(self) -> None:
        self.mock_project_id_patcher.stop()
        self.mock_project_number_patcher.stop()
        self.fs_patcher.stop()
        self.task_manager_patcher.stop()
        self.pubsub_helper_patcher.stop()

    def assertIsOnlySchemaLocked(self, schema_type: SchemaType) -> None:
        for s in SchemaType:
            if s == schema_type:
                self.assertTrue(self.mock_lock_manager.is_locked(schema_type))
            else:
                self.assertFalse(self.mock_lock_manager.is_locked(s), f"Locked for {s}")

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.kick_all_schedulers")
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_schema_state_incremental(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_kick_all_schedulers: mock.MagicMock,
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
        route = f"/refresh_bq_schema/{module}"
        data = json.dumps(
            {PIPELINE_RUN_TYPE_REQUEST_ARG: MetricPipelineRunType.INCREMENTAL.value}
        )

        response = self.mock_flask_client.post(
            route,
            data=data,
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_federated_refresh.assert_called_with(schema_type=SchemaType.STATE)
        self.mock_pubsub_helper.publish_message_to_topic.assert_called_with(
            message=mock.ANY, topic="v1.calculator.trigger_incremental_pipelines"
        )
        self.assertFalse(self.mock_lock_manager.is_locked(SchemaType.STATE))
        mock_kick_all_schedulers.assert_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.kick_all_schedulers")
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_schema_state_historical(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_kick_all_schedulers: mock.MagicMock,
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
        route = f"/refresh_bq_schema/{module}"
        data = json.dumps(
            {PIPELINE_RUN_TYPE_REQUEST_ARG: MetricPipelineRunType.HISTORICAL.value}
        )

        response = self.mock_flask_client.post(
            route,
            data=data,
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_federated_refresh.assert_called_with(schema_type=SchemaType.STATE)
        self.mock_pubsub_helper.publish_message_to_topic.assert_called_with(
            message=mock.ANY, topic="v1.calculator.trigger_historical_pipelines"
        )
        self.mock_task_manager.create_update_managed_views_task.assert_not_called()
        self.assertFalse(self.mock_lock_manager.is_locked(SchemaType.STATE))
        mock_kick_all_schedulers.assert_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.kick_all_schedulers")
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_schema_state_update_managed_views(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_kick_all_schedulers: mock.MagicMock,
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
        route = f"/refresh_bq_schema/{module}"
        data = json.dumps(
            {
                UPDATE_MANAGED_VIEWS_REQUEST_ARG: "true",
                PIPELINE_RUN_TYPE_REQUEST_ARG: PIPELINE_RUN_TYPE_NONE_VALUE,
            }
        )

        response = self.mock_flask_client.post(
            route,
            data=data,
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_federated_refresh.assert_called_with(schema_type=SchemaType.STATE)
        self.mock_pubsub_helper.publish_message_to_topic.assert_not_called()
        self.mock_task_manager.create_update_managed_views_task.assert_called()
        self.assertFalse(self.mock_lock_manager.is_locked(SchemaType.STATE))
        mock_kick_all_schedulers.assert_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.kick_all_schedulers")
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_schema_state_update_managed_views_trigger_historical(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_kick_all_schedulers: mock.MagicMock,
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
        route = f"/refresh_bq_schema/{module}"
        data = json.dumps(
            {
                UPDATE_MANAGED_VIEWS_REQUEST_ARG: "true",
                PIPELINE_RUN_TYPE_REQUEST_ARG: MetricPipelineRunType.HISTORICAL.value,
            }
        )

        response = self.mock_flask_client.post(
            route,
            data=data,
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_federated_refresh.assert_called_with(schema_type=SchemaType.STATE)
        self.mock_pubsub_helper.publish_message_to_topic.assert_called_with(
            message=mock.ANY, topic="v1.calculator.trigger_historical_pipelines"
        )
        self.mock_task_manager.create_update_managed_views_task.assert_called()
        self.assertFalse(self.mock_lock_manager.is_locked(SchemaType.STATE))
        mock_kick_all_schedulers.assert_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.kick_all_schedulers")
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_schema_jails(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_kick_all_schedulers: mock.MagicMock,
    ) -> None:
        # Grab lock, just like the /create_tasks... endpoint does
        self.mock_lock_manager.acquire_lock("any_lock_id", schema_type=SchemaType.JAILS)

        def mock_federated_refresh_fn(
            # pylint: disable=unused-argument
            schema_type: SchemaType,
            dataset_override_prefix: Optional[str] = None,
        ) -> None:
            self.assertTrue(self.mock_lock_manager.can_proceed(SchemaType.JAILS))
            # At the moment the federated refresh is called, the state schema should
            # be locked.
            self.assertIsOnlySchemaLocked(SchemaType.JAILS)

        mock_federated_refresh.side_effect = mock_federated_refresh_fn

        module = SchemaType.JAILS.value
        route = f"/refresh_bq_schema/{module}"

        response = self.mock_flask_client.post(
            route,
            data=json.dumps({}),
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_federated_refresh.assert_called_with(schema_type=SchemaType.JAILS)
        self.mock_pubsub_helper.publish_message_to_topic.assert_not_called()
        self.assertFalse(self.mock_lock_manager.is_locked(SchemaType.JAILS))
        mock_kick_all_schedulers.assert_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.kick_all_schedulers")
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_schema_garbage_schema(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_kick_all_schedulers: mock.MagicMock,
    ) -> None:
        route = "/refresh_bq_schema/GARBAGE_SCHEMA"

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
        self.mock_pubsub_helper.publish_message_to_topic.assert_not_called()
        mock_kick_all_schedulers.assert_not_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.kick_all_schedulers")
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_schema_unsupported_schema(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_kick_all_schedulers: mock.MagicMock,
    ) -> None:
        route = f"/refresh_bq_schema/{SchemaType.JUSTICE_COUNTS.value}"

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
        self.mock_pubsub_helper.publish_message_to_topic.assert_not_called()
        mock_kick_all_schedulers.assert_not_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.kick_all_schedulers")
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_schema_not_locked(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_kick_all_schedulers: mock.MagicMock,
    ) -> None:
        # Do not grab lock
        # self.mock_lock_manager.acquire_lock("any_lock_id", schema_type=SchemaType.JAILS)

        route = f"/refresh_bq_schema/{SchemaType.JAILS.value}"

        response = self.mock_flask_client.post(
            route,
            data=json.dumps({}),
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(response.status_code, HTTPStatus.EXPECTATION_FAILED)
        self.assertEqual(
            response.data.decode(),
            "Expected lock for [JAILS] BQ refresh to already exist.",
        )
        mock_federated_refresh.assert_not_called()
        self.mock_pubsub_helper.publish_message_to_topic.assert_not_called()
        mock_kick_all_schedulers.assert_not_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.kick_all_schedulers")
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_schema_ingest_still_locked(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_kick_all_schedulers: mock.MagicMock,
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

            route = f"/refresh_bq_schema/{SchemaType.STATE.value}"

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
        self.mock_pubsub_helper.publish_message_to_topic.assert_not_called()
        mock_kick_all_schedulers.assert_not_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.kick_all_schedulers")
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_schema_ingest_locked_other_schema(
        self,
        mock_federated_refresh: mock.MagicMock,
        mock_kick_all_schedulers: mock.MagicMock,
    ) -> None:
        ingest_lock_manager = DirectIngestRegionLockManager(
            region_code="US_XX_YYYY",
            blocking_locks=[],
            ingest_instance=DirectIngestInstance.PRIMARY,
        )

        # Attempt to refresh while ingest is still locked for a COUNTY
        with ingest_lock_manager.using_region_lock(expiration_in_seconds=10):
            # Grab lock, just like the /create_tasks... endpoint does
            self.mock_lock_manager.acquire_lock(
                "any_lock_id", schema_type=SchemaType.STATE
            )

            route = f"/refresh_bq_schema/{SchemaType.STATE.value}"

            data = json.dumps(
                {PIPELINE_RUN_TYPE_REQUEST_ARG: MetricPipelineRunType.HISTORICAL.value}
            )

            response = self.mock_flask_client.post(
                route,
                data=data,
                content_type="application/json",
                headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
            )
        self.assertEqual(HTTPStatus.OK, response.status_code)
        mock_federated_refresh.assert_called()
        self.mock_pubsub_helper.publish_message_to_topic.assert_called()
        mock_kick_all_schedulers.assert_called()

    @mock.patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="production"),
    )
    def test_create_refresh_bq_schema_task_state_incremental(
        self,
    ) -> None:
        # Act
        body = {PIPELINE_RUN_TYPE_REQUEST_ARG: MetricPipelineRunType.INCREMENTAL.value}
        data = json.dumps(body)

        response = self.mock_flask_client.get(
            "/create_refresh_bq_schema_task/state",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
            data=data,
        )

        # Assert
        self.assertIsOnlySchemaLocked(SchemaType.STATE)

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.mock_task_manager.create_refresh_bq_schema_task.assert_called_with(
            SchemaType.STATE,
            body,
        )

    @mock.patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="production"),
    )
    def test_create_refresh_bq_schema_task_state_incremental_default(
        self,
    ) -> None:
        # Act
        response = self.mock_flask_client.get(
            "/create_refresh_bq_schema_task/state",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )

        expected_task_body = {
            PIPELINE_RUN_TYPE_REQUEST_ARG: MetricPipelineRunType.INCREMENTAL.value
        }

        # Assert
        self.assertIsOnlySchemaLocked(SchemaType.STATE)

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.mock_task_manager.create_refresh_bq_schema_task.assert_called_with(
            SchemaType.STATE,
            expected_task_body,
        )

    @mock.patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="production"),
    )
    def test_create_refresh_bq_schema_task_state_historical(
        self,
    ) -> None:
        # Act
        body = {PIPELINE_RUN_TYPE_REQUEST_ARG: MetricPipelineRunType.HISTORICAL.value}
        data = json.dumps(body)

        response = self.mock_flask_client.get(
            "/create_refresh_bq_schema_task/state",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
            data=data,
        )

        # Assert
        self.assertIsOnlySchemaLocked(SchemaType.STATE)

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.mock_task_manager.create_refresh_bq_schema_task.assert_called_with(
            SchemaType.STATE,
            body,
        )

    @mock.patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="production"),
    )
    def test_create_refresh_bq_schema_task_state_garbage_run_type(
        self,
    ) -> None:
        # Act
        body = {PIPELINE_RUN_TYPE_REQUEST_ARG: "GARBAGE"}
        data = json.dumps(body)

        response = self.mock_flask_client.get(
            "/create_refresh_bq_schema_task/state",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
            data=data,
        )

        # Assert
        self.assertEqual(HTTPStatus.BAD_REQUEST, response.status_code)
        self.mock_task_manager.create_reattempt_create_refresh_tasks_task.assert_not_called()
        self.mock_task_manager.create_refresh_bq_schema_task.assert_not_called()

    def test_create_wait_to_refresh_bq_tasks_state_ingest_locked(
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
                "/create_refresh_bq_schema_task/state",
                headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
            )

        # Assert
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertIsOnlySchemaLocked(SchemaType.STATE)
        self.mock_task_manager.create_reattempt_create_refresh_tasks_task.assert_called_once()
        self.mock_task_manager.create_refresh_bq_schema_task.assert_not_called()

    def test_create_wait_to_refresh_bq_tasks_state_export_locked(
        self,
    ) -> None:
        # Arrange
        self.mock_lock_manager.acquire_lock(
            lock_id="any_lock_id", schema_type=SchemaType.STATE
        )

        # Act
        with self.assertRaises(GCSPseudoLockAlreadyExists):
            self.mock_flask_client.get(
                "/create_refresh_bq_schema_task/state",
                headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
            )

        self.mock_task_manager.create_refresh_bq_schema_task.assert_not_called()

    def test_create_refresh_bq_schema_task_justice_counts(
        self,
    ) -> None:
        # Act
        response = self.mock_flask_client.get(
            "/create_refresh_bq_schema_task/justice_counts",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )

        # Assert
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.mock_task_manager.create_refresh_bq_schema_task.assert_not_called()

    def test_pipeline_run_type_historical_value(self) -> None:
        self.assertEqual(
            PIPELINE_RUN_TYPE_HISTORICAL_VALUE,
            MetricPipelineRunType.HISTORICAL.value.lower(),
        )
