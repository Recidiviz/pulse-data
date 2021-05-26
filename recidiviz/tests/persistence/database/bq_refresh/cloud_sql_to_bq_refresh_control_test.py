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

from http import HTTPStatus
import json
import unittest
from typing import Optional
from unittest import mock

import flask
from mock import ANY, Mock, create_autospec

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.bq_refresh import cloud_sql_to_bq_refresh_control
from recidiviz.cloud_storage.gcs_pseudo_lock_manager import (
    GCSPseudoLockAlreadyExists,
)
from recidiviz.ingest.direct.controllers.direct_ingest_region_lock_manager import (
    DirectIngestRegionLockManager,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_lock_manager import (
    CloudSqlToBQLockManager,
)
from recidiviz.common.google_cloud.cloud_task_queue_manager import CloudTaskQueueInfo
from recidiviz.ingest.direct import direct_ingest_control
from recidiviz.persistence.database.bq_refresh import cloud_sql_to_bq_refresh_runner
from recidiviz.persistence.database.bq_refresh.bq_refresh_cloud_task_manager import (
    BQRefreshCloudTaskManager,
)
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem

REFRESH_CONTROL_PACKAGE_NAME = cloud_sql_to_bq_refresh_control.__name__
CLOUD_SQL_BQ_EXPORT_RUNNER_PACKAGE_NAME = cloud_sql_to_bq_refresh_runner.__name__
INGEST_CONTROL_PACKAGE_NAME = direct_ingest_control.__name__


class CloudSqlToBQExportControlTest(unittest.TestCase):
    """Tests for cloud_sql_to_bq_refresh_control.py."""

    def setUp(self) -> None:
        self.bq_refresh_patcher = mock.patch(
            f"{CLOUD_SQL_BQ_EXPORT_RUNNER_PACKAGE_NAME}.bq_refresh"
        )
        self.mock_bq_refresh = self.bq_refresh_patcher.start()

        self.client_patcher = mock.patch(
            f"{REFRESH_CONTROL_PACKAGE_NAME}.BigQueryClientImpl"
        )
        self.mock_client = self.client_patcher.start().return_value

        self.cloud_sql_to_gcs_export_patcher = mock.patch(
            f"{CLOUD_SQL_BQ_EXPORT_RUNNER_PACKAGE_NAME}.cloud_sql_to_gcs_export"
        )
        self.mock_cloud_sql_to_gcs_export = self.cloud_sql_to_gcs_export_patcher.start()

        self.fake_table_name = "first_table"

        self.export_config_patcher = mock.patch(
            f"{CLOUD_SQL_BQ_EXPORT_RUNNER_PACKAGE_NAME}.CloudSqlToBQConfig.for_schema_type"
        )
        self.mock_bq_refresh_config_fn = self.export_config_patcher.start()
        self.mock_bq_refresh_config = create_autospec(CloudSqlToBQConfig)
        self.mock_bq_refresh_config_fn.return_value = self.mock_bq_refresh_config

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
        self.bq_refresh_patcher.stop()
        self.client_patcher.stop()
        self.cloud_sql_to_gcs_export_patcher.stop()
        self.export_config_patcher.stop()
        self.fs_patcher.stop()
        self.task_manager_patcher.stop()
        self.pubsub_helper_patcher.stop()

    def assertIsOnlySchemaLocked(self, schema_type: SchemaType) -> None:
        for s in SchemaType:
            if s == schema_type:
                self.assertTrue(self.mock_lock_manager.is_locked(schema_type))
            else:
                self.assertFalse(self.mock_lock_manager.is_locked(s), f"Locked for {s}")

    # TODO(#7397): Delete this test once federated export ships to production.
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.export_table_then_load_table")
    def test_refresh_bq_table(self, mock_export: mock.MagicMock) -> None:
        """Tests that the export is called for a given table and module when
        the /cloud_sql_to_bq/refresh_bq_table endpoint is hit."""
        mock_export.return_value = True

        self.mock_bq_refresh_config.for_schema_type.return_value = (
            self.mock_bq_refresh_config
        )

        table = "fake_table"
        module = SchemaType.JAILS.value
        route = "/refresh_bq_table"
        data = {"table_name": table, "schema_type": module}

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_export.assert_called_with(self.mock_client, table, SchemaType.JAILS)

    # TODO(#7397): Delete this test once federated export ships to production.
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.export_table_then_load_table")
    def test_refresh_bq_table_invalid_module(self, mock_export: mock.MagicMock) -> None:
        """Tests that there is an error when the /cloud_sql_to_bq/refresh_bq_table
        endpoint is hit with an invalid module."""
        mock_export.return_value = True
        table = "fake_table"
        module = "INVALID"
        route = "/refresh_bq_table"
        data = {"table_name": table, "schema_type": module}

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        mock_export.assert_not_called()

    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.kick_all_schedulers")
    @mock.patch(f"{REFRESH_CONTROL_PACKAGE_NAME}.federated_bq_schema_refresh")
    def test_refresh_bq_schema_state(
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
        ):
            self.assertTrue(self.mock_lock_manager.can_proceed(SchemaType.STATE))
            # At the moment the federated refresh is called, the state schema should
            # be locked.
            self.assertIsOnlySchemaLocked(SchemaType.STATE)

        mock_federated_refresh.side_effect = mock_federated_refresh_fn

        self.mock_bq_refresh_config.for_schema_type.return_value = (
            self.mock_bq_refresh_config
        )

        module = SchemaType.STATE.value
        route = f"/refresh_bq_schema/{module}"

        response = self.mock_flask_client.post(
            route,
            data=json.dumps({}),
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_federated_refresh.assert_called_with(schema_type=SchemaType.STATE)
        self.mock_pubsub_helper.publish_message_to_topic.assert_called_with(
            message=mock.ANY, topic="v1.calculator.trigger_daily_pipelines"
        )
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
        ):
            self.assertTrue(self.mock_lock_manager.can_proceed(SchemaType.JAILS))
            # At the moment the federated refresh is called, the state schema should
            # be locked.
            self.assertIsOnlySchemaLocked(SchemaType.JAILS)

        mock_federated_refresh.side_effect = mock_federated_refresh_fn

        self.mock_bq_refresh_config.for_schema_type.return_value = (
            self.mock_bq_refresh_config
        )

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
        route = f"/refresh_bq_schema/{SchemaType.CASE_TRIAGE.value}"

        response = self.mock_flask_client.post(
            route,
            data=json.dumps({}),
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.data.decode(), "Unsuppported schema type: [SchemaType.CASE_TRIAGE]"
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
            region_code=StateCode.US_XX.value, blocking_locks=[]
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
            region_code="US_XX_YYYY", blocking_locks=[]
        )

        # Attempt to refresh while ingest is still locked for a COUNTY
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
        self.assertEqual(response.status_code, HTTPStatus.OK)
        mock_federated_refresh.assert_called()
        self.mock_pubsub_helper.publish_message_to_topic.assert_called()
        mock_kick_all_schedulers.assert_called()

    # TODO(#7397): Delete this test once federated export ships to production.
    @mock.patch(
        f"{INGEST_CONTROL_PACKAGE_NAME}.get_supported_direct_ingest_region_codes"
    )
    @mock.patch(
        "recidiviz.utils.environment.get_gcp_environment", Mock(return_value="staging")
    )
    @mock.patch("recidiviz.utils.regions.get_region", Mock(return_value="us_mo"))
    def test_monitor_refresh_bq_tasks_requeue_unlock_no_publish(
        self,
        mock_supported_region_codes: mock.MagicMock,
    ) -> None:
        """Test that a bq monitor task does not publish topic/message
        with empty topic/message and that it unlocks export lock"""
        mock_supported_region_codes.return_value = []

        schema = "jails"
        topic = ""
        message = ""
        route = "/monitor_refresh_bq_tasks"
        data = {"schema": schema, "topic": topic, "message": message}

        self.mock_lock_manager.acquire_lock(
            lock_id="any_lock_id", schema_type=SchemaType.JAILS
        )

        self.mock_task_manager.get_bq_queue_info.return_value = CloudTaskQueueInfo(
            queue_name="queue_name", task_names=[]
        )

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.mock_task_manager.create_bq_refresh_monitor_task.assert_not_called()
        self.mock_pubsub_helper.publish_message_to_topic.assert_not_called()
        self.assertFalse(self.mock_lock_manager.is_locked(SchemaType.JAILS))

    # TODO(#7397): Delete this test once federated export ships to production.
    @mock.patch(
        f"{INGEST_CONTROL_PACKAGE_NAME}.get_supported_direct_ingest_region_codes"
    )
    @mock.patch(
        "recidiviz.utils.environment.get_gcp_environment", Mock(return_value="staging")
    )
    @mock.patch("recidiviz.utils.regions.get_region", Mock(return_value="us_mo"))
    def test_monitor_refresh_bq_tasks_requeue_with_no_topic_and_message(
        self,
        mock_supported_region_codes: mock.MagicMock,
    ) -> None:
        """Test that a new bq monitor task is added to the queue when there are
        still unfinished tasks on the bq queue, without topic/message to publish."""
        queue_path = "test-queue-path"
        mock_supported_region_codes.return_value = []

        schema = "jails"
        topic = ""
        message = ""
        route = "/monitor_refresh_bq_tasks"
        data = {"schema": schema, "topic": topic, "message": message}

        self.mock_lock_manager.acquire_lock(
            lock_id="any_lock_id", schema_type=SchemaType.JAILS
        )

        self.mock_task_manager.get_bq_queue_info.return_value = CloudTaskQueueInfo(
            queue_name="queue_name",
            task_names=[
                f"{queue_path}/tasks/table_name-123-{schema}",
                f"{queue_path}/tasks/table_name-456-{schema}",
                f"{queue_path}/tasks/table_name-789-{schema}",
            ],
        )

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.mock_task_manager.create_bq_refresh_monitor_task.assert_called_with(
            schema, topic, message
        )
        self.mock_pubsub_helper.publish_message_to_topic.assert_not_called()
        self.assertTrue(self.mock_lock_manager.is_locked(SchemaType.JAILS))

    # TODO(#7397): Delete this test once federated export ships to production.
    @mock.patch(
        f"{INGEST_CONTROL_PACKAGE_NAME}.get_supported_direct_ingest_region_codes"
    )
    @mock.patch(
        "recidiviz.utils.environment.get_gcp_environment", Mock(return_value="staging")
    )
    @mock.patch("recidiviz.utils.regions.get_region", Mock(return_value="us_mo"))
    def test_monitor_refresh_bq_tasks_requeue_with_topic_and_message(
        self,
        mock_supported_region_codes: mock.MagicMock,
    ) -> None:
        """Test that a new bq monitor task is added to the queue when there are
        still unfinished tasks on the bq queue, with topic/message to publish."""
        queue_path = "test-queue-path"
        mock_supported_region_codes.return_value = []

        schema = "jails"
        topic = "fake_topic"
        message = "fake_message"
        route = "/monitor_refresh_bq_tasks"
        data = {"schema": schema, "topic": topic, "message": message}

        self.mock_lock_manager.acquire_lock(
            lock_id="any_lock_id", schema_type=SchemaType.JAILS
        )

        self.mock_task_manager.get_bq_queue_info.return_value = CloudTaskQueueInfo(
            queue_name="queue_name",
            task_names=[
                f"{queue_path}/tasks/table_name-123-{schema}",
                f"{queue_path}/tasks/table_name-456-{schema}",
                f"{queue_path}/tasks/table_name-789-{schema}",
            ],
        )

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.mock_task_manager.create_bq_refresh_monitor_task.assert_called_with(
            schema, topic, message
        )
        self.mock_pubsub_helper.publish_message_to_topic.assert_not_called()
        self.assertTrue(self.mock_lock_manager.is_locked(SchemaType.JAILS))

    # TODO(#7397): Delete this test once federated export ships to production.
    @mock.patch(
        f"{INGEST_CONTROL_PACKAGE_NAME}.get_supported_direct_ingest_region_codes"
    )
    @mock.patch(
        "recidiviz.utils.environment.get_gcp_environment", Mock(return_value="staging")
    )
    @mock.patch("recidiviz.utils.regions.get_region", Mock(return_value="us_mo"))
    def test_monitor_refresh_bq_tasks_requeue_publish(
        self,
        mock_supported_region_codes: mock.MagicMock,
    ) -> None:
        """Test that a new bq monitor task is added to the queue when there are
        still unfinished tasks on the bq queue, with topic/message to publish."""
        queue_path = "test-queue-path"
        mock_supported_region_codes.return_value = []

        schema = SchemaType.STATE.value
        topic = "fake_topic"
        message = "fake_message"
        route = "/monitor_refresh_bq_tasks"
        data = {"schema": schema, "topic": topic, "message": message}

        self.mock_lock_manager.acquire_lock(
            lock_id="any_lock_id", schema_type=SchemaType.STATE
        )

        self.mock_task_manager.get_bq_queue_info.return_value = CloudTaskQueueInfo(
            queue_name="queue_name",
            task_names=[f"{queue_path}/tasks/table_name-123"],
        )

        response = self.mock_flask_client.post(
            route,
            data=json.dumps(data),
            content_type="application/json",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.mock_task_manager.create_bq_refresh_monitor_task.assert_not_called()
        self.mock_pubsub_helper.publish_message_to_topic.assert_called_with(
            message=message, topic=topic
        )
        self.assertFalse(self.mock_lock_manager.is_locked(SchemaType.STATE))

    # TODO(#7397): Delete this test once federated export ships to production.
    @mock.patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="production"),
    )
    def test_legacy_create_refresh_bq_tasks_state(
        self,
    ) -> None:
        # Arrange
        mock_table = Mock()
        mock_table.name = "test_table"
        self.mock_bq_refresh_config.get_tables_to_export.return_value = [mock_table]

        # Act
        response = self.mock_flask_client.get(
            "/create_refresh_bq_tasks/state",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )

        # Assert
        self.assertIsOnlySchemaLocked(SchemaType.STATE)

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.mock_bq_refresh_config.for_schema_type.assert_called_with(SchemaType.STATE)
        self.mock_task_manager.create_refresh_bq_table_task.assert_called_with(
            "test_table", SchemaType.STATE
        )
        self.mock_task_manager.create_bq_refresh_monitor_task.assert_called_with(
            SchemaType.STATE.value, "v1.calculator.trigger_daily_pipelines", ANY
        )

    # TODO(#7397): Change this environment when federated export ships to production.
    @mock.patch(
        "recidiviz.utils.environment.get_gcp_environment",
        Mock(return_value="staging"),
    )
    def test_create_refresh_bq_tasks_state(
        self,
    ) -> None:
        # Arrange
        mock_table = Mock()
        mock_table.name = "test_table"
        self.mock_bq_refresh_config.get_tables_to_export.return_value = [mock_table]

        # Act
        response = self.mock_flask_client.get(
            "/create_refresh_bq_tasks/state",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )

        # Assert
        self.assertIsOnlySchemaLocked(SchemaType.STATE)

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.mock_bq_refresh_config.for_schema_type.assert_called_with(SchemaType.STATE)
        self.mock_task_manager.create_refresh_bq_schema_task.assert_called_with(
            SchemaType.STATE
        )

    def test_create_wait_to_refresh_bq_tasks_state_ingest_locked(
        self,
    ) -> None:
        # Arrange
        mock_table = Mock()
        mock_table.name = "test_table"
        self.mock_bq_refresh_config.get_tables_to_export.return_value = [mock_table]
        lock_manager = DirectIngestRegionLockManager(
            region_code="US_XX", blocking_locks=[]
        )

        # Act
        with lock_manager.using_region_lock(expiration_in_seconds=10):
            response = self.mock_flask_client.get(
                "/create_refresh_bq_tasks/state",
                headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
            )

        # Assert
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.assertIsOnlySchemaLocked(SchemaType.STATE)
        self.mock_task_manager.create_reattempt_create_refresh_tasks_task.assert_called_once()
        self.mock_task_manager.create_refresh_bq_table_task.assert_not_called()
        self.mock_task_manager.create_bq_refresh_monitor_task.assert_not_called()

    def test_create_wait_to_refresh_bq_tasks_state_export_locked(
        self,
    ) -> None:
        # Arrange
        mock_table = Mock()
        mock_table.name = "test_table"
        self.mock_bq_refresh_config.for_schema_type.return_value.get_tables_to_export.return_value = [
            mock_table
        ]
        self.mock_lock_manager.acquire_lock(
            lock_id="any_lock_id", schema_type=SchemaType.STATE
        )

        # Act
        with self.assertRaises(GCSPseudoLockAlreadyExists):
            self.mock_flask_client.get(
                "/create_refresh_bq_tasks/state",
                headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
            )

        self.mock_task_manager.create_refresh_bq_table_task.assert_not_called()
        self.mock_task_manager.create_bq_refresh_monitor_task.assert_not_called()

    def test_create_refresh_bq_tasks_justice_counts(
        self,
    ) -> None:
        # Arrange
        mock_table = Mock()
        mock_table.name = "test_table"
        self.mock_bq_refresh_config.for_schema_type.return_value.get_tables_to_export.return_value = [
            mock_table
        ]

        # Act
        response = self.mock_flask_client.get(
            "/create_refresh_bq_tasks/justice_counts",
            headers={"X-Appengine-Inbound-Appid": "recidiviz-456"},
        )

        # Assert
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
        self.mock_bq_refresh_config.for_schema_type.assert_not_called()
        self.mock_task_manager.create_refresh_bq_table_task.assert_not_called()
