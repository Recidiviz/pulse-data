# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for supplemental_dataset_manager.py"""
import datetime
import unittest
import uuid
from http import HTTPStatus
from typing import Dict, List
from unittest.mock import MagicMock, create_autospec, patch

from flask import Flask
from freezegun import freeze_time
from parameterized import parameterized

from recidiviz.calculator.supplemental import supplemental_dataset_manager
from recidiviz.calculator.supplemental.supplemental_dataset import (
    StateSpecificSupplementalDatasetGenerator,
    SupplementalDatasetTable,
)
from recidiviz.common.google_cloud.cloud_task_queue_manager import CloudTaskQueueManager
from recidiviz.tests.calculator.supplemental.supplemental_dataset_test import (
    UsXxSupplementalDatasetTable,
)

APP_ENGINE_HEADERS = {
    "X-Appengine-Cron": "test-cron",
    "X-AppEngine-TaskName": "my-task-id",
}


class TestSupplementalDatasetGenerator(StateSpecificSupplementalDatasetGenerator):
    def __init__(
        self, supplemental_dataset_tables: List[SupplementalDatasetTable]
    ) -> None:
        with patch(
            "recidiviz.calculator.supplemental.supplemental_dataset.BigQueryClientImpl",
            MagicMock,
        ) as _:
            super().__init__(supplemental_dataset_tables)


@patch("recidiviz.utils.metadata.project_number", MagicMock(return_value="12345678"))
@patch(
    "recidiviz.utils.metadata.project_id", MagicMock(return_value="recidiviz-project")
)
class TestSupplementalDatasetManager(unittest.TestCase):
    """Tests the requests to the Supplemental Dataset API"""

    def setUp(self) -> None:
        app = Flask(__name__)
        app.register_blueprint(
            supplemental_dataset_manager.supplemental_dataset_manager_blueprint
        )
        app.config["TESTING"] = True
        self.client = app.test_client()

        self.cloud_task_manager_patcher = patch(
            "recidiviz.calculator.supplemental.supplemental_dataset_manager.CloudTaskQueueManager"
        )
        self.mock_cloud_task_manager = create_autospec(CloudTaskQueueManager)
        self.cloud_task_manager_patcher.start().return_value = (
            self.mock_cloud_task_manager
        )

    def tearDown(self) -> None:
        self.cloud_task_manager_patcher.stop()

    @patch(
        "recidiviz.calculator.supplemental.supplemental_dataset_manager.SupplementalDatasetGeneratorFactory.build",
        MagicMock(
            return_value=TestSupplementalDatasetGenerator(
                [UsXxSupplementalDatasetTable()]
            )
        ),
    )
    @patch(
        "recidiviz.calculator.supplemental.supplemental_dataset.rematerialize_views_for_view_builders",
        MagicMock(side_effect=None),
    )
    def test_generate_supplemental_dataset(self) -> None:
        response = self.client.post(
            "/generate_supplemental_dataset",
            query_string={"region": "us_xx", "table_id": "test_table"},
            headers=APP_ENGINE_HEADERS,
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)

    @parameterized.expand(
        [
            ("missing table id", {"region": "US_XX"}),
            ("missing region", {"table_id": "table"}),
        ]
    )
    def test_generate_supplemental_dataset_bad_request_due_to_bad_params(
        self, _name: str, query_string: Dict[str, str]
    ) -> None:
        response = self.client.post(
            "/generate_supplemental_dataset",
            query_string=query_string,
            headers=APP_ENGINE_HEADERS,
        )
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)

    def test_generate_supplemental_dataset_bad_request_due_to_nonexistent_region_generator(
        self,
    ) -> None:
        response = self.client.post(
            "/generate_supplemental_dataset",
            query_string={"region": "us_xx", "table_id": "test_table"},
            headers=APP_ENGINE_HEADERS,
        )
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)

    @patch(
        "recidiviz.calculator.supplemental.supplemental_dataset_manager.SupplementalDatasetGeneratorFactory.build",
        MagicMock(
            return_value=TestSupplementalDatasetGenerator(
                [UsXxSupplementalDatasetTable()]
            )
        ),
    )
    @patch(
        "recidiviz.calculator.supplemental.supplemental_dataset.rematerialize_views_for_view_builders",
        MagicMock(side_effect=RuntimeError),
    )
    def test_generate_supplemental_dataset_captures_internal_errors(self) -> None:
        response = self.client.post(
            "/generate_supplemental_dataset",
            query_string={"region": "us_xx", "table_id": "test_table"},
            headers=APP_ENGINE_HEADERS,
        )
        self.assertEqual(response.status_code, HTTPStatus.INTERNAL_SERVER_ERROR)

    @freeze_time("2022-01-26 00:00")
    @patch("uuid.uuid4")
    def test_handle_supplemental_dataset_generation(self, mock_uuid: MagicMock) -> None:
        mock_uuid.side_effect = [uuid.UUID("504ad25e-1b61-4263-9de0-bad0ae1dfd1b")]
        response = self.client.post(
            "/handle_supplemental_dataset_generation",
            query_string={"region": "US_XX", "table_id": "test_table"},
            headers=APP_ENGINE_HEADERS,
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        str_date = str(
            datetime.datetime(
                2022, 1, 26, 0, 0, 0, 0, tzinfo=datetime.timezone.utc
            ).date()
        )
        self.mock_cloud_task_manager.create_task.assert_called_with(
            task_id=f"generate_supplemental_dataset-{str_date}-504ad25e-1b61-4263-9de0-bad0ae1dfd1b",
            relative_uri="/supplemental_dataset/generate_supplemental_dataset?region=us_xx&table_id=test_table",
            body={},
        )

    @freeze_time("2022-01-26 00:00")
    @patch("uuid.uuid4")
    def test_handle_supplemental_dataset_generation_with_dataset_override(
        self, mock_uuid: MagicMock
    ) -> None:
        mock_uuid.side_effect = [uuid.UUID("504ad25e-1b61-4263-9de0-bad0ae1dfd1b")]
        response = self.client.post(
            "/handle_supplemental_dataset_generation",
            query_string={
                "region": "US_XX",
                "table_id": "test_table",
                "dataset_override": "dataset",
            },
            headers=APP_ENGINE_HEADERS,
        )
        self.assertEqual(response.status_code, HTTPStatus.OK)
        str_date = str(
            datetime.datetime(
                2022, 1, 26, 0, 0, 0, 0, tzinfo=datetime.timezone.utc
            ).date()
        )
        self.mock_cloud_task_manager.create_task.assert_called_with(
            task_id=f"generate_supplemental_dataset-{str_date}-504ad25e-1b61-4263-9de0-bad0ae1dfd1b",
            relative_uri="/supplemental_dataset/generate_supplemental_dataset?region=us_xx&table_id=test_table&dataset_override=dataset",
            body={},
        )

    @parameterized.expand(
        [
            ("missing table id", {"region": "US_XX"}),
            ("missing region", {"table_id": "table"}),
        ]
    )
    def test_handle_supplemental_dataset_generation_bad_request(
        self, _name: str, query_string: Dict[str, str]
    ) -> None:
        response = self.client.post(
            "/handle_supplemental_dataset_generation",
            query_string=query_string,
            headers=APP_ENGINE_HEADERS,
        )
        self.assertEqual(response.status_code, HTTPStatus.BAD_REQUEST)
