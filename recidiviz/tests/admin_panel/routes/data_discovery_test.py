# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Implements tests for the data discovery routes."""
import csv
import os
import uuid
from http import HTTPStatus
from io import StringIO
from typing import Dict
from unittest import TestCase, mock
from unittest.mock import patch, create_autospec, MagicMock

import fakeredis
import gcsfs
import pandas
from flask import Flask, Blueprint

from recidiviz.admin_panel.data_discovery.cache_ingest_file_as_parquet import (
    SingleIngestFileParquetCache,
)
from recidiviz.admin_panel.routes import data_discovery
from recidiviz.admin_panel.routes.data_discovery import add_data_discovery_routes
from recidiviz.cloud_memorystore.redis_communicator import (
    RedisCommunicator,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath


fixture_path = os.path.join(
    os.path.dirname(__file__),
    "fixtures/data_discovery_malformed_csv.csv",
)


class TestDataDiscoveryRoutes(TestCase):
    """TestCase for data discovery routes."""

    def setUp(self) -> None:
        self.test_app = Flask(__name__)
        blueprint = Blueprint("data_discovery_test", __name__)
        self.test_client = self.test_app.test_client()
        self.fakeredis = fakeredis.FakeRedis()
        self.files: Dict[str, str] = {}
        self.mock_gcsfs = create_autospec(gcsfs.GCSFileSystem)
        self.mock_gcsfs.open = lambda path, encoding, token: StringIO(self.files[path])

        self.project_id_patcher = patch(
            "recidiviz.admin_panel.routes.data_discovery.project_id",
            return_value="recidiviz-456",
        )
        self.project_number_patcher = patch(
            "recidiviz.utils.metadata.project_number", return_value=999
        )
        self.requires_gae_auth_patcher = patch(
            "recidiviz.admin_panel.routes.data_discovery.requires_gae_auth",
            side_effect=lambda route: route,
        )
        self.redis_patcher = patch("redis.Redis", return_value=self.fakeredis)

        self.gcs_factory_patcher = mock.patch(
            "gcsfs.GCSFileSystem",
            return_value=self.mock_gcsfs,
        )

        self.gcs_factory_patcher.start()
        self.project_id_patcher.start()
        self.project_number_patcher.start()
        self.redis_patcher.start()
        self.requires_gae_auth_patcher.start()

        add_data_discovery_routes(blueprint)
        self.test_app.register_blueprint(blueprint)

    def tearDown(self) -> None:
        self.gcs_factory_patcher.stop()
        self.project_id_patcher.stop()
        self.project_number_patcher.stop()
        self.redis_patcher.start()
        self.requires_gae_auth_patcher.stop()

    def cache_ingest_file(
        self, path: GcsfsFilePath, csv_text: str, separator: str = ","
    ) -> None:
        self.files[path.uri()] = csv_text
        response = self.test_client.post(
            "/data_discovery/cache_ingest_file_as_parquet_task",
            json={
                "gcs_file_uri": path.uri(),
                "file_encoding": "UTF-8",
                "file_separator": separator,
                "file_quoting": csv.QUOTE_MINIMAL,
            },
        )
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

    def test_cache_ingest_file_as_parquet(self) -> None:
        path = GcsfsFilePath(
            bucket_name="test-bucket",
            blob_name="storage_bucket/raw/2021/04/20/processed_2021-05-03T00:00:00:000000_raw_test_file-(1).csv",
        )
        input_df = pandas.DataFrame(data=[[1, 2], [2, 3]], columns=["x", "y"])

        self.cache_ingest_file(path, input_df.to_csv(index=False))

        cache = SingleIngestFileParquetCache(self.fakeredis, path)
        self.assertEqual(1, self.fakeredis.llen(cache.cache_key))

        expected = pandas.DataFrame(
            data=[
                ["1", "2", "05/03/21"],
                ["2", "3", "05/03/21"],
            ],
            columns=["x", "y", "ingest_processing_date"],
        )
        actual = [
            pandas.read_parquet(parquet_file)
            for parquet_file in cache.get_parquet_files()
        ][0]

        self.assertTrue(expected.compare(actual).empty)

    def test_cache_ingest_file_as_parquet_malformed(self) -> None:
        path = GcsfsFilePath(
            bucket_name="test-bucket",
            blob_name="storage_bucket/raw/2021/04/20/processed_2021-05-03T00:00:00:000000_raw_test_file-(1).csv",
        )

        with open(fixture_path, "r") as f:
            self.cache_ingest_file(path, f.read(), separator="|")

        cache = SingleIngestFileParquetCache(self.fakeredis, path)
        self.assertEqual(1, self.fakeredis.llen(cache.cache_key))

        expected = pandas.DataFrame(
            data=[["val1", "val2", "", "val4", "", "05/03/21"]],
            columns=[
                "col1",
                "col2",
                "col3",
                "col4",
                "col5",
                "ingest_processing_date",
            ],
        )
        actual = [
            pandas.read_parquet(parquet_file)
            for parquet_file in cache.get_parquet_files()
        ][0]

        self.assertTrue(expected.compare(actual).empty, expected.compare(actual))

    def test_discovery_task(self) -> None:
        discovery_uuid = uuid.uuid4()
        response = self.test_client.post(
            "/data_discovery/discovery_task", json={"discovery_id": discovery_uuid}
        )

        self.assertEqual(HTTPStatus.OK, response.status_code)

    @patch.object(data_discovery, "DevelopmentAdminPanelDataDiscoveryCloudTaskManager")
    def test_create_discovery(self, _mock_task_manager: MagicMock) -> None:
        response = self.test_client.post(
            "/data_discovery/create_discovery",
            json={
                "region_code": "us_id",
                "start_date": "2021/04/01",
                "end_date": "2021/04/07",
                "raw_files": ["mittimus"],
                "ingest_views": [
                    "mittimus_judge_sentence_offense_sentprob_supervision_sentences"
                ],
                "condition_groups": [
                    {
                        "conditions": [
                            {"column": "docno", "operator": "=", "value": "999"}
                        ]
                    }
                ],
            },
        )
        self.assertEqual(HTTPStatus.OK, response.status_code)
        self.assertIsNotNone(response.get_json()["id"])

    def test_discovery_status(self) -> None:
        communicator = RedisCommunicator.create(self.fakeredis, max_messages=2)
        first_message = communicator.communicate("A message!")
        second_message = communicator.communicate("A new message!")

        response = self.test_client.post(
            "/data_discovery/discovery_status",
            json={
                "discovery_id": communicator.channel_uuid,
                "message_cursor": first_message.cursor,
            },
        )
        self.assertEqual(HTTPStatus.OK, response.status_code)
        self.assertEqual(second_message.to_json(), response.get_json())

    def test_files(self) -> None:
        response = self.test_client.post(
            "/data_discovery/files", json={"region_code": "us_id"}
        )
        self.assertEqual(HTTPStatus.OK, response.status_code)
