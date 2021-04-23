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
""" Tests for data discovery"""
from datetime import date
from io import BytesIO
from typing import List
from unittest import TestCase
from unittest.mock import patch

import fakeredis
import pandas as pd
import pyarrow

from recidiviz.admin_panel.data_discovery.arguments import (
    Condition,
    DataDiscoveryArgs,
    ConditionGroup,
)
from recidiviz.admin_panel.data_discovery.cache_ingest_file_as_parquet import (
    SingleIngestFileParquetCache,
)
from recidiviz.admin_panel.data_discovery.discovery import (
    get_filters,
    collect_file_paths,
    load_dataframe,
)
from recidiviz.admin_panel.data_discovery.file_configs import (
    DataDiscoveryStandardizedFileConfig,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
)


class TestDataDiscovery(TestCase):
    """TestCase for data discovery."""

    def setUp(self) -> None:
        self.fakeredis = fakeredis.FakeRedis()
        self.get_data_discovery_cache_patcher = patch("redis.Redis")
        self.get_data_discovery_cache_patcher.start().return_value = self.fakeredis

    def tearDown(self) -> None:
        self.get_data_discovery_cache_patcher.stop()

    def test_get_filters(self) -> None:
        df = pd.DataFrame(
            data=[[1, 2, 3], [4, 5, 6]],
            columns=["x", "y", "z"],
        )

        parquet_file = BytesIO(df.to_parquet())
        conditions = [
            Condition(column="x", operator="in", values=["1"]),
            Condition(column="y", operator="in", values=["123"]),
            Condition(column="nonexistent_column", operator="in", values=["123"]),
        ]

        self.assertEqual(
            [
                # Values are cast to appropriate data type
                [("x", "in", ["1"]), ("y", "in", ["123"])]
                # Columns that don't exist within the DataFrame / Parquet schema are ignored
            ],
            get_filters(parquet_file, [ConditionGroup(conditions=conditions)]),
        )

    def test_collect_file_paths(self) -> None:
        data_discovery_args = DataDiscoveryArgs(
            region_code="us_id",
            id="123",
            start_date=date.fromisoformat("2021-03-24"),
            end_date=date.fromisoformat("2021-03-25"),
            condition_groups=[],
            raw_files=["ofndr"],
            ingest_views=["mittimus"],
        )

        configs = {
            GcsfsDirectIngestFileType.RAW_DATA: {
                "ofndr": DataDiscoveryStandardizedFileConfig(
                    file_tag="ofndr", primary_keys=["docno"], columns=["docno"]
                )
            },
            GcsfsDirectIngestFileType.INGEST_VIEW: {
                "mittimus": DataDiscoveryStandardizedFileConfig(
                    file_tag="mittimus", primary_keys=["mitt_srl"], columns=["mitt_srl"]
                )
            },
        }

        def assert_length(files: List[str], length: int) -> None:
            self.assertEqual(
                len(collect_file_paths(data_discovery_args, configs, files)), length
            )

        # Miscellaneous files are ignored
        assert_length(["gs://excluded-bucket/file"], 0)

        # Files from outside the date range specified in our arguments are ignored
        assert_length(
            [
                "gs://direct-ingest-state-storage/us_id/raw/2021/03/19/processed_2021-03-19T09:01:56:000000_raw_ofndr.txt"
                "gs://direct-ingest-state-storage/us_id/ingest_view/2021/03/19/processed_2021-03-19T09:01:56:000000_ingest_view_mittimus.txt"
            ],
            0,
        )

        # Files that are not within the collected configs are ignored
        assert_length(
            [
                "gs://direct-ingest-state-storage/us_id/raw/2021/03/24/processed_2021-03-24T09:01:56:000000_raw_NOT_APPLICABLE.txt"
                "gs://direct-ingest-state-storage/us_id/ingest_view/2021/03/24/processed_2021-03-24T09:01:56:000000_ingest_view_RANDOM_VIEW.txt",
            ],
            0,
        )

        # Files within the collected configs and time range are returned
        assert_length(
            [
                "gs://direct-ingest-state-storage/us_id/raw/2021/03/24/processed_2021-03-24T09:01:56:000000_raw_ofndr.txt",
                "gs://direct-ingest-state-storage/us_id/ingest_view/2021/03/24/processed_2021-03-24T09:01:56:000000_ingest_view_mittimus.txt",
            ],
            2,
        )

    def test_load_dataframe(self) -> None:
        file_type = GcsfsDirectIngestFileType.INGEST_VIEW
        file_tag = "test_ingest_view"
        gcs_file = GcsfsFilePath.from_absolute_path("gs://test/test_ingest_view")

        df = pd.DataFrame(data=[["First", 1]], columns=["x", "y"])
        second_df = pd.DataFrame(data=[["Second", 2]], columns=["x", "y"])

        self.fakeredis.rpush(
            SingleIngestFileParquetCache.parquet_cache_key(gcs_file),
            df.to_parquet(),
            second_df.to_parquet(),
        )

        # Without and condition_groups
        file_type, file_tag, dataframe = load_dataframe(
            file_type, file_tag, gcs_file, []
        )

        # It loads all DataFrame chunks
        self.assertEqual(len(dataframe), 2)

        # With condition groups
        condition_groups = [
            ConditionGroup(
                conditions=[
                    Condition(column="x", operator="in", values=["Second"]),
                    Condition(column="y", operator="in", values=["2"]),
                ]
            ),
        ]
        file_type, file_tag, dataframe = load_dataframe(
            file_type, file_tag, gcs_file, condition_groups
        )

        # It applies filters against the dataframe
        self.assertEqual(len(dataframe), 1)
        self.assertEqual(dataframe.iloc[0]["x"], "Second")
        self.assertEqual(dataframe.iloc[0]["y"], 2)
