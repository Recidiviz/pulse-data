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
"""Tests for DirectIngestRawFileLoadManager"""
import datetime
import os
from typing import List, Tuple
from unittest.mock import create_autospec, patch

import pandas as pd
from google.cloud.bigquery import LoadJob, LoadJobConfig, TableReference

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_load_manager import (
    DirectIngestRawFileLoadManager,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import LoadPrepSummary
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
from recidiviz.tests.ingest.direct.fixture_util import load_dataframe_from_path
from recidiviz.tests.ingest.direct.raw_data import load_manager_fixtures


class TestDirectIngestRawFileLoadManager(BigQueryEmulatorTestCase):
    """Tests for DirectIngestRawFileLoadManager"""

    def setUp(self) -> None:
        self.load_job_patch = patch(
            "recidiviz.big_query.big_query_client.bigquery.Client.load_table_from_uri"
        )
        self.load_job_mock = self.load_job_patch.start()
        self.load_job_mock.side_effect = self._mock_load

        super().setUp()

        self.region_raw_file_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx", region_module=fake_regions_module
        )
        self.fs = FakeGCSFileSystem()
        self.raw_data_instance = DirectIngestInstance.PRIMARY

        self.manager = DirectIngestRawFileLoadManager(
            raw_data_instance=self.raw_data_instance,
            region_raw_file_config=self.region_raw_file_config,
            fs=DirectIngestGCSFileSystem(self.fs),
        )

        self.bucket = GcsfsBucketPath.from_absolute_path("gs://fake-raw-data-import")
        self.fake_job = create_autospec(LoadJob)

    def tearDown(self) -> None:
        self.load_job_patch.stop()
        super().tearDown()

    def _mock_load(
        self,
        source_uris: List[str],
        destination_table: TableReference,
        *,
        job_config: LoadJobConfig,
    ) -> None:
        address = BigQueryAddress(
            dataset_id=destination_table.dataset_id,
            table_id=destination_table.table_id,
        )
        self.create_mock_table(
            address=address,
            schema=job_config.schema,
        )
        total_rows = 0
        for uri in source_uris:
            data = load_dataframe_from_path(
                self.fs.real_absolute_path_for_path(
                    GcsfsFilePath.from_absolute_path(uri)
                ),
                fixture_columns=None,
            )
            total_rows += data.shape[0]

            self.load_rows_into_table(address, data=data.to_dict("records"))

        self.fake_job.output_rows = total_rows
        return self.fake_job

    def _prep_test(
        self, fixture_directory_name: str
    ) -> Tuple[List[GcsfsFilePath], pd.DataFrame]:
        input_paths, output_path = self._get_paths_for_test(fixture_directory_name)

        input_files = []
        for input_path in input_paths:
            input_file = GcsfsFilePath.from_directory_and_file_name(
                self.bucket, os.path.basename(input_path)
            )
            self.fs.test_add_path(
                input_file,
                input_path,
            )
            input_files.append(input_file)

        return input_files, load_dataframe_from_path(output_path, fixture_columns=None)

    def _get_paths_for_test(
        self,
        fixture_directory_name: str,
    ) -> Tuple[List[str], str]:
        raw_fixture_directory = os.path.join(
            os.path.dirname(load_manager_fixtures.__file__),
            fixture_directory_name,
        )

        ins = []
        out = None
        for file in os.listdir(raw_fixture_directory):
            if "input" in file:
                ins.append(os.path.join(raw_fixture_directory, file))
            if "output" in file:
                out = os.path.join(raw_fixture_directory, file)

        if not isinstance(out, str) or not all(isinstance(_in, str) for _in in ins):
            raise ValueError()

        return ins, out

    def compare_output_against_expected(
        self, summary: LoadPrepSummary, expected_df: pd.DataFrame
    ) -> None:
        actual_df = self.query(
            f"select * from {summary.append_ready_table_address}",
        )

        self.compare_expected_and_result_dfs(expected=expected_df, results=actual_df)

    def test_no_migrations_no_changes_single_file(self) -> None:
        input_paths, output_df = self._prep_test("no_migrations_no_changes_single_file")
        load_prep_summary = self.manager.load_and_prep_paths(
            1,
            "singlePrimaryKey",
            datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
            input_paths,
        )

        assert load_prep_summary.raw_rows_count == 2
        assert (
            load_prep_summary.append_ready_table_address
            == "us_xx_primary_raw_data_temp_load.singlePrimaryKey__1__transformed"
        )

        self.compare_output_against_expected(load_prep_summary, output_df)

    # TODO(#30788) add integration test back once struct in works in emulator
    def _test_migration_depends_on_transforms(self) -> None:
        input_paths, output_df = self._prep_test("migration_depends_on_transforms")
        load_prep_summary = self.manager.load_and_prep_paths(
            1,
            "tagBasicData",
            datetime.datetime(2020, 6, 10, 0, 0, 0, tzinfo=datetime.UTC),
            input_paths,
        )

        self.compare_output_against_expected(load_prep_summary, output_df)
