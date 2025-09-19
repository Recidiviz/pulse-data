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
import re
from typing import Any, List, Tuple
from unittest.mock import MagicMock, create_autospec, patch

import pandas as pd
from google.cloud.bigquery import LoadJob, LoadJobConfig, TableReference

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath, GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_load_manager import (
    DirectIngestRawFileLoadManager,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder import (
    RawDataTableBigQuerySchemaBuilder,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_blocking_validation import (
    RawDataImportBlockingValidationError,
)
from recidiviz.ingest.direct.types.raw_data_import_types import (
    AppendReadyFile,
    AppendSummary,
    ImportReadyFile,
    RawFileBigQueryLoadConfig,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
from recidiviz.tests.ingest.direct.fixture_util import load_dataframe_from_path
from recidiviz.tests.ingest.direct.raw_data import load_manager_fixtures
from recidiviz.utils.encoding import to_python_standard


class TestDirectIngestRawFileLoadManager(BigQueryEmulatorTestCase):
    """Tests for DirectIngestRawFileLoadManager"""

    def setUp(self) -> None:
        self.load_job_patch = patch(
            "recidiviz.big_query.big_query_client.bigquery.Client.load_table_from_uri"
        )
        self.load_job_mock = self.load_job_patch.start()
        self.load_job_mock.side_effect = self._mock_load

        self.pruning_patches = [
            patch(path)
            for path in [
                "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_load_manager.automatic_raw_data_pruning_enabled_for_state_and_instance",
                "recidiviz.ingest.direct.views.raw_data_diff_query_builder.automatic_raw_data_pruning_enabled_for_state_and_instance",
                "recidiviz.ingest.direct.views.raw_table_query_builder.automatic_raw_data_pruning_enabled_for_state_and_instance",
            ]
        ]
        self.pruning_mocks = [p.start() for p in self.pruning_patches]
        self._set_pruning_mocks(False)

        super().setUp()

        self.region_raw_file_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx", region_module=fake_regions_module
        )
        self.fs = FakeGCSFileSystem()
        self.raw_data_instance = DirectIngestInstance.PRIMARY

        self.manager = DirectIngestRawFileLoadManager(
            raw_data_instance=self.raw_data_instance,
            region_raw_file_config=self.region_raw_file_config,
            fs=self.fs,
        )

        self.bucket = GcsfsBucketPath.from_absolute_path("gs://fake-raw-data-import")
        self.fake_job = create_autospec(LoadJob)

    def tearDown(self) -> None:
        self.load_job_patch.stop()
        for p in self.pruning_patches:
            p.stop()
        super().tearDown()

    def _set_pruning_mocks(self, b: bool) -> None:
        for m in self.pruning_mocks:
            m.return_value = b

    def _mock_fail(self, *_: Any, **__: Any) -> None:
        raise ValueError("We hit an error!")

    def _mock_load(
        self,
        source_uris: List[str],
        destination_table: TableReference,
        *,
        job_config: LoadJobConfig,
    ) -> None:
        """Mocks loading to Big Query by loading data into a DataFrame
        and then loading that DataFrame into a BQ emulator table"""
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
                encoding=to_python_standard(job_config.encoding),
                separator=job_config.field_delimiter,
            )
            total_rows += data.shape[0]

            self.load_rows_into_table(address, data=data.to_dict("records"))

        self.fake_job.output_rows = total_rows
        return self.fake_job

    def _prep_test(
        self, fixture_directory_name: str
    ) -> Tuple[str, List[GcsfsFilePath], pd.DataFrame, pd.DataFrame, BigQueryAddress]:
        """preps test for |fixture_directory_name|"""
        (
            file_tag,
            input_paths,
            append_seed,
            load_output,
            append_output,
        ) = self._get_paths_for_test(fixture_directory_name)

        # load input files into fake fs
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

        # create and seed existing raw data table if necessary
        seed_data = load_dataframe_from_path(append_seed, fixture_columns=None)
        mock_raw_table = BigQueryAddress(
            dataset_id=raw_tables_dataset_for_region(
                state_code=StateCode(self.region_raw_file_config.region_code.upper()),
                instance=self.raw_data_instance,
            ),
            table_id=file_tag,
        )
        self.create_mock_table(
            mock_raw_table,
            schema=RawDataTableBigQuerySchemaBuilder.build_bq_schema_for_config(
                raw_file_config=self.region_raw_file_config.raw_file_configs[file_tag]
            ),
        )
        if seed_data.shape[1]:
            self.load_rows_into_table(mock_raw_table, seed_data.to_dict("records"))

        # return back to caller
        return (
            file_tag,
            input_files,
            load_dataframe_from_path(load_output, fixture_columns=None),
            load_dataframe_from_path(append_output, fixture_columns=None),
            mock_raw_table,
        )

    def _get_paths_for_test(
        self, fixture_directory_name: str
    ) -> Tuple[str, List[str], str, str, str]:
        """gets paths and metadata for |fixture_directory_name|"""
        raw_fixture_directory = os.path.join(
            os.path.dirname(load_manager_fixtures.__file__),
            fixture_directory_name,
        )

        file_tag = ""
        load_ins = []
        load_out = ""
        append_out = ""
        append_seed = ""

        for file in os.listdir(raw_fixture_directory):
            if "load_input" in file:
                file_tag = file.split("_load_input")[0]
                load_ins.append(os.path.join(raw_fixture_directory, file))
            if "load_output" in file:
                load_out = os.path.join(raw_fixture_directory, file)
            if "append_table_output" in file:
                append_out = os.path.join(raw_fixture_directory, file)
            if "append_table_seed" in file:
                append_seed = os.path.join(raw_fixture_directory, file)

        if not all(
            bool(_out) for _out in [file_tag, load_out, append_out, append_seed]
        ) or not (load_ins or all(isinstance(_in, str) for _in in load_ins)):
            raise ValueError("Failed to instantiate directory for load manager test")

        return file_tag, load_ins, append_seed, load_out, append_out

    def compare_output_against_expected(
        self, output_table: BigQueryAddress, expected_df: pd.DataFrame
    ) -> None:
        actual_df = self.query(
            f"select * from {output_table.to_str()}",
        )

        self.compare_expected_and_result_dfs(expected=expected_df, results=actual_df)

    def test_no_migrations_no_changes_single_file_starts_empty(self) -> None:
        (
            file_tag,
            input_paths,
            prep_output,
            append_output,
            raw_data_table,
        ) = self._prep_test("no_migrations_no_changes_single_file")
        irf = ImportReadyFile(
            file_id=1,
            file_tag=file_tag,
            update_datetime=datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
            original_file_paths=input_paths,
            pre_import_normalized_file_paths=None,
            bq_load_config=RawFileBigQueryLoadConfig.from_raw_file_config(
                raw_file_config=self.region_raw_file_config.raw_file_configs[file_tag],
            ),
        )
        append_ready_file = self.manager.load_and_prep_paths(
            irf, temp_table_prefix="test"
        )

        assert append_ready_file.import_ready_file == irf
        assert append_ready_file.raw_rows_count == 2
        assert (
            append_ready_file.append_ready_table_address.to_str()
            == "us_xx_primary_raw_data_temp_load.test__singlePrimaryKey__1__transformed"
        )

        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1",
                )
            )
        )

        self.assertTrue(len(self.fs.all_paths) == 1)

        self.compare_output_against_expected(
            append_ready_file.append_ready_table_address, prep_output
        )

        _ = self.manager.append_to_raw_data_table(append_ready_file)

        self.compare_output_against_expected(raw_data_table, append_output)

        # make sure we cleaned up properly

        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1__transformed",
                )
            )
        )

        self.assertFalse(
            self.bq_client.dataset_exists("us_xx_raw_data_pruning_diff_results_primary")
        )

        self.assertFalse(
            self.bq_client.dataset_exists("us_xx_new_pruned_raw_data_primary")
        )

        assert not list(self.bq_client.list_tables("us_xx_primary_raw_data_temp_load"))
        assert len(list(self.bq_client.list_tables("us_xx_raw_data"))) == 1

    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_load_manager.DirectIngestRawTablePreImportValidator"
    )
    def test_no_migrations_no_changes_skip_import_blocking(
        self, pre_import_validator: MagicMock
    ) -> None:
        (
            file_tag,
            input_paths,
            prep_output,
            _append_output,
            _raw_data_table,
        ) = self._prep_test("no_migrations_no_changes_single_file")
        irf = ImportReadyFile(
            file_id=1,
            file_tag=file_tag,
            update_datetime=datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
            original_file_paths=input_paths,
            pre_import_normalized_file_paths=None,
            bq_load_config=RawFileBigQueryLoadConfig.from_raw_file_config(
                raw_file_config=self.region_raw_file_config.raw_file_configs[file_tag],
            ),
        )
        append_ready_file = self.manager.load_and_prep_paths(
            irf, temp_table_prefix="test", skip_blocking_validations=True
        )

        assert append_ready_file.import_ready_file == irf
        assert append_ready_file.raw_rows_count == 2
        assert (
            append_ready_file.append_ready_table_address.to_str()
            == "us_xx_primary_raw_data_temp_load.test__singlePrimaryKey__1__transformed"
        )

        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1",
                )
            )
        )

        self.assertTrue(len(self.fs.all_paths) == 1)

        self.compare_output_against_expected(
            append_ready_file.append_ready_table_address, prep_output
        )

        pre_import_validator.assert_not_called()

    @patch(
        "recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_load_manager.DirectIngestRawTableMigrationCollector"
    )
    def test_skip_migrations(self, migration_collector: MagicMock) -> None:
        (
            file_tag,
            input_paths,
            prep_output,
            _append_output,
            _raw_data_table,
        ) = self._prep_test("no_migrations_no_changes_single_file")
        irf = ImportReadyFile(
            file_id=1,
            file_tag=file_tag,
            update_datetime=datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
            original_file_paths=input_paths,
            pre_import_normalized_file_paths=None,
            bq_load_config=RawFileBigQueryLoadConfig.from_raw_file_config(
                raw_file_config=self.region_raw_file_config.raw_file_configs[file_tag],
            ),
        )
        append_ready_file = self.manager.load_and_prep_paths(
            irf, temp_table_prefix="test", skip_blocking_validations=True
        )

        assert append_ready_file.import_ready_file == irf
        assert append_ready_file.raw_rows_count == 2
        assert (
            append_ready_file.append_ready_table_address.to_str()
            == "us_xx_primary_raw_data_temp_load.test__singlePrimaryKey__1__transformed"
        )

        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1",
                )
            )
        )

        self.assertTrue(len(self.fs.all_paths) == 1)

        self.compare_output_against_expected(
            append_ready_file.append_ready_table_address, prep_output
        )

        migration_collector.assert_not_called()

    def test_no_migrations_no_changes_no_clean_up(self) -> None:
        (
            file_tag,
            input_paths,
            prep_output,
            append_output,
            raw_data_table,
        ) = self._prep_test("no_migrations_no_changes_single_file")
        irf = ImportReadyFile(
            file_id=1,
            file_tag=file_tag,
            update_datetime=datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
            original_file_paths=input_paths,
            pre_import_normalized_file_paths=None,
            bq_load_config=RawFileBigQueryLoadConfig.from_raw_file_config(
                raw_file_config=self.region_raw_file_config.raw_file_configs[file_tag],
            ),
        )
        append_ready_file = self.manager.load_and_prep_paths(
            irf, temp_table_prefix="test", persist_intermediary_tables=True
        )

        assert append_ready_file.import_ready_file == irf
        assert append_ready_file.raw_rows_count == 2
        assert (
            append_ready_file.append_ready_table_address.to_str()
            == "us_xx_primary_raw_data_temp_load.test__singlePrimaryKey__1__transformed"
        )

        self.assertTrue(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1",
                )
            )
        )

        self.assertTrue(len(self.fs.all_paths) == 1)

        self.compare_output_against_expected(
            append_ready_file.append_ready_table_address, prep_output
        )

        _ = self.manager.append_to_raw_data_table(
            append_ready_file, persist_intermediary_tables=True
        )

        self.compare_output_against_expected(raw_data_table, append_output)

        # make sure we didn't clean up

        self.assertTrue(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1",
                )
            )
        )

        self.assertTrue(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1__transformed",
                )
            )
        )

        assert (
            len(list(self.bq_client.list_tables("us_xx_primary_raw_data_temp_load")))
            == 2
        )
        assert len(list(self.bq_client.list_tables("us_xx_raw_data"))) == 1

    def test_duplicate_rows_in_raw_data_table(self) -> None:
        (
            file_tag,
            input_paths,
            prep_output,
            append_output,
            raw_data_table,
        ) = self._prep_test("no_migrations_no_changes_single_file")
        irf = ImportReadyFile(
            file_id=1,
            file_tag=file_tag,
            update_datetime=datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
            original_file_paths=input_paths,
            pre_import_normalized_file_paths=None,
            bq_load_config=RawFileBigQueryLoadConfig.from_raw_file_config(
                raw_file_config=self.region_raw_file_config.raw_file_configs[file_tag],
            ),
        )
        append_ready_file = self.manager.load_and_prep_paths(
            irf, temp_table_prefix="test"
        )

        assert append_ready_file.import_ready_file == irf
        assert append_ready_file.raw_rows_count == 2
        assert (
            append_ready_file.append_ready_table_address.to_str()
            == "us_xx_primary_raw_data_temp_load.test__singlePrimaryKey__1__transformed"
        )

        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1",
                )
            )
        )

        self.assertTrue(len(self.fs.all_paths) == 1)

        self.compare_output_against_expected(
            append_ready_file.append_ready_table_address, prep_output
        )

        # load duplicate rows first
        raw_data_table = BigQueryAddress(
            dataset_id=raw_tables_dataset_for_region(
                StateCode.US_XX, DirectIngestInstance.PRIMARY
            ),
            table_id=file_tag,
        )
        self.manager._append_data_to_raw_table(  # pylint: disable=protected-access
            append_ready_file.append_ready_table_address, raw_data_table
        )

        # TODO(#31751) add logs asserts back once num_dml_affected_rows is populated
        # by the emulator

        # with self.assertLogs(level="ERROR") as logs:

        _ = self.manager.append_to_raw_data_table(append_ready_file)

        # assert len(logs.output) == 1
        # self.assertRegex(
        #     logs.output[0],
        #     re.escape(
        #         "Found [2] already existing rows with file id [1] in [us_xx_raw_data.singlePrimaryKey]"
        #     ),
        # )

        self.compare_output_against_expected(raw_data_table, append_output)

        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1__transformed",
                )
            )
        )

        self.assertFalse(
            self.bq_client.dataset_exists("us_xx_raw_data_pruning_diff_results_primary")
        )

        self.assertFalse(
            self.bq_client.dataset_exists("us_xx_new_pruned_raw_data_primary")
        )

        assert not list(self.bq_client.list_tables("us_xx_primary_raw_data_temp_load"))
        assert len(list(self.bq_client.list_tables("us_xx_raw_data"))) == 1

    # TODO(#30788) add integration test back once struct in works in emulator
    def _test_migration_depends_on_transforms(self) -> None:
        file_tag, input_paths, output_df, *_ = self._prep_test(
            "migration_depends_on_transforms"
        )
        irf = ImportReadyFile(
            file_id=1,
            file_tag=file_tag,
            update_datetime=datetime.datetime(
                2020, 6, 10, 0, 0, 0, tzinfo=datetime.UTC
            ),
            original_file_paths=input_paths,
            pre_import_normalized_file_paths=None,
            bq_load_config=RawFileBigQueryLoadConfig.from_raw_file_config(
                raw_file_config=self.region_raw_file_config.raw_file_configs[file_tag],
            ),
        )
        append_ready_file = self.manager.load_and_prep_paths(
            irf, temp_table_prefix="test"
        )

        assert append_ready_file.import_ready_file == irf

        self.compare_output_against_expected(
            append_ready_file.append_ready_table_address, output_df
        )

    def test_historical_diff_depends_on_transform(self) -> None:
        expected_append_summary = AppendSummary(
            file_id=10,
            net_new_or_updated_rows=1,
            deleted_rows=1,
            historical_diffs_active=True,
        )
        self._set_pruning_mocks(True)
        (
            file_tag,
            input_paths,
            prep_output,
            append_output,
            raw_data_table,
        ) = self._prep_test("historical_diff_depends_on_transform")
        irf = ImportReadyFile(
            file_id=10,
            file_tag=file_tag,
            update_datetime=datetime.datetime(2023, 4, 8, 0, 0, 1, tzinfo=datetime.UTC),
            original_file_paths=input_paths,
            pre_import_normalized_file_paths=None,
            bq_load_config=RawFileBigQueryLoadConfig.from_raw_file_config(
                raw_file_config=self.region_raw_file_config.raw_file_configs[file_tag],
            ),
        )
        append_ready_file = self.manager.load_and_prep_paths(
            irf, temp_table_prefix="test"
        )

        assert append_ready_file.import_ready_file == irf
        self.compare_output_against_expected(
            append_ready_file.append_ready_table_address, prep_output
        )

        append_summary = self.manager.append_to_raw_data_table(append_ready_file)

        assert append_summary == expected_append_summary
        self.compare_output_against_expected(raw_data_table, append_output)

    def test_fail_load_failure_to_start(self) -> None:
        file_tag, input_paths, *_ = self._prep_test(
            "no_migrations_no_changes_single_file"
        )

        self.load_job_mock.side_effect = self._mock_fail

        with self.assertRaisesRegex(ValueError, "We hit an error!"):
            _ = self.manager.load_and_prep_paths(
                ImportReadyFile(
                    file_id=1,
                    file_tag=file_tag,
                    update_datetime=datetime.datetime(
                        2023, 4, 8, 0, 0, 1, tzinfo=datetime.UTC
                    ),
                    original_file_paths=input_paths,
                    pre_import_normalized_file_paths=None,
                    bq_load_config=RawFileBigQueryLoadConfig.from_raw_file_config(
                        raw_file_config=self.region_raw_file_config.raw_file_configs[
                            file_tag
                        ],
                    ),
                ),
                temp_table_prefix="test",
            )

        self.assertTrue(len(self.fs.all_paths) == 1)
        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1",
                )
            )
        )
        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1__transformed",
                )
            )
        )

    def test_fail_load_failure_to_start_with_normalized(self) -> None:
        file_tag, input_paths, *_ = self._prep_test(
            "no_migrations_no_changes_single_file"
        )

        self.load_job_mock.side_effect = self._mock_fail

        with self.assertRaisesRegex(ValueError, "We hit an error!"):
            _ = self.manager.load_and_prep_paths(
                ImportReadyFile(
                    file_id=1,
                    file_tag=file_tag,
                    update_datetime=datetime.datetime(
                        2023, 4, 8, 0, 0, 1, tzinfo=datetime.UTC
                    ),
                    original_file_paths=input_paths,
                    pre_import_normalized_file_paths=input_paths,
                    bq_load_config=RawFileBigQueryLoadConfig.from_raw_file_config(
                        raw_file_config=self.region_raw_file_config.raw_file_configs[
                            file_tag
                        ],
                    ),
                ),
                temp_table_prefix="test",
            )

        self.assertTrue(len(self.fs.all_paths) == 1)
        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1",
                )
            )
        )
        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1__transformed",
                )
            )
        )

    def test_fail_load_result_failure(self) -> None:
        file_tag, input_paths, *_ = self._prep_test(
            "no_migrations_no_changes_single_file"
        )

        self.fake_job.result.side_effect = self._mock_fail

        with self.assertRaisesRegex(ValueError, "We hit an error!"):
            _ = self.manager.load_and_prep_paths(
                ImportReadyFile(
                    file_id=1,
                    file_tag=file_tag,
                    update_datetime=datetime.datetime(
                        2023, 4, 8, 0, 0, 1, tzinfo=datetime.UTC
                    ),
                    original_file_paths=input_paths,
                    pre_import_normalized_file_paths=input_paths,
                    bq_load_config=RawFileBigQueryLoadConfig.from_raw_file_config(
                        raw_file_config=self.region_raw_file_config.raw_file_configs[
                            file_tag
                        ],
                    ),
                ),
                temp_table_prefix="test",
            )

        self.assertTrue(len(self.fs.all_paths) == 1)
        self.assertTrue(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1",
                )
            )
        )
        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1__transformed",
                )
            )
        )

    def test_fail_transformation_result_failure(self) -> None:
        file_tag, input_paths, *_ = self._prep_test(
            "no_migrations_no_changes_single_file"
        )

        self.fake_job.result.side_effect = self._mock_fail

        with patch(
            "recidiviz.big_query.big_query_client.bigquery.Client.query",
            return_value=self.fake_job,
        ):
            with self.assertRaisesRegex(ValueError, "We hit an error!"):
                _ = self.manager.load_and_prep_paths(
                    ImportReadyFile(
                        file_id=1,
                        file_tag=file_tag,
                        update_datetime=datetime.datetime(
                            2023, 4, 8, 0, 0, 1, tzinfo=datetime.UTC
                        ),
                        original_file_paths=input_paths,
                        pre_import_normalized_file_paths=input_paths,
                        bq_load_config=RawFileBigQueryLoadConfig.from_raw_file_config(
                            raw_file_config=self.region_raw_file_config.raw_file_configs[
                                file_tag
                            ],
                        ),
                    ),
                    temp_table_prefix="test",
                )

        self.assertTrue(len(self.fs.all_paths) == 1)
        self.assertTrue(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1",
                )
            )
        )

        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1__transformed",
                )
            )
        )

    def test_fail_migration_result_failure(self) -> None:
        file_tag, input_paths, *_ = self._prep_test("migration_depends_on_transforms")

        self.fake_job.result.side_effect = self._mock_fail

        with patch(
            "recidiviz.big_query.big_query_client.BigQueryClientImpl.run_query_async",
            return_value=self.fake_job,
        ):
            with self.assertRaisesRegex(ValueError, "We hit an error!"):
                _ = self.manager.load_and_prep_paths(
                    ImportReadyFile(
                        file_id=1,
                        file_tag=file_tag,
                        update_datetime=datetime.datetime(
                            2023, 4, 8, 0, 0, 1, tzinfo=datetime.UTC
                        ),
                        original_file_paths=input_paths,
                        pre_import_normalized_file_paths=None,
                        bq_load_config=RawFileBigQueryLoadConfig.from_raw_file_config(
                            raw_file_config=self.region_raw_file_config.raw_file_configs[
                                file_tag
                            ],
                        ),
                    ),
                    temp_table_prefix="test",
                )

        self.assertTrue(len(self.fs.all_paths) == 1)
        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1",
                )
            )
        )

        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1__transformed",
                )
            )
        )

    def test_fail_source_table_missing(self) -> None:
        with patch(
            "recidiviz.big_query.big_query_client.BigQueryClientImpl.run_query_async",
            return_value=self.fake_job,
        ):
            with self.assertRaisesRegex(
                ValueError,
                re.escape(
                    "Destination table [recidiviz-bq-emulator-project.us_xx_raw_data.tagBasicData] does not exist!"
                ),
            ):
                _ = self.manager.append_to_raw_data_table(
                    AppendReadyFile(
                        import_ready_file=ImportReadyFile(
                            file_id=1,
                            file_tag="tagBasicData",
                            update_datetime=datetime.datetime(
                                2023, 4, 8, 0, 0, 1, tzinfo=datetime.UTC
                            ),
                            original_file_paths=[],
                            pre_import_normalized_file_paths=None,
                            bq_load_config=RawFileBigQueryLoadConfig(
                                schema_fields=[], skip_leading_rows=0
                            ),
                        ),
                        append_ready_table_address=BigQueryAddress.from_str(
                            '"us_xx_primary_raw_data_temp_load.test__singlePrimaryKey__1__transformed"'
                        ),
                        raw_rows_count=0,
                    ),
                )

        self.assertTrue(len(self.fs.all_paths) == 0)
        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="singlePrimaryKey__1",
                )
            )
        )

        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1__transformed",
                )
            )
        )

    def test_fail_validations_keeps_temp_table(self) -> None:
        self._set_pruning_mocks(True)
        file_tag, input_paths, *_ = self._prep_test(
            "no_migrations_no_changes_single_file"
        )

        with patch(
            "recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_pre_import_validator.DirectIngestRawTablePreImportValidator.run_raw_data_temp_table_validations",
            side_effect=RawDataImportBlockingValidationError(file_tag, failures=[]),
        ):
            with self.assertRaises(RawDataImportBlockingValidationError):
                _ = self.manager.load_and_prep_paths(
                    ImportReadyFile(
                        file_id=1,
                        file_tag=file_tag,
                        update_datetime=datetime.datetime(
                            2023, 4, 8, 0, 0, 1, tzinfo=datetime.UTC
                        ),
                        original_file_paths=input_paths,
                        pre_import_normalized_file_paths=None,
                        bq_load_config=RawFileBigQueryLoadConfig.from_raw_file_config(
                            raw_file_config=self.region_raw_file_config.raw_file_configs[
                                file_tag
                            ],
                        ),
                    ),
                    temp_table_prefix="test",
                )

        self.assertTrue(len(self.fs.all_paths) == 1)
        self.assertTrue(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1",
                )
            )
        )

        self.assertTrue(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1__transformed",
                )
            )
        )

    def test_iso_8859_file_no_migrations_single_file_starts_empty(
        self,
    ) -> None:
        """Tests loading a single ISO-8859-1 file with no migrations.
        The file contains NBSP characters that should be stripped during
        the transform."""
        (
            file_tag,
            input_paths,
            prep_output,
            append_output,
            raw_data_table,
        ) = self._prep_test("no_migrations_single_iso_8859_file")
        irf = ImportReadyFile(
            file_id=1,
            file_tag=file_tag,
            update_datetime=datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
            original_file_paths=input_paths,
            pre_import_normalized_file_paths=None,
            bq_load_config=RawFileBigQueryLoadConfig.from_raw_file_config(
                raw_file_config=self.region_raw_file_config.raw_file_configs[file_tag],
            ),
        )
        append_ready_file = self.manager.load_and_prep_paths(
            irf,
            temp_table_prefix="test",
        )

        assert append_ready_file.import_ready_file == irf
        assert append_ready_file.raw_rows_count == 2
        assert (
            append_ready_file.append_ready_table_address.to_str()
            == "us_xx_primary_raw_data_temp_load.test__tagPipeSeparatedNonUTF8__1__transformed"
        )

        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__tagPipeSeparatedNonUTF8__1",
                )
            )
        )

        self.assertTrue(len(self.fs.all_paths) == 1)

        self.compare_output_against_expected(
            append_ready_file.append_ready_table_address, prep_output
        )

        _ = self.manager.append_to_raw_data_table(append_ready_file)

        self.compare_output_against_expected(raw_data_table, append_output)

        # make sure we cleaned up properly

        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__tagPipeSeparatedNonUTF8__1__transformed",
                )
            )
        )

        self.assertFalse(
            self.bq_client.dataset_exists("us_xx_raw_data_pruning_diff_results_primary")
        )

        self.assertFalse(
            self.bq_client.dataset_exists("us_xx_new_pruned_raw_data_primary")
        )

        assert not list(self.bq_client.list_tables("us_xx_primary_raw_data_temp_load"))
        assert len(list(self.bq_client.list_tables("us_xx_raw_data"))) == 1

    def test_windows_file_no_migrations_single_file_starts_empty(
        self,
    ) -> None:
        """Tests loading a single windows file with no migrations.
        The file uses a pipe separator, which is non-standard but BQ compliant
        so it will not be replaced."""
        (
            file_tag,
            input_paths,
            prep_output,
            append_output,
            raw_data_table,
        ) = self._prep_test("no_migrations_windows_file")
        irf = ImportReadyFile(
            file_id=1,
            file_tag=file_tag,
            update_datetime=datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
            original_file_paths=input_paths,
            pre_import_normalized_file_paths=None,
            bq_load_config=RawFileBigQueryLoadConfig.from_raw_file_config(
                raw_file_config=self.region_raw_file_config.raw_file_configs[file_tag],
            ),
        )
        append_ready_file = self.manager.load_and_prep_paths(
            irf,
            temp_table_prefix="test",
        )

        assert append_ready_file.import_ready_file == irf
        assert append_ready_file.raw_rows_count == 2
        assert (
            append_ready_file.append_ready_table_address.to_str()
            == "us_xx_primary_raw_data_temp_load.test__tagPipeSeparatedWindows__1__transformed"
        )

        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__tagPipeSeparatedWindows__1",
                )
            )
        )

        self.assertTrue(len(self.fs.all_paths) == 1)

        self.compare_output_against_expected(
            append_ready_file.append_ready_table_address, prep_output
        )

        _ = self.manager.append_to_raw_data_table(append_ready_file)

        self.compare_output_against_expected(raw_data_table, append_output)

        # make sure we cleaned up properly

        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__tagPipeSeparatedWindows__1__transformed",
                )
            )
        )

        self.assertFalse(
            self.bq_client.dataset_exists("us_xx_raw_data_pruning_diff_results_primary")
        )

        self.assertFalse(
            self.bq_client.dataset_exists("us_xx_new_pruned_raw_data_primary")
        )

        assert not list(self.bq_client.list_tables("us_xx_primary_raw_data_temp_load"))
        assert len(list(self.bq_client.list_tables("us_xx_raw_data"))) == 1

    def test_skip_raw_data_pruning(self) -> None:
        expected_append_summary = AppendSummary(
            file_id=1,
            net_new_or_updated_rows=None,
            deleted_rows=None,
            historical_diffs_active=False,
        )
        self._set_pruning_mocks(True)
        (
            file_tag,
            input_paths,
            prep_output,
            append_output,
            raw_data_table,
        ) = self._prep_test("no_migrations_no_changes_single_file")
        irf = ImportReadyFile(
            file_id=1,
            file_tag=file_tag,
            update_datetime=datetime.datetime(2024, 1, 1, 1, 1, 1, tzinfo=datetime.UTC),
            original_file_paths=input_paths,
            pre_import_normalized_file_paths=None,
            bq_load_config=RawFileBigQueryLoadConfig.from_raw_file_config(
                raw_file_config=self.region_raw_file_config.raw_file_configs[file_tag],
            ),
        )
        append_ready_file = self.manager.load_and_prep_paths(
            irf, temp_table_prefix="test"
        )

        assert append_ready_file.import_ready_file == irf
        assert append_ready_file.raw_rows_count == 2
        assert (
            append_ready_file.append_ready_table_address.to_str()
            == "us_xx_primary_raw_data_temp_load.test__singlePrimaryKey__1__transformed"
        )

        self.compare_output_against_expected(
            append_ready_file.append_ready_table_address, prep_output
        )

        append_summary = self.manager.append_to_raw_data_table(
            append_ready_file, skip_raw_data_pruning=True
        )

        assert append_summary == expected_append_summary
        self.compare_output_against_expected(raw_data_table, append_output)

        # Verify cleanup happened properly
        self.assertFalse(
            self.bq_client.table_exists(
                BigQueryAddress(
                    dataset_id="us_xx_primary_raw_data_temp_load",
                    table_id="test__singlePrimaryKey__1__transformed",
                )
            )
        )

        self.assertFalse(
            self.bq_client.dataset_exists("us_xx_raw_data_pruning_diff_results_primary")
        )

        self.assertFalse(
            self.bq_client.dataset_exists("us_xx_new_pruned_raw_data_primary")
        )

        assert not list(self.bq_client.list_tables("us_xx_primary_raw_data_temp_load"))
        assert len(list(self.bq_client.list_tables("us_xx_raw_data"))) == 1
