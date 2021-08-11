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
"""Tests for DirectIngestRawFileImportManager."""
import datetime
import unittest
from typing import Any, List
from unittest import mock

import attr
import pandas as pd
from google.cloud import bigquery
from mock import call, create_autospec, patch
from more_itertools import one
from pandas.errors import ParserError

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
)
from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import (
    ColumnEnumValueInfo,
    DirectIngestRawFileImportManager,
    DirectIngestRegionRawFileConfig,
    RawTableColumnInfo,
)
from recidiviz.ingest.direct.controllers.gcsfs_csv_reader import GcsfsCsvReader
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import (
    GcsfsDirectIngestFileType,
    filename_parts_from_path,
)
from recidiviz.persistence.entity.operations.entities import DirectIngestRawFileMetadata
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
from recidiviz.tests.ingest.direct import fixture_util
from recidiviz.tests.ingest.direct.direct_ingest_util import (
    path_for_fixture_file_in_test_gcs_directory,
)
from recidiviz.tests.ingest.direct.fake_regions.us_xx.raw_data.migrations import (
    migrations_tagC,
)
from recidiviz.tests.utils.fake_region import fake_region


class DirectIngestRegionRawFileConfigTest(unittest.TestCase):
    """Tests for DirectIngestRegionRawFileConfig."""

    def test_parse_no_defaults_throws(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "^Missing default raw data configs for region: us_yy"
        ):
            _ = DirectIngestRegionRawFileConfig(
                region_code="us_yy",
                region_module=fake_regions_module,
            )

    def test_missing_primary_key_columns(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Column\(s\) marked as primary keys not listed in columns list"
            r" for file \[us_zz_tagPrimaryKeysMissing.yaml\]: \{'PRIMARY_COL2'\}$",
        ):
            _ = DirectIngestRegionRawFileConfig(
                region_code="us_zz",
                region_module=fake_regions_module,
            )

    def test_parse_yaml(self) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )
        self.assertEqual(13, len(region_config.raw_file_configs))
        self.assertEqual(
            {
                "file_tag_first",
                "file_tag_second",
                "tagC",
                "tagColCapsDoNotMatchConfig",
                "tagFullHistoricalExport",
                "tagInvalidCharacters",
                "tagNormalizationConflict",
                "tagCustomLineTerminatorNonUTF8",
                "tagPipeSeparatedNonUTF8",
                "tagDoubleDaggerWINDOWS1252",
                "tagColumnsMissing",
                "tagRowExtraColumns",
                "tagRowMissingColumns",
            },
            set(region_config.raw_file_configs.keys()),
        )

        config_1 = region_config.raw_file_configs["file_tag_first"]
        self.assertEqual("file_tag_first", config_1.file_tag)
        self.assertEqual("First raw file.", config_1.file_description)
        self.assertEqual(["col_name_1a", "col_name_1b"], config_1.primary_key_cols)
        self.assertEqual("ISO-456-7", config_1.encoding)
        self.assertEqual(",", config_1.separator)
        self.assertIsNone(config_1.custom_line_terminator)
        expected_column2_description = (
            "A column description that is long enough to take up\nmultiple lines. This"
            " text block will be interpreted\nliterally and trailing/leading whitespace"
            " is removed."
        )
        expected_columns_config_1 = [
            RawTableColumnInfo(
                name="col_name_1a",
                is_datetime=False,
                description="First column.",
                known_values=[
                    ColumnEnumValueInfo(value="A", description="A description"),
                    ColumnEnumValueInfo(value="B", description=None),
                ],
            ),
            RawTableColumnInfo(
                name="col_name_1b",
                is_datetime=False,
                description=expected_column2_description,
            ),
            RawTableColumnInfo(
                name="undocumented_column", is_datetime=False, description=None
            ),
        ]
        self.assertEqual(expected_columns_config_1, config_1.columns)

        config_2 = region_config.raw_file_configs["file_tag_second"]
        expected_file_description_config_2 = (
            "Some special/unusual character's in the description &\nlong enough to"
            " make a second line!\\n Trailing/leading white\nspace is stripped & the"
            " text block is interpreted literally."
        )
        self.assertEqual("file_tag_second", config_2.file_tag)
        self.assertEqual(expected_file_description_config_2, config_2.file_description)
        self.assertEqual(["col_name_2a"], config_2.primary_key_cols)
        self.assertEqual("UTF-8", config_2.encoding)
        self.assertEqual("$", config_2.separator)
        self.assertEqual(
            [
                RawTableColumnInfo(
                    name="col_name_2a",
                    is_datetime=False,
                    description="column description",
                )
            ],
            config_2.columns,
        )

        config_3 = region_config.raw_file_configs["tagC"]
        self.assertEqual("tagC", config_3.file_tag)
        self.assertEqual("tagC file description", config_3.file_description)
        self.assertEqual(["COL1"], config_3.primary_key_cols)
        self.assertEqual("UTF-8", config_3.encoding)
        self.assertEqual(",", config_3.separator)
        self.assertEqual(
            [
                RawTableColumnInfo(
                    name="COL1", is_datetime=False, description=None, known_values=None
                )
            ],
            config_3.columns,
        )

        config_4 = region_config.raw_file_configs["tagPipeSeparatedNonUTF8"]
        self.assertEqual("tagPipeSeparatedNonUTF8", config_4.file_tag)
        self.assertEqual(["PRIMARY_COL1"], config_4.primary_key_cols)
        self.assertEqual("ISO-8859-1", config_4.encoding)
        self.assertEqual("|", config_4.separator)

    def test_custom_line_terminator(self) -> None:
        region_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx",
            region_module=fake_regions_module,
        )
        config = region_config.raw_file_configs["tagCustomLineTerminatorNonUTF8"]
        self.assertEqual("tagCustomLineTerminatorNonUTF8", config.file_tag)
        self.assertEqual(["PRIMARY_COL1"], config.primary_key_cols)
        self.assertEqual("ISO-8859-1", config.encoding)
        self.assertEqual(",", config.separator)
        self.assertEqual("@", config.custom_line_terminator)

    def test_missing_configs_for_region(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "^Missing raw data configs for region: us_xy"
        ):
            region_config = DirectIngestRegionRawFileConfig(
                region_code="us_xy",
                region_module=fake_regions_module,
            )
            _configs = region_config.raw_file_configs


class DirectIngestRawFileImportManagerTest(unittest.TestCase):
    """Tests for DirectIngestRawFileImportManager."""

    def setUp(self) -> None:
        self.project_id = "recidiviz-456"
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id
        self.test_region = fake_region(
            region_code="us_xx", region_module=fake_regions_module
        )

        self.fs = DirectIngestGCSFileSystem(FakeGCSFileSystem())
        self.ingest_bucket_path = GcsfsBucketPath(bucket_name="my_ingest_bucket")
        self.temp_output_path = GcsfsDirectoryPath(bucket_name="temp_bucket")

        self.region_raw_file_config = DirectIngestRegionRawFileConfig(
            region_code="us_xx", region_module=fake_regions_module
        )

        self.mock_big_query_client = create_autospec(BigQueryClient)
        self.num_lines_uploaded = 0

        self.mock_big_query_client.load_into_table_from_cloud_storage_async.side_effect = (
            self.mock_import_raw_file_to_big_query
        )

        self.import_manager = DirectIngestRawFileImportManager(
            region=self.test_region,
            fs=self.fs,
            ingest_bucket_path=self.ingest_bucket_path,
            temp_output_directory_path=self.temp_output_path,
            region_raw_file_config=self.region_raw_file_config,
            big_query_client=self.mock_big_query_client,
        )
        self.import_manager.csv_reader = GcsfsCsvReader(self.fs.gcs_file_system)

        self.time_patcher = patch(
            "recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager.time"
        )
        self.mock_time = self.time_patcher.start()

        def fake_get_dataset_ref(dataset_id: str) -> bigquery.DatasetReference:
            return bigquery.DatasetReference(
                project=self.project_id, dataset_id=dataset_id
            )

        self.mock_big_query_client.dataset_ref_for_id = fake_get_dataset_ref

    def tearDown(self) -> None:
        self.time_patcher.stop()
        self.project_id_patcher.stop()

    def mock_import_raw_file_to_big_query(
        self,
        *,
        source_uri: str,
        destination_table_schema: List[bigquery.SchemaField],
        **_kwargs: Any,
    ) -> mock.MagicMock:
        col_names = [schema_field.name for schema_field in destination_table_schema]
        temp_path = GcsfsFilePath.from_absolute_path(source_uri)
        local_temp_path = self.fs.gcs_file_system.real_absolute_path_for_path(temp_path)

        df = pd.read_csv(local_temp_path, header=None, dtype=str)
        for value in df.values:
            for cell in value:
                if isinstance(cell, str):
                    stripped_cell = cell.strip()
                    if stripped_cell != cell:
                        raise ValueError("Did not strip white space from raw data cell")

                if cell in col_names:
                    raise ValueError(f"Wrote column row to output file: {value}")
        self.num_lines_uploaded += len(df)

        return mock.MagicMock()

    def _metadata_for_unprocessed_file_path(
        self, path: GcsfsFilePath
    ) -> DirectIngestRawFileMetadata:
        parts = filename_parts_from_path(path)
        return DirectIngestRawFileMetadata(
            region_code=self.test_region.region_code,
            file_tag=parts.file_tag,
            file_id=123,
            processed_time=None,
            normalized_file_name=path.file_name,
            discovery_time=datetime.datetime.now(),
            datetimes_contained_upper_bound_inclusive=parts.utc_upload_datetime,
        )

    def _check_no_temp_files_remain(self) -> None:
        for path in self.fs.gcs_file_system.all_paths:
            if path.abs_path().startswith(self.temp_output_path.abs_path()):
                self.fail(f"Expected temp path {path.abs_path()} to be cleaned up")

    def test_get_unprocessed_raw_files_to_import(self) -> None:
        self.assertEqual([], self.import_manager.get_unprocessed_raw_files_to_import())

        raw_unprocessed = path_for_fixture_file_in_test_gcs_directory(
            bucket_path=self.ingest_bucket_path,
            filename="file_tag_first.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        ingest_view_unprocessed = path_for_fixture_file_in_test_gcs_directory(
            bucket_path=self.ingest_bucket_path,
            filename="file_tag_second.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
        )

        fixture_util.add_direct_ingest_path(
            self.fs.gcs_file_system,
            raw_unprocessed,
            region_code=self.test_region.region_code,
            has_fixture=False,
        )
        fixture_util.add_direct_ingest_path(
            self.fs.gcs_file_system,
            ingest_view_unprocessed,
            region_code=self.test_region.region_code,
            has_fixture=False,
        )

        self.assertEqual(
            [raw_unprocessed], self.import_manager.get_unprocessed_raw_files_to_import()
        )

    def test_import_bq_file_not_in_tags(self) -> None:
        file_path = path_for_fixture_file_in_test_gcs_directory(
            bucket_path=self.ingest_bucket_path,
            filename="this_path_tag_not_in_yaml.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Attempting to import raw file with tag \[this_path_tag_not_in_yaml\] "
            r"unspecified by \[us_xx\] config.$",
        ):
            self.import_manager.import_raw_file_to_big_query(
                file_path, create_autospec(DirectIngestRawFileMetadata)
            )

    def test_import_wrong_separator_cols_do_not_parse(self) -> None:
        file_config = self.import_manager.region_raw_file_config.raw_file_configs[
            "tagC"
        ]
        updated_file_config = attr.evolve(file_config, separator="#")
        self.import_manager.region_raw_file_config.raw_file_configs[
            "tagC"
        ] = updated_file_config

        file_path = path_for_fixture_file_in_test_gcs_directory(
            bucket_path=self.ingest_bucket_path,
            filename="tagC.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        fixture_util.add_direct_ingest_path(
            self.fs.gcs_file_system, file_path, region_code=self.test_region.region_code
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Found only one column: \[COL1__COL2_COL3\]. "
            r"Columns likely did not parse properly.",
        ):
            self.import_manager.import_raw_file_to_big_query(
                file_path, create_autospec(DirectIngestRawFileMetadata)
            )

    def test_import_bq_file_with_ingest_view_file(self) -> None:
        file_path = path_for_fixture_file_in_test_gcs_directory(
            bucket_path=self.ingest_bucket_path,
            filename="file_tag_first.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.INGEST_VIEW,
        )

        with self.assertRaisesRegex(
            ValueError,
            r"^Unexpected file type \[GcsfsDirectIngestFileType.INGEST_VIEW\] for "
            r"path \[file_tag_first\].$",
        ):
            self.import_manager.import_raw_file_to_big_query(
                file_path, create_autospec(DirectIngestRawFileMetadata)
            )

    def test_import_bq_file_with_raw_file(self) -> None:
        file_path = path_for_fixture_file_in_test_gcs_directory(
            bucket_path=self.ingest_bucket_path,
            filename="tagC.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        fixture_util.add_direct_ingest_path(
            self.fs.gcs_file_system, file_path, region_code=self.test_region.region_code
        )

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))
        path = one(self.fs.gcs_file_system.uploaded_paths)

        self.mock_big_query_client.load_into_table_from_cloud_storage_async.assert_called_with(
            source_uri=path.uri(),
            destination_dataset_ref=bigquery.DatasetReference(
                self.project_id, "us_xx_raw_data"
            ),
            destination_table_id="tagC",
            destination_table_schema=[
                bigquery.SchemaField("COL1", "STRING", "NULLABLE"),
                bigquery.SchemaField("COL2", "STRING", "NULLABLE"),
                bigquery.SchemaField("COL3", "STRING", "NULLABLE"),
                bigquery.SchemaField("file_id", "INTEGER", "REQUIRED"),
                bigquery.SchemaField("update_datetime", "DATETIME", "REQUIRED"),
            ],
        )
        self.assertEqual(2, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_with_row_extra_columns(self) -> None:
        file_path = path_for_fixture_file_in_test_gcs_directory(
            bucket_path=self.ingest_bucket_path,
            filename="tagRowExtraColumns.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        fixture_util.add_direct_ingest_path(
            self.fs.gcs_file_system, file_path, region_code=self.test_region.region_code
        )

        with self.assertRaisesRegex(ParserError, "Expected 4 fields in line 3, saw 5"):
            self.import_manager.import_raw_file_to_big_query(
                file_path, self._metadata_for_unprocessed_file_path(file_path)
            )

        self.assertEqual(0, len(self.fs.gcs_file_system.uploaded_paths))
        self._check_no_temp_files_remain()

    # TODO(#7318): This should fail because a row is missing values
    def test_import_bq_file_with_row_missing_columns(self) -> None:
        file_path = path_for_fixture_file_in_test_gcs_directory(
            bucket_path=self.ingest_bucket_path,
            filename="tagRowMissingColumns.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        fixture_util.add_direct_ingest_path(
            self.fs.gcs_file_system, file_path, region_code=self.test_region.region_code
        )

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))
        path = one(self.fs.gcs_file_system.uploaded_paths)

        self.mock_big_query_client.load_into_table_from_cloud_storage_async.assert_called_with(
            source_uri=path.uri(),
            destination_dataset_ref=bigquery.DatasetReference(
                self.project_id, "us_xx_raw_data"
            ),
            destination_table_id="tagRowMissingColumns",
            destination_table_schema=[
                bigquery.SchemaField("id_column", "STRING", "NULLABLE"),
                bigquery.SchemaField("comment", "STRING", "NULLABLE"),
                bigquery.SchemaField("termination_code", "STRING", "NULLABLE"),
                bigquery.SchemaField("update_date", "STRING", "NULLABLE"),
                bigquery.SchemaField("file_id", "INTEGER", "REQUIRED"),
                bigquery.SchemaField("update_datetime", "DATETIME", "REQUIRED"),
            ],
        )
        self.assertEqual(2, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_with_raw_file_alternate_separator_and_encoding(
        self,
    ) -> None:
        file_path = path_for_fixture_file_in_test_gcs_directory(
            bucket_path=self.ingest_bucket_path,
            filename="tagPipeSeparatedNonUTF8.txt",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        fixture_util.add_direct_ingest_path(
            self.fs.gcs_file_system, file_path, region_code=self.test_region.region_code
        )

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))
        path = one(self.fs.gcs_file_system.uploaded_paths)

        self.mock_big_query_client.load_into_table_from_cloud_storage_async.assert_called_with(
            source_uri=path.uri(),
            destination_dataset_ref=bigquery.DatasetReference(
                self.project_id, "us_xx_raw_data"
            ),
            destination_table_id="tagPipeSeparatedNonUTF8",
            destination_table_schema=[
                bigquery.SchemaField("PRIMARY_COL1", "STRING", "NULLABLE"),
                bigquery.SchemaField("COL2", "STRING", "NULLABLE"),
                bigquery.SchemaField("COL3", "STRING", "NULLABLE"),
                bigquery.SchemaField("COL4", "STRING", "NULLABLE"),
                bigquery.SchemaField("file_id", "INTEGER", "REQUIRED"),
                bigquery.SchemaField("update_datetime", "DATETIME", "REQUIRED"),
            ],
        )
        self.assertEqual(5, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_with_raw_file_line_terminator(
        self,
    ) -> None:
        file_path = path_for_fixture_file_in_test_gcs_directory(
            bucket_path=self.ingest_bucket_path,
            filename="tagCustomLineTerminatorNonUTF8.txt",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        fixture_util.add_direct_ingest_path(
            self.fs.gcs_file_system, file_path, region_code=self.test_region.region_code
        )

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))
        path = one(self.fs.gcs_file_system.uploaded_paths)

        self.mock_big_query_client.load_into_table_from_cloud_storage_async.assert_called_with(
            source_uri=path.uri(),
            destination_dataset_ref=bigquery.DatasetReference(
                self.project_id, "us_xx_raw_data"
            ),
            destination_table_id="tagCustomLineTerminatorNonUTF8",
            destination_table_schema=[
                bigquery.SchemaField("PRIMARY_COL1", "STRING", "NULLABLE"),
                bigquery.SchemaField("COL2", "STRING", "NULLABLE"),
                bigquery.SchemaField("COL3", "STRING", "NULLABLE"),
                bigquery.SchemaField("COL4", "STRING", "NULLABLE"),
                bigquery.SchemaField("file_id", "INTEGER", "REQUIRED"),
                bigquery.SchemaField("update_datetime", "DATETIME", "REQUIRED"),
            ],
        )
        self.assertEqual(5, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_with_multibyte_raw_file_alternate_separator_and_encoding(
        self,
    ) -> None:
        file_path = path_for_fixture_file_in_test_gcs_directory(
            bucket_path=self.ingest_bucket_path,
            filename="tagDoubleDaggerWINDOWS1252.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        fixture_util.add_direct_ingest_path(
            self.fs.gcs_file_system, file_path, region_code=self.test_region.region_code
        )

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))
        path = one(self.fs.gcs_file_system.uploaded_paths)

        self.mock_big_query_client.load_into_table_from_cloud_storage_async.assert_called_with(
            source_uri=path.uri(),
            destination_dataset_ref=bigquery.DatasetReference(
                self.project_id, "us_xx_raw_data"
            ),
            destination_table_id="tagDoubleDaggerWINDOWS1252",
            destination_table_schema=[
                bigquery.SchemaField("PRIMARY_COL1", "STRING", "NULLABLE"),
                bigquery.SchemaField("COL2", "STRING", "NULLABLE"),
                bigquery.SchemaField("COL3", "STRING", "NULLABLE"),
                bigquery.SchemaField("COL4", "STRING", "NULLABLE"),
                bigquery.SchemaField("file_id", "INTEGER", "REQUIRED"),
                bigquery.SchemaField("update_datetime", "DATETIME", "REQUIRED"),
            ],
        )
        self.assertEqual(5, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_multiple_chunks_even_division(self) -> None:

        self.import_manager.upload_chunk_size = 1

        file_path = path_for_fixture_file_in_test_gcs_directory(
            bucket_path=self.ingest_bucket_path,
            filename="tagPipeSeparatedNonUTF8.txt",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        fixture_util.add_direct_ingest_path(
            self.fs.gcs_file_system, file_path, region_code=self.test_region.region_code
        )

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(5, len(self.fs.gcs_file_system.uploaded_paths))

        expected_insert_calls = [
            call(
                source_uri=uploaded_path.uri(),
                destination_dataset_ref=bigquery.DatasetReference(
                    self.project_id, "us_xx_raw_data"
                ),
                destination_table_id="tagPipeSeparatedNonUTF8",
                destination_table_schema=[
                    bigquery.SchemaField("PRIMARY_COL1", "STRING", "NULLABLE"),
                    bigquery.SchemaField("COL2", "STRING", "NULLABLE"),
                    bigquery.SchemaField("COL3", "STRING", "NULLABLE"),
                    bigquery.SchemaField("COL4", "STRING", "NULLABLE"),
                    bigquery.SchemaField("file_id", "INTEGER", "REQUIRED"),
                    bigquery.SchemaField("update_datetime", "DATETIME", "REQUIRED"),
                ],
            )
            for uploaded_path in self.fs.gcs_file_system.uploaded_paths
        ]

        self.mock_big_query_client.load_into_table_from_cloud_storage_async.assert_has_calls(
            expected_insert_calls, any_order=True
        )
        self.assertEqual(
            len(expected_insert_calls) - 1, self.mock_time.sleep.call_count
        )
        self.assertEqual(5, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_multiple_chunks_uneven_division(self) -> None:

        self.import_manager.upload_chunk_size = 2

        file_path = path_for_fixture_file_in_test_gcs_directory(
            bucket_path=self.ingest_bucket_path,
            filename="tagPipeSeparatedNonUTF8.txt",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        fixture_util.add_direct_ingest_path(
            self.fs.gcs_file_system, file_path, region_code=self.test_region.region_code
        )

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(3, len(self.fs.gcs_file_system.uploaded_paths))

        expected_insert_calls = [
            call(
                source_uri=uploaded_path.uri(),
                destination_dataset_ref=bigquery.DatasetReference(
                    self.project_id, "us_xx_raw_data"
                ),
                destination_table_id="tagPipeSeparatedNonUTF8",
                destination_table_schema=[
                    bigquery.SchemaField("PRIMARY_COL1", "STRING", "NULLABLE"),
                    bigquery.SchemaField("COL2", "STRING", "NULLABLE"),
                    bigquery.SchemaField("COL3", "STRING", "NULLABLE"),
                    bigquery.SchemaField("COL4", "STRING", "NULLABLE"),
                    bigquery.SchemaField("file_id", "INTEGER", "REQUIRED"),
                    bigquery.SchemaField("update_datetime", "DATETIME", "REQUIRED"),
                ],
            )
            for uploaded_path in self.fs.gcs_file_system.uploaded_paths
        ]

        self.mock_big_query_client.load_into_table_from_cloud_storage_async.assert_has_calls(
            expected_insert_calls, any_order=True
        )
        self.assertEqual(
            len(expected_insert_calls) - 1, self.mock_time.sleep.call_count
        )
        self.assertEqual(5, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_with_raw_file_invalid_column_chars(self) -> None:
        file_path = path_for_fixture_file_in_test_gcs_directory(
            bucket_path=self.ingest_bucket_path,
            filename="tagInvalidCharacters.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        fixture_util.add_direct_ingest_path(
            self.fs.gcs_file_system, file_path, region_code=self.test_region.region_code
        )

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))
        path = one(self.fs.gcs_file_system.uploaded_paths)

        self.mock_big_query_client.load_into_table_from_cloud_storage_async.assert_called_with(
            source_uri=path.uri(),
            destination_dataset_ref=bigquery.DatasetReference(
                self.project_id, "us_xx_raw_data"
            ),
            destination_table_id="tagInvalidCharacters",
            destination_table_schema=[
                bigquery.SchemaField("COL_1", "STRING", "NULLABLE"),
                bigquery.SchemaField("_COL2", "STRING", "NULLABLE"),
                bigquery.SchemaField("_3COL", "STRING", "NULLABLE"),
                bigquery.SchemaField("_4_COL", "STRING", "NULLABLE"),
                bigquery.SchemaField("file_id", "INTEGER", "REQUIRED"),
                bigquery.SchemaField("update_datetime", "DATETIME", "REQUIRED"),
            ],
        )
        self.assertEqual(1, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_with_cols_not_matching_capitalization(self) -> None:
        file_path = path_for_fixture_file_in_test_gcs_directory(
            bucket_path=self.ingest_bucket_path,
            filename="tagColCapsDoNotMatchConfig.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
        )

        fixture_util.add_direct_ingest_path(
            self.fs.gcs_file_system, file_path, region_code=self.test_region.region_code
        )

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))
        path = one(self.fs.gcs_file_system.uploaded_paths)

        self.mock_big_query_client.load_into_table_from_cloud_storage_async.assert_called_with(
            source_uri=path.uri(),
            destination_dataset_ref=bigquery.DatasetReference(
                self.project_id, "us_xx_raw_data"
            ),
            destination_table_id="tagColCapsDoNotMatchConfig",
            destination_table_schema=[
                bigquery.SchemaField("COL1", "STRING", "NULLABLE"),
                bigquery.SchemaField("COL_2", "STRING", "NULLABLE"),
                bigquery.SchemaField("Col3", "STRING", "NULLABLE"),
                bigquery.SchemaField("file_id", "INTEGER", "REQUIRED"),
                bigquery.SchemaField("update_datetime", "DATETIME", "REQUIRED"),
            ],
        )
        self.assertEqual(2, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_with_raw_file_normalization_conflict(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Multiple columns with name \[_4COL\] after normalization.$"
        ):
            file_path = path_for_fixture_file_in_test_gcs_directory(
                bucket_path=self.ingest_bucket_path,
                filename="tagNormalizationConflict.csv",
                should_normalize=True,
                file_type=GcsfsDirectIngestFileType.RAW_DATA,
            )

            fixture_util.add_direct_ingest_path(
                self.fs.gcs_file_system,
                file_path,
                region_code=self.test_region.region_code,
            )

            self.import_manager.import_raw_file_to_big_query(
                file_path, self._metadata_for_unprocessed_file_path(file_path)
            )

    def test_import_bq_file_with_migrations(self) -> None:
        file_datetime = migrations_tagC.DATE_1
        file_path = path_for_fixture_file_in_test_gcs_directory(
            bucket_path=self.ingest_bucket_path,
            filename="tagC.csv",
            should_normalize=True,
            file_type=GcsfsDirectIngestFileType.RAW_DATA,
            dt=file_datetime,
        )
        fixture_util.add_direct_ingest_path(
            self.fs.gcs_file_system, file_path, region_code=self.test_region.region_code
        )

        mock_query_jobs = [
            mock.MagicMock(),
            mock.MagicMock(),
        ]

        self.mock_big_query_client.run_query_async.side_effect = mock_query_jobs

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.mock_big_query_client.run_query_async.assert_has_calls(
            [
                mock.call(
                    query_str="UPDATE `recidiviz-456.us_xx_raw_data.tagC` original\n"
                    "SET COL1 = updates.new__COL1\n"
                    "FROM (SELECT * FROM UNNEST([\n"
                    "    STRUCT('123' AS COL1, CAST('2020-06-10T00:00:00' AS DATETIME) AS update_datetime, '456' AS new__COL1),\n"
                    "    STRUCT('123' AS COL1, CAST('2020-09-21T00:00:00' AS DATETIME) AS update_datetime, '456' AS new__COL1)\n"
                    "])) updates\n"
                    "WHERE original.COL1 = updates.COL1 AND original.update_datetime = updates.update_datetime;"
                ),
                mock.call(
                    query_str="DELETE FROM `recidiviz-456.us_xx_raw_data.tagC`\n"
                    "WHERE STRUCT(COL1) IN (\n"
                    "    STRUCT('789')\n"
                    ");"
                ),
            ]
        )

        for mock_query_job in mock_query_jobs:
            mock_query_job.result.assert_called_once()
