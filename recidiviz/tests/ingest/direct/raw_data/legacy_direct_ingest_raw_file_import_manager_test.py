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
"""Tests for LegacyDirectIngestRawFileImportManager."""
import datetime
import os
import unittest
from typing import Any, List, Optional
from unittest import mock

import attr
import pandas as pd
from freezegun import freeze_time
from google.cloud import bigquery
from mock import create_autospec, patch
from more_itertools import one
from pandas.errors import ParserError
from pandas.testing import assert_frame_equal

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.cloud_storage.gcsfs_csv_reader import (
    GcsfsCsvReader,
    GcsfsCsvReaderDelegate,
)
from recidiviz.cloud_storage.gcsfs_csv_reader_delegates import (
    SimpleGcsfsCsvReaderDelegate,
)
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.fakes.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.ingest.direct import raw_data
from recidiviz.ingest.direct.gcs.direct_ingest_gcs_file_system import (
    DirectIngestGCSFileSystem,
)
from recidiviz.ingest.direct.gcs.filename_parts import filename_parts_from_path
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder import (
    RawDataTableBigQuerySchemaBuilder,
)
from recidiviz.ingest.direct.raw_data.legacy_direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileReader,
    LegacyDirectIngestRawFileImportManager,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_region_codes,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.entity.operations.entities import DirectIngestRawFileMetadata
from recidiviz.tests.ingest.direct import fake_regions as fake_regions_module
from recidiviz.tests.ingest.direct import fixture_util
from recidiviz.tests.ingest.direct.fake_regions.us_xx.raw_data.migrations import (
    migrations_tagBasicData,
)
from recidiviz.tests.utils.fake_region import fake_region
from recidiviz.utils.yaml_dict import YAMLDict
from recidiviz.utils.yaml_dict_validator import validate_yaml_matches_schema


class LegacyDirectIngestRawFileImportManagerTest(unittest.TestCase):
    """Tests for LegacyDirectIngestRawFileImportManager."""

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

        self.mock_big_query_client.load_table_from_cloud_storage.side_effect = (
            self.mock_import_raw_file_to_big_query
        )

        self.import_manager = LegacyDirectIngestRawFileImportManager(
            region=self.test_region,
            fs=self.fs,
            temp_output_directory_path=self.temp_output_path,
            region_raw_file_config=self.region_raw_file_config,
            big_query_client=self.mock_big_query_client,
            csv_reader=GcsfsCsvReader(self.fs.gcs_file_system),
            instance=DirectIngestInstance.PRIMARY,
        )

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    def mock_import_raw_file_to_big_query(
        self,
        *,
        source_uris: List[str],
        destination_table_schema: List[bigquery.SchemaField],
        **_kwargs: Any,
    ) -> mock.MagicMock:
        col_names = [schema_field.name for schema_field in destination_table_schema]
        for source_uri in source_uris:
            temp_path = GcsfsFilePath.from_absolute_path(source_uri)
            local_temp_path = self.fs.gcs_file_system.real_absolute_path_for_path(
                temp_path
            )

            df = pd.read_csv(local_temp_path, header=None, dtype=str)
            for value in df.values:
                for cell in value:
                    if isinstance(cell, str):
                        stripped_cell = cell.strip()
                        if stripped_cell != cell:
                            raise ValueError(
                                "Did not strip white space from raw data cell"
                            )

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
            file_processed_time=None,
            normalized_file_name=path.file_name,
            file_discovery_time=datetime.datetime.now(),
            update_datetime=parts.utc_upload_datetime,
            raw_data_instance=DirectIngestInstance.PRIMARY,
            is_invalidated=False,
        )

    def _check_no_temp_files_remain(self) -> None:
        for path in self.fs.gcs_file_system.all_paths:
            if path.abs_path().startswith(self.temp_output_path.abs_path()):
                self.fail(f"Expected temp path {path.abs_path()} to be cleaned up")

    def _add_raw_data_path(
        self,
        filename: str,
        has_fixture: bool = True,
        dt: Optional[datetime.datetime] = None,
    ) -> GcsfsFilePath:
        return fixture_util.add_direct_ingest_path(
            fs=self.fs.gcs_file_system,
            bucket_path=self.ingest_bucket_path,
            filename=filename,
            should_normalize=True,
            region_code=self.test_region.region_code,
            has_fixture=has_fixture,
            dt=dt,
        )

    def test_import_bq_file_not_in_tags(self) -> None:
        file_path = self._add_raw_data_path(
            "this_path_tag_not_in_yaml.csv", has_fixture=False
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
            "tagBasicData"
        ]
        updated_file_config = attr.evolve(file_config, separator="#")
        self.import_manager.region_raw_file_config.raw_file_configs[
            "tagBasicData"
        ] = updated_file_config

        file_path = self._add_raw_data_path("tagBasicData.csv")

        with self.assertRaisesRegex(
            ValueError,
            r"^Found only one column: \[COL1__COL2_COL3\]. "
            r"Columns likely did not parse properly.",
        ):
            self.import_manager.import_raw_file_to_big_query(
                file_path, self._metadata_for_unprocessed_file_path(file_path)
            )

    def test_import_bq_file_with_new_raw_file(self) -> None:
        file_path = self._add_raw_data_path("tagBasicData.csv")

        # This is a brand new file so the table does not exist
        self.mock_big_query_client.table_exists.return_value = False

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))
        path = one(self.fs.gcs_file_system.uploaded_paths)

        # Delete should not be called since the table does not exist
        self.mock_big_query_client.delete_from_table_async.assert_not_called()

        self.mock_big_query_client.load_table_from_cloud_storage.assert_called_with(
            source_uris=[path.uri()],
            destination_address=BigQueryAddress(
                dataset_id="us_xx_raw_data",
                table_id="tagBasicData",
            ),
            destination_table_schema=[
                bigquery.SchemaField(
                    "COL1", "STRING", "NULLABLE", description="column 1 description"
                ),
                bigquery.SchemaField(
                    "COL2", "STRING", "NULLABLE", description="column 2 description"
                ),
                bigquery.SchemaField(
                    "COL3", "STRING", "NULLABLE", description="column 3 description"
                ),
            ]
            + list(RawDataTableBigQuerySchemaBuilder.RECIDIVIZ_MANAGED_FIELDS.values()),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        self.assertEqual(2, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_with_raw_file(self) -> None:
        file_path = self._add_raw_data_path("tagBasicData.csv")

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))
        path = one(self.fs.gcs_file_system.uploaded_paths)

        self.mock_big_query_client.delete_from_table_async.assert_called_with(
            address=BigQueryAddress(
                dataset_id="us_xx_raw_data",
                table_id=self._metadata_for_unprocessed_file_path(file_path).file_tag,
            ),
            filter_clause="WHERE file_id = "
            + str(self._metadata_for_unprocessed_file_path(file_path).file_id),
        )

        self.mock_big_query_client.load_table_from_cloud_storage.assert_called_with(
            source_uris=[path.uri()],
            destination_address=BigQueryAddress(
                dataset_id="us_xx_raw_data",
                table_id="tagBasicData",
            ),
            destination_table_schema=[
                bigquery.SchemaField(
                    "COL1", "STRING", "NULLABLE", description="column 1 description"
                ),
                bigquery.SchemaField(
                    "COL2", "STRING", "NULLABLE", description="column 2 description"
                ),
                bigquery.SchemaField(
                    "COL3", "STRING", "NULLABLE", description="column 3 description"
                ),
            ]
            + list(RawDataTableBigQuerySchemaBuilder.RECIDIVIZ_MANAGED_FIELDS.values()),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        self.assertEqual(2, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    @patch(
        "recidiviz.ingest.direct.raw_data.legacy_direct_ingest_raw_file_import_manager.raw_data_pruning_enabled_in_state_and_instance",
        mock.MagicMock(return_value=True),
    )
    @patch(
        "recidiviz.ingest.direct.views.raw_data_diff_query_builder.raw_data_pruning_enabled_in_state_and_instance",
        mock.MagicMock(return_value=True),
    )
    @freeze_time("2023-05-05")
    def test_import_bq_file_with_raw_file_with_raw_data_pruning(self) -> None:
        file_path = self._add_raw_data_path("multipleColPrimaryKeyHistorical.csv")

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))

        self.mock_big_query_client.create_table_from_query.assert_called_with(
            address=BigQueryAddress(
                dataset_id="pruning_us_xx_raw_data_diff_results_primary",
                table_id="multipleColPrimaryKeyHistorical__123",
            ),
            query="""
WITH current_table_rows AS 
( 
 
WITH max_update_datetime AS (
    SELECT
        MAX(update_datetime) AS update_datetime
    FROM
        `recidiviz-456.us_xx_raw_data.multipleColPrimaryKeyHistorical`
    
),
max_file_id AS (
    SELECT
        MAX(file_id) AS file_id
    FROM
        `recidiviz-456.us_xx_raw_data.multipleColPrimaryKeyHistorical`
    WHERE
        update_datetime = (SELECT update_datetime FROM max_update_datetime)
),
filtered_rows AS (
    SELECT *
    FROM
        `recidiviz-456.us_xx_raw_data.multipleColPrimaryKeyHistorical`
    WHERE
        file_id = (SELECT file_id FROM max_file_id)
)
SELECT primary_key_1, primary_key_2, primary_key_3, value
FROM filtered_rows

), 
new_table_rows AS ( 
    SELECT 
      *
    FROM `recidiviz-456.pruning_us_xx_new_raw_data_primary.multipleColPrimaryKeyHistorical__123`
),
added_or_updated_diff AS ( 
  SELECT * FROM new_table_rows 
  EXCEPT DISTINCT 
  SELECT * FROM current_table_rows
), 
deleted_diff AS ( 
  SELECT 
    current_table_rows.* 
  FROM 
    current_table_rows
  LEFT OUTER JOIN
    new_table_rows
  ON 
    
    (
        (current_table_rows.primary_key_1 IS NULL AND new_table_rows.primary_key_1 IS NULL) OR 
        (current_table_rows.primary_key_1 = new_table_rows.primary_key_1)
    ) AND
    (
        (current_table_rows.primary_key_2 IS NULL AND new_table_rows.primary_key_2 IS NULL) OR 
        (current_table_rows.primary_key_2 = new_table_rows.primary_key_2)
    ) AND
    (
        (current_table_rows.primary_key_3 IS NULL AND new_table_rows.primary_key_3 IS NULL) OR 
        (current_table_rows.primary_key_3 = new_table_rows.primary_key_3)
    ) 
  WHERE 
    
new_table_rows.primary_key_1 IS NULL
 AND
new_table_rows.primary_key_2 IS NULL
 AND
new_table_rows.primary_key_3 IS NULL

) 
SELECT 
  *, 
  123 AS file_id,
  CAST('2023-05-05T00:00:00.000000' AS DATETIME) AS update_datetime,
  false AS is_deleted 
FROM added_or_updated_diff 

UNION ALL 

SELECT 
  *, 
  123 AS file_id,
  CAST('2023-05-05T00:00:00.000000' AS DATETIME) AS update_datetime,
  true AS is_deleted 
FROM deleted_diff
""",
            overwrite=True,
            use_query_cache=False,
        )

        self.mock_big_query_client.insert_into_table_from_table_async.assert_called_with(
            source_address=BigQueryAddress(
                dataset_id="pruning_us_xx_raw_data_diff_results_primary",
                table_id="multipleColPrimaryKeyHistorical__123",
            ),
            destination_address=BigQueryAddress(
                dataset_id="us_xx_raw_data",
                table_id="multipleColPrimaryKeyHistorical",
            ),
            use_query_cache=False,
        )

        self.mock_big_query_client.delete_table.assert_called()
        self.mock_big_query_client.delete_table.assert_called()

    def test_import_bq_file_with_row_extra_columns(self) -> None:
        file_path = self._add_raw_data_path("tagRowExtraColumns.csv")

        with self.assertRaisesRegex(ParserError, "Expected 4 fields in line 3, saw 5"):
            self.import_manager.import_raw_file_to_big_query(
                file_path, self._metadata_for_unprocessed_file_path(file_path)
            )

        self.assertEqual(0, len(self.fs.gcs_file_system.uploaded_paths))
        self._check_no_temp_files_remain()

    # TODO(#7318): This should fail because a row is missing values
    def test_import_bq_file_with_row_missing_columns(self) -> None:
        file_path = self._add_raw_data_path("tagRowMissingColumns.csv")

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))
        path = one(self.fs.gcs_file_system.uploaded_paths)

        self.mock_big_query_client.load_table_from_cloud_storage.assert_called_with(
            source_uris=[path.uri()],
            destination_address=BigQueryAddress(
                dataset_id="us_xx_raw_data",
                table_id="tagRowMissingColumns",
            ),
            destination_table_schema=[
                bigquery.SchemaField("id_column", "STRING", "NULLABLE", description=""),
                bigquery.SchemaField("comment", "STRING", "NULLABLE", description=""),
                bigquery.SchemaField(
                    "termination_code", "STRING", "NULLABLE", description=""
                ),
                bigquery.SchemaField(
                    "update_date", "STRING", "NULLABLE", description=""
                ),
            ]
            + list(RawDataTableBigQuerySchemaBuilder.RECIDIVIZ_MANAGED_FIELDS.values()),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        self.assertEqual(2, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_with_raw_file_alternate_separator_and_encoding(
        self,
    ) -> None:
        file_path = self._add_raw_data_path("tagPipeSeparatedNonUTF8.txt")

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))
        path = one(self.fs.gcs_file_system.uploaded_paths)

        self.mock_big_query_client.load_table_from_cloud_storage.assert_called_with(
            source_uris=[path.uri()],
            destination_address=BigQueryAddress(
                dataset_id="us_xx_raw_data",
                table_id="tagPipeSeparatedNonUTF8",
            ),
            destination_table_schema=[
                bigquery.SchemaField(
                    "PRIMARY_COL1",
                    "STRING",
                    "NULLABLE",
                    description="PRIMARY_COL1 description",
                ),
                bigquery.SchemaField("COL2", "STRING", "NULLABLE", description=""),
                bigquery.SchemaField("COL3", "STRING", "NULLABLE", description=""),
                bigquery.SchemaField("COL4", "STRING", "NULLABLE", description=""),
            ]
            + list(RawDataTableBigQuerySchemaBuilder.RECIDIVIZ_MANAGED_FIELDS.values()),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        self.assertEqual(5, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_with_raw_file_line_terminator(
        self,
    ) -> None:
        file_path = self._add_raw_data_path("tagCustomLineTerminatorNonUTF8.txt")

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))
        path = one(self.fs.gcs_file_system.uploaded_paths)

        self.mock_big_query_client.load_table_from_cloud_storage.assert_called_with(
            source_uris=[path.uri()],
            destination_address=BigQueryAddress(
                dataset_id="us_xx_raw_data",
                table_id="tagCustomLineTerminatorNonUTF8",
            ),
            destination_table_schema=[
                bigquery.SchemaField(
                    "PRIMARY_COL1",
                    "STRING",
                    "NULLABLE",
                    description="PRIMARY_COL1 description",
                ),
                bigquery.SchemaField("COL2", "STRING", "NULLABLE", description=""),
                bigquery.SchemaField("COL3", "STRING", "NULLABLE", description=""),
                bigquery.SchemaField("COL4", "STRING", "NULLABLE", description=""),
            ]
            + list(RawDataTableBigQuerySchemaBuilder.RECIDIVIZ_MANAGED_FIELDS.values()),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        self.assertEqual(5, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_with_multibyte_raw_file_alternate_separator_and_encoding(
        self,
    ) -> None:
        file_path = self._add_raw_data_path("tagDoubleDaggerWINDOWS1252.csv")

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))
        path = one(self.fs.gcs_file_system.uploaded_paths)

        self.mock_big_query_client.load_table_from_cloud_storage.assert_called_with(
            source_uris=[path.uri()],
            destination_address=BigQueryAddress(
                dataset_id="us_xx_raw_data",
                table_id="tagDoubleDaggerWINDOWS1252",
            ),
            destination_table_schema=[
                bigquery.SchemaField(
                    "PRIMARY_COL1",
                    "STRING",
                    "NULLABLE",
                    description="PRIMARY_COL1 description",
                ),
                bigquery.SchemaField(
                    "COL2", "STRING", "NULLABLE", description="COL2 description"
                ),
                bigquery.SchemaField(
                    "COL3", "STRING", "NULLABLE", description="COL3 description"
                ),
                bigquery.SchemaField(
                    "COL4", "STRING", "NULLABLE", description="COL4 description"
                ),
            ]
            + list(RawDataTableBigQuerySchemaBuilder.RECIDIVIZ_MANAGED_FIELDS.values()),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        self.assertEqual(5, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_multiple_chunks_even_division(self) -> None:
        file_tag = "tagPipeSeparatedNonUTF8"

        # Update config chunk size to 1
        config = attr.evolve(
            self.region_raw_file_config.raw_file_configs[file_tag],
            import_chunk_size_rows=1,
        )
        self.region_raw_file_config.raw_file_configs[file_tag] = config

        file_path = self._add_raw_data_path(f"{file_tag}.txt")

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(5, len(self.fs.gcs_file_system.uploaded_paths))

        self.mock_big_query_client.load_table_from_cloud_storage.assert_called_with(
            source_uris=sorted(
                [p.uri() for p in self.fs.gcs_file_system.uploaded_paths]
            ),
            destination_address=BigQueryAddress(
                dataset_id="us_xx_raw_data",
                table_id="tagPipeSeparatedNonUTF8",
            ),
            destination_table_schema=[
                bigquery.SchemaField(
                    name="PRIMARY_COL1",
                    field_type="STRING",
                    mode="NULLABLE",
                    description="PRIMARY_COL1 description",
                ),
                bigquery.SchemaField(
                    name="COL2", field_type="STRING", mode="NULLABLE", description=""
                ),
                bigquery.SchemaField(
                    name="COL3", field_type="STRING", mode="NULLABLE", description=""
                ),
                bigquery.SchemaField(
                    name="COL4", field_type="STRING", mode="NULLABLE", description=""
                ),
            ]
            + list(RawDataTableBigQuerySchemaBuilder.RECIDIVIZ_MANAGED_FIELDS.values()),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        self.assertEqual(5, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_multiple_chunks_uneven_division(self) -> None:
        file_tag = "tagPipeSeparatedNonUTF8"

        # Update config chunk size to 2
        config = attr.evolve(
            self.region_raw_file_config.raw_file_configs[file_tag],
            import_chunk_size_rows=2,
        )
        self.region_raw_file_config.raw_file_configs[file_tag] = config

        file_path = self._add_raw_data_path(f"{file_tag}.txt")

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(3, len(self.fs.gcs_file_system.uploaded_paths))

        self.mock_big_query_client.load_table_from_cloud_storage.assert_called_with(
            source_uris=sorted(
                [p.uri() for p in self.fs.gcs_file_system.uploaded_paths]
            ),
            destination_address=BigQueryAddress(
                dataset_id="us_xx_raw_data",
                table_id="tagPipeSeparatedNonUTF8",
            ),
            destination_table_schema=[
                bigquery.SchemaField(
                    "PRIMARY_COL1",
                    "STRING",
                    "NULLABLE",
                    description="PRIMARY_COL1 description",
                ),
                bigquery.SchemaField("COL2", "STRING", "NULLABLE", description=""),
                bigquery.SchemaField("COL3", "STRING", "NULLABLE", description=""),
                bigquery.SchemaField("COL4", "STRING", "NULLABLE", description=""),
            ]
            + list(RawDataTableBigQuerySchemaBuilder.RECIDIVIZ_MANAGED_FIELDS.values()),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        self.assertEqual(5, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_with_raw_file_invalid_column_chars(self) -> None:
        file_path = self._add_raw_data_path("tagInvalidCharacters.csv")

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))
        path = one(self.fs.gcs_file_system.uploaded_paths)

        self.mock_big_query_client.load_table_from_cloud_storage.assert_called_with(
            source_uris=[path.uri()],
            destination_address=BigQueryAddress(
                dataset_id="us_xx_raw_data",
                table_id="tagInvalidCharacters",
            ),
            destination_table_schema=[
                bigquery.SchemaField(
                    "COL_1", "STRING", "NULLABLE", description="COL_1 description"
                ),
                bigquery.SchemaField("_COL2", "STRING", "NULLABLE", description=""),
                bigquery.SchemaField("_3COL", "STRING", "NULLABLE", description=""),
                bigquery.SchemaField("_4_COL", "STRING", "NULLABLE", description=""),
            ]
            + list(RawDataTableBigQuerySchemaBuilder.RECIDIVIZ_MANAGED_FIELDS.values()),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        self.assertEqual(1, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_with_cols_not_matching_capitalization(self) -> None:
        file_path = self._add_raw_data_path("tagColCapsDoNotMatchConfig.csv")

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))
        path = one(self.fs.gcs_file_system.uploaded_paths)

        self.mock_big_query_client.load_table_from_cloud_storage.assert_called_with(
            source_uris=[path.uri()],
            destination_address=BigQueryAddress(
                dataset_id="us_xx_raw_data",
                table_id="tagColCapsDoNotMatchConfig",
            ),
            destination_table_schema=[
                bigquery.SchemaField("COL1", "STRING", "NULLABLE", description=""),
                bigquery.SchemaField("COL_2", "STRING", "NULLABLE", description=""),
                bigquery.SchemaField("Col3", "STRING", "NULLABLE", description=""),
            ]
            + list(RawDataTableBigQuerySchemaBuilder.RECIDIVIZ_MANAGED_FIELDS.values()),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        self.assertEqual(2, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_with_migrations(self) -> None:
        file_datetime = migrations_tagBasicData.DATE_1
        file_path = self._add_raw_data_path("tagBasicData.csv", dt=file_datetime)

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
                    query_str="UPDATE `recidiviz-456.us_xx_raw_data.tagBasicData` original\n"
                    "SET COL1 = updates.new__COL1\n"
                    "FROM (SELECT * FROM UNNEST([\n"
                    "    STRUCT('123' AS COL1, CAST('2020-06-10T00:00:00' AS DATETIME) AS update_datetime, '456' AS new__COL1),\n"
                    "    STRUCT('123' AS COL1, CAST('2020-09-21T00:00:00' AS DATETIME) AS update_datetime, '456' AS new__COL1)\n"
                    "])) updates\n"
                    "WHERE original.COL1 = updates.COL1 AND original.update_datetime = updates.update_datetime;",
                    use_query_cache=False,
                ),
                mock.call(
                    query_str="DELETE FROM `recidiviz-456.us_xx_raw_data.tagBasicData`\n"
                    "WHERE STRUCT(COL1) IN (\n"
                    '    STRUCT("789")\n'
                    ");",
                    use_query_cache=False,
                ),
            ]
        )

        for mock_query_job in mock_query_jobs:
            mock_query_job.result.assert_called_once()

    def test_import_bq_file_using_file_config_as_header(self) -> None:
        file_path = self._add_raw_data_path("tagFileConfigHeaders.csv")

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))
        path = one(self.fs.gcs_file_system.uploaded_paths)

        self.mock_big_query_client.load_table_from_cloud_storage.assert_called_with(
            source_uris=[path.uri()],
            destination_address=BigQueryAddress(
                dataset_id="us_xx_raw_data",
                table_id="tagFileConfigHeaders",
            ),
            destination_table_schema=[
                bigquery.SchemaField("COL1", "STRING", "NULLABLE", description=""),
                bigquery.SchemaField("COL2", "STRING", "NULLABLE", description=""),
                bigquery.SchemaField("COL3", "STRING", "NULLABLE", description=""),
            ]
            + list(RawDataTableBigQuerySchemaBuilder.RECIDIVIZ_MANAGED_FIELDS.values()),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        self.assertEqual(2, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_with_all_nulls_row(self) -> None:
        file_path = self._add_raw_data_path("tagOneAllNullRowTwoGoodRows.csv")

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(1, len(self.fs.gcs_file_system.uploaded_paths))
        path = one(self.fs.gcs_file_system.uploaded_paths)

        self.mock_big_query_client.load_table_from_cloud_storage.assert_called_with(
            source_uris=[path.uri()],
            destination_address=BigQueryAddress(
                dataset_id="us_xx_raw_data",
                table_id="tagOneAllNullRowTwoGoodRows",
            ),
            destination_table_schema=[
                bigquery.SchemaField("COL1", "STRING", "NULLABLE", description=""),
                bigquery.SchemaField("COL2", "STRING", "NULLABLE", description=""),
                bigquery.SchemaField("COL3", "STRING", "NULLABLE", description=""),
                bigquery.SchemaField("COL4", "STRING", "NULLABLE", description=""),
            ]
            + list(RawDataTableBigQuerySchemaBuilder.RECIDIVIZ_MANAGED_FIELDS.values()),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        self.assertEqual(2, self.num_lines_uploaded)
        self._check_no_temp_files_remain()

    def test_import_bq_file_with_only_one_all_nulls_row(self) -> None:
        file_path = self._add_raw_data_path("tagOneAllNullRow.csv")

        self.import_manager.import_raw_file_to_big_query(
            file_path, self._metadata_for_unprocessed_file_path(file_path)
        )

        self.assertEqual(0, len(self.fs.gcs_file_system.uploaded_paths))

        self.mock_big_query_client.load_table_from_cloud_storage.assert_not_called()
        self.assertEqual(0, self.num_lines_uploaded)
        self._check_no_temp_files_remain()


class CapturingGcsfsCsvReaderDelegate(GcsfsCsvReaderDelegate):
    """Counts rows for use in testing."""

    def __init__(self) -> None:
        super().__init__()
        self.df = None

    def on_start_read_with_encoding(self, encoding: str) -> None:
        pass

    def on_file_stream_normalization(
        self, old_encoding: str, new_encoding: str
    ) -> None:
        pass

    def on_dataframe(self, encoding: str, chunk_num: int, df: pd.DataFrame) -> bool:
        if self.df is None:
            self.df = df
        else:
            self.df += df
        return True

    def on_unicode_decode_error(self, encoding: str, e: UnicodeError) -> bool:
        return False

    def on_exception(self, encoding: str, e: Exception) -> bool:
        return True

    def on_file_read_success(self, encoding: str) -> None:
        pass


class DirectIngestRawFileReaderTest(unittest.TestCase):
    """Tests for DirectIngestRawFileReader."""

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

        self.reader = DirectIngestRawFileReader(
            region_raw_file_config=self.region_raw_file_config,
            csv_reader=GcsfsCsvReader(self.fs.gcs_file_system),
        )

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    def _add_raw_data_path(
        self,
        filename: str,
        has_fixture: bool = True,
        dt: Optional[datetime.datetime] = None,
    ) -> GcsfsFilePath:
        return fixture_util.add_direct_ingest_path(
            fs=self.fs.gcs_file_system,
            bucket_path=self.ingest_bucket_path,
            filename=filename,
            should_normalize=True,
            region_code=self.test_region.region_code,
            has_fixture=has_fixture,
            dt=dt,
        )

    def test_import_wrong_separator_cols_do_not_parse(self) -> None:
        file_config = self.reader.region_raw_file_config.raw_file_configs[
            "tagBasicData"
        ]
        updated_file_config = attr.evolve(file_config, separator="#")
        self.reader.region_raw_file_config.raw_file_configs[
            "tagBasicData"
        ] = updated_file_config

        file_path = self._add_raw_data_path("tagBasicData.csv")

        with self.assertRaisesRegex(
            ValueError,
            r"^Found only one column: \[COL1__COL2_COL3\]. "
            r"Columns likely did not parse properly.",
        ):
            self.reader.read_raw_file_from_gcs(
                file_path, SimpleGcsfsCsvReaderDelegate()
            )

    def test_import_bq_file_with_row_extra_columns(self) -> None:
        file_path = self._add_raw_data_path("tagRowExtraColumns.csv")

        with self.assertRaisesRegex(ParserError, "Expected 4 fields in line 3, saw 5"):
            self.reader.read_raw_file_from_gcs(
                file_path, SimpleGcsfsCsvReaderDelegate()
            )

    # TODO(#7318): This should fail because a row is missing values
    def test_import_bq_file_with_row_missing_columns(self) -> None:
        file_path = self._add_raw_data_path("tagRowMissingColumns.csv")
        delegate = CapturingGcsfsCsvReaderDelegate()

        self.reader.read_raw_file_from_gcs(file_path, delegate)

        assert delegate.df is not None
        assert_frame_equal(
            delegate.df,
            pd.DataFrame(
                {
                    "id_column": ["1", "2"],
                    "comment": ["They are no longer in custody", "XXX"],
                    "termination_code": ["XXX", "2021-05-05"],
                    "update_date": ["2021-05-05", ""],
                }
            ),
        )

    def test_import_bq_file_with_raw_file_alternate_separator_and_encoding(
        self,
    ) -> None:
        file_path = self._add_raw_data_path("tagPipeSeparatedNonUTF8.txt")
        delegate = CapturingGcsfsCsvReaderDelegate()

        self.reader.read_raw_file_from_gcs(file_path, delegate)

        assert delegate.df is not None
        assert_frame_equal(
            delegate.df,
            pd.DataFrame(
                {
                    "PRIMARY_COL1": ["1000", "1001", "1002", "1003", "1004"],
                    "COL2": ["VALUE1", '"VALUE2', "VALUE3", "VALUE4", "VALUE5"],
                    "COL3": ["VALUE1b", "VALUE2b", "VALUE3b", "VALUE4b", ""],
                    "COL4": ["", "Y", "N", "246", "Y"],
                }
            ),
        )

    def test_import_bq_file_with_raw_file_line_terminator(
        self,
    ) -> None:
        file_path = self._add_raw_data_path("tagCustomLineTerminatorNonUTF8.txt")
        delegate = CapturingGcsfsCsvReaderDelegate()

        self.reader.read_raw_file_from_gcs(file_path, delegate)

        assert delegate.df is not None
        assert_frame_equal(
            delegate.df,
            pd.DataFrame(
                {
                    "PRIMARY_COL1": ["1000", "1001", "1002", "1003", "1004"],
                    "COL2": ["VALUE1", '"VALUE2', "VALUE3", "VALUE4", "VALUE5"],
                    "COL3": ["VALUE1b", "VALUE2b", "VALUE3b", "Y", ""],
                    "COL4": ["", "Y", "N", "246", "Y"],
                }
            ),
        )

    def test_import_bq_file_with_multibyte_raw_file_alternate_separator_and_encoding(
        self,
    ) -> None:
        file_path = self._add_raw_data_path("tagDoubleDaggerWINDOWS1252.csv")
        delegate = CapturingGcsfsCsvReaderDelegate()

        self.reader.read_raw_file_from_gcs(file_path, delegate)

        assert delegate.df is not None
        assert_frame_equal(
            delegate.df,
            pd.DataFrame(
                {
                    "PRIMARY_COL1": ["1000", "1001", "1002", "1003", "1004"],
                    "COL2": ["VALUE1", '"VALUE2', "VALUE3", "VALUE4", "VALUE5"],
                    "COL3": ["VALUE1b", "VALUE2b", "VALUE3b", "VALUE4b", ""],
                    "COL4": ["", "Y", "N", "246", "Y"],
                }
            ),
        )

    def test_import_bq_file_with_raw_file_invalid_column_chars(self) -> None:
        file_path = self._add_raw_data_path("tagInvalidCharacters.csv")
        delegate = CapturingGcsfsCsvReaderDelegate()

        self.reader.read_raw_file_from_gcs(file_path, delegate)

        assert delegate.df is not None
        assert_frame_equal(
            delegate.df,
            pd.DataFrame(
                {
                    "COL_1": ["VAL1"],
                    "_COL2": ["VAL2"],
                    "_3COL": ["VAL3"],
                    "_4_COL": ["VAL4"],
                }
            ),
        )

    def test_import_bq_file_with_cols_not_matching_capitalization(self) -> None:
        file_path = self._add_raw_data_path("tagColCapsDoNotMatchConfig.csv")
        delegate = CapturingGcsfsCsvReaderDelegate()

        self.reader.read_raw_file_from_gcs(file_path, delegate)

        assert delegate.df is not None
        assert_frame_equal(
            delegate.df,
            pd.DataFrame(
                {
                    "COL1": ["VAL1", "VAL4"],
                    "COL_2": ["VAL2 ", "VAL5"],
                    "Col3": ["VAL3", "VAL6"],
                }
            ),
        )

    def test_import_bq_file_with_raw_file_normalization_conflict(self) -> None:
        file_path = self._add_raw_data_path("tagNormalizationConflict.csv")
        with self.assertRaisesRegex(
            ValueError, r"^Multiple columns with name \[_4COL\] after normalization.$"
        ):
            self.reader.read_raw_file_from_gcs(
                file_path, SimpleGcsfsCsvReaderDelegate()
            )

    def test_import_bq_file_using_file_config_as_header(self) -> None:
        file_path = self._add_raw_data_path("tagFileConfigHeaders.csv")
        delegate = CapturingGcsfsCsvReaderDelegate()

        self.reader.read_raw_file_from_gcs(file_path, delegate)

        assert delegate.df is not None
        assert_frame_equal(
            delegate.df,
            pd.DataFrame(
                {
                    "COL1": ["value1", "value4"],
                    "COL2": ["value2", "value5"],
                    "COL3": ["value3", "value6"],
                }
            ),
        )

    def test_import_bq_file_using_file_config_as_headers_missing_columns(self) -> None:
        file_path = self._add_raw_data_path("tagInvalidFileConfigHeaders.csv")
        with self.assertRaisesRegex(
            ValueError,
            r".*Make sure all expected columns are defined in the raw data configuration.$",
        ):
            self.reader.read_raw_file_from_gcs(
                file_path, SimpleGcsfsCsvReaderDelegate()
            )

    def test_import_bq_file_using_file_config_as_headers_unexpected_header_row(
        self,
    ) -> None:
        file_path = self._add_raw_data_path("tagFileConfigHeadersUnexpectedHeader.csv")
        with self.assertRaisesRegex(
            ValueError,
            r".*Found an unexpected header in the CSV. Please remove the header row from the CSV.$",
        ):
            self.reader.read_raw_file_from_gcs(
                file_path, SimpleGcsfsCsvReaderDelegate()
            )

    def test_import_bq_file_extra_columns_in_csv_not_in_config(self) -> None:
        file_path = self._add_raw_data_path("tagMissingColumnsDefined.csv")
        with self.assertRaisesRegex(
            ValueError,
            r".*Make sure that all columns from CSV are defined in the raw data configuration.$",
        ):
            self.reader.read_raw_file_from_gcs(
                file_path, SimpleGcsfsCsvReaderDelegate()
            )


def test_validate_all_raw_yaml_schemas() -> None:
    """
    Validates YAML raw configuration files against our JSON schema.
    We want to do this validation so that we
    don't forget to add JSON schema (and therefore
    IDE) support for new features in the language.
    """
    json_schema_path = os.path.join(
        os.path.dirname(raw_data.__file__), "yaml_schema", "schema.json"
    )
    for region_code in get_existing_region_codes():
        region_raw_file_config = DirectIngestRegionRawFileConfig(region_code)
        for file_path in region_raw_file_config.get_raw_data_file_config_paths():
            validate_yaml_matches_schema(
                yaml_dict=YAMLDict.from_path(file_path),
                json_schema_path=json_schema_path,
            )

    # Test the US_XX fake region to catch new additions that are only tested in US_XX
    region_raw_file_config = DirectIngestRegionRawFileConfig(
        region_code=StateCode.US_XX.value, region_module=fake_regions_module
    )
    for file_path in region_raw_file_config.get_raw_data_file_config_paths():
        validate_yaml_matches_schema(
            yaml_dict=YAMLDict.from_path(file_path),
            json_schema_path=json_schema_path,
        )


# TODO(#20341): Remove this test once we can do raw data pruning on non-historical files
def test_validate_all_raw_yaml_no_valid_primary_keys() -> None:
    for region_code in get_existing_region_codes():
        region_raw_file_config = DirectIngestRegionRawFileConfig(region_code)
        for file_tag in region_raw_file_config.raw_file_tags:
            file_config = region_raw_file_config.raw_file_configs[file_tag]
            if (
                file_config.no_valid_primary_keys
                and not file_config.always_historical_export
            ):
                raise ValueError(
                    f"[state_code={region_code.upper()}][file_tag={file_tag}]: Cannot set "
                    "`no_valid_primary_keys=True` if `always_historical_export=False`. If this file is "
                    "always historical, set `always_historical_export=True`."
                )
