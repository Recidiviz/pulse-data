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
import unittest

from recidiviz.ingest.direct.controllers.direct_ingest_gcs_file_system import to_normalized_unprocessed_file_path
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import DirectIngestRawFileImportManager
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import GcsfsDirectIngestFileType
from recidiviz.ingest.direct.controllers.gcsfs_path import GcsfsFilePath, GcsfsDirectoryPath
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.ingest.direct.direct_ingest_util import FakeDirectIngestGCSFileSystem
from recidiviz.tests.utils.fake_region import fake_region


class DirectIngestRawFileImportManagerTest(unittest.TestCase):
    """Tests for DirectIngestRawFileImportManager."""

    def setUp(self) -> None:
        self.test_region = fake_region(region_code='us_xx',
                                       are_raw_data_bq_imports_enabled_in_env=True)
        self.fs = FakeDirectIngestGCSFileSystem()
        self.ingest_directory_path = GcsfsDirectoryPath(bucket_name='my_bucket')

    def test_parse_yaml(self):
        import_manager = DirectIngestRawFileImportManager(
            region=self.test_region,
            fs=self.fs,
            ingest_directory_path=self.ingest_directory_path,
            yaml_config_file_path=fixtures.as_filepath('us_xx_raw_data_files.yaml')
        )

        self.assertEqual(2, len(import_manager.raw_file_configs))

        tag_to_config = {config.file_tag: config for config in import_manager.raw_file_configs}

        self.assertEqual({'file_tag_first', 'file_tag_second'}, tag_to_config.keys())

        self.assertEqual(['col_name_1a', 'col_name_1b'], tag_to_config['file_tag_first'].primary_key_cols)
        self.assertEqual(['col_name_2a'], tag_to_config['file_tag_second'].primary_key_cols)

    def test_parse_empty_yaml_throws(self):
        with self.assertRaises(ValueError):
            _ = DirectIngestRawFileImportManager(
                region=self.test_region,
                fs=self.fs,
                ingest_directory_path=self.ingest_directory_path,
                yaml_config_file_path=fixtures.as_filepath('empty_raw_data_files.yaml')
            )

    def test_get_unprocessed_raw_files_to_import(self):
        import_manager = DirectIngestRawFileImportManager(
            region=self.test_region,
            fs=self.fs,
            ingest_directory_path=self.ingest_directory_path,
            yaml_config_file_path=fixtures.as_filepath('us_xx_raw_data_files.yaml')
        )

        self.assertEqual([], import_manager.get_unprocessed_raw_files_to_import())

        raw_unprocessed = GcsfsFilePath.from_directory_and_file_name(
            self.ingest_directory_path,
            to_normalized_unprocessed_file_path('file_tag_first.csv', file_type=GcsfsDirectIngestFileType.RAW_DATA))
        ingest_view_unprocessed = GcsfsFilePath.from_directory_and_file_name(
            self.ingest_directory_path,
            to_normalized_unprocessed_file_path('file_tag_second.csv', file_type=GcsfsDirectIngestFileType.INGEST_VIEW))

        self.fs.test_add_path(raw_unprocessed)
        self.fs.test_add_path(ingest_view_unprocessed)

        self.assertEqual([raw_unprocessed], import_manager.get_unprocessed_raw_files_to_import())

    def test_import_bq_file_not_in_tags(self):
        import_manager = DirectIngestRawFileImportManager(
            region=self.test_region,
            fs=self.fs,
            ingest_directory_path=self.ingest_directory_path,
            yaml_config_file_path=fixtures.as_filepath('us_xx_raw_data_files.yaml')
        )

        file_path_str = to_normalized_unprocessed_file_path('bucket/this_path_tag_not_in_yaml.csv',
                                                            GcsfsDirectIngestFileType.RAW_DATA)
        file_path = GcsfsFilePath.from_absolute_path(file_path_str)

        with self.assertRaises(ValueError):
            import_manager.import_raw_file_to_big_query(file_path)

    def test_import_bq_file_with_ingest_view_file(self):
        import_manager = DirectIngestRawFileImportManager(
            region=self.test_region,
            fs=self.fs,
            ingest_directory_path=self.ingest_directory_path,
            yaml_config_file_path=fixtures.as_filepath('us_xx_raw_data_files.yaml')
        )

        file_path_str = to_normalized_unprocessed_file_path('bucket/file_tag_first.csv',
                                                            GcsfsDirectIngestFileType.INGEST_VIEW)
        file_path = GcsfsFilePath.from_absolute_path(file_path_str)

        with self.assertRaises(ValueError):
            import_manager.import_raw_file_to_big_query(file_path)

    def test_import_bq_file_with_unspecified_type_file(self):
        import_manager = DirectIngestRawFileImportManager(
            region=self.test_region,
            fs=self.fs,
            ingest_directory_path=self.ingest_directory_path,
            yaml_config_file_path=fixtures.as_filepath('us_xx_raw_data_files.yaml')
        )

        file_path_str = to_normalized_unprocessed_file_path('bucket/file_tag_first.csv',
                                                            GcsfsDirectIngestFileType.UNSPECIFIED)
        file_path = GcsfsFilePath.from_absolute_path(file_path_str)

        with self.assertRaises(ValueError):
            import_manager.import_raw_file_to_big_query(file_path)

    def test_import_bq_file_feature_not_released_throws(self):
        import_manager = DirectIngestRawFileImportManager(
            region=fake_region(region_code='us_xx',
                               are_raw_data_bq_imports_enabled_in_env=False),
            fs=self.fs,
            ingest_directory_path=self.ingest_directory_path,
            yaml_config_file_path=fixtures.as_filepath('us_xx_raw_data_files.yaml')
        )

        file_path_str = to_normalized_unprocessed_file_path('bucket/file_tag_first.csv',
                                                            GcsfsDirectIngestFileType.RAW_DATA)
        file_path = GcsfsFilePath.from_absolute_path(file_path_str)

        with self.assertRaises(ValueError):
            import_manager.import_raw_file_to_big_query(file_path)
