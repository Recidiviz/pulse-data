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
"""Tests for direct_ingest_ingest_view_export_manager.py."""
import datetime
import unittest
from typing import List

import attr
import mock
import pytest
from freezegun import freeze_time
from google.cloud.bigquery import ScalarQueryParameter
from mock import patch
from more_itertools import one

from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import DirectIngestPreProcessedIngestView
from recidiviz.ingest.direct.controllers.direct_ingest_ingest_view_export_manager import \
    DirectIngestIngestViewExportManager
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import DirectIngestRegionRawFileConfig
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import GcsfsIngestViewExportArgs
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.ingest.direct.controllers.postgres_direct_ingest_file_metadata_manager import \
    PostgresDirectIngestFileMetadataManager
from recidiviz.persistence.database.base_schema import OperationsBase
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem
from recidiviz.tests.utils import fakes
from recidiviz.tests.utils.fake_region import fake_region
from recidiviz.utils.regions import Region

_ID = 1
_DATE_1 = datetime.datetime(year=2019, month=7, day=20)
_DATE_2 = datetime.datetime(year=2020, month=7, day=20)
_DATE_3 = datetime.datetime(year=2021, month=7, day=20)
_DATE_4 = datetime.datetime(year=2022, month=7, day=20)


class _ViewCollector(BigQueryViewCollector[DirectIngestPreProcessedIngestView]):
    """Fake ViewCollector for tests."""
    def __init__(self,
                 region: Region,
                 controller_file_tags: List[str],
                 is_detect_row_deletion_view: bool):
        self.region = region
        self.controller_file_tags = controller_file_tags
        self.is_detect_row_deletion_view = is_detect_row_deletion_view

    def collect_views(self) -> List[DirectIngestPreProcessedIngestView]:

        region_config = DirectIngestRegionRawFileConfig(
            region_code='us_xx',
            yaml_config_file_path=fixtures.as_filepath('us_xx_raw_data_files.yaml', subdir='fixtures'),
        )

        query = 'select * from {file_tag_first} JOIN {tagFullHistoricalExport} USING (COL_1)'
        primary_key_tables_for_entity_deletion = \
            [] if not self.is_detect_row_deletion_view else ['tagFullHistoricalExport']
        views = [
            DirectIngestPreProcessedIngestView(
                ingest_view_name=tag,
                view_query_template=query,
                region_raw_table_config=region_config,
                order_by_cols='colA, colC',
                is_detect_row_deletion_view=self.is_detect_row_deletion_view,
                primary_key_tables_for_entity_deletion=primary_key_tables_for_entity_deletion,
            )
            for tag in self.controller_file_tags
        ]

        return views


class DirectIngestIngestViewExportManagerTest(unittest.TestCase):
    """Tests for the DirectIngestIngestViewExportManager class"""

    def setUp(self) -> None:
        self.metadata_patcher = patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = 'recidiviz-456'

        fakes.use_in_memory_sqlite_database(OperationsBase)
        self.client_patcher = patch('recidiviz.big_query.big_query_client.BigQueryClient')
        self.mock_client = self.client_patcher.start().return_value
        project_id_mock = mock.PropertyMock(return_value='recidiviz-456')
        type(self.mock_client).project_id = project_id_mock

    def tearDown(self) -> None:
        self.client_patcher.stop()
        self.metadata_patcher.stop()
        fakes.teardown_in_memory_sqlite_databases()

    def to_entity(self, schema_obj):
        return converter.convert_schema_object_to_entity(schema_obj, populate_back_edges=False)

    @staticmethod
    def create_fake_region(ingest_view_exports_enabled=True):
        return fake_region(region_code='US_XX',
                           is_raw_vs_ingest_file_name_detection_enabled=True,
                           are_raw_data_bq_imports_enabled_in_env=True,
                           are_ingest_view_exports_enabled_in_env=ingest_view_exports_enabled)

    def create_export_manager(self, region, is_detect_row_deletion_view=False):
        metadata_manager = PostgresDirectIngestFileMetadataManager(region.region_code)
        return DirectIngestIngestViewExportManager(
            region=region,
            fs=FakeGCSFileSystem(),
            ingest_directory_path=GcsfsDirectoryPath.from_absolute_path('ingest_bucket'),
            big_query_client=self.mock_client,
            file_metadata_manager=metadata_manager,
            view_collector=_ViewCollector(
                region,
                controller_file_tags=['ingest_view'],
                is_detect_row_deletion_view=is_detect_row_deletion_view))

    @staticmethod
    def generate_query_params_for_date(date_param):
        return ScalarQueryParameter('update_timestamp', 'DATETIME', date_param)

    def assert_exported_to_gcs_with_query(self, expected_query):
        self.mock_client.export_query_results_to_cloud_storage.assert_called_once()
        _, kwargs = one(self.mock_client.export_query_results_to_cloud_storage.call_args_list)
        export_config = one(kwargs['export_configs'])
        normalized_exported_query = export_config.query.replace('\n', '')
        normalized_expected_query = expected_query.replace('\n', '')
        self.assertEqual(normalized_expected_query, normalized_exported_query)

    def test_exportViewForArgs_ingestViewExportsDisabled(self):
        # Arrange
        region = self.create_fake_region(ingest_view_exports_enabled=False)
        export_manager = self.create_export_manager(region)
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name='ingest_view',
            upper_bound_datetime_prev=_DATE_1,
            upper_bound_datetime_to_export=_DATE_2)

        # Act
        with pytest.raises(ValueError):
            export_manager.export_view_for_args(export_args)

        # Assert
        self.mock_client.create_table_from_query_async.assert_not_called()
        self.mock_client.export_query_results_to_cloud_storage.assert_not_called()
        self.mock_client.delete_table.assert_not_called()

    def test_exportViewForArgs_noExistingMetadata(self):
        # Arrange
        region = self.create_fake_region()
        export_manager = self.create_export_manager(region)
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name='ingest_view',
            upper_bound_datetime_prev=_DATE_1,
            upper_bound_datetime_to_export=_DATE_2)

        # Act
        with pytest.raises(ValueError):
            export_manager.export_view_for_args(export_args)

        # Assert
        self.mock_client.create_table_from_query_async.assert_not_called()
        self.mock_client.export_query_results_to_cloud_storage.assert_not_called()
        self.mock_client.delete_table.assert_not_called()

    def test_exportViewForArgs_alreadyExported(self):
        # Arrange
        region = self.create_fake_region()
        export_manager = self.create_export_manager(region)
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name='ingest_view',
            upper_bound_datetime_prev=_DATE_1,
            upper_bound_datetime_to_export=_DATE_2)

        session = SessionFactory.for_schema_base(OperationsBase)
        metadata = schema.DirectIngestIngestFileMetadata(
            file_id=_ID,
            region_code=region.region_code,
            file_tag=export_args.ingest_view_name,
            normalized_file_name='normalized_file_name',
            is_invalidated=False,
            is_file_split=False,
            job_creation_time=_DATE_1,
            export_time=_DATE_2,
            datetimes_contained_lower_bound_exclusive=export_args.upper_bound_datetime_prev,
            datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_to_export
        )
        expected_metadata = self.to_entity(metadata)
        session.add(metadata)
        session.commit()
        session.close()

        # Act
        export_manager.export_view_for_args(export_args)

        # Assert
        self.mock_client.create_table_from_query_async.assert_not_called()
        self.mock_client.export_query_results_to_cloud_storage.assert_not_called()
        self.mock_client.delete_table.assert_not_called()
        assert_session = SessionFactory.for_schema_base(OperationsBase)
        found_metadata = self.to_entity(one(assert_session.query(schema.DirectIngestIngestFileMetadata).all()))
        self.assertEqual(expected_metadata, found_metadata)
        assert_session.close()

    def test_exportViewForArgs_noLowerBound(self):
        # Arrange
        region = self.create_fake_region()
        export_manager = self.create_export_manager(region)
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name='ingest_view',
            upper_bound_datetime_prev=None,
            upper_bound_datetime_to_export=_DATE_2)

        session = SessionFactory.for_schema_base(OperationsBase)
        metadata = schema.DirectIngestIngestFileMetadata(
            file_id=_ID,
            region_code=region.region_code,
            file_tag=export_args.ingest_view_name,
            normalized_file_name='normalized_file_name',
            is_invalidated=False,
            is_file_split=False,
            job_creation_time=_DATE_1,
            export_time=None,
            datetimes_contained_lower_bound_exclusive=export_args.upper_bound_datetime_prev,
            datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_to_export
        )
        expected_metadata = attr.evolve(self.to_entity(metadata), export_time=_DATE_4)

        session.add(metadata)
        session.commit()
        session.close()

        # Act
        with freeze_time(_DATE_4.isoformat()):
            export_manager.export_view_for_args(export_args)

        # Assert
        self.mock_client.create_table_from_query_async.assert_has_calls([
            mock.call(
                dataset_id='us_xx_ingest_views',
                overwrite=True,
                query=mock.ANY,
                query_parameters=[self.generate_query_params_for_date(export_args.upper_bound_datetime_to_export)],
                table_id='ingest_view_2020_07_20_00_00_00_upper_bound'),
        ])
        expected_query = \
            'SELECT * FROM `recidiviz-456.us_xx_ingest_views.ingest_view_2020_07_20_00_00_00_upper_bound` ' \
            'ORDER BY colA, colC;'
        self.assert_exported_to_gcs_with_query(expected_query)
        self.mock_client.delete_table.assert_has_calls([
            mock.call(dataset_id='us_xx_ingest_views', table_id='ingest_view_2020_07_20_00_00_00_upper_bound')])
        assert_session = SessionFactory.for_schema_base(OperationsBase)
        found_metadata = self.to_entity(one(assert_session.query(schema.DirectIngestIngestFileMetadata).all()))
        self.assertEqual(expected_metadata, found_metadata)
        assert_session.close()

    def test_exportViewForArgs(self):
        # Arrange
        region = self.create_fake_region()
        export_manager = self.create_export_manager(region)
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name='ingest_view',
            upper_bound_datetime_prev=_DATE_1,
            upper_bound_datetime_to_export=_DATE_2)

        session = SessionFactory.for_schema_base(OperationsBase)
        metadata = schema.DirectIngestIngestFileMetadata(
            file_id=_ID,
            region_code=region.region_code,
            file_tag=export_args.ingest_view_name,
            normalized_file_name='normalized_file_name',
            is_invalidated=False,
            is_file_split=False,
            job_creation_time=_DATE_1,
            export_time=None,
            datetimes_contained_lower_bound_exclusive=export_args.upper_bound_datetime_prev,
            datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_to_export
        )
        expected_metadata = attr.evolve(self.to_entity(metadata), export_time=_DATE_4)
        session.add(metadata)
        session.commit()
        session.close()

        # Act
        with freeze_time(_DATE_4.isoformat()):
            export_manager.export_view_for_args(export_args)

        # Assert
        self.mock_client.create_table_from_query_async.assert_has_calls([
            mock.call(
                dataset_id='us_xx_ingest_views',
                overwrite=True,
                query=mock.ANY,
                query_parameters=[self.generate_query_params_for_date(export_args.upper_bound_datetime_to_export)],
                table_id='ingest_view_2020_07_20_00_00_00_upper_bound'),
            mock.call(
                dataset_id='us_xx_ingest_views',
                overwrite=True,
                query=mock.ANY,
                query_parameters=[self.generate_query_params_for_date(export_args.upper_bound_datetime_prev)],
                table_id='ingest_view_2019_07_20_00_00_00_lower_bound'),
        ])
        expected_query = \
            '(SELECT * FROM `recidiviz-456.us_xx_ingest_views.ingest_view_2020_07_20_00_00_00_upper_bound`) ' \
            'EXCEPT DISTINCT ' \
            '(SELECT * FROM `recidiviz-456.us_xx_ingest_views.ingest_view_2019_07_20_00_00_00_lower_bound`) ' \
            'ORDER BY colA, colC;'
        self.assert_exported_to_gcs_with_query(expected_query)
        self.mock_client.delete_table.assert_has_calls([
            mock.call(dataset_id='us_xx_ingest_views', table_id='ingest_view_2020_07_20_00_00_00_upper_bound'),
            mock.call(dataset_id='us_xx_ingest_views', table_id='ingest_view_2019_07_20_00_00_00_lower_bound'),
        ])

        assert_session = SessionFactory.for_schema_base(OperationsBase)
        found_metadata = self.to_entity(one(assert_session.query(schema.DirectIngestIngestFileMetadata).all()))
        self.assertEqual(expected_metadata, found_metadata)
        assert_session.close()

    def test_exportViewForArgs_detectRowDeletionView_noLowerBound(self):
        # Arrange
        region = self.create_fake_region()
        export_manager = self.create_export_manager(region, is_detect_row_deletion_view=True)
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name='ingest_view',
            upper_bound_datetime_prev=None,
            upper_bound_datetime_to_export=_DATE_2)

        session = SessionFactory.for_schema_base(OperationsBase)
        metadata = schema.DirectIngestIngestFileMetadata(
            file_id=_ID,
            region_code=region.region_code,
            file_tag=export_args.ingest_view_name,
            normalized_file_name='normalized_file_name',
            is_invalidated=False,
            is_file_split=False,
            job_creation_time=_DATE_1,
            export_time=None,
            datetimes_contained_lower_bound_exclusive=export_args.upper_bound_datetime_prev,
            datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_to_export
        )

        session.add(metadata)
        session.commit()
        session.close()

        # Act
        with freeze_time(_DATE_4.isoformat()):
            export_manager.export_view_for_args(export_args)

        # Assert
        self.mock_client.create_table_from_query_async.assert_not_called()
        self.mock_client.export_query_results_to_cloud_storage.assert_not_called()
        self.mock_client.delete_table.assert_not_called()

    def test_exportViewForArgs_detectRowDeletionView(self):
        # Arrange
        region = self.create_fake_region()
        export_manager = self.create_export_manager(region, is_detect_row_deletion_view=True)
        export_args = GcsfsIngestViewExportArgs(
            ingest_view_name='ingest_view',
            upper_bound_datetime_prev=_DATE_1,
            upper_bound_datetime_to_export=_DATE_2)

        session = SessionFactory.for_schema_base(OperationsBase)
        metadata = schema.DirectIngestIngestFileMetadata(
            file_id=_ID,
            region_code=region.region_code,
            file_tag=export_args.ingest_view_name,
            normalized_file_name='normalized_file_name',
            is_invalidated=False,
            is_file_split=False,
            job_creation_time=_DATE_1,
            export_time=None,
            datetimes_contained_lower_bound_exclusive=export_args.upper_bound_datetime_prev,
            datetimes_contained_upper_bound_inclusive=export_args.upper_bound_datetime_to_export
        )
        expected_metadata = attr.evolve(self.to_entity(metadata), export_time=_DATE_4)
        session.add(metadata)
        session.commit()
        session.close()

        # Act
        with freeze_time(_DATE_4.isoformat()):
            export_manager.export_view_for_args(export_args)

        # Assert
        self.mock_client.create_table_from_query_async.assert_has_calls([
            mock.call(
                dataset_id='us_xx_ingest_views',
                overwrite=True,
                query=mock.ANY,
                query_parameters=[self.generate_query_params_for_date(export_args.upper_bound_datetime_to_export)],
                table_id='ingest_view_2020_07_20_00_00_00_upper_bound'),
            mock.call(
                dataset_id='us_xx_ingest_views',
                overwrite=True,
                query=mock.ANY,
                query_parameters=[self.generate_query_params_for_date(export_args.upper_bound_datetime_prev)],
                table_id='ingest_view_2019_07_20_00_00_00_lower_bound'),
        ])
        # Lower bound is the first part of the subquery, not upper bound.
        expected_query = \
            '(SELECT * FROM `recidiviz-456.us_xx_ingest_views.ingest_view_2019_07_20_00_00_00_lower_bound`) ' \
            'EXCEPT DISTINCT ' \
            '(SELECT * FROM `recidiviz-456.us_xx_ingest_views.ingest_view_2020_07_20_00_00_00_upper_bound`) ' \
            'ORDER BY colA, colC;'
        self.assert_exported_to_gcs_with_query(expected_query)
        self.mock_client.delete_table.assert_has_calls([
            mock.call(dataset_id='us_xx_ingest_views', table_id='ingest_view_2020_07_20_00_00_00_upper_bound'),
            mock.call(dataset_id='us_xx_ingest_views', table_id='ingest_view_2019_07_20_00_00_00_lower_bound'),
        ])

        assert_session = SessionFactory.for_schema_base(OperationsBase)
        found_metadata = self.to_entity(one(assert_session.query(schema.DirectIngestIngestFileMetadata).all()))
        self.assertEqual(expected_metadata, found_metadata)
        assert_session.close()
