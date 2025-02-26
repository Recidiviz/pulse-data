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

"""Tests for composite_big_query_view_exporter.py."""

import unittest

import pytest

from mock import call, create_autospec, patch

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.export.big_query_view_exporter import BigQueryViewExporter
from recidiviz.big_query.export.composite_big_query_view_exporter import CompositeBigQueryViewExporter
from recidiviz.big_query.export.export_query_config import ExportBigQueryViewConfig
from recidiviz.cloud_storage.gcs_file_system import GCSFileSystem
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath, GcsfsDirectoryPath


class CompositeBigQueryViewExporterTest(unittest.TestCase):
    """Tests for the CompositeBigQueryViewExporter."""

    def setUp(self) -> None:
        self.metadata_patcher = patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = 'project-id'

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_export_happy_path(self):
        metric_view_one = MetricBigQueryViewBuilder(
            dataset_id='dataset',
            view_id='view1',
            view_query_template='select * from table',
            dimensions=['a', 'b', 'c'],
        ).build()

        export_config_one = ExportBigQueryViewConfig(
            view=metric_view_one,
            view_filter_clause='WHERE state_code = \'US_XX\'',
            intermediate_table_name='intermediate_table',
            output_directory=GcsfsDirectoryPath.from_absolute_path('gs://bucket1/US_XX'),
        )
        export_config_one_staging = ExportBigQueryViewConfig(
            view=metric_view_one,
            view_filter_clause='WHERE state_code = \'US_XX\'',
            intermediate_table_name='intermediate_table',
            output_directory=GcsfsDirectoryPath.from_absolute_path('gs://bucket1/staging/US_XX'),
        )

        metric_view_two = MetricBigQueryViewBuilder(
            dataset_id='dataset',
            view_id='view2',
            view_query_template='select * from view2',
            dimensions=['d', 'e', 'f'],
        ).build()

        export_config_two = ExportBigQueryViewConfig(
            view=metric_view_two,
            view_filter_clause='WHERE state_code = \'US_XX\'',
            intermediate_table_name='intermediate_table2',
            output_directory=GcsfsDirectoryPath.from_absolute_path('gs://bucket2/US_XX'),
        )
        export_config_two_staging = ExportBigQueryViewConfig(
            view=metric_view_two,
            view_filter_clause='WHERE state_code = \'US_XX\'',
            intermediate_table_name='intermediate_table2',
            output_directory=GcsfsDirectoryPath.from_absolute_path('gs://bucket2/staging/US_XX'),
        )

        mock_bq_client = create_autospec(BigQueryClient)
        mock_fs = create_autospec(GCSFileSystem)

        mock_fs.exists.return_value = True

        delegate_one = create_autospec(BigQueryViewExporter)
        delegate_two = create_autospec(BigQueryViewExporter)

        delegate_one.export.return_value = [export_config_one_staging.output_path('json'),
                                            export_config_two_staging.output_path('json')]
        delegate_two.export.return_value = [export_config_one_staging.output_path('txt'),
                                            export_config_two_staging.output_path('txt')]

        # Make the actual call
        exporter = CompositeBigQueryViewExporter(mock_bq_client, mock_fs, [delegate_one, delegate_two])
        exporter.export([export_config_one, export_config_two])

        # Assert all mocks called as expected
        delegate_one.export.assert_has_calls([
            call([export_config_one_staging, export_config_two_staging]),
        ])

        delegate_two.export.assert_has_calls([
            call([export_config_one_staging, export_config_two_staging]),
        ])

        mock_fs.exists.assert_has_calls([
            call(GcsfsFilePath(bucket_name='bucket1', blob_name='staging/US_XX/view1.json')),
            call(GcsfsFilePath(bucket_name='bucket2', blob_name='staging/US_XX/view2.json')),
            call(GcsfsFilePath(bucket_name='bucket1', blob_name='staging/US_XX/view1.txt')),
            call(GcsfsFilePath(bucket_name='bucket2', blob_name='staging/US_XX/view2.txt'))
        ])
        mock_fs.copy.assert_has_calls([
            call(GcsfsFilePath(bucket_name='bucket1', blob_name='staging/US_XX/view1.json'),
                 GcsfsFilePath(bucket_name='bucket1', blob_name='US_XX/view1.json')),
            call(GcsfsFilePath(bucket_name='bucket2', blob_name='staging/US_XX/view2.json'),
                 GcsfsFilePath(bucket_name='bucket2', blob_name='US_XX/view2.json')),
            call(GcsfsFilePath(bucket_name='bucket1', blob_name='staging/US_XX/view1.txt'),
                 GcsfsFilePath(bucket_name='bucket1', blob_name='US_XX/view1.txt')),
            call(GcsfsFilePath(bucket_name='bucket2', blob_name='staging/US_XX/view2.txt'),
                 GcsfsFilePath(bucket_name='bucket2', blob_name='US_XX/view2.txt'))
        ])

        mock_fs.delete.assert_has_calls([
            call(GcsfsFilePath(bucket_name='bucket1', blob_name='staging/US_XX/view1.json')),
            call(GcsfsFilePath(bucket_name='bucket2', blob_name='staging/US_XX/view2.json')),
            call(GcsfsFilePath(bucket_name='bucket1', blob_name='staging/US_XX/view1.txt')),
            call(GcsfsFilePath(bucket_name='bucket2', blob_name='staging/US_XX/view2.txt'))
        ])

    def test_export_staging_blob_not_found(self):
        metric_view_one = MetricBigQueryViewBuilder(
            dataset_id='dataset',
            view_id='view1',
            view_query_template='select * from table',
            dimensions=['a', 'b', 'c'],
        ).build()

        export_config_one = ExportBigQueryViewConfig(
            view=metric_view_one,
            view_filter_clause='WHERE state_code = \'US_XX\'',
            intermediate_table_name='intermediate_table',
            output_directory=GcsfsDirectoryPath.from_absolute_path('gs://bucket1/US_XX'),
        )
        export_config_one_staging = ExportBigQueryViewConfig(
            view=metric_view_one,
            view_filter_clause='WHERE state_code = \'US_XX\'',
            intermediate_table_name='intermediate_table',
            output_directory=GcsfsDirectoryPath.from_absolute_path('gs://bucket1/staging/US_XX'),
        )

        metric_view_two = MetricBigQueryViewBuilder(
            dataset_id='dataset',
            view_id='view2',
            view_query_template='select * from view2',
            dimensions=['d', 'e', 'f'],
        ).build()

        export_config_two = ExportBigQueryViewConfig(
            view=metric_view_two,
            view_filter_clause='WHERE state_code = \'US_XX\'',
            intermediate_table_name='intermediate_table2',
            output_directory=GcsfsDirectoryPath.from_absolute_path('gs://bucket2/US_XX'),
        )
        export_config_two_staging = ExportBigQueryViewConfig(
            view=metric_view_two,
            view_filter_clause='WHERE state_code = \'US_XX\'',
            intermediate_table_name='intermediate_table2',
            output_directory=GcsfsDirectoryPath.from_absolute_path('gs://bucket2/staging/US_XX'),
        )

        mock_bq_client = create_autospec(BigQueryClient)
        mock_fs = create_autospec(GCSFileSystem)

        # The False value should trigger a ValueError - file not found in staging location
        mock_fs.exists.side_effect = [True, False, True, True]

        delegate_one = create_autospec(BigQueryViewExporter)
        delegate_two = create_autospec(BigQueryViewExporter)

        delegate_one.export.return_value = [export_config_one_staging.output_path('json'),
                                            export_config_two_staging.output_path('json')]
        delegate_two.export.return_value = [export_config_one_staging.output_path('txt'),
                                            export_config_two_staging.output_path('txt')]

        # Make the actual call
        exporter = CompositeBigQueryViewExporter(mock_bq_client, mock_fs, [delegate_one, delegate_two])

        with pytest.raises(ValueError) as e:
            exporter.export([export_config_one, export_config_two])

        self.assertEqual(
            'File at path [gs://bucket2/staging/US_XX/view2.json] does not exist - export did not complete properly',
            str(e.value))
