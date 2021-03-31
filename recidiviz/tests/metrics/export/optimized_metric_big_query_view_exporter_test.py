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

"""Tests for optimized_metric_big_query_view_exporter.py."""

import unittest
from typing import Dict, Set, List, Callable

from google.cloud import bigquery

import pytest
from mock import call, create_autospec, patch

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.export.export_query_config import ExportBigQueryViewConfig
from recidiviz.metrics.export.export_config import VIEW_COLLECTION_EXPORT_CONFIGS
from recidiviz.metrics.export.optimized_metric_big_query_view_export_validator import (
    OptimizedMetricBigQueryViewExportValidator,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath
from recidiviz.metrics.export import optimized_metric_big_query_view_exporter
from recidiviz.metrics.export.optimized_metric_big_query_view_exporter import (
    OptimizedMetricRepresentation,
    OptimizedMetricBigQueryViewExporter,
)

_DATA_POINTS = [
    {
        "district": "4",
        "year": 2020,
        "month": 11,
        "supervision_type": "PAROLE",
        "total_revocations": 100,
    },
    {
        "district": "4",
        "year": 2020,
        "month": 11,
        "supervision_type": "PROBATION",
        "total_revocations": 68,
    },
    {
        "district": "5",
        "year": 2020,
        "month": 11,
        "supervision_type": "PAROLE",
        "total_revocations": 73,
    },
    {
        "district": "5",
        "year": 2020,
        "month": 11,
        "supervision_type": "PROBATION",
        "total_revocations": 41,
    },
    {
        "district": "6",
        "year": 2020,
        "month": 11,
        "supervision_type": "PAROLE",
        "total_revocations": 10,
    },
    {
        "district": "4",
        "year": 2020,
        "month": 12,
        "supervision_type": "PAROLE",
        "total_revocations": 30,
    },
    {
        "district": "4",
        "year": 2020,
        "month": 12,
        "supervision_type": "PROBATION",
        "total_revocations": 36,
    },
    {
        "district": "5",
        "year": 2020,
        "month": 12,
        "supervision_type": "PAROLE",
        "total_revocations": 51,
    },
    {
        "district": "5",
        "year": 2020,
        "month": 12,
        "supervision_type": "PROBATION",
        "total_revocations": 38,
    },
    {
        "district": "6",
        "year": 2020,
        "month": 12,
        "supervision_type": "PAROLE",
        "total_revocations": 15,
    },
    {
        "district": "6",
        "year": 2020,
        "month": 12,
        "supervision_type": "PROBATION",
        "total_revocations": 4,
    },
]

_DIMENSION_MANIFEST = [
    ("district", ["4", "5", "6"]),
    ("month", ["11", "12"]),
    ("supervision_type", ["parole", "probation"]),
    ("year", ["2020"]),
]

_VALUE_KEYS = ["total_revocations"]

_DATA_VALUES = [
    [0, 0, 1, 1, 2, 0, 0, 1, 1, 2, 2],
    [0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1],
    [0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 1],
    [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    [100, 68, 73, 41, 10, 30, 36, 51, 38, 15, 4],
]


class PlaceInCompactMatrixTest(unittest.TestCase):
    """Tests for place_in_compact_matrix"""

    @staticmethod
    def _create_new_data_values():
        return [[], [], [], [], []]

    def test_place_happy_path_single_value(self):
        fresh_data_values = self._create_new_data_values()
        optimized_metric_big_query_view_exporter.place_in_compact_matrix(
            _DATA_POINTS[3],
            fresh_data_values,
            _VALUE_KEYS,
            _DIMENSION_MANIFEST,
        )
        self.assertEqual([[1], [0], [1], [0], [41]], fresh_data_values)

        optimized_metric_big_query_view_exporter.place_in_compact_matrix(
            _DATA_POINTS[8],
            fresh_data_values,
            _VALUE_KEYS,
            _DIMENSION_MANIFEST,
        )
        self.assertEqual([[1, 1], [0, 1], [1, 1], [0, 0], [41, 38]], fresh_data_values)

    def test_place_each_data_point_single_value(self):
        fresh_data_values = self._create_new_data_values()
        for data_point in _DATA_POINTS:
            optimized_metric_big_query_view_exporter.place_in_compact_matrix(
                data_point,
                fresh_data_values,
                _VALUE_KEYS,
                _DIMENSION_MANIFEST,
            )

        self.assertEqual(_DATA_VALUES, fresh_data_values)

    def test_place_each_data_point_multi_value(self):
        multi_data_points = [{**dp, "total_population": 100} for dp in _DATA_POINTS]
        multi_value_keys = ["total_population", "total_revocations"]

        expected = [
            [0, 0, 1, 1, 2, 0, 0, 1, 1, 2, 2],
            [0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1],
            [0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 1],
            [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            [100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100],
            [100, 68, 73, 41, 10, 30, 36, 51, 38, 15, 4],
        ]

        fresh_data_values = self._create_new_data_values()
        fresh_data_values.append([])  # Add one more for this test case
        for data_point in multi_data_points:
            optimized_metric_big_query_view_exporter.place_in_compact_matrix(
                data_point,
                fresh_data_values,
                multi_value_keys,
                _DIMENSION_MANIFEST,
            )

        self.assertEqual(expected, fresh_data_values)

    def test_place_value_not_in_dataset(self):
        fresh_data_values = self._create_new_data_values()
        new_data_point = {
            "district": "6",
            "year": 2020,
            "month": 11,
            "supervision_type": "PROBATION",
            "other_value": 5,
        }
        optimized_metric_big_query_view_exporter.place_in_compact_matrix(
            new_data_point,
            fresh_data_values,
            _VALUE_KEYS,
            _DIMENSION_MANIFEST,
        )
        self.assertEqual([[2], [0], [1], [0], [0]], fresh_data_values)

    def test_place_dimensions_not_in_dataset(self):
        fresh_data_values = self._create_new_data_values()
        with pytest.raises(KeyError) as e:
            new_data_point = {
                "district": "4",
                "year": 2020,
                "month": 5,
                "supervision_type": "PAROLE",
                "total_revocations": 10,
            }
            optimized_metric_big_query_view_exporter.place_in_compact_matrix(
                new_data_point,
                fresh_data_values,
                _VALUE_KEYS,
                _DIMENSION_MANIFEST,
            )

            self.assertIn("month: 5", e.message)


class AddToManifestTest(unittest.TestCase):
    """Tests for add_to_dimension_manifest"""

    def test_add_all_new_dimensions(self):
        data_point = _DATA_POINTS[2]
        fresh_manifest: Dict[str, Set[str]] = {
            "district": set(),
            "month": set(),
            "supervision_type": set(),
            "year": set(),
        }

        expected = {
            "district": {"5"},
            "month": {"11"},
            "supervision_type": {"parole"},
            "year": {"2020"},
        }

        updated_manifest = (
            optimized_metric_big_query_view_exporter.add_to_dimension_manifest(
                data_point, fresh_manifest
            )
        )
        self.assertEqual(expected, updated_manifest)

    def test_add_to_existing_dimensions(self):
        data_point = _DATA_POINTS[2]
        existing_manifest: Dict[str, Set[str]] = {
            "district": {"4"},
            "month": {"11"},
            "supervision_type": {"parole", "probation"},
            "year": {"2020"},
        }

        expected = {
            "district": {"4", "5"},
            "month": {"11"},
            "supervision_type": {"parole", "probation"},
            "year": {"2020"},
        }

        updated_manifest = (
            optimized_metric_big_query_view_exporter.add_to_dimension_manifest(
                data_point, existing_manifest
            )
        )
        self.assertEqual(expected, updated_manifest)

    def test_add_data_point_with_empty_values(self):
        data_point = {
            "district": None,
            "month": None,
            "supervision_type": None,
            "year": None,
        }

        existing_manifest: Dict[str, Set[str]] = {
            "district": {"4"},
            "month": {"11"},
            "supervision_type": {"parole", "probation"},
            "year": {"2020"},
        }

        expected: Dict[str, Set[str]] = {
            "district": {"4", "none"},
            "month": {"11", "none"},
            "supervision_type": {"parole", "probation", "none"},
            "year": {"2020", "none"},
        }

        updated_manifest = (
            optimized_metric_big_query_view_exporter.add_to_dimension_manifest(
                data_point, existing_manifest
            )
        )
        self.assertEqual(expected, updated_manifest)

    @staticmethod
    def test_add_empty_data_point():
        data_point = {}
        existing_manifest: Dict[str, Set[str]] = {
            "district": {"4"},
            "month": {"11"},
            "supervision_type": {"parole", "probation"},
            "year": {"2020"},
        }

        with pytest.raises(KeyError):
            optimized_metric_big_query_view_exporter.add_to_dimension_manifest(
                data_point, existing_manifest
            )


class TransformManifestTest(unittest.TestCase):
    """Tests for transform_dimension_manifest"""

    def test_transform_happy_path(self):
        dimensions_as_dictionary = {
            "district": {"6", "5", "4"},
            "year": {"2020"},
            "supervision_type": {"probation", "parole"},
            "month": {"11", "12"},
        }

        dimension_manifest = optimized_metric_big_query_view_exporter.transform_manifest_to_order_enforced_form(
            dimensions_as_dictionary
        )
        self.assertEqual(_DIMENSION_MANIFEST, dimension_manifest)

    def test_transform_with_other_types(self):
        dimensions_as_dictionary = {
            "district": {"6", "5", "4"},
            "year": {"2020"},
            "supervision_type": {"probation", "parole"},
            "month": {"11", "12"},
        }

        dimension_manifest = optimized_metric_big_query_view_exporter.transform_manifest_to_order_enforced_form(
            dimensions_as_dictionary
        )
        self.assertEqual(_DIMENSION_MANIFEST, dimension_manifest)

    def test_transform_with_nones(self):
        dimensions_as_dictionary = {
            "district": {"all", "6", "5", "4"},
            "year": {"2020"},
            "supervision_type": {"probation", "parole", "none"},
            "month": {"11", "12"},
        }

        expected = [
            ("district", ["4", "5", "6", "all"]),
            ("month", ["11", "12"]),
            ("supervision_type", ["none", "parole", "probation"]),
            ("year", ["2020"]),
        ]

        dimension_manifest = optimized_metric_big_query_view_exporter.transform_manifest_to_order_enforced_form(
            dimensions_as_dictionary
        )
        self.assertEqual(expected, dimension_manifest)

    def test_transform_empty(self):
        dimension_manifest = optimized_metric_big_query_view_exporter.transform_manifest_to_order_enforced_form(
            {}
        )
        self.assertEqual([], dimension_manifest)


class GetRowValuesTest(unittest.TestCase):
    """Tests for get_row_values"""

    def test_transform_happy_path_single_value(self):
        data_point = _DATA_POINTS[1]
        value_keys = ["total_revocations"]

        expected = [data_point["total_revocations"]]
        self.assertEqual(
            expected,
            optimized_metric_big_query_view_exporter.get_row_values(
                data_point, value_keys
            ),
        )

    def test_transform_happy_path_multi_value(self):
        data_point = {**_DATA_POINTS[1], "total_population": 752}
        value_keys = ["total_population", "total_revocations"]

        expected = [752, 68]
        self.assertEqual(
            expected,
            optimized_metric_big_query_view_exporter.get_row_values(
                data_point, value_keys
            ),
        )

    def test_transform_no_value_keys(self):
        data_point = _DATA_POINTS[1]
        value_keys = []

        self.assertEqual(
            [],
            optimized_metric_big_query_view_exporter.get_row_values(
                data_point, value_keys
            ),
        )


class ConvertQueryResultsTest(unittest.TestCase):
    """Tests for convert_query_results_to_optimized_value_matrix"""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "project-id"

    def tearDown(self):
        self.metadata_patcher.stop()

    def test_convert_happy_path(self):
        mock_bq_client = create_autospec(BigQueryClient)

        mock_dataset_ref = create_autospec(bigquery.DatasetReference)

        table_ref = bigquery.TableReference(mock_dataset_ref, "test_view")
        schema_fields = [
            bigquery.SchemaField("district", "STRING"),
            bigquery.SchemaField("year", "STRING"),
            bigquery.SchemaField("month", "STRING"),
            bigquery.SchemaField("supervision_type", "STRING"),
            bigquery.SchemaField("total_revocations", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema_fields)

        mock_bq_client.dataset_ref_for_id.return_value = mock_dataset_ref
        mock_bq_client.get_table.return_value = table

        all_rows = _transform_dicts_to_bq_row(_DATA_POINTS)

        mock_query_job = create_autospec(bigquery.QueryJob)
        mock_query_job.result.side_effect = [
            all_rows,
            all_rows,
        ]

        def fake_paged_process_fn(
            query_job: bigquery.QueryJob,
            _page_size: int,
            process_fn: Callable[[bigquery.table.Row], None],
        ) -> None:
            for row in query_job.result(
                max_results=optimized_metric_big_query_view_exporter.QUERY_PAGE_SIZE,
                start_index=0,
            ):
                process_fn(row)

        mock_bq_client.paged_read_and_process.side_effect = fake_paged_process_fn
        mock_validator = create_autospec(OptimizedMetricBigQueryViewExportValidator)

        view_exporter = OptimizedMetricBigQueryViewExporter(
            mock_bq_client, mock_validator
        )

        export_config = ExportBigQueryViewConfig(
            view=MetricBigQueryViewBuilder(
                dataset_id="test_dataset",
                view_id="test_view",
                description="test_view description",
                view_query_template="you know",
                dimensions=("district", "year", "month", "supervision_type"),
            ).build(),
            view_filter_clause="WHERE state_code = 'US_XX'",
            intermediate_table_name="tubular",
            output_directory=GcsfsDirectoryPath.from_absolute_path("gs://gnarly/blob"),
        )

        optimized_representation = (
            view_exporter.convert_query_results_to_optimized_value_matrix(
                mock_query_job, export_config
            )
        )
        expected = OptimizedMetricRepresentation(
            value_matrix=_DATA_VALUES,
            dimension_manifest=_DIMENSION_MANIFEST,
            value_keys=_VALUE_KEYS,
        )

        self.assertEqual(expected, optimized_representation)

        mock_query_job.result.assert_has_calls(
            [
                call(
                    max_results=optimized_metric_big_query_view_exporter.QUERY_PAGE_SIZE,
                    start_index=0,
                ),
                call(
                    max_results=optimized_metric_big_query_view_exporter.QUERY_PAGE_SIZE,
                    start_index=0,
                ),
            ]
        )

        mock_bq_client.paged_read_and_process.assert_called()
        mock_bq_client.dataset_ref_for_id.assert_called()
        mock_bq_client.get_table.assert_called()


class TestInitializeDimensionManifest(unittest.TestCase):
    """Tests the _initialize_dimension_manifest function."""

    # pylint: disable=protected-access
    def test_initialize_dimension_manifest(self):
        for export_config in VIEW_COLLECTION_EXPORT_CONFIGS:
            for view_builder in export_config.view_builders_to_export:
                if isinstance(view_builder, MetricBigQueryViewBuilder):
                    dimension_manifest = optimized_metric_big_query_view_exporter._initialize_dimension_manifest(
                        view_builder.dimensions
                    )
                    dimension_manifest_keys = list(dimension_manifest.keys())

                    self.assertEqual(
                        list(view_builder.dimensions), dimension_manifest_keys
                    )
                    # Makes sure that all of the dimensions are the full dimension column
                    # string, and that the dimensions haven't been split into chars
                    self.assertNotIn("_", dimension_manifest_keys)


def _transform_dicts_to_bq_row(data_points: List[Dict]) -> List[bigquery.table.Row]:
    rows: List[bigquery.table.Row] = []
    for data_point in data_points:
        values = []
        indices = {}
        for key, value in data_point.items():
            values.append(value)
            indices[key] = len(values) - 1
        rows.append(bigquery.table.Row(values, indices))

    return rows
