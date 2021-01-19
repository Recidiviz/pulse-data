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
"""Tests for BigQueryViewCollector."""

import os
import unittest
from typing import List

from mock import patch

import recidiviz
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import \
    DirectIngestPreProcessedIngestViewBuilder
from recidiviz.tests.big_query import test_views
from recidiviz.tests.big_query.fake_big_query_view_builder import FakeBigQueryViewBuilder
from recidiviz.tests.big_query.test_views.good_view_1 import GOOD_VIEW_1
from recidiviz.tests.big_query.test_views.good_view_2 import GOOD_VIEW_2

VIEWS_DIR_RELATIVE_PATH = os.path.relpath(os.path.dirname(test_views.__file__),
                                          os.path.dirname(recidiviz.__file__))


class BigQueryViewCollectorTest(unittest.TestCase):
    """Tests for BigQueryViewCollector."""

    def setUp(self) -> None:
        self.metadata_patcher = patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = 'project-id'

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_collect_view_builders(self) -> None:
        builders = BigQueryViewCollector.collect_view_builders_in_dir(
            FakeBigQueryViewBuilder, VIEWS_DIR_RELATIVE_PATH, view_file_prefix_filter='good_')
        views: List[BigQueryView] = [builder.build() for builder in builders]
        self.assertCountEqual([GOOD_VIEW_1, GOOD_VIEW_2], views)

    def test_collect_views_too_narrow_view_type(self) -> None:
        with self.assertRaises(ValueError):
            # One of the views is only a BigQueryView, not a DirectIngestPreProcessedIngestView
            _ = BigQueryViewCollector.collect_view_builders_in_dir(
                DirectIngestPreProcessedIngestViewBuilder, VIEWS_DIR_RELATIVE_PATH, view_file_prefix_filter='good_')

    def test_collect_views_narrow_view_type_ok(self) -> None:
        builders = BigQueryViewCollector.collect_view_builders_in_dir(
            FakeBigQueryViewBuilder, VIEWS_DIR_RELATIVE_PATH, view_file_prefix_filter='good_view_2')

        self.assertCountEqual([GOOD_VIEW_2], [b.build() for b in builders])

    def test_file_no_builder_raises(self) -> None:
        with self.assertRaises(ValueError):
            _ = BigQueryViewCollector.collect_view_builders_in_dir(
                FakeBigQueryViewBuilder, VIEWS_DIR_RELATIVE_PATH, view_file_prefix_filter='bad_view_no_builder')

    def test_file_builder_wrong_name_raises(self) -> None:
        with self.assertRaises(ValueError):
            _ = BigQueryViewCollector.collect_view_builders_in_dir(
                FakeBigQueryViewBuilder, VIEWS_DIR_RELATIVE_PATH, view_file_prefix_filter='bad_view_builder_wrong_name')

    def test_file_builder_wrong_type_raises(self) -> None:
        with self.assertRaises(ValueError):
            _ = BigQueryViewCollector.collect_view_builders_in_dir(
                FakeBigQueryViewBuilder, VIEWS_DIR_RELATIVE_PATH, view_file_prefix_filter='bad_view_builder_wrong_type')
