# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for classes in big_query_view_dag_walker.py"""

import re
import unittest
from typing import Dict, List, Set
from unittest.mock import patch

from recidiviz.big_query.big_query_table_checker import BigQueryTableChecker
from recidiviz.big_query.big_query_view import BigQueryAddress, BigQueryView
from recidiviz.big_query.big_query_view_dag_walker import BigQueryViewDagWalker
from recidiviz.big_query.view_update_manager_utils import (
    get_managed_views_for_dataset_map,
)
from recidiviz.view_registry.deployed_views import DEPLOYED_VIEW_BUILDERS

LATEST_VIEW_DATASET_REGEX = re.compile(r"(us_[a-z]{2})_raw_data_up_to_date_views")
MOCK_VIEW_PROCESS_TIME_SECONDS = 0.01


class TestViewUpdateManagerUtils(unittest.TestCase):
    """Tests for ViewUpdateManagerUtils"""

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "recidiviz-456"

        with patch.object(
            BigQueryTableChecker, "_table_has_column"
        ) as mock_table_has_column, patch.object(
            BigQueryTableChecker, "_table_exists"
        ) as mock_table_exists:
            mock_table_has_column.return_value = True
            mock_table_exists.return_value = True

            self.all_views = [
                view_builder.build() for view_builder in DEPLOYED_VIEW_BUILDERS
            ]
        self.empty_view_list: List[BigQueryView] = []
        self.one_view_list = [
            BigQueryView(
                dataset_id="dataset_0",
                view_id="table_0",
                description="table_0 description",
                view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
            )
        ]
        # Views forming a DAG shaped like an X:
        #  1     2
        #   \   /
        #     3
        #   /   \
        #  4     5
        self.x_shaped_dag_views_list = [
            BigQueryView(
                dataset_id="dataset_1",
                view_id="table_1",
                description="table_1 description",
                view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
            ),
            BigQueryView(
                dataset_id="dataset_2",
                view_id="table_2",
                description="table_2 description",
                view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
            ),
            BigQueryView(
                dataset_id="dataset_3",
                view_id="table_3",
                description="table_3 description",
                view_query_template="""
            SELECT * FROM `{project_id}.dataset_1.table_1`
            JOIN `{project_id}.dataset_2.table_2`
            USING (col)""",
            ),
            BigQueryView(
                dataset_id="dataset_4",
                view_id="table_4",
                description="table_4 description",
                view_query_template="""
            SELECT * FROM `{project_id}.dataset_3.table_3`""",
            ),
            BigQueryView(
                dataset_id="dataset_5",
                view_id="table_5",
                description="table_5 description",
                view_query_template="""
            SELECT * FROM `{project_id}.dataset_3.table_3`""",
            ),
        ]

        # Views forming a DAG with a diamond in it:
        #  1     2
        #   \   /
        #     3
        #   /   \
        #  4     5
        #   \   /
        #     6
        self.diamond_shaped_dag_views_list = self.x_shaped_dag_views_list.copy()
        self.diamond_shaped_dag_views_list.append(
            BigQueryView(
                dataset_id="dataset_6",
                view_id="table_6",
                description="table_6 description",
                view_query_template="""
            SELECT * FROM `{project_id}.dataset_4.table_4`
            JOIN `{project_id}.dataset_5.table_5`
            USING (col)""",
            )
        )
        self.multiple_views_one_dataset_list = [
            BigQueryView(
                dataset_id="dataset_1",
                view_id="table_1",
                description="table_1 description",
                view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
            ),
            BigQueryView(
                dataset_id="dataset_1",
                view_id="table_2",
                description="table_2 description",
                view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
            ),
            BigQueryView(
                dataset_id="dataset_3",
                view_id="table_3",
                description="table_3 description",
                view_query_template="""
                    SELECT * FROM `{project_id}.dataset_1.table_1`
                    JOIN `{project_id}.dataset_1.table_2`
                    USING (col)""",
            ),
            BigQueryView(
                dataset_id="dataset_4",
                view_id="table_4",
                description="table_4 description",
                view_query_template="""
                    SELECT * FROM `{project_id}.dataset_3.table_3`""",
            ),
        ]

        self.all_views_same_dataset = [
            BigQueryView(
                dataset_id="dataset_1",
                view_id="table_1",
                description="table_1 description",
                view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table`",
            ),
            BigQueryView(
                dataset_id="dataset_1",
                view_id="table_2",
                description="table_2 description",
                view_query_template="SELECT * FROM `{project_id}.source_dataset.source_table_2`",
            ),
            BigQueryView(
                dataset_id="dataset_1",
                view_id="table_3",
                description="table_3 description",
                view_query_template="""
                    SELECT * FROM `{project_id}.dataset_1.table_1`
                    JOIN `{project_id}.dataset_1.table_2`
                    USING (col)""",
            ),
        ]

    def tearDown(self) -> None:
        self.project_id_patcher.stop()

    def test_get_managed_views_for_dataset_map_empty_list(self) -> None:
        # Arrange
        walker = BigQueryViewDagWalker(self.empty_view_list)
        # Act
        result_dict = get_managed_views_for_dataset_map(walker)
        # Assert
        expected_result: Dict[str, Set[BigQueryAddress]] = {}
        self.assertEqual(expected_result, result_dict)

    def test_get_managed_views_for_dataset_map_one_view_list(self) -> None:
        # Arrange
        walker = BigQueryViewDagWalker(self.one_view_list)
        # Act
        result_dict = get_managed_views_for_dataset_map(walker)
        # Assert
        expected_result: Dict[str, Set[BigQueryAddress]] = {
            "dataset_0": {BigQueryAddress(dataset_id="dataset_0", table_id="table_0")}
        }
        self.assertEqual(expected_result, result_dict)

    def test_get_managed_views_for_dataset_map_x_shaped_dag(self) -> None:
        # Arrange
        walker = BigQueryViewDagWalker(self.x_shaped_dag_views_list)
        # Act
        result_dict = get_managed_views_for_dataset_map(walker)
        # Assert
        expected_result: Dict[str, Set[BigQueryAddress]] = {
            "dataset_1": {BigQueryAddress(dataset_id="dataset_1", table_id="table_1")},
            "dataset_2": {BigQueryAddress(dataset_id="dataset_2", table_id="table_2")},
            "dataset_3": {BigQueryAddress(dataset_id="dataset_3", table_id="table_3")},
            "dataset_4": {BigQueryAddress(dataset_id="dataset_4", table_id="table_4")},
            "dataset_5": {BigQueryAddress(dataset_id="dataset_5", table_id="table_5")},
        }
        self.assertEqual(expected_result, result_dict)

    def test_get_managed_views_for_dataset_map_diamond_shaped_dag(self) -> None:
        # Arrange
        walker = BigQueryViewDagWalker(self.diamond_shaped_dag_views_list)
        # Act
        result_dict = get_managed_views_for_dataset_map(walker)
        # Assert
        expected_result: Dict[str, Set[BigQueryAddress]] = {
            "dataset_1": {BigQueryAddress(dataset_id="dataset_1", table_id="table_1")},
            "dataset_2": {BigQueryAddress(dataset_id="dataset_2", table_id="table_2")},
            "dataset_3": {BigQueryAddress(dataset_id="dataset_3", table_id="table_3")},
            "dataset_4": {BigQueryAddress(dataset_id="dataset_4", table_id="table_4")},
            "dataset_5": {BigQueryAddress(dataset_id="dataset_5", table_id="table_5")},
            "dataset_6": {BigQueryAddress(dataset_id="dataset_6", table_id="table_6")},
        }
        self.assertEqual(expected_result, result_dict)

    def test_get_managed_views_for_dataset_map_all_views_same_dataset(self) -> None:
        # Arrange
        walker = BigQueryViewDagWalker(self.all_views_same_dataset)
        # Act
        result_dict = get_managed_views_for_dataset_map(walker)
        # Assert
        expected_result: Dict[str, Set[BigQueryAddress]] = {
            "dataset_1": {
                BigQueryAddress(dataset_id="dataset_1", table_id="table_1"),
                BigQueryAddress(dataset_id="dataset_1", table_id="table_2"),
                BigQueryAddress(dataset_id="dataset_1", table_id="table_3"),
            },
        }
        self.assertEqual(expected_result, result_dict)

    def test_get_managed_views_for_dataset_map_multiple_views_one_dataset(self) -> None:
        # Arrange
        walker = BigQueryViewDagWalker(self.multiple_views_one_dataset_list)
        # Act
        result_dict = get_managed_views_for_dataset_map(walker)
        # Assert
        expected_result: Dict[str, Set[BigQueryAddress]] = {
            "dataset_1": {
                BigQueryAddress(dataset_id="dataset_1", table_id="table_1"),
                BigQueryAddress(dataset_id="dataset_1", table_id="table_2"),
            },
            "dataset_3": {BigQueryAddress(dataset_id="dataset_3", table_id="table_3")},
            "dataset_4": {BigQueryAddress(dataset_id="dataset_4", table_id="table_4")},
        }
        self.assertEqual(expected_result, result_dict)
