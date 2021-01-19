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
"""Tests for BigQueryView"""
import unittest

import pytest
from mock import patch

from recidiviz.big_query.big_query_view import BigQueryView


class BigQueryViewTest(unittest.TestCase):
    """Tests for BigQueryView"""

    PROJECT_ID = 'recidiviz-project-id'

    def setUp(self) -> None:
        self.metadata_patcher = patch('recidiviz.utils.metadata.project_id')
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = self.PROJECT_ID

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_simple_view_no_extra_args(self) -> None:
        view = BigQueryView(
            dataset_id='view_dataset',
            view_id='my_view',
            view_query_template='SELECT * FROM `{project_id}.some_dataset.table`'
        )

        self.assertEqual(self.PROJECT_ID, view.project)
        self.assertEqual('view_dataset', view.dataset_id)
        self.assertEqual('my_view', view.table_id)
        self.assertEqual('my_view', view.view_id)
        self.assertEqual(f"SELECT * FROM `{self.PROJECT_ID}.some_dataset.table`", view.view_query)
        self.assertEqual(f"SELECT * FROM `{self.PROJECT_ID}.view_dataset.my_view`", view.select_query)

    def test_simple_view_overwrite_project_id(self) -> None:
        view = BigQueryView(
            project_id='other-project',
            dataset_id='view_dataset',
            view_id='my_view',
            view_query_template='SELECT * FROM `{project_id}.some_dataset.table`'
        )

        self.assertEqual('other-project', view.project)
        self.assertEqual('view_dataset', view.dataset_id)
        self.assertEqual('my_view', view.table_id)
        self.assertEqual('my_view', view.view_id)
        self.assertEqual('SELECT * FROM `other-project.some_dataset.table`', view.view_query)
        self.assertEqual('SELECT * FROM `other-project.view_dataset.my_view`', view.select_query)

    @patch('recidiviz.big_query.big_query_view.GCP_PROJECTS', [PROJECT_ID])
    def test_simple_view_invalid_raw_project_id(self) -> None:
        with pytest.raises(ValueError):
            _ = BigQueryView(
                project_id='other-project',
                dataset_id='view_dataset',
                view_id='my_view',
                view_query_template=f"SELECT * FROM `{self.PROJECT_ID}.some_dataset.table`"
            )

    def test_extra_format_args(self) -> None:
        view = BigQueryView(
            dataset_id='view_dataset',
            view_id='my_view',
            view_query_template='SELECT {select_col_1}, {select_col_2} FROM `{project_id}.{some_dataset}.table`',
            some_dataset='a_dataset',
            select_col_1='name',
            select_col_2='date'
        )

        self.assertEqual(self.PROJECT_ID, view.project)
        self.assertEqual('view_dataset', view.dataset_id)
        self.assertEqual('my_view', view.table_id)
        self.assertEqual('my_view', view.view_id)
        self.assertEqual(f"SELECT name, date FROM `{self.PROJECT_ID}.a_dataset.table`", view.view_query)
        self.assertEqual(f"SELECT * FROM `{self.PROJECT_ID}.view_dataset.my_view`", view.select_query)

    def test_missing_format_arg_throws_on_instantiation(self) -> None:
        with self.assertRaises(KeyError):
            _ = BigQueryView(
                dataset_id='view_dataset',
                view_id='my_view',
                view_query_template='SELECT {select_col_1}, {select_col_2} FROM `{project_id}.{some_dataset}.table`',
                some_dataset='a_dataset',
                select_col_2='date'
            )
