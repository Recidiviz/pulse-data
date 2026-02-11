# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for view description query providers."""
import unittest

from mock import patch

from recidiviz.big_query.big_query_create_or_replace_view_query_provider import (
    CreateOrReplaceViewQueryProvider,
)
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.big_query_view_column import Integer, String

PROJECT_ID = "recidiviz-test"


class CreateOrReplaceViewQueryProviderTest(unittest.TestCase):
    """Tests for CreateOrReplaceViewQueryProvider."""

    def setUp(self) -> None:
        self.metadata_patcher = patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = PROJECT_ID

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_generates_create_view_with_column_descriptions(self) -> None:
        view = BigQueryView(
            dataset_id="my_dataset",
            view_id="my_view",
            description="test view",
            bq_description="test view",
            view_query_template="SELECT 1 AS col1, 2 AS col2",
            schema=[
                String(name="col1", description="First column", mode="NULLABLE"),
                Integer(name="col2", description="Second column", mode="NULLABLE"),
            ],
        )

        provider = CreateOrReplaceViewQueryProvider(view=view, project_id=PROJECT_ID)
        query = provider.get_query()

        self.assertEqual(
            query,
            """CREATE OR REPLACE VIEW `recidiviz-test.my_dataset.my_view`
(
  col1 OPTIONS(description='''First column'''),
  col2 OPTIONS(description='''Second column''')
)
OPTIONS(description='''test view
Explore this view\\'s lineage at https://go/lineage-staging/my_dataset.my_view''')
AS
SELECT 1 AS col1, 2 AS col2""",
        )

    def test_generates_create_view_without_schema(self) -> None:
        view = BigQueryView(
            dataset_id="my_dataset",
            view_id="my_view",
            description="test view",
            bq_description="test view",
            view_query_template="SELECT 1 AS col1",
        )

        provider = CreateOrReplaceViewQueryProvider(view=view, project_id=PROJECT_ID)
        query = provider.get_query()

        self.assertEqual(
            query,
            """CREATE OR REPLACE VIEW `recidiviz-test.my_dataset.my_view`
OPTIONS(description='''test view
Explore this view\\'s lineage at https://go/lineage-staging/my_dataset.my_view''')
AS
SELECT 1 AS col1""",
        )

    def test_single_quotes_preserved_in_descriptions(self) -> None:
        view = BigQueryView(
            dataset_id="my_dataset",
            view_id="my_view",
            description="It's a view",
            bq_description="It's a view",
            view_query_template="SELECT 1 AS col1",
            schema=[
                String(
                    name="col1",
                    description="It's a column with 'quotes'",
                    mode="NULLABLE",
                ),
            ],
        )

        provider = CreateOrReplaceViewQueryProvider(view=view, project_id=PROJECT_ID)
        query = provider.get_query()

        # Trailing single quote is escaped to avoid ambiguity with closing '''
        self.assertEqual(
            query,
            """CREATE OR REPLACE VIEW `recidiviz-test.my_dataset.my_view`
(
  col1 OPTIONS(description='''It\\'s a column with \\'quotes\\'''')
)
OPTIONS(description='''It\\'s a view
Explore this view\\'s lineage at https://go/lineage-staging/my_dataset.my_view''')
AS
SELECT 1 AS col1""",
        )

    def test_emulator_sql_skips_column_options(self) -> None:
        view = BigQueryView(
            dataset_id="my_dataset",
            view_id="my_view",
            description="test view",
            bq_description="test view",
            view_query_template="SELECT 1 AS col1, 2 AS col2",
            schema=[
                String(name="col1", description="First column", mode="NULLABLE"),
                Integer(name="col2", description="Second column", mode="NULLABLE"),
            ],
        )

        provider = CreateOrReplaceViewQueryProvider(
            view=view, project_id=PROJECT_ID, use_emulator_sql=True
        )
        query = provider.get_query()

        self.assertEqual(
            query,
            """CREATE OR REPLACE VIEW `recidiviz-test.my_dataset.my_view`
OPTIONS(description='''test view
Explore this view\\'s lineage at https://go/lineage-staging/my_dataset.my_view''')
AS
SELECT 1 AS col1, 2 AS col2""",
        )

    def test_emulator_sql_without_schema(self) -> None:
        view = BigQueryView(
            dataset_id="my_dataset",
            view_id="my_view",
            description="test view",
            bq_description="test view",
            view_query_template="SELECT 1 AS col1",
        )

        provider = CreateOrReplaceViewQueryProvider(
            view=view, project_id=PROJECT_ID, use_emulator_sql=True
        )
        query = provider.get_query()

        self.assertEqual(
            query,
            """CREATE OR REPLACE VIEW `recidiviz-test.my_dataset.my_view`
OPTIONS(description='''test view
Explore this view\\'s lineage at https://go/lineage-staging/my_dataset.my_view''')
AS
SELECT 1 AS col1""",
        )
