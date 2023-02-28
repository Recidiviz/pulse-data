# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Utility class for testing BQ views against Postgres"""
import re
import unittest
from typing import Dict, List, Sequence, Type, Union

import pandas as pd
import pytest

from recidiviz.big_query.big_query_view import BigQueryView, BigQueryViewBuilder
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.tests.big_query.big_query_test_helper import (
    BigQueryTestHelper,
    query_view,
)
from recidiviz.tests.big_query.fakes.fake_big_query_database import FakeBigQueryDatabase
from recidiviz.tests.big_query.fakes.fake_table_schema import PostgresTableSchema
from recidiviz.tools.postgres import local_postgres_helpers


# TODO(#15020): Delete this once all tests are migrated to the emulator.
@pytest.mark.uses_db
class BigQueryViewTestCase(unittest.TestCase, BigQueryTestHelper):
    """This is a utility class that allows BQ views to be tested using Postgres instead.

    This is NOT fully featured and has some shortcomings, most notably:
    1. It uses naive regexes to rewrite parts of the query. This works for the most part but may produce invalid
       queries in some cases. For instance, the lazy capture groups may capture the wrong tokens in nested function
       calls.
    2. Postgres can only use ORDINALS when unnesting and indexing into arrays, while BigQuery uses OFFSETS (or both).
       This does not translate the results (add or subtract one). So long as the query consistently uses one or the
       other, it should produce correct results.
    3. This does not (yet) support chaining of views. To test a view query, any tables or views that it queries from
       must be created and seeded with data using `create_table`.
    4. Not all BigQuery SQL syntax has been translated, and it is possible that some features may not have equivalent
       functionality in Postgres and therefore can't be translated.

    Given these, it may not make sense to use this for all of our views. If it prevents you from using BQ features that
    would be helpful, or creates more headaches than value it provides, it may not be necessary.
    """

    temp_db_dir: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        # View specific regex patterns to replace in the BigQuery SQL for the Postgres server. These are applied before
        # the rest of the SQL rewrites.
        self.sql_regex_replacements: Dict[str, str] = {}
        self.fake_bq_db = FakeBigQueryDatabase()

    def tearDown(self) -> None:
        self.fake_bq_db.teardown_databases()

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def create_mock_bq_table(
        self,
        dataset_id: str,
        table_id: str,
        mock_schema: PostgresTableSchema,
        mock_data: pd.DataFrame,
    ) -> None:
        self.fake_bq_db.create_mock_bq_table(
            dataset_id=dataset_id,
            table_id=table_id,
            mock_schema=mock_schema,
            mock_data=mock_data,
        )

    def create_view(self, view_builder: BigQueryViewBuilder) -> None:
        self.fake_bq_db.create_view(view_builder)

    def query(self, query: str) -> pd.DataFrame:
        if self.sql_regex_replacements:
            for bq_sql_regex_pattern, pg_sql in self.sql_regex_replacements.items():
                query = re.sub(bq_sql_regex_pattern, pg_sql, query)

        return self.fake_bq_db.run_query(query)

    def query_view_for_builder(
        self,
        view_builder: BigQueryViewBuilder,
        data_types: Dict[str, Union[Type, str]],
        dimensions: List[str],
    ) -> pd.DataFrame:
        if isinstance(view_builder, DirectIngestPreProcessedIngestViewBuilder):
            raise ValueError(
                f"Found view builder type [{type(view_builder)}] - use "
                f"query_ingest_view_for_builder() for this type instead."
            )

        view: BigQueryView = view_builder.build()
        results = query_view(self, view.table_for_query, view.view_query)

        # TODO(#5533): If we add `dimensions` to all `BigQueryViewBuilder`, instead of
        # just `MetricBigQueryViewBuilder`, then we can reuse that here instead of
        # forcing the caller to specify them manually.

        return self.apply_types_and_sort(
            results, data_types=data_types, sort_dimensions=dimensions
        )

    def query_view_chain(
        self,
        view_builders: Sequence[BigQueryViewBuilder],
        data_types: Dict[str, Union[Type, str]],
        dimensions: List[str],
    ) -> pd.DataFrame:
        for view_builder in view_builders[:-1]:
            self.create_view(view_builder)
        return self.query_view_for_builder(view_builders[-1], data_types, dimensions)
