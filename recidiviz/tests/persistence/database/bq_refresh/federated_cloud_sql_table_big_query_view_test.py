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
"""Tests for federated_cloud_sql_table_big_query_view.py."""

import unittest
from unittest import mock

from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.persistence.database.base_schema import OperationsBase
from recidiviz.persistence.database.bq_refresh.federated_cloud_sql_table_big_query_view import (
    FederatedCloudSQLTableBigQueryView,
    FederatedCloudSQLTableBigQueryViewBuilder,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


class FederatedCloudSQLTableBigQueryViewTest(unittest.TestCase):
    """Tests for federated_cloud_sql_table_big_query_view.py."""

    def setUp(self) -> None:
        self.metadata_patcher = mock.patch("recidiviz.utils.metadata.project_id")
        self.mock_project_id_fn = self.metadata_patcher.start()
        self.mock_project_id_fn.return_value = "test-project"

    def tearDown(self) -> None:
        self.metadata_patcher.stop()

    def test_build_view_operations_db(self) -> None:
        table = one(
            t
            for t in OperationsBase.metadata.sorted_tables
            if t.name == "direct_ingest_dataflow_job"
        )
        view_builder = FederatedCloudSQLTableBigQueryViewBuilder(
            connection_region="us-east2",
            table=table,
            view_id=table.name,
            cloud_sql_query="SELECT * FROM direct_ingest_dataflow_job;",
            database_key=SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS),
            materialized_address_override=BigQueryAddress(
                dataset_id="materialized_dataset", table_id="materialized_table"
            ),
        )
        expected_description = """View providing a connection to the [direct_ingest_dataflow_job]
table in the [postgres] database in the [OPERATIONS] schema. This view is 
managed outside of regular view update operations and the results can be found in the 
schema-specific datasets (`state`, `justice_counts`, etc)."""

        expected_view_query = """
SELECT
    *
FROM EXTERNAL_QUERY(
    "test-project.us-east2.operations_v2_cloudsql",
    "SELECT * FROM direct_ingest_dataflow_job;"
)"""

        # Build without dataset overrides
        view = view_builder.build()

        self.assertIsInstance(view, FederatedCloudSQLTableBigQueryView)
        self.assertEqual(expected_view_query, view.view_query)
        self.assertEqual(expected_description, view.bq_description)
        self.assertEqual(expected_description, view.description)
        self.assertEqual(
            BigQueryAddress(
                dataset_id="operations_v2_cloudsql_connection",
                table_id="direct_ingest_dataflow_job",
            ),
            view.address,
        )
        self.assertEqual(
            BigQueryAddress(
                dataset_id="materialized_dataset", table_id="materialized_table"
            ),
            view.materialized_address,
        )
