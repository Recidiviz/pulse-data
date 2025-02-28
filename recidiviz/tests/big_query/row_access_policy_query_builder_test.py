# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Test cases for RowAccessPolicyQueryBuilder."""
import unittest

from google.cloud import bigquery

from recidiviz.big_query.row_access_policy_query_builder import (
    RowAccessPolicyQueryBuilder,
)


class TestRowAccessPolicyQueryBuilder(unittest.TestCase):
    """Test cases for RowAccessPolicyQueryBuilder."""

    def test_build_column_based_row_level_policy(self) -> None:
        expected_queries = [
            """CREATE OR REPLACE ROW ACCESS POLICY
                EXPLICIT_ACCESS_TO_US_OZ_STATE_CODE
                ON `test_project.test_dataset.test_table`
                GRANT TO ("group:s-oz-data@recidiviz.org")
                FILTER USING (UPPER(state_code) = "US_OZ");""",
            """CREATE OR REPLACE ROW ACCESS POLICY
                NON_RESTRICTIVE_STATE_DATA_ACCESS_STATE_CODE
                ON `test_project.test_dataset.test_table`
                GRANT TO ("group:s-default-state-data@recidiviz.org")
                FILTER USING (UPPER(state_code) NOT IN ("US_OZ"));""",
            """CREATE OR REPLACE ROW ACCESS POLICY
                ADMIN_ACCESS_TO_ALL_STATE_DATA_STATE_CODE
                ON `test_project.test_dataset.test_table`
                GRANT TO ("group:s-big-query-admins@recidiviz.org")
                FILTER USING (TRUE);""",
        ]

        table_ref = bigquery.TableReference(
            dataset_ref=bigquery.DatasetReference(
                project="test_project", dataset_id="test_dataset"
            ),
            table_id="test_table",
        )
        schema = [
            bigquery.SchemaField("state_code", "STRING"),
            bigquery.SchemaField("value", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)

        self.assertListEqual(
            expected_queries,
            RowAccessPolicyQueryBuilder.build_queries_to_create_row_access_policy(
                table
            ),
        )

    def test_build_dataset_based_row_level_policy(self) -> None:
        expected_queries = [
            """CREATE OR REPLACE ROW ACCESS POLICY
                RESTRICT_DATASET_TO_MEMBERS_OF_STATE_SECURITY_GROUP
                ON `test_project.us_oz_dataset.test_table`
                GRANT TO ("group:s-oz-data@recidiviz.org")
                FILTER USING (TRUE);""",
            """CREATE OR REPLACE ROW ACCESS POLICY
                ADMIN_ACCESS_TO_ALL_ROWS
                ON `test_project.us_oz_dataset.test_table`
                GRANT TO ("group:s-big-query-admins@recidiviz.org")
                FILTER USING (TRUE);""",
        ]

        table_ref = bigquery.TableReference(
            dataset_ref=bigquery.DatasetReference(
                project="test_project", dataset_id="us_oz_dataset"
            ),
            table_id="test_table",
        )
        schema = [
            # Even with state_code field, the policy should be dataset-based
            bigquery.SchemaField("state_code", "STRING"),
            bigquery.SchemaField("value", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)

        self.assertListEqual(
            expected_queries,
            RowAccessPolicyQueryBuilder.build_queries_to_create_row_access_policy(
                table
            ),
        )

    def test_non_restricted_state(self) -> None:
        table_ref = bigquery.TableReference(
            dataset_ref=bigquery.DatasetReference(
                project="test_project", dataset_id="us_or_dataset"
            ),
            table_id="test_table",
        )
        schema = [
            # Even though this table has a state_code field, it exists in a non-restricted state-specific dataset
            bigquery.SchemaField("state_code", "STRING"),
            bigquery.SchemaField("value", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)

        self.assertListEqual(
            [],
            RowAccessPolicyQueryBuilder.build_queries_to_create_row_access_policy(
                table
            ),
        )

    def test_no_policies(self) -> None:
        table_ref = bigquery.TableReference(
            dataset_ref=bigquery.DatasetReference(
                project="test_project", dataset_id="test_dataset"
            ),
            table_id="test_table",
        )
        schema = [
            bigquery.SchemaField("value", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)

        self.assertListEqual(
            [],
            RowAccessPolicyQueryBuilder.build_queries_to_create_row_access_policy(
                table
            ),
        )

    def test_drop_row_access_policies(self) -> None:
        expected_query = """DROP ALL ROW ACCESS POLICIES
            ON `test_project.test_dataset.test_table`;"""

        table_ref = bigquery.TableReference(
            dataset_ref=bigquery.DatasetReference(
                project="test_project", dataset_id="test_dataset"
            ),
            table_id="test_table",
        )
        table = bigquery.Table(table_ref)

        self.assertEqual(
            expected_query,
            RowAccessPolicyQueryBuilder.build_query_to_drop_row_access_policy(table),
        )
